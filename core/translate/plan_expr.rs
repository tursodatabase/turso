//! Resolved expressions owned by a VDBE plan.
//!
//! [`PlanExpr`] is deliberately separate from parser syntax and Semantic HIR.
//! Semantic analysis decides what every name means. Plan lowering maps those
//! resolved identities into the plan identity space, and later planner passes
//! may move or rewrite these expressions without retaining document-local HIR
//! identities.

use std::{cell::RefCell, fmt, num::NonZeroU32};

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use turso_parser::ast::{
    Distinctness, FrameExclude, FrameMode, LikeOperator, Literal, NullsOrder, Operator,
    ResolveType, SortOrder, UnaryOperator,
};

use super::semantic::hir::{
    self, CteId, DatabaseId, OutputId, QueryId, ResolvedCollation, ResolvedFunction, ResolvedTable,
    ResolvedType, SchemaProgramId, SourceId, TypeFact,
};
use crate::{
    function::Func,
    schema::{Sequence, Type},
    sync::Arc,
    translate::collate::CollationSeq,
    util::{check_literal_equivalency, parse_numeric_literal},
    vdbe::affinity::Affinity,
    LimboError, Result, Value,
};

macro_rules! plan_id {
    ($name:ident, $display_prefix:literal) => {
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[repr(transparent)]
        pub struct $name(usize);

        impl $name {
            pub const fn new(index: usize) -> Self {
                Self(index)
            }

            pub const fn index(self) -> usize {
                self.0
            }
        }

        impl From<$name> for usize {
            fn from(value: $name) -> Self {
                value.index()
            }
        }

        impl From<usize> for $name {
            fn from(value: usize) -> Self {
                Self::new(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!($display_prefix, "{}"), self.0)
            }
        }
    };
}

plan_id!(PlanSourceId, "s");
plan_id!(PlanOutputId, "o");
plan_id!(PlanSubqueryId, "q");
plan_id!(PlanCteId, "c");

/// Allocates identities for one plan tree.
///
/// The allocator owns the complete plan identity space. Planner-created
/// sources and outputs therefore cannot collide with identities assigned while
/// lowering HIR queries or CTEs.
#[derive(Debug, Default)]
pub struct PlanIdentityAllocator {
    next_source: usize,
    next_output: usize,
    next_subquery: usize,
    next_cte: usize,
}

impl PlanIdentityAllocator {
    pub const fn new() -> Self {
        Self {
            next_source: 0,
            next_output: 0,
            next_subquery: 0,
            next_cte: 0,
        }
    }

    pub fn next_source(&mut self) -> PlanSourceId {
        let id = PlanSourceId::new(self.next_source);
        self.next_source = self
            .next_source
            .checked_add(1)
            .expect("plan source identity overflow");
        id
    }

    pub fn next_output(&mut self) -> PlanOutputId {
        let id = PlanOutputId::new(self.next_output);
        self.next_output = self
            .next_output
            .checked_add(1)
            .expect("plan output identity overflow");
        id
    }

    pub fn next_subquery(&mut self) -> PlanSubqueryId {
        let id = PlanSubqueryId::new(self.next_subquery);
        self.next_subquery = self
            .next_subquery
            .checked_add(1)
            .expect("plan subquery identity overflow");
        id
    }

    pub fn next_cte(&mut self) -> PlanCteId {
        let id = PlanCteId::new(self.next_cte);
        self.next_cte = self
            .next_cte
            .checked_add(1)
            .expect("plan CTE identity overflow");
        id
    }
}

/// Explicit mapping from Semantic HIR identities to one VDBE plan tree.
///
/// The mapping is populated by statement lowering. Downstream lowering only
/// reads it; a missing entry is an internal error, never a request to search a
/// catalog or resolve a name.
#[derive(Debug, Default)]
pub struct PlanIdentityMap {
    sources: HashMap<SourceId, PlanSourceId>,
    outputs: HashMap<OutputId, PlanOutputId>,
    semantic_outputs: HashMap<PlanOutputId, OutputId>,
    subqueries: HashMap<QueryId, PlanSubqueryId>,
    semantic_subqueries: HashMap<PlanSubqueryId, QueryId>,
    ctes: HashMap<CteId, PlanCteId>,
    source_columns: HashMap<(SourceId, usize), hir::SourceColumn>,
    schema_programs: Vec<hir::BoundSchemaProgram>,
    lowered_schema_programs: RefCell<Vec<Option<Arc<PlanBoundSchemaProgram>>>>,
    schema_programs_being_lowered: RefCell<HashSet<SchemaProgramId>>,
}

impl PlanIdentityMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(sources: usize, outputs: usize, subqueries: usize, ctes: usize) -> Self {
        Self {
            sources: HashMap::with_capacity_and_hasher(sources, Default::default()),
            outputs: HashMap::with_capacity_and_hasher(outputs, Default::default()),
            semantic_outputs: HashMap::with_capacity_and_hasher(outputs, Default::default()),
            subqueries: HashMap::with_capacity_and_hasher(subqueries, Default::default()),
            semantic_subqueries: HashMap::with_capacity_and_hasher(subqueries, Default::default()),
            ctes: HashMap::with_capacity_and_hasher(ctes, Default::default()),
            source_columns: HashMap::default(),
            schema_programs: Vec::new(),
            lowered_schema_programs: RefCell::new(Vec::new()),
            schema_programs_being_lowered: RefCell::new(HashSet::default()),
        }
    }

    /// Allocate one complete plan identity space before lowering expressions.
    ///
    /// Pre-allocation matters because a HIR expression may refer forward to a
    /// nested query or to an output owned by a later query block. Lowering is
    /// therefore a pure lookup and never depends on traversal order.
    pub fn allocate_document(
        document: &hir::HirDocument,
        allocator: &mut PlanIdentityAllocator,
    ) -> Self {
        let output_capacity = document
            .queries
            .iter()
            .flat_map(|query| &query.blocks)
            .map(|block| block.outputs.len())
            .sum::<usize>()
            + document
                .sources
                .iter()
                .flat_map(|source| &source.index_method_patterns)
                .map(|pattern| pattern.outputs.len())
                .sum::<usize>()
            + root_outputs(&document.root).len();
        let mut identities = Self::with_capacity(
            document.sources.len(),
            output_capacity,
            document.queries.len(),
            document.ctes.len(),
        );
        identities.schema_programs = document.schema_programs.clone();
        identities.lowered_schema_programs =
            RefCell::new(vec![None; document.schema_programs.len()]);

        for cte in &document.ctes {
            identities.bind_cte(cte.id, allocator.next_cte());
        }
        for query in &document.queries {
            identities.bind_subquery(query.id, allocator.next_subquery());
            for block in &query.blocks {
                for output in &block.outputs {
                    identities.bind_output(output.id, allocator.next_output());
                }
            }
        }
        for source in &document.sources {
            identities.bind_source(source.id, allocator.next_source());
            for (column_index, column) in source.columns.iter().enumerate() {
                identities
                    .source_columns
                    .insert((source.id, column_index), column.clone());
            }
            for pattern in &source.index_method_patterns {
                for output in &pattern.outputs {
                    identities.bind_output(output.id, allocator.next_output());
                }
            }
        }
        for output in root_outputs(&document.root) {
            identities.bind_output(output.id, allocator.next_output());
        }
        identities
    }

    pub fn bind_source(&mut self, semantic: SourceId, plan: PlanSourceId) -> Option<PlanSourceId> {
        self.sources.insert(semantic, plan)
    }

    /// Bind one semantic source and snapshot the column facts needed while
    /// lowering expressions that refer to it.
    ///
    /// Standalone schema expressions do not own a complete [`hir::HirDocument`],
    /// so they cannot use [`Self::allocate_document`]. They still need the same
    /// source-column contract as statement lowering: expression lowering is a
    /// pure identity lookup and never consults a live schema.
    pub fn bind_source_definition(
        &mut self,
        source: &hir::Source,
        plan: PlanSourceId,
    ) -> Option<PlanSourceId> {
        for (column_index, column) in source.columns.iter().enumerate() {
            self.source_columns
                .insert((source.id, column_index), column.clone());
        }
        self.bind_source(source.id, plan)
    }

    pub fn bind_output(&mut self, semantic: OutputId, plan: PlanOutputId) -> Option<PlanOutputId> {
        let previous = self.outputs.insert(semantic, plan);
        if let Some(previous) = previous {
            self.semantic_outputs.remove(&previous);
        }
        self.semantic_outputs.insert(plan, semantic);
        previous
    }

    pub fn bind_subquery(
        &mut self,
        semantic: QueryId,
        plan: PlanSubqueryId,
    ) -> Option<PlanSubqueryId> {
        let previous = self.subqueries.insert(semantic, plan);
        if let Some(previous) = previous {
            self.semantic_subqueries.remove(&previous);
        }
        self.semantic_subqueries.insert(plan, semantic);
        previous
    }

    pub fn bind_cte(&mut self, semantic: CteId, plan: PlanCteId) -> Option<PlanCteId> {
        self.ctes.insert(semantic, plan)
    }

    pub fn source(&self, semantic: SourceId) -> Option<PlanSourceId> {
        self.sources.get(&semantic).copied()
    }

    pub fn output(&self, semantic: OutputId) -> Option<PlanOutputId> {
        self.outputs.get(&semantic).copied()
    }

    pub fn semantic_output(&self, plan: PlanOutputId) -> Option<OutputId> {
        self.semantic_outputs.get(&plan).copied()
    }

    pub fn subquery(&self, semantic: QueryId) -> Option<PlanSubqueryId> {
        self.subqueries.get(&semantic).copied()
    }

    pub fn semantic_subquery(&self, plan: PlanSubqueryId) -> Option<QueryId> {
        self.semantic_subqueries.get(&plan).copied()
    }

    pub fn cte(&self, semantic: CteId) -> Option<PlanCteId> {
        self.ctes.get(&semantic).copied()
    }

    pub fn source_column(&self, source: SourceId, column: usize) -> Option<&hir::SourceColumn> {
        self.source_columns.get(&(source, column))
    }
}

#[derive(Clone, Debug)]
pub struct PlanParameter {
    pub index: NonZeroU32,
    pub name: Option<String>,
    pub type_fact: TypeFact,
}

#[derive(Clone, Debug)]
pub struct PlanColumnRef {
    pub source: PlanSourceId,
    pub column: usize,
    /// Whether this logical column is backed by the table rowid rather than a
    /// stored record field (for example `INTEGER PRIMARY KEY`).
    pub rowid_alias: bool,
    pub type_fact: TypeFact,
    pub affinity: Affinity,
    pub has_affinity: bool,
    pub collation: Option<ResolvedCollation>,
}

#[derive(Clone, Debug)]
pub struct PlanMergedColumn {
    pub left: Box<PlanExpr>,
    pub right: PlanColumnRef,
    pub value: PlanMergedColumnValue,
    pub type_fact: TypeFact,
    pub affinity: Affinity,
    pub has_affinity: bool,
    pub collation: Option<ResolvedCollation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanMergedColumnValue {
    Left,
    Right,
    Coalesce,
}

#[derive(Clone, Debug)]
pub struct PlanTypeName {
    pub name: String,
    pub parameters: Vec<PlanExpr>,
    pub array_dimensions: u32,
    pub type_fact: TypeFact,
    pub programs: PlanCastPrograms,
}

/// One schema expression lowered into the plan's identity space.
#[derive(Clone, Debug)]
pub struct PlanBoundSchemaProgram {
    pub input_source: PlanSourceId,
    pub body: PlanExpr,
}

/// One invocation of a lowered schema program.
///
/// The runtime value is supplied separately from the explicit arguments.
#[derive(Clone, Debug)]
pub struct PlanBoundSchemaCall {
    pub program: Arc<PlanBoundSchemaProgram>,
    pub arguments: Vec<PlanExpr>,
}

/// Static metadata needed to encode an array into its stored representation.
#[derive(Clone, Debug)]
pub struct PlanArrayStorage {
    pub element_affinity: Affinity,
    pub element_type: String,
    pub table_name: String,
    pub column_name: String,
    pub dimensions: u32,
}

/// Lowered transformations for one source column's declared type.
#[derive(Clone, Debug)]
pub struct PlanColumnTypePrograms {
    pub encode: Vec<PlanBoundSchemaCall>,
    pub decode: Vec<PlanBoundSchemaCall>,
    pub array: Option<PlanArrayStorage>,
    pub encode_nulls: bool,
}

/// One lowered domain check and its runtime failure message.
#[derive(Clone, Debug)]
pub struct PlanDomainCheck {
    pub call: PlanBoundSchemaCall,
    pub failure_description: String,
}

/// Constraints inherited from a resolved domain cast target.
#[derive(Clone, Debug)]
pub struct PlanDomainConstraints {
    pub not_null_description: Option<String>,
    pub checks: Vec<PlanDomainCheck>,
}

/// Custom-type work resolved for one cast target.
#[derive(Clone, Debug)]
pub struct PlanCastPrograms {
    pub encode: Vec<PlanBoundSchemaCall>,
    pub domain: Option<PlanDomainConstraints>,
    pub apply_builtin_affinity: bool,
}

impl PlanTypeName {
    /// Return the affinity applied by this CAST.
    ///
    /// A registered custom type can share a name with a SQLite type. When its
    /// required arguments are absent, analysis chooses SQLite's built-in CAST,
    /// so comparison affinity must come from the written type name rather than
    /// the custom type's storage format.
    pub(crate) fn cast_affinity(&self) -> Affinity {
        if self.programs.apply_builtin_affinity {
            if self.name.is_empty() {
                Affinity::Numeric
            } else {
                Affinity::affinity(&self.name)
            }
        } else {
            type_fact_affinity(&self.type_fact)
        }
    }
}

#[derive(Clone, Debug)]
pub struct PlanOrderTerm {
    pub expr: PlanExpr,
    pub order: SortOrder,
    pub nulls: Option<NullsOrder>,
}

#[derive(Clone, Debug)]
pub struct PlanWindowSpec {
    pub partition_by: Vec<PlanExpr>,
    pub order_by: Vec<PlanOrderTerm>,
    pub frame: Option<PlanWindowFrame>,
}

#[derive(Clone, Debug)]
pub struct PlanWindowFrame {
    pub mode: FrameMode,
    pub start: PlanFrameBound,
    pub end: Option<PlanFrameBound>,
    pub exclude: Option<FrameExclude>,
}

#[derive(Clone, Debug)]
pub enum PlanFrameBound {
    CurrentRow,
    Following(Box<PlanExpr>),
    Preceding(Box<PlanExpr>),
    UnboundedFollowing,
    UnboundedPreceding,
}

/// Pre-resolved behavior for the custom-type scalar functions that do not use
/// the generic function opcode.
#[derive(Clone, Debug)]
pub enum PlanCustomTypeOperation {
    UnionValue {
        union_type: ResolvedType,
        tag_index: u8,
        result_type: TypeFact,
    },
    UnionTag {
        union_type: ResolvedType,
        tag_names: Arc<[String]>,
    },
    UnionExtract {
        union_type: ResolvedType,
        tag_index: u8,
        result_type: TypeFact,
    },
    StructExtract {
        struct_type: ResolvedType,
        field_index: usize,
        result_type: TypeFact,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanBinaryOperand {
    Left,
    Right,
}

#[derive(Clone, Debug)]
pub struct PlanCustomBinaryLiteralEncoding {
    /// Original SQL operand, before `swap_args` is applied.
    pub operand: PlanBinaryOperand,
    pub encoder: Option<PlanBoundSchemaCall>,
}

#[derive(Clone, Debug)]
pub struct PlanCustomBinaryOperator {
    pub function: ResolvedFunction,
    pub swap_args: bool,
    pub negate: bool,
    pub literal_encoding: Option<PlanCustomBinaryLiteralEncoding>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanSequenceOperationKind {
    NextValue,
    SetValue,
}

#[derive(Clone, Debug)]
pub struct PlanSequenceOperation {
    pub kind: PlanSequenceOperationKind,
    pub database: DatabaseId,
    pub user_name: String,
    pub normalized_name: String,
    pub backing_table: ResolvedTable,
    pub sequence: Arc<Sequence>,
    pub schema_cookie: u32,
}

#[derive(Clone, Debug)]
pub struct PlanFunctionCall {
    pub function: ResolvedFunction,
    pub arguments: Vec<PlanExpr>,
    pub star: bool,
    pub distinctness: Option<Distinctness>,
    pub argument_order: Vec<PlanOrderTerm>,
    pub within_group: Vec<PlanOrderTerm>,
    pub filter: Option<Box<PlanExpr>>,
    pub window: Option<PlanWindowSpec>,
    pub custom_type_operation: Option<PlanCustomTypeOperation>,
    pub sequence_operation: Option<PlanSequenceOperation>,
    pub result_type: TypeFact,
}

#[derive(Clone, Debug)]
pub struct PlanFieldAccess {
    pub base: Box<PlanExpr>,
    pub field_name: String,
    pub kind: PlanFieldAccessKind,
    pub container_type: ResolvedType,
    pub result_type: TypeFact,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanFieldAccessKind {
    Struct { field_index: usize },
    Union { tag_index: u8 },
}

#[derive(Clone, Debug)]
pub enum PlanSubqueryExpr {
    Scalar {
        query: PlanSubqueryId,
        output: usize,
    },
    Exists(PlanSubqueryId),
    In {
        lhs: Box<PlanExpr>,
        query: PlanSubqueryId,
        negated: bool,
    },
}

/// Resolved expression owned by a VDBE plan.
///
/// Runtime registers, cursors, labels, raw names, stars, defaults, and parser
/// subqueries cannot be represented here.
#[derive(Clone, Debug)]
pub enum PlanExpr {
    Literal(Literal),
    Parameter(PlanParameter),
    Column(PlanColumnRef),
    MergedColumn(PlanMergedColumn),
    RowId(PlanSourceId),
    Output(PlanOutputId),
    Unary {
        operator: UnaryOperator,
        expr: Box<PlanExpr>,
    },
    Binary {
        lhs: Box<PlanExpr>,
        operator: Operator,
        rhs: Box<PlanExpr>,
        custom: Option<PlanCustomBinaryOperator>,
    },
    Between {
        expr: Box<PlanExpr>,
        negated: bool,
        start: Box<PlanExpr>,
        end: Box<PlanExpr>,
    },
    Case {
        base: Option<Box<PlanExpr>>,
        when_then: Vec<(PlanExpr, PlanExpr)>,
        else_expr: Option<Box<PlanExpr>>,
    },
    Cast {
        expr: Box<PlanExpr>,
        target: PlanTypeName,
    },
    Collate {
        expr: Box<PlanExpr>,
        collation: ResolvedCollation,
    },
    Function(PlanFunctionCall),
    IsNull(Box<PlanExpr>),
    NotNull(Box<PlanExpr>),
    InList {
        lhs: Box<PlanExpr>,
        negated: bool,
        values: Vec<PlanExpr>,
    },
    Subquery(PlanSubqueryExpr),
    Like {
        lhs: Box<PlanExpr>,
        negated: bool,
        operator: LikeOperator,
        function: ResolvedFunction,
        argument_count: usize,
        rhs: Box<PlanExpr>,
        escape: Option<Box<PlanExpr>>,
    },
    Row(Vec<PlanExpr>),
    Array(Vec<PlanExpr>),
    Subscript {
        base: Box<PlanExpr>,
        index: Box<PlanExpr>,
    },
    FieldAccess(PlanFieldAccess),
    Raise {
        action: ResolveType,
        message: Option<Box<PlanExpr>>,
    },
}

impl PlanExpr {
    pub fn literal(literal: Literal) -> Self {
        Self::Literal(literal)
    }

    pub fn parameter(index: NonZeroU32, name: Option<String>, type_fact: TypeFact) -> Self {
        Self::Parameter(PlanParameter {
            index,
            name,
            type_fact,
        })
    }

    pub fn column(source: PlanSourceId, column: usize) -> Self {
        Self::column_with_metadata(source, column, TypeFact::dynamic(), Affinity::Blob, None)
    }

    pub fn column_with_metadata(
        source: PlanSourceId,
        column: usize,
        type_fact: TypeFact,
        affinity: Affinity,
        collation: Option<ResolvedCollation>,
    ) -> Self {
        Self::Column(PlanColumnRef {
            source,
            column,
            rowid_alias: false,
            type_fact,
            affinity,
            has_affinity: true,
            collation,
        })
    }

    pub fn rowid(source: PlanSourceId) -> Self {
        Self::RowId(source)
    }

    pub fn output(output: PlanOutputId) -> Self {
        Self::Output(output)
    }

    pub fn unary(operator: UnaryOperator, expr: Self) -> Self {
        Self::Unary {
            operator,
            expr: Box::new(expr),
        }
    }

    pub fn binary(lhs: Self, operator: Operator, rhs: Self) -> Self {
        Self::Binary {
            lhs: Box::new(lhs),
            operator,
            rhs: Box::new(rhs),
            custom: None,
        }
    }

    pub fn between(expr: Self, negated: bool, start: Self, end: Self) -> Self {
        Self::Between {
            expr: Box::new(expr),
            negated,
            start: Box::new(start),
            end: Box::new(end),
        }
    }

    pub fn is_null(expr: Self) -> Self {
        Self::IsNull(Box::new(expr))
    }

    pub fn not_null(expr: Self) -> Self {
        Self::NotNull(Box::new(expr))
    }

    pub fn scalar_subquery(subquery: PlanSubqueryId) -> Self {
        Self::scalar_subquery_output(subquery, 0)
    }

    pub fn scalar_subquery_output(subquery: PlanSubqueryId, output: usize) -> Self {
        Self::Subquery(PlanSubqueryExpr::Scalar {
            query: subquery,
            output,
        })
    }

    pub fn exists_subquery(subquery: PlanSubqueryId) -> Self {
        Self::Subquery(PlanSubqueryExpr::Exists(subquery))
    }

    pub fn in_subquery(lhs: Self, query: PlanSubqueryId, negated: bool) -> Self {
        Self::Subquery(PlanSubqueryExpr::In {
            lhs: Box::new(lhs),
            query,
            negated,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanWalkControl {
    Continue,
    SkipChildren,
}

/// Visit every expression node in stable, left-to-right order.
///
/// Query bodies referenced by [`PlanSubqueryExpr`] are deliberately not
/// traversed. They are separate plan nodes and are reached through their
/// [`PlanSubqueryId`].
pub fn walk_plan_expr<'a, F>(expr: &'a PlanExpr, visit: &mut F) -> Result<PlanWalkControl>
where
    F: FnMut(&'a PlanExpr) -> Result<PlanWalkControl>,
{
    if matches!(visit(expr)?, PlanWalkControl::SkipChildren) {
        return Ok(PlanWalkControl::Continue);
    }

    match expr {
        PlanExpr::MergedColumn(column) => {
            walk_plan_expr(&column.left, visit)?;
        }
        PlanExpr::Unary { expr, .. }
        | PlanExpr::IsNull(expr)
        | PlanExpr::NotNull(expr)
        | PlanExpr::Collate { expr, .. } => {
            walk_plan_expr(expr, visit)?;
        }
        PlanExpr::Binary {
            lhs, rhs, custom, ..
        } => {
            walk_plan_expr(lhs, visit)?;
            walk_plan_expr(rhs, visit)?;
            if let Some(encoder) = custom
                .as_ref()
                .and_then(|custom| custom.literal_encoding.as_ref())
                .and_then(|encoding| encoding.encoder.as_ref())
            {
                walk_plan_schema_call(encoder, visit)?;
            }
        }
        PlanExpr::Between {
            expr, start, end, ..
        } => {
            walk_plan_expr(expr, visit)?;
            walk_plan_expr(start, visit)?;
            walk_plan_expr(end, visit)?;
        }
        PlanExpr::Case {
            base,
            when_then,
            else_expr,
        } => {
            if let Some(base) = base {
                walk_plan_expr(base, visit)?;
            }
            for (when, then) in when_then {
                walk_plan_expr(when, visit)?;
                walk_plan_expr(then, visit)?;
            }
            if let Some(else_expr) = else_expr {
                walk_plan_expr(else_expr, visit)?;
            }
        }
        PlanExpr::Cast { expr, target } => {
            walk_plan_expr(expr, visit)?;
            walk_plan_exprs(&target.parameters, visit)?;
            walk_plan_cast_programs(&target.programs, visit)?;
        }
        PlanExpr::Function(function) => walk_plan_function(function, visit)?,
        PlanExpr::InList { lhs, values, .. } => {
            walk_plan_expr(lhs, visit)?;
            walk_plan_exprs(values, visit)?;
        }
        PlanExpr::Subquery(PlanSubqueryExpr::In { lhs, .. }) => {
            walk_plan_expr(lhs, visit)?;
        }
        PlanExpr::Like {
            lhs, rhs, escape, ..
        } => {
            walk_plan_expr(lhs, visit)?;
            walk_plan_expr(rhs, visit)?;
            if let Some(escape) = escape {
                walk_plan_expr(escape, visit)?;
            }
        }
        PlanExpr::Row(values) | PlanExpr::Array(values) => {
            walk_plan_exprs(values, visit)?;
        }
        PlanExpr::Subscript { base, index } => {
            walk_plan_expr(base, visit)?;
            walk_plan_expr(index, visit)?;
        }
        PlanExpr::FieldAccess(access) => {
            walk_plan_expr(&access.base, visit)?;
        }
        PlanExpr::Raise {
            message: Some(message),
            ..
        } => {
            walk_plan_expr(message, visit)?;
        }
        PlanExpr::Literal(_)
        | PlanExpr::Parameter(_)
        | PlanExpr::Column(_)
        | PlanExpr::RowId(_)
        | PlanExpr::Output(_)
        | PlanExpr::Subquery(PlanSubqueryExpr::Scalar { .. })
        | PlanExpr::Subquery(PlanSubqueryExpr::Exists(_))
        | PlanExpr::Raise { message: None, .. } => {}
    }

    Ok(PlanWalkControl::Continue)
}

fn walk_plan_exprs<'a, F>(exprs: &'a [PlanExpr], visit: &mut F) -> Result<()>
where
    F: FnMut(&'a PlanExpr) -> Result<PlanWalkControl>,
{
    for expr in exprs {
        walk_plan_expr(expr, visit)?;
    }
    Ok(())
}

fn walk_plan_schema_call<'a, F>(call: &'a PlanBoundSchemaCall, visit: &mut F) -> Result<()>
where
    F: FnMut(&'a PlanExpr) -> Result<PlanWalkControl>,
{
    walk_plan_exprs(&call.arguments, visit)
}

fn walk_plan_cast_programs<'a, F>(programs: &'a PlanCastPrograms, visit: &mut F) -> Result<()>
where
    F: FnMut(&'a PlanExpr) -> Result<PlanWalkControl>,
{
    for call in &programs.encode {
        walk_plan_schema_call(call, visit)?;
    }
    if let Some(domain) = &programs.domain {
        for check in &domain.checks {
            walk_plan_schema_call(&check.call, visit)?;
        }
    }
    Ok(())
}

fn walk_plan_function<'a, F>(function: &'a PlanFunctionCall, visit: &mut F) -> Result<()>
where
    F: FnMut(&'a PlanExpr) -> Result<PlanWalkControl>,
{
    walk_plan_exprs(&function.arguments, visit)?;
    walk_plan_order_terms(&function.argument_order, visit)?;
    walk_plan_order_terms(&function.within_group, visit)?;
    if let Some(filter) = &function.filter {
        walk_plan_expr(filter, visit)?;
    }
    if let Some(window) = &function.window {
        walk_plan_exprs(&window.partition_by, visit)?;
        walk_plan_order_terms(&window.order_by, visit)?;
        if let Some(frame) = &window.frame {
            walk_plan_frame_bound(&frame.start, visit)?;
            if let Some(end) = &frame.end {
                walk_plan_frame_bound(end, visit)?;
            }
        }
    }
    Ok(())
}

fn walk_plan_order_terms<'a, F>(terms: &'a [PlanOrderTerm], visit: &mut F) -> Result<()>
where
    F: FnMut(&'a PlanExpr) -> Result<PlanWalkControl>,
{
    for term in terms {
        walk_plan_expr(&term.expr, visit)?;
    }
    Ok(())
}

fn walk_plan_frame_bound<'a, F>(bound: &'a PlanFrameBound, visit: &mut F) -> Result<()>
where
    F: FnMut(&'a PlanExpr) -> Result<PlanWalkControl>,
{
    match bound {
        PlanFrameBound::Following(expr) | PlanFrameBound::Preceding(expr) => {
            walk_plan_expr(expr, visit)?;
        }
        PlanFrameBound::CurrentRow
        | PlanFrameBound::UnboundedFollowing
        | PlanFrameBound::UnboundedPreceding => {}
    }
    Ok(())
}

/// Mutable counterpart of [`walk_plan_expr`].
pub fn walk_plan_expr_mut<F>(expr: &mut PlanExpr, visit: &mut F) -> Result<PlanWalkControl>
where
    F: FnMut(&mut PlanExpr) -> Result<PlanWalkControl>,
{
    if matches!(visit(expr)?, PlanWalkControl::SkipChildren) {
        return Ok(PlanWalkControl::Continue);
    }

    match expr {
        PlanExpr::MergedColumn(column) => {
            walk_plan_expr_mut(&mut column.left, visit)?;
        }
        PlanExpr::Unary { expr, .. }
        | PlanExpr::IsNull(expr)
        | PlanExpr::NotNull(expr)
        | PlanExpr::Collate { expr, .. } => {
            walk_plan_expr_mut(expr, visit)?;
        }
        PlanExpr::Binary {
            lhs, rhs, custom, ..
        } => {
            walk_plan_expr_mut(lhs, visit)?;
            walk_plan_expr_mut(rhs, visit)?;
            if let Some(encoder) = custom
                .as_mut()
                .and_then(|custom| custom.literal_encoding.as_mut())
                .and_then(|encoding| encoding.encoder.as_mut())
            {
                walk_plan_schema_call_mut(encoder, visit)?;
            }
        }
        PlanExpr::Between {
            expr, start, end, ..
        } => {
            walk_plan_expr_mut(expr, visit)?;
            walk_plan_expr_mut(start, visit)?;
            walk_plan_expr_mut(end, visit)?;
        }
        PlanExpr::Case {
            base,
            when_then,
            else_expr,
        } => {
            if let Some(base) = base {
                walk_plan_expr_mut(base, visit)?;
            }
            for (when, then) in when_then {
                walk_plan_expr_mut(when, visit)?;
                walk_plan_expr_mut(then, visit)?;
            }
            if let Some(else_expr) = else_expr {
                walk_plan_expr_mut(else_expr, visit)?;
            }
        }
        PlanExpr::Cast { expr, target } => {
            walk_plan_expr_mut(expr, visit)?;
            walk_plan_exprs_mut(&mut target.parameters, visit)?;
            walk_plan_cast_programs_mut(&mut target.programs, visit)?;
        }
        PlanExpr::Function(function) => walk_plan_function_mut(function, visit)?,
        PlanExpr::InList { lhs, values, .. } => {
            walk_plan_expr_mut(lhs, visit)?;
            walk_plan_exprs_mut(values, visit)?;
        }
        PlanExpr::Subquery(PlanSubqueryExpr::In { lhs, .. }) => {
            walk_plan_expr_mut(lhs, visit)?;
        }
        PlanExpr::Like {
            lhs, rhs, escape, ..
        } => {
            walk_plan_expr_mut(lhs, visit)?;
            walk_plan_expr_mut(rhs, visit)?;
            if let Some(escape) = escape {
                walk_plan_expr_mut(escape, visit)?;
            }
        }
        PlanExpr::Row(values) | PlanExpr::Array(values) => {
            walk_plan_exprs_mut(values, visit)?;
        }
        PlanExpr::Subscript { base, index } => {
            walk_plan_expr_mut(base, visit)?;
            walk_plan_expr_mut(index, visit)?;
        }
        PlanExpr::FieldAccess(access) => {
            walk_plan_expr_mut(&mut access.base, visit)?;
        }
        PlanExpr::Raise {
            message: Some(message),
            ..
        } => {
            walk_plan_expr_mut(message, visit)?;
        }
        PlanExpr::Literal(_)
        | PlanExpr::Parameter(_)
        | PlanExpr::Column(_)
        | PlanExpr::RowId(_)
        | PlanExpr::Output(_)
        | PlanExpr::Subquery(PlanSubqueryExpr::Scalar { .. })
        | PlanExpr::Subquery(PlanSubqueryExpr::Exists(_))
        | PlanExpr::Raise { message: None, .. } => {}
    }

    Ok(PlanWalkControl::Continue)
}

fn walk_plan_exprs_mut<F>(exprs: &mut [PlanExpr], visit: &mut F) -> Result<()>
where
    F: FnMut(&mut PlanExpr) -> Result<PlanWalkControl>,
{
    for expr in exprs {
        walk_plan_expr_mut(expr, visit)?;
    }
    Ok(())
}

fn walk_plan_schema_call_mut<F>(call: &mut PlanBoundSchemaCall, visit: &mut F) -> Result<()>
where
    F: FnMut(&mut PlanExpr) -> Result<PlanWalkControl>,
{
    walk_plan_exprs_mut(&mut call.arguments, visit)
}

fn walk_plan_cast_programs_mut<F>(programs: &mut PlanCastPrograms, visit: &mut F) -> Result<()>
where
    F: FnMut(&mut PlanExpr) -> Result<PlanWalkControl>,
{
    for call in &mut programs.encode {
        walk_plan_schema_call_mut(call, visit)?;
    }
    if let Some(domain) = &mut programs.domain {
        for check in &mut domain.checks {
            walk_plan_schema_call_mut(&mut check.call, visit)?;
        }
    }
    Ok(())
}

fn walk_plan_function_mut<F>(function: &mut PlanFunctionCall, visit: &mut F) -> Result<()>
where
    F: FnMut(&mut PlanExpr) -> Result<PlanWalkControl>,
{
    walk_plan_exprs_mut(&mut function.arguments, visit)?;
    walk_plan_order_terms_mut(&mut function.argument_order, visit)?;
    walk_plan_order_terms_mut(&mut function.within_group, visit)?;
    if let Some(filter) = &mut function.filter {
        walk_plan_expr_mut(filter, visit)?;
    }
    if let Some(window) = &mut function.window {
        walk_plan_exprs_mut(&mut window.partition_by, visit)?;
        walk_plan_order_terms_mut(&mut window.order_by, visit)?;
        if let Some(frame) = &mut window.frame {
            walk_plan_frame_bound_mut(&mut frame.start, visit)?;
            if let Some(end) = &mut frame.end {
                walk_plan_frame_bound_mut(end, visit)?;
            }
        }
    }
    Ok(())
}

fn walk_plan_order_terms_mut<F>(terms: &mut [PlanOrderTerm], visit: &mut F) -> Result<()>
where
    F: FnMut(&mut PlanExpr) -> Result<PlanWalkControl>,
{
    for term in terms {
        walk_plan_expr_mut(&mut term.expr, visit)?;
    }
    Ok(())
}

fn walk_plan_frame_bound_mut<F>(bound: &mut PlanFrameBound, visit: &mut F) -> Result<()>
where
    F: FnMut(&mut PlanExpr) -> Result<PlanWalkControl>,
{
    match bound {
        PlanFrameBound::Following(expr) | PlanFrameBound::Preceding(expr) => {
            walk_plan_expr_mut(expr, visit)?;
        }
        PlanFrameBound::CurrentRow
        | PlanFrameBound::UnboundedFollowing
        | PlanFrameBound::UnboundedPreceding => {}
    }
    Ok(())
}

/// Structural equality for resolved plan expressions.
///
/// Catalog-backed values compare by their snapshot/object/database identity
/// through [`hir::CatalogObject`]'s equality implementation. Plan sources,
/// outputs, and subqueries compare by their plan identity. No display name or
/// live catalog lookup participates in the decision.
pub fn plan_exprs_are_equivalent(lhs: &PlanExpr, rhs: &PlanExpr) -> bool {
    match (lhs, rhs) {
        (PlanExpr::Literal(lhs), PlanExpr::Literal(rhs)) => check_literal_equivalency(lhs, rhs),
        (PlanExpr::Parameter(lhs), PlanExpr::Parameter(rhs)) => {
            lhs.index == rhs.index && lhs.name == rhs.name && lhs.type_fact == rhs.type_fact
        }
        (PlanExpr::Column(lhs), PlanExpr::Column(rhs)) => equivalent_column_ref(lhs, rhs),
        (PlanExpr::MergedColumn(lhs), PlanExpr::MergedColumn(rhs)) => {
            plan_exprs_are_equivalent(&lhs.left, &rhs.left)
                && equivalent_column_ref(&lhs.right, &rhs.right)
                && lhs.value == rhs.value
                && lhs.type_fact == rhs.type_fact
                && lhs.collation == rhs.collation
        }
        (PlanExpr::RowId(lhs), PlanExpr::RowId(rhs)) => lhs == rhs,
        (PlanExpr::Output(lhs), PlanExpr::Output(rhs)) => lhs == rhs,
        (
            PlanExpr::Unary {
                operator: lhs_operator,
                expr: lhs_expr,
            },
            PlanExpr::Unary {
                operator: rhs_operator,
                expr: rhs_expr,
            },
        ) => lhs_operator == rhs_operator && plan_exprs_are_equivalent(lhs_expr, rhs_expr),
        (
            PlanExpr::Binary {
                lhs: lhs_left,
                operator: lhs_operator,
                rhs: lhs_right,
                custom: lhs_custom,
            },
            PlanExpr::Binary {
                lhs: rhs_left,
                operator: rhs_operator,
                rhs: rhs_right,
                custom: rhs_custom,
            },
        ) if lhs_operator == rhs_operator => {
            let same_order = plan_exprs_are_equivalent(lhs_left, rhs_left)
                && plan_exprs_are_equivalent(lhs_right, rhs_right)
                && plan_custom_binary_operators_are_equivalent(
                    lhs_custom.as_ref(),
                    rhs_custom.as_ref(),
                    false,
                );
            same_order
                || (lhs_operator.is_commutative()
                    && plan_exprs_are_equivalent(lhs_left, rhs_right)
                    && plan_exprs_are_equivalent(lhs_right, rhs_left)
                    && plan_custom_binary_operators_are_equivalent(
                        lhs_custom.as_ref(),
                        rhs_custom.as_ref(),
                        true,
                    ))
        }
        (
            PlanExpr::Between {
                expr: lhs_expr,
                negated: lhs_negated,
                start: lhs_start,
                end: lhs_end,
            },
            PlanExpr::Between {
                expr: rhs_expr,
                negated: rhs_negated,
                start: rhs_start,
                end: rhs_end,
            },
        ) => {
            lhs_negated == rhs_negated
                && plan_exprs_are_equivalent(lhs_expr, rhs_expr)
                && plan_exprs_are_equivalent(lhs_start, rhs_start)
                && plan_exprs_are_equivalent(lhs_end, rhs_end)
        }
        (
            PlanExpr::Case {
                base: lhs_base,
                when_then: lhs_when_then,
                else_expr: lhs_else,
            },
            PlanExpr::Case {
                base: rhs_base,
                when_then: rhs_when_then,
                else_expr: rhs_else,
            },
        ) => {
            equivalent_optional_expr(lhs_base.as_deref(), rhs_base.as_deref())
                && lhs_when_then.len() == rhs_when_then.len()
                && lhs_when_then.iter().zip(rhs_when_then).all(
                    |((lhs_when, lhs_then), (rhs_when, rhs_then))| {
                        plan_exprs_are_equivalent(lhs_when, rhs_when)
                            && plan_exprs_are_equivalent(lhs_then, rhs_then)
                    },
                )
                && equivalent_optional_expr(lhs_else.as_deref(), rhs_else.as_deref())
        }
        (
            PlanExpr::Cast {
                expr: lhs_expr,
                target: lhs_target,
            },
            PlanExpr::Cast {
                expr: rhs_expr,
                target: rhs_target,
            },
        ) => {
            plan_exprs_are_equivalent(lhs_expr, rhs_expr)
                && equivalent_type_name(lhs_target, rhs_target)
        }
        (
            PlanExpr::Collate {
                expr: lhs_expr,
                collation: lhs_collation,
            },
            PlanExpr::Collate {
                expr: rhs_expr,
                collation: rhs_collation,
            },
        ) => lhs_collation == rhs_collation && plan_exprs_are_equivalent(lhs_expr, rhs_expr),
        (PlanExpr::Function(lhs), PlanExpr::Function(rhs)) => equivalent_function(lhs, rhs),
        (PlanExpr::IsNull(lhs), PlanExpr::IsNull(rhs))
        | (PlanExpr::NotNull(lhs), PlanExpr::NotNull(rhs)) => plan_exprs_are_equivalent(lhs, rhs),
        (
            PlanExpr::InList {
                lhs: lhs_expr,
                negated: lhs_negated,
                values: lhs_values,
            },
            PlanExpr::InList {
                lhs: rhs_expr,
                negated: rhs_negated,
                values: rhs_values,
            },
        ) => {
            lhs_negated == rhs_negated
                && plan_exprs_are_equivalent(lhs_expr, rhs_expr)
                && equivalent_expr_slices(lhs_values, rhs_values)
        }
        (PlanExpr::Subquery(lhs), PlanExpr::Subquery(rhs)) => equivalent_subquery_expr(lhs, rhs),
        (
            PlanExpr::Like {
                lhs: lhs_left,
                negated: lhs_negated,
                operator: lhs_operator,
                function: lhs_function,
                argument_count: lhs_argument_count,
                rhs: lhs_right,
                escape: lhs_escape,
            },
            PlanExpr::Like {
                lhs: rhs_left,
                negated: rhs_negated,
                operator: rhs_operator,
                function: rhs_function,
                argument_count: rhs_argument_count,
                rhs: rhs_right,
                escape: rhs_escape,
            },
        ) => {
            lhs_negated == rhs_negated
                && lhs_operator == rhs_operator
                && lhs_function == rhs_function
                && lhs_argument_count == rhs_argument_count
                && plan_exprs_are_equivalent(lhs_left, rhs_left)
                && plan_exprs_are_equivalent(lhs_right, rhs_right)
                && equivalent_optional_expr(lhs_escape.as_deref(), rhs_escape.as_deref())
        }
        (PlanExpr::Row(lhs), PlanExpr::Row(rhs)) | (PlanExpr::Array(lhs), PlanExpr::Array(rhs)) => {
            equivalent_expr_slices(lhs, rhs)
        }
        (
            PlanExpr::Subscript {
                base: lhs_base,
                index: lhs_index,
            },
            PlanExpr::Subscript {
                base: rhs_base,
                index: rhs_index,
            },
        ) => {
            plan_exprs_are_equivalent(lhs_base, rhs_base)
                && plan_exprs_are_equivalent(lhs_index, rhs_index)
        }
        (PlanExpr::FieldAccess(lhs), PlanExpr::FieldAccess(rhs)) => {
            plan_exprs_are_equivalent(&lhs.base, &rhs.base)
                && lhs.kind == rhs.kind
                && lhs.container_type == rhs.container_type
                && lhs.result_type == rhs.result_type
        }
        (
            PlanExpr::Raise {
                action: lhs_action,
                message: lhs_message,
            },
            PlanExpr::Raise {
                action: rhs_action,
                message: rhs_message,
            },
        ) => {
            lhs_action == rhs_action
                && equivalent_optional_expr(lhs_message.as_deref(), rhs_message.as_deref())
        }
        _ => false,
    }
}

fn equivalent_column_ref(lhs: &PlanColumnRef, rhs: &PlanColumnRef) -> bool {
    lhs.source == rhs.source && lhs.column == rhs.column
}

fn plan_custom_binary_operators_are_equivalent(
    lhs: Option<&PlanCustomBinaryOperator>,
    rhs: Option<&PlanCustomBinaryOperator>,
    operands_swapped: bool,
) -> bool {
    match (lhs, rhs) {
        (None, None) => true,
        (Some(lhs), Some(rhs)) => {
            lhs.function == rhs.function
                && lhs.swap_args == rhs.swap_args
                && lhs.negate == rhs.negate
                && match (&lhs.literal_encoding, &rhs.literal_encoding) {
                    (None, None) => true,
                    (Some(lhs), Some(rhs)) => {
                        let rhs_operand = if operands_swapped {
                            match rhs.operand {
                                PlanBinaryOperand::Left => PlanBinaryOperand::Right,
                                PlanBinaryOperand::Right => PlanBinaryOperand::Left,
                            }
                        } else {
                            rhs.operand
                        };
                        lhs.operand == rhs_operand
                            && equivalent_optional_schema_call(
                                lhs.encoder.as_ref(),
                                rhs.encoder.as_ref(),
                            )
                    }
                    _ => false,
                }
        }
        _ => false,
    }
}

fn equivalent_optional_expr(lhs: Option<&PlanExpr>, rhs: Option<&PlanExpr>) -> bool {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => plan_exprs_are_equivalent(lhs, rhs),
        (None, None) => true,
        _ => false,
    }
}

fn equivalent_expr_slices(lhs: &[PlanExpr], rhs: &[PlanExpr]) -> bool {
    lhs.len() == rhs.len()
        && lhs
            .iter()
            .zip(rhs)
            .all(|(lhs, rhs)| plan_exprs_are_equivalent(lhs, rhs))
}

fn equivalent_optional_schema_call(
    lhs: Option<&PlanBoundSchemaCall>,
    rhs: Option<&PlanBoundSchemaCall>,
) -> bool {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => equivalent_schema_call(lhs, rhs),
        (None, None) => true,
        _ => false,
    }
}

fn equivalent_schema_call(lhs: &PlanBoundSchemaCall, rhs: &PlanBoundSchemaCall) -> bool {
    Arc::ptr_eq(&lhs.program, &rhs.program)
        && equivalent_expr_slices(&lhs.arguments, &rhs.arguments)
}

fn equivalent_schema_calls(lhs: &[PlanBoundSchemaCall], rhs: &[PlanBoundSchemaCall]) -> bool {
    lhs.len() == rhs.len()
        && lhs
            .iter()
            .zip(rhs)
            .all(|(lhs, rhs)| equivalent_schema_call(lhs, rhs))
}

fn equivalent_cast_programs(lhs: &PlanCastPrograms, rhs: &PlanCastPrograms) -> bool {
    lhs.apply_builtin_affinity == rhs.apply_builtin_affinity
        && equivalent_schema_calls(&lhs.encode, &rhs.encode)
        && match (&lhs.domain, &rhs.domain) {
            (Some(lhs), Some(rhs)) => equivalent_domain_constraints(lhs, rhs),
            (None, None) => true,
            _ => false,
        }
}

fn equivalent_domain_constraints(lhs: &PlanDomainConstraints, rhs: &PlanDomainConstraints) -> bool {
    lhs.not_null_description == rhs.not_null_description
        && lhs.checks.len() == rhs.checks.len()
        && lhs.checks.iter().zip(&rhs.checks).all(|(lhs, rhs)| {
            lhs.failure_description == rhs.failure_description
                && equivalent_schema_call(&lhs.call, &rhs.call)
        })
}

fn equivalent_type_name(lhs: &PlanTypeName, rhs: &PlanTypeName) -> bool {
    lhs.name == rhs.name
        && lhs.array_dimensions == rhs.array_dimensions
        && lhs.type_fact == rhs.type_fact
        && equivalent_expr_slices(&lhs.parameters, &rhs.parameters)
        && equivalent_cast_programs(&lhs.programs, &rhs.programs)
}

fn equivalent_order_terms(lhs: &[PlanOrderTerm], rhs: &[PlanOrderTerm]) -> bool {
    lhs.len() == rhs.len()
        && lhs.iter().zip(rhs).all(|(lhs, rhs)| {
            lhs.order == rhs.order
                && lhs.nulls == rhs.nulls
                && plan_exprs_are_equivalent(&lhs.expr, &rhs.expr)
        })
}

fn equivalent_window_spec(lhs: &PlanWindowSpec, rhs: &PlanWindowSpec) -> bool {
    equivalent_expr_slices(&lhs.partition_by, &rhs.partition_by)
        && equivalent_order_terms(&lhs.order_by, &rhs.order_by)
        && match (&lhs.frame, &rhs.frame) {
            (Some(lhs), Some(rhs)) => equivalent_window_frame(lhs, rhs),
            (None, None) => true,
            _ => false,
        }
}

fn equivalent_window_frame(lhs: &PlanWindowFrame, rhs: &PlanWindowFrame) -> bool {
    lhs.mode == rhs.mode
        && lhs.exclude == rhs.exclude
        && equivalent_frame_bound(&lhs.start, &rhs.start)
        && match (&lhs.end, &rhs.end) {
            (Some(lhs), Some(rhs)) => equivalent_frame_bound(lhs, rhs),
            (None, None) => true,
            _ => false,
        }
}

fn equivalent_frame_bound(lhs: &PlanFrameBound, rhs: &PlanFrameBound) -> bool {
    match (lhs, rhs) {
        (PlanFrameBound::CurrentRow, PlanFrameBound::CurrentRow)
        | (PlanFrameBound::UnboundedFollowing, PlanFrameBound::UnboundedFollowing)
        | (PlanFrameBound::UnboundedPreceding, PlanFrameBound::UnboundedPreceding) => true,
        (PlanFrameBound::Following(lhs), PlanFrameBound::Following(rhs))
        | (PlanFrameBound::Preceding(lhs), PlanFrameBound::Preceding(rhs)) => {
            plan_exprs_are_equivalent(lhs, rhs)
        }
        _ => false,
    }
}

fn equivalent_function(lhs: &PlanFunctionCall, rhs: &PlanFunctionCall) -> bool {
    lhs.function == rhs.function
        && lhs.star == rhs.star
        && lhs.distinctness == rhs.distinctness
        && lhs.result_type == rhs.result_type
        && equivalent_expr_slices(&lhs.arguments, &rhs.arguments)
        && equivalent_order_terms(&lhs.argument_order, &rhs.argument_order)
        && equivalent_order_terms(&lhs.within_group, &rhs.within_group)
        && equivalent_optional_expr(lhs.filter.as_deref(), rhs.filter.as_deref())
        && match (&lhs.window, &rhs.window) {
            (Some(lhs), Some(rhs)) => equivalent_window_spec(lhs, rhs),
            (None, None) => true,
            _ => false,
        }
        && equivalent_custom_operation(
            lhs.custom_type_operation.as_ref(),
            rhs.custom_type_operation.as_ref(),
        )
        && equivalent_sequence_operation(
            lhs.sequence_operation.as_ref(),
            rhs.sequence_operation.as_ref(),
        )
}

fn equivalent_sequence_operation(
    lhs: Option<&PlanSequenceOperation>,
    rhs: Option<&PlanSequenceOperation>,
) -> bool {
    match (lhs, rhs) {
        (None, None) => true,
        (Some(lhs), Some(rhs)) => {
            lhs.kind == rhs.kind
                && lhs.database == rhs.database
                && lhs.user_name == rhs.user_name
                && lhs.normalized_name == rhs.normalized_name
                && lhs.backing_table == rhs.backing_table
                && lhs.schema_cookie == rhs.schema_cookie
                && lhs.sequence.name == rhs.sequence.name
                && lhs.sequence.start_value == rhs.sequence.start_value
                && lhs.sequence.increment_by == rhs.sequence.increment_by
                && lhs.sequence.min_value == rhs.sequence.min_value
                && lhs.sequence.max_value == rhs.sequence.max_value
                && lhs.sequence.cycle == rhs.sequence.cycle
        }
        _ => false,
    }
}

fn equivalent_custom_operation(
    lhs: Option<&PlanCustomTypeOperation>,
    rhs: Option<&PlanCustomTypeOperation>,
) -> bool {
    match (lhs, rhs) {
        (
            Some(PlanCustomTypeOperation::UnionValue {
                union_type: lhs_type,
                tag_index: lhs_tag,
                result_type: lhs_result,
            }),
            Some(PlanCustomTypeOperation::UnionValue {
                union_type: rhs_type,
                tag_index: rhs_tag,
                result_type: rhs_result,
            }),
        )
        | (
            Some(PlanCustomTypeOperation::UnionExtract {
                union_type: lhs_type,
                tag_index: lhs_tag,
                result_type: lhs_result,
            }),
            Some(PlanCustomTypeOperation::UnionExtract {
                union_type: rhs_type,
                tag_index: rhs_tag,
                result_type: rhs_result,
            }),
        ) => lhs_type == rhs_type && lhs_tag == rhs_tag && lhs_result == rhs_result,
        (
            Some(PlanCustomTypeOperation::UnionTag {
                union_type: lhs_type,
                tag_names: lhs_names,
            }),
            Some(PlanCustomTypeOperation::UnionTag {
                union_type: rhs_type,
                tag_names: rhs_names,
            }),
        ) => lhs_type == rhs_type && lhs_names == rhs_names,
        (
            Some(PlanCustomTypeOperation::StructExtract {
                struct_type: lhs_type,
                field_index: lhs_field,
                result_type: lhs_result,
            }),
            Some(PlanCustomTypeOperation::StructExtract {
                struct_type: rhs_type,
                field_index: rhs_field,
                result_type: rhs_result,
            }),
        ) => lhs_type == rhs_type && lhs_field == rhs_field && lhs_result == rhs_result,
        (None, None) => true,
        _ => false,
    }
}

fn equivalent_subquery_expr(lhs: &PlanSubqueryExpr, rhs: &PlanSubqueryExpr) -> bool {
    match (lhs, rhs) {
        (
            PlanSubqueryExpr::Scalar {
                query: lhs_query,
                output: lhs_output,
            },
            PlanSubqueryExpr::Scalar {
                query: rhs_query,
                output: rhs_output,
            },
        ) => lhs_query == rhs_query && lhs_output == rhs_output,
        (PlanSubqueryExpr::Exists(lhs), PlanSubqueryExpr::Exists(rhs)) => lhs == rhs,
        (
            PlanSubqueryExpr::In {
                lhs: lhs_expr,
                query: lhs_query,
                negated: lhs_negated,
            },
            PlanSubqueryExpr::In {
                lhs: rhs_expr,
                query: rhs_query,
                negated: rhs_negated,
            },
        ) => {
            lhs_query == rhs_query
                && lhs_negated == rhs_negated
                && plan_exprs_are_equivalent(lhs_expr, rhs_expr)
        }
        _ => false,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PlanColumnUse {
    Column(usize),
    RowId,
}

#[derive(Clone, Debug, Default)]
pub struct PlanExprDependencies {
    pub source_uses: HashSet<(PlanSourceId, PlanColumnUse)>,
    pub outputs: HashSet<PlanOutputId>,
    pub subqueries: HashSet<PlanSubqueryId>,
}

impl PlanExprDependencies {
    pub fn sources(&self) -> impl Iterator<Item = PlanSourceId> + '_ {
        self.source_uses.iter().map(|(source, _)| *source)
    }

    pub fn is_constant(&self) -> bool {
        self.source_uses.is_empty() && self.outputs.is_empty() && self.subqueries.is_empty()
    }
}

/// Collect all resolved identities read by one expression tree.
pub fn plan_expr_dependencies(expr: &PlanExpr) -> Result<PlanExprDependencies> {
    let mut dependencies = PlanExprDependencies::default();
    walk_plan_expr(expr, &mut |expr| {
        match expr {
            PlanExpr::Column(column) => {
                dependencies
                    .source_uses
                    .insert((column.source, PlanColumnUse::Column(column.column)));
            }
            PlanExpr::MergedColumn(column) => {
                dependencies.source_uses.insert((
                    column.right.source,
                    PlanColumnUse::Column(column.right.column),
                ));
            }
            PlanExpr::RowId(source) => {
                dependencies
                    .source_uses
                    .insert((*source, PlanColumnUse::RowId));
            }
            PlanExpr::Output(output) => {
                dependencies.outputs.insert(*output);
            }
            PlanExpr::Subquery(PlanSubqueryExpr::Scalar { query, .. })
            | PlanExpr::Subquery(PlanSubqueryExpr::Exists(query))
            | PlanExpr::Subquery(PlanSubqueryExpr::In { query, .. }) => {
                dependencies.subqueries.insert(*query);
            }
            _ => {}
        }
        Ok(PlanWalkControl::Continue)
    })?;
    Ok(dependencies)
}

/// Visit every complete subexpression together with all identities used below
/// that node. This is the form used by expression-index registration: callers
/// retain the whole candidate expression while also seeing its exact columns.
pub fn walk_plan_expr_dependencies<F>(expr: &PlanExpr, visit: &mut F) -> Result<PlanWalkControl>
where
    F: FnMut(&PlanExpr, &PlanExprDependencies) -> Result<PlanWalkControl>,
{
    walk_plan_expr(expr, &mut |expr| {
        let dependencies = plan_expr_dependencies(expr)?;
        visit(expr, &dependencies)
    })
}

pub fn plan_expr_references_subquery_id(expr: &PlanExpr, query: PlanSubqueryId) -> bool {
    let mut referenced = false;
    let _ = walk_plan_expr(expr, &mut |expr| {
        let current = match expr {
            PlanExpr::Subquery(PlanSubqueryExpr::Scalar { query, .. })
            | PlanExpr::Subquery(PlanSubqueryExpr::Exists(query))
            | PlanExpr::Subquery(PlanSubqueryExpr::In { query, .. }) => *query,
            _ => return Ok(PlanWalkControl::Continue),
        };
        if current == query {
            referenced = true;
            return Ok(PlanWalkControl::SkipChildren);
        }
        Ok(PlanWalkControl::Continue)
    });
    referenced
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PlanExprAffinity {
    pub affinity: Affinity,
    pub has_affinity: bool,
}

impl PlanExprAffinity {
    pub const fn with_affinity(affinity: Affinity) -> Self {
        Self {
            affinity,
            has_affinity: true,
        }
    }

    pub const fn no_affinity() -> Self {
        Self {
            affinity: Affinity::Blob,
            has_affinity: false,
        }
    }
}

/// Resolved facts supplied by the plan that owns an expression.
///
/// The expression helpers never search a schema. Sources and outputs that
/// need metadata provide it through this interface after HIR lowering.
pub trait PlanExprFactSource {
    fn column_type_fact(&self, _source: PlanSourceId, _column: usize) -> Option<TypeFact> {
        None
    }

    fn output_type_fact(&self, _output: PlanOutputId) -> Option<TypeFact> {
        None
    }

    fn subquery_output_type_fact(
        &self,
        _query: PlanSubqueryId,
        _output: usize,
    ) -> Option<TypeFact> {
        None
    }

    fn subquery_width(&self, _query: PlanSubqueryId) -> Option<usize> {
        None
    }

    fn column_affinity(&self, _source: PlanSourceId, _column: usize) -> Option<PlanExprAffinity> {
        None
    }

    fn output_affinity(&self, _output: PlanOutputId) -> Option<PlanExprAffinity> {
        None
    }

    fn subquery_output_affinity(
        &self,
        _query: PlanSubqueryId,
        _output: usize,
    ) -> Option<PlanExprAffinity> {
        None
    }

    fn column_collation(&self, _source: PlanSourceId, _column: usize) -> Option<CollationSeq> {
        None
    }

    fn rowid_collation(&self, _source: PlanSourceId) -> Option<CollationSeq> {
        None
    }

    fn output_collation(&self, _output: PlanOutputId) -> Option<CollationSeq> {
        None
    }

    fn subquery_output_collation(
        &self,
        _query: PlanSubqueryId,
        _output: usize,
    ) -> Option<CollationSeq> {
        None
    }
}

impl PlanExprFactSource for () {}

pub fn plan_expr_type_fact(expr: &PlanExpr, facts: &impl PlanExprFactSource) -> TypeFact {
    match expr {
        PlanExpr::Literal(literal) => plan_literal_type_fact(literal),
        PlanExpr::Parameter(parameter) => parameter.type_fact.clone(),
        PlanExpr::Column(column) => column.type_fact.clone(),
        PlanExpr::MergedColumn(column) => column.type_fact.clone(),
        PlanExpr::RowId(_) => TypeFact::known(Type::Integer),
        PlanExpr::Output(output) => facts.output_type_fact(*output).unwrap_or_default(),
        PlanExpr::Cast { target, .. } => target.type_fact.clone(),
        PlanExpr::Function(function) => function.result_type.clone(),
        PlanExpr::FieldAccess(access) => access.result_type.clone(),
        PlanExpr::Collate { expr, .. } => plan_expr_type_fact(expr, facts),
        PlanExpr::Unary {
            operator: UnaryOperator::Not | UnaryOperator::BitwiseNot,
            ..
        } => TypeFact::known(Type::Integer),
        PlanExpr::Unary { expr, .. } => plan_expr_type_fact(expr, facts),
        PlanExpr::IsNull(_)
        | PlanExpr::NotNull(_)
        | PlanExpr::Like { .. }
        | PlanExpr::Between { .. }
        | PlanExpr::InList { .. } => TypeFact::known(Type::Integer),
        PlanExpr::Subquery(PlanSubqueryExpr::Exists(_))
        | PlanExpr::Subquery(PlanSubqueryExpr::In { .. }) => TypeFact::known(Type::Integer),
        PlanExpr::Subquery(PlanSubqueryExpr::Scalar { query, output }) => facts
            .subquery_output_type_fact(*query, *output)
            .unwrap_or_default(),
        PlanExpr::Array(elements) => TypeFact::array_literal_result(
            elements
                .iter()
                .map(|element| plan_expr_type_fact(element, facts)),
        ),
        PlanExpr::Subscript { base, .. } => {
            let mut fact = plan_expr_type_fact(base, facts);
            if !fact.is_array() {
                return TypeFact::dynamic();
            }
            fact.array_dimensions = fact.array_dimensions.saturating_sub(1);
            if let Some(declared) = fact.declared.as_mut() {
                declared.array_dimensions = declared.array_dimensions.saturating_sub(1);
                declared.storage = if declared.array_dimensions == 0 {
                    storage_type_for_name(&declared.name)
                } else {
                    Type::Blob
                };
                fact.storage = Some(declared.storage);
            } else if fact.is_array() {
                fact.storage = Some(Type::Blob);
            } else {
                fact.storage = None;
            }
            fact
        }
        PlanExpr::Binary {
            lhs,
            operator: Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide,
            rhs,
            ..
        } => {
            let lhs = plan_expr_type_fact(lhs, facts);
            let rhs = plan_expr_type_fact(rhs, facts);
            TypeFact::arithmetic_result(&lhs, &rhs)
        }
        PlanExpr::Binary {
            operator:
                Operator::Modulus
                | Operator::BitwiseAnd
                | Operator::BitwiseOr
                | Operator::LeftShift
                | Operator::RightShift,
            ..
        } => TypeFact::known(Type::Integer),
        PlanExpr::Binary {
            lhs,
            operator: Operator::Concat,
            rhs,
            ..
        } => TypeFact::concat_result(
            &plan_expr_type_fact(lhs, facts),
            &plan_expr_type_fact(rhs, facts),
        ),
        PlanExpr::Binary {
            operator: Operator::ArrowRight,
            ..
        } => TypeFact::known(Type::Text),
        PlanExpr::Binary { operator, .. }
            if matches!(
                operator,
                Operator::And
                    | Operator::Or
                    | Operator::Equals
                    | Operator::NotEquals
                    | Operator::Less
                    | Operator::LessEquals
                    | Operator::Greater
                    | Operator::GreaterEquals
                    | Operator::Is
                    | Operator::IsNot
            ) =>
        {
            TypeFact::known(Type::Integer)
        }
        PlanExpr::Case {
            when_then,
            else_expr,
            ..
        } => {
            let mut results = Vec::with_capacity(when_then.len() + else_expr.iter().count());
            results.extend(
                when_then
                    .iter()
                    .map(|(_, result)| plan_expr_type_fact(result, facts)),
            );
            if let Some(else_expr) = else_expr {
                results.push(plan_expr_type_fact(else_expr, facts));
            }
            TypeFact::selected_value_result(&results)
        }
        PlanExpr::Binary { .. } | PlanExpr::Row(_) | PlanExpr::Raise { .. } => TypeFact::dynamic(),
    }
}

fn plan_literal_type_fact(literal: &Literal) -> TypeFact {
    match literal {
        Literal::Numeric(value)
            if value
                .as_bytes()
                .iter()
                .any(|byte| matches!(byte, b'.' | b'e' | b'E')) =>
        {
            TypeFact::known(Type::Real)
        }
        Literal::Numeric(_) | Literal::True | Literal::False => TypeFact::known(Type::Integer),
        Literal::String(_)
        | Literal::Keyword(_)
        | Literal::CurrentDate
        | Literal::CurrentTime
        | Literal::CurrentTimestamp => TypeFact::known(Type::Text),
        Literal::Blob(_) => TypeFact::known(Type::Blob),
        Literal::Null => TypeFact::known(Type::Null),
    }
}

fn storage_type_for_name(name: &str) -> Type {
    match Affinity::affinity(name) {
        Affinity::Integer => Type::Integer,
        Affinity::Text => Type::Text,
        Affinity::Blob => Type::Blob,
        Affinity::Real => Type::Real,
        Affinity::Numeric => Type::Numeric,
    }
}

pub fn plan_expr_affinity(expr: &PlanExpr, facts: &impl PlanExprFactSource) -> PlanExprAffinity {
    match expr {
        PlanExpr::Column(column) => {
            if column.has_affinity {
                PlanExprAffinity::with_affinity(column.affinity)
            } else {
                PlanExprAffinity::no_affinity()
            }
        }
        PlanExpr::MergedColumn(column) => {
            if column.has_affinity {
                PlanExprAffinity::with_affinity(column.affinity)
            } else {
                PlanExprAffinity::no_affinity()
            }
        }
        PlanExpr::RowId(_) => PlanExprAffinity::with_affinity(Affinity::Integer),
        PlanExpr::Output(output) => facts
            .output_affinity(*output)
            .or_else(|| {
                facts
                    .output_type_fact(*output)
                    .map(|fact| PlanExprAffinity::with_affinity(type_fact_affinity(&fact)))
            })
            .unwrap_or_else(PlanExprAffinity::no_affinity),
        PlanExpr::Cast { target, .. } => PlanExprAffinity::with_affinity(target.cast_affinity()),
        PlanExpr::Collate { expr, .. } => plan_expr_affinity(expr, facts),
        PlanExpr::Subquery(PlanSubqueryExpr::Scalar { query, output }) => facts
            .subquery_output_affinity(*query, *output)
            .or_else(|| {
                facts
                    .subquery_output_type_fact(*query, *output)
                    .map(|fact| PlanExprAffinity::with_affinity(type_fact_affinity(&fact)))
            })
            .unwrap_or_else(PlanExprAffinity::no_affinity),
        _ => PlanExprAffinity::no_affinity(),
    }
}

/// Choose comparison affinity using the same two-sided rule as SQLite: when
/// both operands have affinity, any numeric affinity wins; otherwise the one
/// operand that has affinity supplies it. Expressions such as literals retain
/// "no affinity" rather than inventing one from their storage class.
pub fn resolve_plan_comparison_affinity(
    lhs: &PlanExpr,
    rhs: &PlanExpr,
    facts: &impl PlanExprFactSource,
) -> Affinity {
    let lhs = plan_expr_affinity(lhs, facts);
    let rhs = plan_expr_affinity(rhs, facts);
    match (lhs.has_affinity, rhs.has_affinity) {
        (true, true) if lhs.affinity.is_numeric() || rhs.affinity.is_numeric() => Affinity::Numeric,
        (true, true) => Affinity::Blob,
        (true, false) => lhs.affinity,
        (false, true) => rhs.affinity,
        (false, false) => Affinity::Blob,
    }
}

fn type_fact_affinity(fact: &TypeFact) -> Affinity {
    if fact.is_array() {
        return Affinity::Blob;
    }
    let Some(declared) = &fact.declared else {
        return fact.storage.map_or(Affinity::Blob, affinity_for_storage);
    };
    if declared.custom().is_some() {
        affinity_for_storage(declared.storage)
    } else {
        Affinity::affinity(&declared.name)
    }
}

const fn affinity_for_storage(storage: Type) -> Affinity {
    match storage {
        Type::Null | Type::Blob => Affinity::Blob,
        Type::Text => Affinity::Text,
        Type::Numeric => Affinity::Numeric,
        Type::Integer => Affinity::Integer,
        Type::Real => Affinity::Real,
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlanExprCollations {
    pub explicit: Option<CollationSeq>,
    pub implicit: Option<CollationSeq>,
}

/// Return the leftmost explicit COLLATE attached anywhere in this operand.
pub fn plan_expr_explicit_collation(expr: &PlanExpr) -> Result<Option<ResolvedCollation>> {
    let mut explicit = None;
    walk_plan_expr(expr, &mut |expr| match expr {
        PlanExpr::Collate { collation, .. } => {
            if explicit.is_none() {
                explicit = Some(collation.clone());
            }
            Ok(PlanWalkControl::SkipChildren)
        }
        // A merged USING column is a column boundary. Its source expression
        // supplies inherited metadata, not explicit COLLATE syntax to the
        // expression that reads the merged column.
        PlanExpr::MergedColumn(_) => Ok(PlanWalkControl::SkipChildren),
        _ => Ok(PlanWalkControl::Continue),
    })?;
    Ok(explicit)
}

fn plan_expr_inherited_collation(
    expr: &PlanExpr,
    facts: &impl PlanExprFactSource,
) -> Option<CollationSeq> {
    match expr {
        PlanExpr::Column(column) => column.collation.as_ref().map(|value| *value.value()),
        PlanExpr::MergedColumn(column) => column.collation.as_ref().map(|value| *value.value()),
        PlanExpr::Output(output) => facts.output_collation(*output),
        PlanExpr::Subquery(PlanSubqueryExpr::Scalar { query, output }) => {
            facts.subquery_output_collation(*query, *output)
        }
        PlanExpr::Unary {
            operator: UnaryOperator::Positive,
            expr,
        }
        | PlanExpr::Cast { expr, .. } => plan_expr_inherited_collation(expr, facts),
        _ => None,
    }
}

pub fn plan_expr_collations(
    expr: &PlanExpr,
    facts: &impl PlanExprFactSource,
) -> Result<PlanExprCollations> {
    Ok(PlanExprCollations {
        explicit: plan_expr_explicit_collation(expr)?.map(|collation| *collation.value()),
        implicit: plan_expr_inherited_collation(expr, facts),
    })
}

pub fn plan_expr_collation(
    expr: &PlanExpr,
    facts: &impl PlanExprFactSource,
) -> Result<Option<CollationSeq>> {
    let collations = plan_expr_collations(expr, facts)?;
    Ok(collations.explicit.or(collations.implicit))
}

pub fn resolve_plan_comparison_collation(
    lhs: &PlanExpr,
    rhs: &PlanExpr,
    facts: &impl PlanExprFactSource,
) -> Result<CollationSeq> {
    let lhs = plan_expr_collations(lhs, facts)?;
    let rhs = plan_expr_collations(rhs, facts)?;
    Ok(lhs
        .explicit
        .or(rhs.explicit)
        .or(lhs.implicit)
        .or(rhs.implicit)
        .unwrap_or(CollationSeq::Binary))
}

/// Validate row-value use and return the number of values produced by an
/// expression at this point in the plan.
pub fn plan_expr_vector_size(expr: &PlanExpr, facts: &impl PlanExprFactSource) -> Result<usize> {
    Ok(match expr {
        PlanExpr::Between {
            expr, start, end, ..
        } => {
            let lhs = plan_expr_vector_size(expr, facts)?;
            let start = plan_expr_vector_size(start, facts)?;
            let end = plan_expr_vector_size(end, facts)?;
            if lhs != start || lhs != end {
                crate::bail_parse_error!("row value misused");
            }
            1
        }
        PlanExpr::Binary {
            lhs, operator, rhs, ..
        } => {
            let lhs = plan_expr_vector_size(lhs, facts)?;
            let rhs = plan_expr_vector_size(rhs, facts)?;
            if lhs != rhs || (lhs > 1 && !supports_row_value_binary_comparison(*operator)) {
                crate::bail_parse_error!("row value misused");
            }
            1
        }
        PlanExpr::Case {
            base,
            when_then,
            else_expr,
        } => {
            if let Some(base) = base {
                require_scalar_plan_expr(base, facts)?;
            }
            for (when, then) in when_then {
                require_scalar_plan_expr(when, facts)?;
                require_scalar_plan_expr(then, facts)?;
            }
            if let Some(else_expr) = else_expr {
                require_scalar_plan_expr(else_expr, facts)?;
            }
            1
        }
        PlanExpr::Cast { expr, target } => {
            require_scalar_plan_expr(expr, facts)?;
            for parameter in &target.parameters {
                require_scalar_plan_expr(parameter, facts)?;
            }
            1
        }
        PlanExpr::Collate { expr, .. }
        | PlanExpr::Unary { expr, .. }
        | PlanExpr::IsNull(expr)
        | PlanExpr::NotNull(expr) => {
            require_scalar_plan_expr(expr, facts)?;
            1
        }
        PlanExpr::Function(function) => {
            for argument in &function.arguments {
                require_scalar_plan_expr(argument, facts)?;
            }
            for term in function.argument_order.iter().chain(&function.within_group) {
                require_scalar_plan_expr(&term.expr, facts)?;
            }
            if let Some(filter) = &function.filter {
                require_scalar_plan_expr(filter, facts)?;
            }
            1
        }
        PlanExpr::InList { lhs, values, .. } => {
            let lhs_size = plan_expr_vector_size(lhs, facts)?;
            for value in values {
                let value_size = plan_expr_vector_size(value, facts)?;
                if lhs_size != value_size {
                    if lhs_size == 1 {
                        crate::bail_parse_error!("row value misused");
                    }
                    crate::bail_parse_error!(
                        "IN(...) element has {value_size} term{} - expected {lhs_size}",
                        if value_size == 1 { "" } else { "s" }
                    );
                }
            }
            1
        }
        PlanExpr::Subquery(PlanSubqueryExpr::Scalar { query, output }) => {
            let width = facts.subquery_width(*query).ok_or_else(|| {
                LimboError::InternalError(format!("missing output width for subquery {query}"))
            })?;
            if *output >= width {
                return Err(LimboError::InternalError(format!(
                    "subquery {query} has {width} outputs, requested output {output}"
                )));
            }
            1
        }
        PlanExpr::Subquery(PlanSubqueryExpr::Exists(_)) => 1,
        PlanExpr::Subquery(PlanSubqueryExpr::In { lhs, query, .. }) => {
            let lhs_size = plan_expr_vector_size(lhs, facts)?;
            let query_size = facts.subquery_width(*query).ok_or_else(|| {
                LimboError::InternalError(format!("missing output width for subquery {query}"))
            })?;
            if lhs_size != query_size {
                crate::bail_parse_error!(
                    "sub-select returns {query_size} columns - expected {lhs_size}"
                );
            }
            1
        }
        PlanExpr::Like {
            lhs,
            operator,
            rhs,
            escape,
            ..
        } => {
            let lhs_size = plan_expr_vector_size(lhs, facts)?;
            if lhs_size != 1 && *operator != LikeOperator::Match {
                crate::bail_parse_error!("row value misused");
            }
            require_scalar_plan_expr(rhs, facts)?;
            if let Some(escape) = escape {
                require_scalar_plan_expr(escape, facts)?;
            }
            1
        }
        PlanExpr::Row(values) => values.len(),
        PlanExpr::Array(values) => {
            for value in values {
                require_scalar_plan_expr(value, facts)?;
            }
            1
        }
        PlanExpr::Subscript { base, index } => {
            require_scalar_plan_expr(base, facts)?;
            require_scalar_plan_expr(index, facts)?;
            1
        }
        PlanExpr::FieldAccess(access) => {
            require_scalar_plan_expr(&access.base, facts)?;
            1
        }
        PlanExpr::MergedColumn(column) => {
            require_scalar_plan_expr(&column.left, facts)?;
            1
        }
        PlanExpr::Raise {
            message: Some(message),
            ..
        } => {
            require_scalar_plan_expr(message, facts)?;
            1
        }
        PlanExpr::Literal(_)
        | PlanExpr::Parameter(_)
        | PlanExpr::Column(_)
        | PlanExpr::RowId(_)
        | PlanExpr::Output(_)
        | PlanExpr::Raise { message: None, .. } => 1,
    })
}

fn require_scalar_plan_expr(expr: &PlanExpr, facts: &impl PlanExprFactSource) -> Result<()> {
    if plan_expr_vector_size(expr, facts)? != 1 {
        crate::bail_parse_error!("row value misused");
    }
    Ok(())
}

const fn supports_row_value_binary_comparison(operator: Operator) -> bool {
    matches!(
        operator,
        Operator::Equals
            | Operator::NotEquals
            | Operator::Less
            | Operator::LessEquals
            | Operator::Greater
            | Operator::GreaterEquals
            | Operator::Is
            | Operator::IsNot
    )
}

pub fn plan_expr_array_dimensions(expr: &PlanExpr, facts: &impl PlanExprFactSource) -> u32 {
    type_fact_array_dimensions(&plan_expr_type_fact(expr, facts))
}

/// Whether semantic analysis found any path that produces an array. This is
/// deliberately separate from the bounded rank because recursive or dynamic
/// expressions can have an unknown maximum depth.
pub fn plan_expr_is_array(expr: &PlanExpr, facts: &impl PlanExprFactSource) -> bool {
    plan_expr_type_fact(expr, facts).is_array()
}

/// Return the array rank fixed during semantic analysis. This includes array
/// literals and computed arrays which have no declared SQL type.
pub fn type_fact_array_dimensions(fact: &TypeFact) -> u32 {
    fact.array_dimensions
}

pub fn plan_expr_as_literal(expr: &PlanExpr) -> Option<&Literal> {
    match expr {
        PlanExpr::Literal(literal) => Some(literal),
        _ => None,
    }
}

pub fn parse_plan_signed_number(expr: &PlanExpr) -> Result<Value> {
    match expr {
        PlanExpr::Literal(Literal::Numeric(number)) => parse_numeric_literal(number),
        PlanExpr::Unary {
            operator: UnaryOperator::Negative,
            expr,
        } => match expr.as_ref() {
            PlanExpr::Literal(Literal::Numeric(number)) => {
                parse_numeric_literal(&format!("-{number}"))
            }
            _ => Err(invalid_plan_signed_number()),
        },
        PlanExpr::Unary {
            operator: UnaryOperator::Positive,
            expr,
        } => match expr.as_ref() {
            PlanExpr::Literal(Literal::Numeric(number)) => parse_numeric_literal(number),
            _ => Err(invalid_plan_signed_number()),
        },
        _ => Err(invalid_plan_signed_number()),
    }
}

fn invalid_plan_signed_number() -> LimboError {
    LimboError::InvalidArgument(
        "signed-number must follow the format: ([+|-] numeric-literal)".to_string(),
    )
}

pub fn plan_function_is_aggregate(function: &PlanFunctionCall) -> bool {
    match function.function.value() {
        Func::Agg(_) => true,
        Func::External(function) => function.func.is_aggregate(),
        _ => false,
    }
}

pub fn plan_function_is_group_aggregate(function: &PlanFunctionCall) -> bool {
    plan_function_is_aggregate(function) && function.window.is_none()
}

pub fn plan_function_is_window(function: &PlanFunctionCall) -> bool {
    function.window.is_some() || matches!(function.function.value(), Func::Window(_))
}

pub fn plan_expr_contains_aggregate(expr: &PlanExpr) -> Result<bool> {
    plan_expr_contains_function(expr, plan_function_is_group_aggregate)
}

pub fn plan_expr_contains_window(expr: &PlanExpr) -> Result<bool> {
    plan_expr_contains_function(expr, plan_function_is_window)
}

fn plan_expr_contains_function(
    expr: &PlanExpr,
    predicate: impl Fn(&PlanFunctionCall) -> bool,
) -> Result<bool> {
    let mut found = false;
    walk_plan_expr(expr, &mut |expr| {
        if let PlanExpr::Function(function) = expr {
            if predicate(function) {
                found = true;
                return Ok(PlanWalkControl::SkipChildren);
            }
        }
        Ok(PlanWalkControl::Continue)
    })?;
    Ok(found)
}

/// Lower one resolved HIR expression into the identity space of a VDBE plan.
///
/// All semantic decisions are already present in `expr`. This operation only
/// clones resolved values and replaces document-local identities with entries
/// supplied by `identities`.
pub fn lower_hir_expr(
    expr: &hir::Expr,
    identities: &PlanIdentityMap,
) -> PlanExprLoweringResult<PlanExpr> {
    Ok(match expr {
        hir::Expr::Literal(literal) => PlanExpr::Literal(literal.clone()),
        hir::Expr::Parameter(parameter) => PlanExpr::Parameter(PlanParameter {
            index: parameter.index,
            name: parameter.name.clone(),
            type_fact: parameter.type_fact.clone(),
        }),
        hir::Expr::Column(column) => PlanExpr::Column(lower_column_ref(*column, identities)?),
        hir::Expr::MergedColumn(column) => {
            PlanExpr::MergedColumn(lower_merged_column(column, identities)?)
        }
        hir::Expr::RowId(source) => PlanExpr::RowId(identities.require_source(*source)?),
        hir::Expr::Output(output) => PlanExpr::Output(identities.require_output(*output)?),
        hir::Expr::Unary { operator, expr } => PlanExpr::Unary {
            operator: *operator,
            expr: Box::new(lower_hir_expr(expr, identities)?),
        },
        hir::Expr::Binary {
            lhs,
            operator,
            rhs,
            custom,
        } => PlanExpr::Binary {
            lhs: Box::new(lower_hir_expr(lhs, identities)?),
            operator: *operator,
            rhs: Box::new(lower_hir_expr(rhs, identities)?),
            custom: custom
                .as_ref()
                .map(|operator| lower_custom_binary_operator(operator, identities))
                .transpose()?,
        },
        hir::Expr::Between {
            expr,
            negated,
            start,
            end,
        } => PlanExpr::Between {
            expr: Box::new(lower_hir_expr(expr, identities)?),
            negated: *negated,
            start: Box::new(lower_hir_expr(start, identities)?),
            end: Box::new(lower_hir_expr(end, identities)?),
        },
        hir::Expr::Case {
            base,
            when_then,
            else_expr,
        } => PlanExpr::Case {
            base: lower_optional_expr(base.as_deref(), identities)?,
            when_then: when_then
                .iter()
                .map(|(when, then)| {
                    Ok((
                        lower_hir_expr(when, identities)?,
                        lower_hir_expr(then, identities)?,
                    ))
                })
                .collect::<PlanExprLoweringResult<Vec<_>>>()?,
            else_expr: lower_optional_expr(else_expr.as_deref(), identities)?,
        },
        hir::Expr::Cast { expr, target } => PlanExpr::Cast {
            expr: Box::new(lower_hir_expr(expr, identities)?),
            target: lower_type_name(target, identities)?,
        },
        hir::Expr::Collate { expr, collation } => PlanExpr::Collate {
            expr: Box::new(lower_hir_expr(expr, identities)?),
            collation: collation.clone(),
        },
        hir::Expr::Function(function) => {
            PlanExpr::Function(lower_function_call(function, identities)?)
        }
        hir::Expr::IsNull(expr) => PlanExpr::IsNull(Box::new(lower_hir_expr(expr, identities)?)),
        hir::Expr::NotNull(expr) => PlanExpr::NotNull(Box::new(lower_hir_expr(expr, identities)?)),
        hir::Expr::InList {
            lhs,
            negated,
            values,
        } => PlanExpr::InList {
            lhs: Box::new(lower_hir_expr(lhs, identities)?),
            negated: *negated,
            values: lower_exprs(values, identities)?,
        },
        hir::Expr::Subquery(subquery) => {
            PlanExpr::Subquery(lower_subquery_expr(subquery, identities)?)
        }
        hir::Expr::Like {
            lhs,
            negated,
            operator,
            function,
            argument_count,
            rhs,
            escape,
        } => PlanExpr::Like {
            lhs: Box::new(lower_hir_expr(lhs, identities)?),
            negated: *negated,
            operator: *operator,
            function: function.clone(),
            argument_count: *argument_count,
            rhs: Box::new(lower_hir_expr(rhs, identities)?),
            escape: lower_optional_expr(escape.as_deref(), identities)?,
        },
        hir::Expr::Row(values) => PlanExpr::Row(lower_exprs(values, identities)?),
        hir::Expr::Array(values) => PlanExpr::Array(lower_exprs(values, identities)?),
        hir::Expr::Subscript { base, index } => PlanExpr::Subscript {
            base: Box::new(lower_hir_expr(base, identities)?),
            index: Box::new(lower_hir_expr(index, identities)?),
        },
        hir::Expr::FieldAccess(access) => {
            PlanExpr::FieldAccess(lower_field_access(access, identities)?)
        }
        hir::Expr::Raise { action, message } => PlanExpr::Raise {
            action: *action,
            message: lower_optional_expr(message.as_deref(), identities)?,
        },
    })
}

fn lower_column_ref(
    column: hir::ColumnRef,
    identities: &PlanIdentityMap,
) -> PlanExprLoweringResult<PlanColumnRef> {
    let metadata = identities
        .source_column(column.source, column.column)
        .ok_or_else(|| {
            missing_identity(
                PlanExprIdentityKind::SourceColumn,
                (column.source, column.column),
            )
        })?;
    Ok(PlanColumnRef {
        source: identities.require_source(column.source)?,
        column: column.column,
        rowid_alias: metadata.rowid_alias,
        type_fact: metadata.type_fact.clone(),
        affinity: metadata.affinity,
        has_affinity: metadata.has_affinity,
        collation: metadata.collation.clone(),
    })
}

fn lower_merged_column(
    column: &hir::MergedColumn,
    identities: &PlanIdentityMap,
) -> PlanExprLoweringResult<PlanMergedColumn> {
    let value = match column.value {
        hir::MergedColumnValue::Left => PlanMergedColumnValue::Left,
        hir::MergedColumnValue::Right => PlanMergedColumnValue::Right,
        hir::MergedColumnValue::Coalesce => PlanMergedColumnValue::Coalesce,
    };
    Ok(PlanMergedColumn {
        left: Box::new(lower_hir_expr(&column.left, identities)?),
        right: lower_column_ref(column.right, identities)?,
        value,
        type_fact: column.type_fact.clone(),
        affinity: column.affinity,
        has_affinity: column.has_affinity,
        collation: column.collation.clone(),
    })
}

fn lower_exprs(
    exprs: &[hir::Expr],
    identities: &PlanIdentityMap,
) -> PlanExprLoweringResult<Vec<PlanExpr>> {
    exprs
        .iter()
        .map(|expr| lower_hir_expr(expr, identities))
        .collect()
}

fn lower_optional_expr(
    expr: Option<&hir::Expr>,
    identities: &PlanIdentityMap,
) -> PlanExprLoweringResult<Option<Box<PlanExpr>>> {
    expr.map(|expr| lower_hir_expr(expr, identities).map(Box::new))
        .transpose()
}

impl PlanIdentityMap {
    /// Lower one invocation while sharing its document-owned program body.
    pub fn lower_schema_call(
        &self,
        call: &hir::BoundSchemaCall,
    ) -> PlanExprLoweringResult<PlanBoundSchemaCall> {
        Ok(PlanBoundSchemaCall {
            program: self.lower_schema_program(call.program)?,
            arguments: lower_exprs(&call.arguments, self)?,
        })
    }

    /// Lower one document-owned schema program at most once.
    pub fn lower_schema_program(
        &self,
        id: SchemaProgramId,
    ) -> PlanExprLoweringResult<Arc<PlanBoundSchemaProgram>> {
        let cached = self
            .lowered_schema_programs
            .borrow()
            .get(id.index())
            .and_then(Clone::clone);
        if let Some(program) = cached {
            return Ok(program);
        }

        let program = self
            .schema_programs
            .get(id.index())
            .cloned()
            .ok_or_else(|| missing_identity(PlanExprIdentityKind::SchemaProgram, id))?;
        if !self.schema_programs_being_lowered.borrow_mut().insert(id) {
            return Err(missing_identity(
                PlanExprIdentityKind::RecursiveSchemaProgram,
                id,
            ));
        }

        let lowered: PlanExprLoweringResult<Arc<PlanBoundSchemaProgram>> = (|| {
            Ok(Arc::new(PlanBoundSchemaProgram {
                input_source: self.require_source(program.input_source)?,
                body: lower_hir_expr(&program.body, self)?,
            }))
        })();

        assert!(
            self.schema_programs_being_lowered.borrow_mut().remove(&id),
            "schema program lowering state must contain {id}"
        );
        let lowered = lowered?;

        let mut cache = self.lowered_schema_programs.borrow_mut();
        let slot = cache
            .get_mut(id.index())
            .expect("schema program cache must match the HIR program arena");
        assert!(
            slot.is_none(),
            "schema program {id} must only be lowered once"
        );
        *slot = Some(lowered.clone());
        Ok(lowered)
    }

    pub fn lower_column_type_programs(
        &self,
        programs: &hir::BoundColumnTypePrograms,
    ) -> PlanExprLoweringResult<PlanColumnTypePrograms> {
        Ok(PlanColumnTypePrograms {
            encode: self.lower_schema_calls(&programs.encode)?,
            decode: self.lower_schema_calls(&programs.decode)?,
            array: programs.array.as_ref().map(|array| PlanArrayStorage {
                element_affinity: array.element_affinity,
                element_type: array.element_type.clone(),
                table_name: array.table_name.clone(),
                column_name: array.column_name.clone(),
                dimensions: array.dimensions,
            }),
            encode_nulls: programs.encode_nulls,
        })
    }

    pub fn lower_cast_programs(
        &self,
        programs: &hir::BoundCastPrograms,
    ) -> PlanExprLoweringResult<PlanCastPrograms> {
        Ok(PlanCastPrograms {
            encode: self.lower_schema_calls(&programs.encode)?,
            domain: programs
                .domain
                .as_ref()
                .map(|constraints| self.lower_domain_constraints(constraints))
                .transpose()?,
            apply_builtin_affinity: programs.apply_builtin_affinity,
        })
    }

    pub fn lower_domain_constraints(
        &self,
        constraints: &hir::BoundDomainConstraints,
    ) -> PlanExprLoweringResult<PlanDomainConstraints> {
        Ok(PlanDomainConstraints {
            not_null_description: constraints.not_null_description.clone(),
            checks: constraints
                .checks
                .iter()
                .map(|check| {
                    Ok(PlanDomainCheck {
                        call: self.lower_schema_call(&check.call)?,
                        failure_description: check.failure_description.clone(),
                    })
                })
                .collect::<PlanExprLoweringResult<Vec<_>>>()?,
        })
    }

    fn lower_schema_calls(
        &self,
        calls: &[hir::BoundSchemaCall],
    ) -> PlanExprLoweringResult<Vec<PlanBoundSchemaCall>> {
        calls
            .iter()
            .map(|call| self.lower_schema_call(call))
            .collect()
    }
}

fn lower_type_name(
    target: &hir::TypeName,
    identities: &PlanIdentityMap,
) -> PlanExprLoweringResult<PlanTypeName> {
    Ok(PlanTypeName {
        name: target.name.clone(),
        parameters: lower_exprs(&target.parameters, identities)?,
        array_dimensions: target.array_dimensions,
        type_fact: target.type_fact.clone(),
        programs: identities.lower_cast_programs(&target.programs)?,
    })
}

fn lower_order_term(
    term: &hir::OrderTerm,
    identities: &PlanIdentityMap,
) -> PlanExprLoweringResult<PlanOrderTerm> {
    Ok(PlanOrderTerm {
        expr: lower_hir_expr(&term.expr, identities)?,
        order: term.order,
        nulls: term.nulls,
    })
}

fn lower_order_terms(
    terms: &[hir::OrderTerm],
    identities: &PlanIdentityMap,
) -> PlanExprLoweringResult<Vec<PlanOrderTerm>> {
    terms
        .iter()
        .map(|term| lower_order_term(term, identities))
        .collect()
}

fn lower_window_spec(
    window: &hir::WindowSpec,
    identities: &PlanIdentityMap,
) -> PlanExprLoweringResult<PlanWindowSpec> {
    Ok(PlanWindowSpec {
        partition_by: lower_exprs(&window.partition_by, identities)?,
        order_by: lower_order_terms(&window.order_by, identities)?,
        frame: window
            .frame
            .as_ref()
            .map(|frame| lower_window_frame(frame, identities))
            .transpose()?,
    })
}

fn lower_window_frame(
    frame: &hir::WindowFrame,
    identities: &PlanIdentityMap,
) -> PlanExprLoweringResult<PlanWindowFrame> {
    Ok(PlanWindowFrame {
        mode: frame.mode,
        start: lower_window_frame_bound(&frame.start, identities)?,
        end: frame
            .end
            .as_ref()
            .map(|bound| lower_window_frame_bound(bound, identities))
            .transpose()?,
        exclude: frame.exclude.clone(),
    })
}

fn lower_window_frame_bound(
    bound: &hir::WindowFrameBound,
    identities: &PlanIdentityMap,
) -> PlanExprLoweringResult<PlanFrameBound> {
    Ok(match bound {
        hir::WindowFrameBound::CurrentRow => PlanFrameBound::CurrentRow,
        hir::WindowFrameBound::Following(expr) => {
            PlanFrameBound::Following(Box::new(lower_hir_expr(expr, identities)?))
        }
        hir::WindowFrameBound::Preceding(expr) => {
            PlanFrameBound::Preceding(Box::new(lower_hir_expr(expr, identities)?))
        }
        hir::WindowFrameBound::UnboundedFollowing => PlanFrameBound::UnboundedFollowing,
        hir::WindowFrameBound::UnboundedPreceding => PlanFrameBound::UnboundedPreceding,
    })
}

fn lower_function_call(
    function: &hir::FunctionCall,
    identities: &PlanIdentityMap,
) -> PlanExprLoweringResult<PlanFunctionCall> {
    Ok(PlanFunctionCall {
        function: function.function.clone(),
        arguments: lower_exprs(&function.arguments, identities)?,
        star: function.star,
        distinctness: function.distinctness,
        argument_order: lower_order_terms(&function.argument_order, identities)?,
        within_group: lower_order_terms(&function.within_group, identities)?,
        filter: lower_optional_expr(function.filter.as_deref(), identities)?,
        window: function
            .window
            .as_ref()
            .map(|window| lower_window_spec(window, identities))
            .transpose()?,
        custom_type_operation: function
            .custom_type_operation
            .as_ref()
            .map(lower_custom_type_operation),
        sequence_operation: function
            .sequence_operation
            .as_ref()
            .map(lower_sequence_operation),
        result_type: function.result_type.clone(),
    })
}

fn lower_sequence_operation(operation: &hir::SequenceOperation) -> PlanSequenceOperation {
    PlanSequenceOperation {
        kind: match operation.kind {
            hir::SequenceOperationKind::NextValue => PlanSequenceOperationKind::NextValue,
            hir::SequenceOperationKind::SetValue => PlanSequenceOperationKind::SetValue,
        },
        database: operation.database,
        user_name: operation.user_name.clone(),
        normalized_name: operation.normalized_name.clone(),
        backing_table: operation.backing_table.clone(),
        sequence: operation.sequence.clone(),
        schema_cookie: operation.schema_cookie,
    }
}

fn lower_custom_type_operation(operation: &hir::CustomTypeOperation) -> PlanCustomTypeOperation {
    match operation {
        hir::CustomTypeOperation::UnionValue {
            union_type,
            tag_index,
            result_type,
        } => PlanCustomTypeOperation::UnionValue {
            union_type: union_type.clone(),
            tag_index: *tag_index,
            result_type: result_type.clone(),
        },
        hir::CustomTypeOperation::UnionTag {
            union_type,
            tag_names,
        } => PlanCustomTypeOperation::UnionTag {
            union_type: union_type.clone(),
            tag_names: tag_names.clone(),
        },
        hir::CustomTypeOperation::UnionExtract {
            union_type,
            tag_index,
            result_type,
        } => PlanCustomTypeOperation::UnionExtract {
            union_type: union_type.clone(),
            tag_index: *tag_index,
            result_type: result_type.clone(),
        },
        hir::CustomTypeOperation::StructExtract {
            struct_type,
            field_index,
            result_type,
        } => PlanCustomTypeOperation::StructExtract {
            struct_type: struct_type.clone(),
            field_index: *field_index,
            result_type: result_type.clone(),
        },
    }
}

fn lower_custom_binary_operator(
    operator: &hir::CustomBinaryOperator,
    identities: &PlanIdentityMap,
) -> PlanExprLoweringResult<PlanCustomBinaryOperator> {
    Ok(PlanCustomBinaryOperator {
        function: operator.function.clone(),
        swap_args: operator.swap_args,
        negate: operator.negate,
        literal_encoding: operator
            .literal_encoding
            .as_ref()
            .map(|encoding| {
                Ok(PlanCustomBinaryLiteralEncoding {
                    operand: match encoding.operand {
                        hir::BinaryOperand::Left => PlanBinaryOperand::Left,
                        hir::BinaryOperand::Right => PlanBinaryOperand::Right,
                    },
                    encoder: encoding
                        .encoder
                        .as_ref()
                        .map(|call| identities.lower_schema_call(call))
                        .transpose()?,
                })
            })
            .transpose()?,
    })
}

fn lower_field_access(
    access: &hir::FieldAccess,
    identities: &PlanIdentityMap,
) -> PlanExprLoweringResult<PlanFieldAccess> {
    let kind = match access.kind {
        hir::FieldAccessKind::Struct { field_index } => PlanFieldAccessKind::Struct { field_index },
        hir::FieldAccessKind::Union { tag_index } => PlanFieldAccessKind::Union { tag_index },
    };
    Ok(PlanFieldAccess {
        base: Box::new(lower_hir_expr(&access.base, identities)?),
        field_name: access.field_name.clone(),
        kind,
        container_type: access.container_type.clone(),
        result_type: access.result_type.clone(),
    })
}

fn lower_subquery_expr(
    subquery: &hir::SubqueryExpr,
    identities: &PlanIdentityMap,
) -> PlanExprLoweringResult<PlanSubqueryExpr> {
    Ok(match subquery {
        hir::SubqueryExpr::Scalar { query, output } => PlanSubqueryExpr::Scalar {
            query: identities.require_subquery(*query)?,
            output: *output,
        },
        hir::SubqueryExpr::Exists(query) => {
            PlanSubqueryExpr::Exists(identities.require_subquery(*query)?)
        }
        hir::SubqueryExpr::In {
            lhs,
            query,
            negated,
        } => PlanSubqueryExpr::In {
            lhs: Box::new(lower_hir_expr(lhs, identities)?),
            query: identities.require_subquery(*query)?,
            negated: *negated,
        },
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanExprIdentityKind {
    Source,
    SourceColumn,
    Output,
    Subquery,
    SchemaProgram,
    RecursiveSchemaProgram,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanExprLoweringError {
    pub kind: PlanExprIdentityKind,
    pub semantic_id: String,
}

impl fmt::Display for PlanExprLoweringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.kind == PlanExprIdentityKind::RecursiveSchemaProgram {
            return write!(f, "recursive schema program {}", self.semantic_id);
        }
        write!(
            f,
            "missing {:?} identity mapping for {}",
            self.kind, self.semantic_id
        )
    }
}

impl std::error::Error for PlanExprLoweringError {}

pub type PlanExprLoweringResult<T> = std::result::Result<T, PlanExprLoweringError>;

fn missing_identity(
    kind: PlanExprIdentityKind,
    semantic_id: impl fmt::Debug,
) -> PlanExprLoweringError {
    PlanExprLoweringError {
        kind,
        semantic_id: format!("{semantic_id:?}"),
    }
}

impl PlanIdentityMap {
    pub(crate) fn require_source(
        &self,
        semantic: SourceId,
    ) -> PlanExprLoweringResult<PlanSourceId> {
        self.source(semantic)
            .ok_or_else(|| missing_identity(PlanExprIdentityKind::Source, semantic))
    }

    pub(crate) fn require_output(
        &self,
        semantic: OutputId,
    ) -> PlanExprLoweringResult<PlanOutputId> {
        self.output(semantic)
            .ok_or_else(|| missing_identity(PlanExprIdentityKind::Output, semantic))
    }

    pub(crate) fn require_subquery(
        &self,
        semantic: QueryId,
    ) -> PlanExprLoweringResult<PlanSubqueryId> {
        self.subquery(semantic)
            .ok_or_else(|| missing_identity(PlanExprIdentityKind::Subquery, semantic))
    }
}

fn root_outputs(root: &hir::HirRoot) -> &[hir::Output] {
    match root {
        hir::HirRoot::Insert(statement) => statement
            .returning
            .as_ref()
            .map_or(&[], |returning| returning.outputs.as_slice()),
        hir::HirRoot::Update(statement) => statement
            .returning
            .as_ref()
            .map_or(&[], |returning| returning.outputs.as_slice()),
        hir::HirRoot::Delete(statement) => statement
            .returning
            .as_ref()
            .map_or(&[], |returning| returning.outputs.as_slice()),
        hir::HirRoot::Query(_) | hir::HirRoot::TriggerPredicate(_) => &[],
    }
}
