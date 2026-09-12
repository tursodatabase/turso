//! Experimental logical plan stage.
//!
//! `PRAGMA unstable_logical_plan = 1` turns it on. The stage raises a prepared
//! `SelectPlan` into a tree of relational operators, applies rewrite rules to
//! the tree, and lowers the tree back into a `SelectPlan` for the existing
//! join optimizer and bytecode emitter.
//!
//! Column references keep their `TableInternalId`, so a node can move
//! anywhere in the tree without renumbering expressions.

pub(crate) mod display;
pub(crate) mod lower;
#[cfg(test)]
pub(crate) mod optgen;
pub(crate) mod raise;
pub(crate) mod rules;
pub(crate) mod walk;

use turso_parser::ast::{self, Expr, SortOrder, TableInternalId};

use crate::translate::emitter::Resolver;
use crate::translate::plan::{
    self as plan, Distinctness, GroupBy, JoinInfo, JoinedTable, NonFromClauseSubquery,
    OuterQueryReference, QueryDestination, ResultSetColumn, SelectPlan, TableReferences, WhereTerm,
};
use crate::vdbe::builder::ProgramBuilder;
use crate::Result;

/// Rewrite one prepared SELECT through the logical plan stage.
///
/// A plan with a shape the tree cannot hold yet is left as it is.
pub(crate) fn rewrite_select_plan(
    program: &mut ProgramBuilder,
    plan: &mut SelectPlan,
    resolver: &Resolver,
) -> Result<()> {
    let Some(mut block) = raise::raise_select_plan(plan) else {
        return Ok(());
    };
    let mut context = rules::RuleContext {
        resolver,
        ids: &mut program.table_reference_counter,
    };
    let changed = rules::rewrite_block(&mut block, &mut context)?;
    tracing::debug!(changed, logical_plan = %block);
    *plan = lower::lower_block(block)?;
    Ok(())
}

/// One query block: a tree of operators plus the block state that the tree
/// does not model.
#[derive(Clone, Debug)]
pub(crate) struct Block {
    pub root: LogicalPlan,
    pub subqueries: Vec<NonFromClauseSubquery>,
    pub outer_query_refs: Vec<OuterQueryReference>,
    pub right_join_swapped: bool,
    pub query_destination: QueryDestination,
    pub input_cardinality_hint: Option<f64>,
    pub phantom_params: Vec<ast::Variable>,
}

#[derive(Clone, Debug)]
pub(crate) enum LogicalPlan {
    /// A SELECT without a FROM clause reads one empty row.
    OneRow,
    /// One base table, virtual table, or a subquery that the tree keeps opaque.
    Scan(Scan),
    /// A FROM subquery that the tree can rewrite.
    DerivedTable(DerivedTable),
    Join(Join),
    /// A join whose right side runs once per row of the left side, because
    /// the right side reads columns of the left side. Section 2 of Neumann
    /// and Kemper, "Unnesting Arbitrary Queries".
    DependentJoin(DependentJoin),
    Filter(Filter),
    Aggregate(Aggregate),
    Project(Project),
    Distinct(Distinct),
    Sort(Sort),
    Limit(Limit),
}

#[derive(Clone, Debug)]
pub(crate) struct Scan {
    pub table: JoinedTable,
}

#[derive(Clone, Debug)]
pub(crate) struct DerivedTable {
    pub identifier: String,
    pub internal_id: TableInternalId,
    /// The table reference from the prepared plan. `None` for a derived table
    /// that a rule created; the lowering then builds the reference.
    pub shell: Option<JoinedTable>,
    /// True when the first column holds a scalar subquery result. Such a
    /// value has no text order, so the column gets no collation.
    pub value_without_collation: bool,
    pub block: Box<Block>,
}

#[derive(Clone, Debug)]
pub(crate) struct Join {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub info: JoinInfo,
}

#[derive(Clone, Debug)]
pub(crate) struct DependentJoin {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub kind: DependentJoinKind,
}

#[derive(Clone, Debug)]
pub(crate) enum DependentJoinKind {
    /// The right side is one scalar subquery. Its value is column 0 of the
    /// table `subquery.internal_id`. The entries restore the prepared form
    /// when no rule removes the join.
    Scalar {
        subquery: NonFromClauseSubquery,
        position: usize,
        duplicates: Vec<(usize, NonFromClauseSubquery)>,
    },
    /// The left side is a domain table: the set of outer values that the
    /// right side reads. The right side references the domain columns.
    Domain {
        domain_id: TableInternalId,
        column_count: usize,
    },
}

/// The WHERE and ON terms of a block, in the order the prepared plan had
/// them. A term of an outer, semi, or anti join keeps its `from_outer_join`
/// marker, so it stays with that join when the tree is lowered.
#[derive(Clone, Debug)]
pub(crate) struct Filter {
    pub input: Box<LogicalPlan>,
    pub terms: Vec<WhereTerm>,
}

#[derive(Clone, Debug)]
pub(crate) struct Aggregate {
    pub input: Box<LogicalPlan>,
    pub group_by: Option<GroupBy>,
    pub aggregates: Vec<plan::Aggregate>,
}

#[derive(Clone, Debug)]
pub(crate) struct Project {
    pub input: Box<LogicalPlan>,
    pub columns: Vec<ResultSetColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct Distinct {
    pub input: Box<LogicalPlan>,
    pub distinctness: Distinctness,
}

pub(crate) type SortKey = (Box<Expr>, SortOrder, Option<ast::NullsOrder>);

#[derive(Clone, Debug)]
pub(crate) struct Sort {
    pub input: Box<LogicalPlan>,
    pub keys: Vec<SortKey>,
}

#[derive(Clone, Debug)]
pub(crate) struct Limit {
    pub input: Box<LogicalPlan>,
    pub limit: Option<Box<Expr>>,
    pub offset: Option<Box<Expr>>,
}

impl LogicalPlan {
    /// The table id of a leaf node.
    pub fn leaf_id(&self) -> Option<TableInternalId> {
        match self {
            LogicalPlan::Scan(scan) => Some(scan.table.internal_id),
            LogicalPlan::DerivedTable(derived) => Some(derived.internal_id),
            _ => None,
        }
    }

    /// The input of a node with exactly one input.
    pub fn input_mut(&mut self) -> Option<&mut LogicalPlan> {
        match self {
            LogicalPlan::Filter(node) => Some(&mut node.input),
            LogicalPlan::Aggregate(node) => Some(&mut node.input),
            LogicalPlan::Project(node) => Some(&mut node.input),
            LogicalPlan::Distinct(node) => Some(&mut node.input),
            LogicalPlan::Sort(node) => Some(&mut node.input),
            LogicalPlan::Limit(node) => Some(&mut node.input),
            LogicalPlan::OneRow
            | LogicalPlan::Scan(_)
            | LogicalPlan::DerivedTable(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::DependentJoin(_) => None,
        }
    }
}

/// A SELECT plan with nothing in it. It fills the place of a plan that the
/// tree took out of a FROM subquery.
pub(crate) fn placeholder_select_plan() -> SelectPlan {
    SelectPlan {
        table_references: TableReferences::new_empty(),
        join_order: Vec::new(),
        result_columns: Vec::new(),
        where_clause: Vec::new(),
        group_by: None,
        order_by: Vec::new(),
        aggregates: Vec::new(),
        limit: None,
        offset: None,
        contains_constant_false_condition: false,
        query_destination: QueryDestination::Unset,
        distinctness: Distinctness::NonDistinct,
        values: Vec::new(),
        window: None,
        non_from_clause_subqueries: Vec::new(),
        input_cardinality_hint: None,
        estimated_output_rows: None,
        estimated_cost: None,
        simple_aggregate: None,
        phantom_params: Vec::new(),
    }
}
