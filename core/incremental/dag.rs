//! The incremental maintenance operator DAG.
//!
//! A materialized view's defining query is decomposed into a directed acyclic
//! graph of incremental operators (scan, filter/project, join, aggregate, set
//! operation). Each node consumes the delta stream(s) of its input node(s) and
//! produces its own delta stream; the root node's stream is the view's change.
//! This is the DBSP circuit model — see `docs/ivm-delta-flow-design.md` — and
//! it replaces the per-shape classifier: a composite query is maintained by
//! composing the incremental maintenance of its operators, so combinations
//! (aggregate-over-join, DISTINCT-over-join, set-ops over either) stop being
//! special cases.
//!
//! This module owns only the *structure*. The delta-stream wiring and bytecode
//! emission live in `vdbe_maintenance`: codegen walks this DAG in topological
//! order (node index order, since inputs are always built before their
//! consumers) and emits each node into one shared program.
//!
//! Expressions are `ast::Expr` throughout, bound through the shared translator
//! — deliberately *not* a parallel expression layer. The old engine's
//! `LogicalExpr` and interpreted operators (deleted at `5e17d8ea0`) are not
//! resurrected; only its DAG composition model is.
//!
//! Introduced ahead of its consumers: the builder (decomposition) and the
//! DAG-walk codegen land in follow-up slices of the delta-flow resurrection.
//! Until codegen consumes it, the representation is unreferenced.
#![allow(dead_code)]

use std::sync::Arc;

use turso_parser::ast;

use crate::schema::BTreeTable;
use crate::translate::collate::CollationSeq;
use crate::translate::plan::Aggregate;

/// Index into [`MaintenanceDag::nodes`]. A node's inputs always have smaller
/// indices, so iterating `0..nodes.len()` visits the DAG in topological order.
pub type NodeId = usize;

/// One incremental operator in the maintenance DAG.
///
/// The operators are pure relational algebra with well-defined *output
/// schemas* — the crux of generic composition. Projection is its own
/// [`OpNode::Project`], not baked into `Join`/`Aggregate`: a `Join` feeding
/// an `Aggregate` must expose its natural join row (every input column) so
/// the `Aggregate` can read it, and a view's output expressions (`SUM(x)+1`)
/// are a `Project` over the `Aggregate`. HAVING is a `Filter` over the
/// `Aggregate`. This is what lets any operator feed any other; there is no
/// combination-specific node.
///
/// Every node has a natural **output schema** (its column list), computed by
/// the builder. An operator's expressions bind against its *input* node's
/// output schema — including across operator boundaries (a `Project`'s
/// `SUM(x)` binds to its `Aggregate` input's aggregate-output column), which
/// is why the old engine kept a `LogicalSchema` per node.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum OpNode {
    /// A base table: the leaf of every DAG. Produces the table's captured
    /// transaction delta (or, during population, a full scan), and exposes
    /// its btree as the post-state arrangement a [`OpNode::Join`] probes.
    /// Output schema: the table's columns.
    Scan {
        table: Arc<BTreeTable>,
        /// The identifier columns bind under (alias or table name).
        identifier: String,
    },

    /// Selection (σ). Linear — its incremental form is itself applied to the
    /// input delta, so it keeps no state. Output schema: its input's.
    Filter {
        input: NodeId,
        /// Binds against the input's output schema.
        predicate: ast::Expr,
    },

    /// Projection (π). Linear. Output schema: `projections` (each an
    /// expression over the input's output schema, with an optional name).
    Project {
        input: NodeId,
        projections: Vec<(ast::Expr, Option<String>)>,
    },

    /// Join (⋈), n-ary over its inputs, with arbitrary ON predicates. Pure:
    /// no projection, no WHERE (those are `Project`/`Filter` above). Output
    /// schema: the concatenation of the input schemas. The bilinear delta
    /// decomposition and the source-rowid-tuple pair map live in the codegen.
    ///
    /// Inputs are currently [`OpNode::Scan`]s (base tables are arrangements
    /// for free). Joining an *interior* operator needs that operator's
    /// integral materialized as an arrangement — a later generalization the
    /// interface admits, not a new special case.
    Join {
        inputs: Vec<NodeId>,
        /// Merged USING/NATURAL column names per input (parallel to `inputs`).
        using: Vec<Vec<String>>,
        /// ON predicates, desugared from USING/NATURAL; bind against the
        /// concatenated input schema.
        on: Vec<ast::Expr>,
        /// A two-table LEFT join (NULL-padded rows for unmatched left rows).
        left_outer: bool,
    },

    /// Aggregation (γ), including scalar aggregates. Keeps its own integral
    /// (per-group accumulators and the value multiset), so it consumes only
    /// its input's delta stream. Pure: HAVING and output expressions are a
    /// `Filter`/`Project` above. Output schema: `group_exprs` followed by one
    /// column per aggregate's finalized value.
    Aggregate {
        input: NodeId,
        /// Group keys, bound against the input's output schema. Empty for a
        /// scalar aggregate (`scalar`).
        group_exprs: Vec<ast::Expr>,
        group_collations: Vec<CollationSeq>,
        /// `aggregates[0]` is the hidden liveness `COUNT(*)`.
        aggregates: Vec<Aggregate>,
        multiset_collation: CollationSeq,
        scalar: bool,
    },

    /// Duplicate elimination (δ) — `SELECT DISTINCT`. Output schema: its
    /// input's. (Modeled distinctly from `Aggregate`, though it is the
    /// degenerate GROUP-BY-over-all-columns; the codegen may share machinery.)
    Distinct { input: NodeId },

    /// A compound set operation (UNION / UNION ALL / INTERSECT / EXCEPT) over
    /// branch inputs of any shape. Keeps its own integral (per-branch presence
    /// counts for the deduplicated prefix; multiplicity for the trailing
    /// UNION ALL), so each branch feeds only its delta stream. Output schema:
    /// the shared branch schema.
    SetOp {
        /// Branch inputs, in chain order.
        inputs: Vec<NodeId>,
        /// One operator per compound entry (`inputs.len() - 1` of them).
        operators: Vec<ast::CompoundOperator>,
        /// Result column count (shared by all branches).
        arity: usize,
        /// Leading branches deduplicated by content (0 for pure UNION ALL).
        prefix_len: usize,
        /// Collation per content-key column (empty when `prefix_len == 0`).
        key_collations: Vec<CollationSeq>,
    },
}

impl OpNode {
    /// The input node ids this operator consumes, in order.
    pub fn inputs(&self) -> &[NodeId] {
        match self {
            OpNode::Scan { .. } => &[],
            OpNode::Filter { input, .. }
            | OpNode::Project { input, .. }
            | OpNode::Aggregate { input, .. }
            | OpNode::Distinct { input } => std::slice::from_ref(input),
            OpNode::Join { inputs, .. } | OpNode::SetOp { inputs, .. } => inputs,
        }
    }
}

/// The operator DAG for a materialized view. Nodes are in topological order:
/// `nodes[i].inputs()` are all `< i`, and `root == nodes.len() - 1`.
#[derive(Debug, Clone)]
pub struct MaintenanceDag {
    pub nodes: Vec<OpNode>,
    pub root: NodeId,
}

/// A column of a node's output relation. `identity` is the base-column
/// expression that produces the column; a parent operator binds its
/// expressions by matching sub-expressions against these identities.
#[derive(Debug, Clone)]
pub struct OutputCol {
    pub identity: ast::Expr,
    pub name: Option<String>,
}

impl MaintenanceDag {
    pub fn root_node(&self) -> &OpNode {
        &self.nodes[self.root]
    }

    /// The output schema of every node, indexed by `NodeId`. Computed in one
    /// forward pass since inputs precede their consumers.
    pub fn output_schemas(&self) -> crate::Result<Vec<Vec<OutputCol>>> {
        let mut schemas: Vec<Vec<OutputCol>> = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            let schema = match node {
                OpNode::Scan { table, identifier } => {
                    let mut cols = Vec::with_capacity(table.columns().len());
                    for column in table.columns() {
                        let name = column.name.clone().ok_or_else(|| {
                            crate::LimboError::InternalError(
                                "btree table column without a name".to_string(),
                            )
                        })?;
                        cols.push(OutputCol {
                            identity: ast::Expr::Qualified(
                                ast::Name::exact(identifier.clone()),
                                ast::Name::exact(name.clone()),
                            ),
                            name: Some(name),
                        });
                    }
                    cols
                }
                OpNode::Filter { input, .. } | OpNode::Distinct { input } => schemas[*input].clone(),
                OpNode::Project { projections, .. } => projections
                    .iter()
                    .map(|(expr, name)| OutputCol {
                        identity: expr.clone(),
                        name: name.clone(),
                    })
                    .collect(),
                OpNode::Join { inputs, using, .. } => {
                    // Concatenate input schemas; a USING/NATURAL-merged column
                    // appears once (dropped from all but the first input that
                    // has it), matching star expansion.
                    let mut cols = Vec::new();
                    for (pos, &input) in inputs.iter().enumerate() {
                        for col in &schemas[input] {
                            if pos > 0
                                && using[pos].iter().any(|u| {
                                    col.name.as_deref().is_some_and(|n| n.eq_ignore_ascii_case(u))
                                })
                            {
                                continue;
                            }
                            cols.push(col.clone());
                        }
                    }
                    cols
                }
                OpNode::Aggregate {
                    group_exprs,
                    aggregates,
                    ..
                } => {
                    // Group keys, then one column per aggregate (identity is
                    // the aggregate call, so a parent's `SUM(x)` binds here).
                    let mut cols: Vec<OutputCol> = group_exprs
                        .iter()
                        .map(|expr| OutputCol {
                            identity: expr.clone(),
                            name: None,
                        })
                        .collect();
                    for agg in aggregates {
                        cols.push(OutputCol {
                            identity: agg.original_expr.clone(),
                            name: None,
                        });
                    }
                    cols
                }
                OpNode::SetOp { inputs, .. } => schemas[inputs[0]].clone(),
            };
            schemas.push(schema);
        }
        Ok(schemas)
    }
}

/// Accumulates nodes during decomposition, handing back the topological index
/// of each pushed node.
#[derive(Default)]
pub struct DagBuilder {
    nodes: Vec<OpNode>,
}

impl DagBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a node whose inputs were already pushed, returning its id.
    pub fn push(&mut self, node: OpNode) -> NodeId {
        debug_assert!(
            node.inputs().iter().all(|&i| i < self.nodes.len()),
            "operator inputs must be built before the operator"
        );
        self.nodes.push(node);
        self.nodes.len() - 1
    }

    /// Finalize with `root` as the output node.
    pub fn finish(self, root: NodeId) -> MaintenanceDag {
        debug_assert_eq!(root, self.nodes.len() - 1, "root must be the last node");
        MaintenanceDag {
            nodes: self.nodes,
            root,
        }
    }
}
