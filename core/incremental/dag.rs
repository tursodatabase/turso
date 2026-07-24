//! The incremental maintenance operator DAG.
//!
//! A materialized view's defining query is decomposed into a directed acyclic
//! graph of incremental operators (scan, filter/project, join, aggregate, set
//! operation). Each node consumes the delta stream(s) of its input node(s) and
//! produces its own delta stream; the root node's stream is the view's change.
//! This is the DBSP circuit model — see `docs/ivm-delta-flow-design.md` — and
//! it is the replacement boundary for the deleted per-shape classifier.
//!
//! This module owns only the *structure*. Delta-stream wiring, validation,
//! hidden-state derivation, and bytecode emission live in
//! `vdbe_maintenance`, all consuming this representation.
//!
//! Expressions are `ast::Expr` throughout, bound through the shared translator
//! — deliberately *not* a parallel expression layer. The old engine's
//! `LogicalExpr` and interpreted operators (deleted at `5e17d8ea0`) are not
//! resurrected; only its DAG composition model is.

use std::sync::Arc;

use turso_parser::ast;

use crate::schema::BTreeTable;
use crate::translate::collate::CollationSeq;
use crate::translate::plan::Aggregate;
use crate::{error::LimboError, Result};
use turso_parser::ast::TableInternalId;

/// Index into [`MaintenanceDag::nodes`]. A node's inputs always have smaller
/// indices, so iterating `0..nodes.len()` visits the DAG in topological order.
pub type NodeId = usize;

/// Base-table namespace needed to bind expressions carried across an
/// ephemeral operator edge. The physical stream may have one cursor, but its
/// values retain the logical names from all contributing inputs.
#[derive(Debug, Clone)]
pub struct StreamBinding {
    pub table: Arc<BTreeTable>,
    pub identifier: String,
    /// Stable logical binding assigned by the main planner. Physical emitter
    /// phases remap this id mechanically to their local cursor binding; names
    /// are presentation metadata and are never resolved again.
    pub logical_id: TableInternalId,
}

/// The value layout of an operator's delta stream.
///
/// Row identity and z-set weight are transport metadata rather than relational
/// columns and are therefore not included here.
#[derive(Debug, Clone)]
pub struct StreamSchema {
    /// Canonical expression used by downstream operators to bind each value
    /// slot. Base-table columns without a SQL name cannot be referenced by a
    /// later expression, so their slot deliberately has no expression.
    pub columns: Vec<Option<ast::Expr>>,
    pub bindings: Vec<StreamBinding>,
}

impl StreamSchema {
    pub fn len(&self) -> usize {
        self.columns.len()
    }
}

/// Join semantics supported by one maintenance node.
///
/// FULL is intentionally absent: it must lower to outer-join/anti-join
/// arrangements plus a set operator, not become another boolean on this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    LeftOuter,
}

/// One incremental operator in the maintenance DAG.
///
/// The operators are pure relational algebra with well-defined *output
/// schemas* — the crux of generic composition. Projection is its own
/// [`OpNode::Project`], not baked into `Join`/`Aggregate`: a `Join` feeding
/// an `Aggregate` must expose its natural join row (every input column) so
/// the `Aggregate` can read it, and a view's output expressions (`SUM(x)+1`)
/// are a `Project` over the `Aggregate`. HAVING is a `Filter` over the
/// `Aggregate`. This is the stream contract needed for any operator to feed
/// any other. Unsupported SQL is rejected while lowering or validating this
/// graph, before hidden storage or bytecode is emitted.
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
        logical_id: TableInternalId,
    },

    /// Selection (σ). Linear — its incremental form is itself applied to the
    /// input delta, so it keeps no state. Output schema: its input's.
    Filter {
        input: NodeId,
        /// Binds against the input's output schema.
        predicate: ast::Expr,
    },

    /// Projection (π). Linear. Output schema: `projections`, each an
    /// expression over the input's output schema.
    Project {
        input: NodeId,
        projections: Vec<ast::Expr>,
    },

    /// Rename a derived relation into the namespace exposed by its FROM-clause
    /// alias. This is a physical stream boundary, not SQL evaluation: values
    /// are copied unchanged while the output schema receives the derived
    /// table's bound column identities.
    Alias {
        input: NodeId,
        table: Arc<BTreeTable>,
        identifier: String,
        logical_id: TableInternalId,
    },

    /// Binary join (⋈) with arbitrary ON predicates. Pure: no projection, no
    /// WHERE (those are `Project`/`Filter` above). Output schema: the
    /// concatenation of the left and right schemas. Keeping this node binary
    /// makes SQL's mixed outer-join tree explicit and bounds every delta
    /// decomposition to the bilinear two-input rule.
    ///
    /// Inputs expose both a delta stream and, where available, an arrangement.
    /// Scans use base-table btrees for free; aggregates expose their persisted
    /// finalized rows. Other interior operators require an explicit output
    /// arrangement before a downstream join can consume them.
    Join {
        inputs: [NodeId; 2],
        /// ON predicates, desugared from USING/NATURAL; bind against the
        /// concatenated input schema.
        on: Vec<ast::Expr>,
        kind: JoinKind,
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
        /// Value-key collation for each aggregate-owned multiset. Entries are
        /// aligned with `aggregates`; `None` means the aggregate has no
        /// per-value state.
        multiset_collations: Vec<Option<CollationSeq>>,
        scalar: bool,
    },

    /// A compound set operation (UNION / UNION ALL / INTERSECT / EXCEPT).
    /// Every branch emits through the same typed stream contract.
    /// Keeps its own integral (per-branch presence counts for the
    /// deduplicated prefix; multiplicity for the trailing UNION ALL). Output
    /// schema: the shared branch schema.
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
            | OpNode::Alias { input, .. }
            | OpNode::Aggregate { input, .. } => std::slice::from_ref(input),
            OpNode::Join { inputs, .. } => inputs,
            OpNode::SetOp { inputs, .. } => inputs,
        }
    }
}

/// The operator DAG for a materialized view. Nodes are in topological order:
/// `nodes[i].inputs()` are all `< i`, and `root == nodes.len() - 1`.
#[derive(Debug, Clone)]
pub struct MaintenanceDag {
    pub nodes: Vec<OpNode>,
    output_schemas: Vec<StreamSchema>,
    pub root: NodeId,
}

impl MaintenanceDag {
    pub fn output_schema(&self, node: NodeId) -> &StreamSchema {
        &self.output_schemas[node]
    }

    pub fn root_schema(&self) -> &StreamSchema {
        self.output_schema(self.root)
    }
}

/// Accumulates nodes during decomposition, handing back the topological index
/// of each pushed node.
#[derive(Default)]
pub struct DagBuilder {
    nodes: Vec<OpNode>,
    output_schemas: Vec<StreamSchema>,
}

impl DagBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a node whose inputs were already pushed, returning its id.
    pub fn push(&mut self, node: OpNode) -> Result<NodeId> {
        if !node.inputs().iter().all(|&input| input < self.nodes.len()) {
            return Err(LimboError::InternalError(
                "operator inputs must be built before the operator".to_string(),
            ));
        }
        let output_schema = self.derive_output_schema(&node)?;
        self.nodes.push(node);
        self.output_schemas.push(output_schema);
        Ok(self.nodes.len() - 1)
    }

    /// Finalize with `root` as the output node.
    pub fn finish(self, root: NodeId) -> Result<MaintenanceDag> {
        if root + 1 != self.nodes.len() || self.nodes.len() != self.output_schemas.len() {
            return Err(LimboError::InternalError(
                "maintenance DAG root must be the last node".to_string(),
            ));
        }
        let mut reachable = vec![false; self.nodes.len()];
        let mut pending = vec![root];
        while let Some(node) = pending.pop() {
            if std::mem::replace(&mut reachable[node], true) {
                continue;
            }
            pending.extend_from_slice(self.nodes[node].inputs());
        }
        if reachable.iter().any(|reachable| !reachable) {
            return Err(LimboError::InternalError(
                "every maintenance DAG node must contribute to the root".to_string(),
            ));
        }
        Ok(MaintenanceDag {
            nodes: self.nodes,
            output_schemas: self.output_schemas,
            root,
        })
    }

    fn derive_output_schema(&self, node: &OpNode) -> Result<StreamSchema> {
        let (columns, bindings) = match node {
            OpNode::Scan {
                table,
                identifier,
                logical_id,
            } => (
                table
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(column_index, column)| {
                        column.name.as_ref().map(|_| ast::Expr::Column {
                            database: None,
                            table: *logical_id,
                            column: column_index,
                            is_rowid_alias: column.is_rowid_alias(),
                        })
                    })
                    .collect(),
                vec![StreamBinding {
                    table: table.clone(),
                    identifier: identifier.clone(),
                    logical_id: *logical_id,
                }],
            ),
            OpNode::Filter { input, .. } => (
                self.output_schemas[*input].columns.clone(),
                self.output_schemas[*input].bindings.clone(),
            ),
            OpNode::Project { input, projections } => (
                projections.iter().map(|expr| Some(expr.clone())).collect(),
                self.output_schemas[*input].bindings.clone(),
            ),
            OpNode::Alias {
                input,
                table,
                identifier,
                logical_id,
            } => {
                if self.output_schemas[*input].len() != table.columns().len() {
                    return Err(LimboError::InternalError(
                        "derived-table alias arity does not match its input stream".to_string(),
                    ));
                }
                (
                    table
                        .columns()
                        .iter()
                        .enumerate()
                        .map(|(column_index, column)| {
                            column.name.as_ref().map(|_| ast::Expr::Column {
                                database: None,
                                table: *logical_id,
                                column: column_index,
                                is_rowid_alias: column.is_rowid_alias(),
                            })
                        })
                        .collect(),
                    vec![StreamBinding {
                        table: table.clone(),
                        identifier: identifier.clone(),
                        logical_id: *logical_id,
                    }],
                )
            }
            OpNode::Join { inputs, .. } => (
                inputs
                    .iter()
                    .flat_map(|input| self.output_schemas[*input].columns.iter().cloned())
                    .collect(),
                inputs
                    .iter()
                    .flat_map(|input| self.output_schemas[*input].bindings.iter().cloned())
                    .collect(),
            ),
            OpNode::Aggregate {
                input,
                group_exprs,
                aggregates,
                ..
            } => (
                group_exprs
                    .iter()
                    .cloned()
                    .chain(
                        aggregates
                            .iter()
                            .map(|aggregate| aggregate.original_expr.clone()),
                    )
                    .map(Some)
                    .collect(),
                self.output_schemas[*input].bindings.clone(),
            ),
            OpNode::SetOp { inputs, arity, .. } => {
                let Some(first) = inputs.first() else {
                    return Err(LimboError::InternalError(
                        "set-op DAG must have at least one input".to_string(),
                    ));
                };
                for input in inputs {
                    if self.output_schemas[*input].len() != *arity {
                        return Err(LimboError::InternalError(
                            "set-op DAG inputs have inconsistent output schemas".to_string(),
                        ));
                    }
                }
                (
                    self.output_schemas[*first].columns.clone(),
                    self.output_schemas[*first].bindings.clone(),
                )
            }
        };
        Ok(StreamSchema { columns, bindings })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::BTreeCharacteristics;

    fn scan(name: &str, id: usize) -> OpNode {
        OpNode::Scan {
            table: Arc::new(BTreeTable::new(
                id as i64 + 1,
                name.to_string(),
                Vec::new(),
                Vec::new(),
                BTreeCharacteristics::HAS_ROWID,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                None,
            )),
            identifier: name.to_string(),
            logical_id: TableInternalId::from(id),
        }
    }

    #[test]
    fn finish_rejects_nodes_disconnected_from_root() {
        let mut builder = DagBuilder::new();
        builder.push(scan("orphan", 0)).unwrap();
        let root = builder.push(scan("root", 1)).unwrap();

        let error = builder.finish(root).unwrap_err();
        assert!(error
            .to_string()
            .contains("every maintenance DAG node must contribute to the root"));
    }
}
