//! The incremental maintenance operator DAG.
//!
//! A materialized view's defining query is decomposed into a directed acyclic
//! graph of incremental operators (scan, filter/project, join, aggregate, set
//! operation). Each node consumes the delta stream(s) of its input node(s) and
//! produces its own delta stream; the root node's stream is the view's change.
//! This is the DBSP circuit model described in
//! `docs/ivm-vdbe-rewrite-writeup.md`.
//!
//! This module owns only the *structure*. Delta-stream wiring, validation,
//! hidden-state derivation, and bytecode emission live in
//! `vdbe_maintenance`, all consuming this representation.
//!
//! Expressions are the analyzed `hir::Expr` of the view's query throughout,
//! lowered by the shared expression lowering over register-bound stream
//! values: deliberately *not* a parallel expression layer.

use std::sync::Arc;

use turso_parser::ast;

use crate::function::AggFunc;
use crate::schema::BTreeTable;
use crate::translate::collate::CollationSeq;
use crate::translate::semantic::hir::{self, SourceId};
use crate::{error::LimboError, Result};

/// Index into [`MaintenanceDag::nodes`]. A node's inputs always have smaller
/// indices, so iterating `0..nodes.len()` visits the DAG in topological order.
pub type NodeId = usize;

/// One source of the view's query whose rows a stream carries. The physical
/// stream may have one cursor, but its values keep the sources of all
/// contributing inputs, so a rowid read names the source it belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamBinding {
    /// The source of the view's analyzed query whose columns this binding
    /// carries. Expressions read it as `hir::Expr::Column` and
    /// `hir::Expr::RowId`; emitters bind those reads to registers.
    pub source: SourceId,
}

/// One aggregate of an [`OpNode::Aggregate`].
#[derive(Debug, Clone)]
pub struct Aggregate {
    pub func: AggFunc,
    /// The argument expressions, bound against the node's input schema.
    /// Empty for `COUNT(*)`.
    pub args: Vec<hir::Expr>,
    pub distinct: bool,
    /// The FILTER clause, bound against the node's input schema.
    pub filter: Option<hir::Expr>,
    /// The expression downstream operators read this aggregate's value
    /// through: the aggregate call of the view's query. `None` for the
    /// hidden liveness `COUNT(*)`, which no expression reads.
    pub published: Option<hir::Expr>,
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
    pub columns: Vec<Option<hir::Expr>>,
    pub bindings: Vec<StreamBinding>,
}

impl StreamSchema {
    pub fn len(&self) -> usize {
        self.columns.len()
    }
}

/// One key column of a stateful operator: which values share a class, and
/// whether one class can hold more than one exact value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyColumn {
    /// Values equal under this collation share a class.
    pub collation: CollationSeq,
    /// Whether one class can hold values that are equal but not identical:
    /// 'a' and 'A' under NOCASE, or the integer 2 and the real 2.0. SQL
    /// leaves open which of them a result shows. The operator then keeps
    /// every exact value of the class with its count, and shows the smallest
    /// one (see `vdbe_maintenance::members`).
    pub ambiguous: bool,
}

/// Join semantics supported by one maintenance node.
///
/// Outer joins are not join kinds. They are compositions of these nodes:
/// `A LEFT JOIN B` is `(A JOIN B) UNION ALL pad(A ANTI JOIN B)`, RIGHT pads
/// `B ANTI JOIN A` instead, and FULL pads both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    /// Every pair of input rows that satisfies the ON predicates.
    Inner,
    /// Rows of the first input that some row of the second input matches,
    /// each row once (`EXISTS`, `IN`). Output schema: the first input's.
    Semi,
    /// Rows of the first input that no row of the second input matches
    /// (`NOT EXISTS`). Output schema: the first input's.
    Anti,
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
        source: SourceId,
    },

    /// Selection (σ). Linear — its incremental form is itself applied to the
    /// input delta, so it keeps no state. Output schema: its input's.
    Filter {
        input: NodeId,
        /// Binds against the input's output schema.
        predicate: hir::Expr,
    },

    /// Projection (π). Linear. Output schema: `projections`, each an
    /// expression over the input's output schema.
    Project {
        input: NodeId,
        projections: Vec<hir::Expr>,
    },

    /// Rename a derived relation into the namespace exposed by its FROM-clause
    /// alias. This is a physical stream boundary, not SQL evaluation: values
    /// are copied unchanged while the output schema receives the derived
    /// table's bound column identities.
    Alias {
        input: NodeId,
        table: Arc<BTreeTable>,
        source: SourceId,
    },

    /// Binary join (⋈) with arbitrary ON predicates. Pure: no projection, no
    /// WHERE (those are `Project`/`Filter` above). Output schema: the
    /// concatenation of the left and right schemas for an inner join, the
    /// left schema for a semi or anti join. Keeping this node binary makes SQL's
    /// mixed outer-join tree explicit and bounds every delta decomposition
    /// to the bilinear two-input rule.
    ///
    /// Inputs expose both a delta stream and, where available, an arrangement.
    /// Scans use base-table btrees for free; aggregates expose their persisted
    /// finalized rows. Other interior operators require an explicit output
    /// arrangement before a downstream join can consume them.
    Join {
        inputs: [NodeId; 2],
        /// ON predicates, desugared from USING/NATURAL; bind against the
        /// concatenated input schema.
        on: Vec<hir::Expr>,
        kind: JoinKind,
    },

    /// Extend every input row with NULL columns: the output row is
    /// `before ++ input ++ after`, where `before` and `after` are all NULL.
    /// Linear. This is how an anti join's rows become the NULL-padded rows of
    /// an outer join. The NULL columns keep the bindings of the relation they
    /// stand for, so a padded row has the same schema as a matched row.
    Pad {
        input: NodeId,
        before: Arc<StreamSchema>,
        after: Arc<StreamSchema>,
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
        group_exprs: Vec<hir::Expr>,
        /// One entry per group key.
        group_keys: Vec<KeyColumn>,
        /// `aggregates[0]` is the hidden liveness `COUNT(*)`. Its physical
        /// output column is not published as a SQL expression.
        aggregates: Vec<Aggregate>,
        /// The value key of each aggregate-owned multiset (MIN/MAX and
        /// DISTINCT aggregates). Entries are aligned with `aggregates`;
        /// `None` means the aggregate has no per-value state.
        multiset_keys: Vec<Option<KeyColumn>>,
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
        /// One entry per content-key column (empty when `prefix_len == 0`).
        key_columns: Vec<KeyColumn>,
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
            | OpNode::Pad { input, .. }
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
    output_schemas: Vec<Arc<StreamSchema>>,
    pub root: NodeId,
}

impl MaintenanceDag {
    pub fn output_schema(&self, node: NodeId) -> &Arc<StreamSchema> {
        &self.output_schemas[node]
    }

    pub fn root_schema(&self) -> &StreamSchema {
        self.output_schema(self.root).as_ref()
    }
}

/// Accumulates nodes during decomposition, handing back the topological index
/// of each pushed node.
#[derive(Default)]
pub struct DagBuilder {
    nodes: Vec<OpNode>,
    output_schemas: Vec<Arc<StreamSchema>>,
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
        self.output_schemas.push(Arc::new(output_schema));
        Ok(self.nodes.len() - 1)
    }

    pub fn output_schema(&self, node: NodeId) -> &Arc<StreamSchema> {
        &self.output_schemas[node]
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
            OpNode::Scan { table, source } => (
                table
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(column_index, column)| {
                        column
                            .name
                            .as_ref()
                            .map(|_| hir::Expr::column(*source, column_index))
                    })
                    .collect(),
                vec![StreamBinding { source: *source }],
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
                source,
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
                            column
                                .name
                                .as_ref()
                                .map(|_| hir::Expr::column(*source, column_index))
                        })
                        .collect(),
                    vec![StreamBinding { source: *source }],
                )
            }
            OpNode::Join {
                inputs,
                kind: JoinKind::Inner,
                ..
            } => (
                inputs
                    .iter()
                    .flat_map(|input| self.output_schemas[*input].columns.iter().cloned())
                    .collect(),
                inputs
                    .iter()
                    .flat_map(|input| self.output_schemas[*input].bindings.iter().cloned())
                    .collect(),
            ),
            OpNode::Join {
                inputs,
                kind: JoinKind::Semi | JoinKind::Anti,
                ..
            } => (
                self.output_schemas[inputs[0]].columns.clone(),
                self.output_schemas[inputs[0]].bindings.clone(),
            ),
            OpNode::Pad {
                input,
                before,
                after,
            } => {
                let input = &self.output_schemas[*input];
                (
                    before
                        .columns
                        .iter()
                        .chain(&input.columns)
                        .chain(&after.columns)
                        .cloned()
                        .collect(),
                    before
                        .bindings
                        .iter()
                        .chain(&input.bindings)
                        .chain(&after.bindings)
                        .cloned()
                        .collect(),
                )
            }
            OpNode::Aggregate {
                input,
                group_exprs,
                aggregates,
                ..
            } => (
                group_exprs
                    .iter()
                    .cloned()
                    .map(Some)
                    .chain(
                        aggregates
                            .iter()
                            .map(|aggregate| aggregate.published.clone()),
                    )
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
                crate::alloc::vec![],
                crate::alloc::vec![],
                BTreeCharacteristics::HAS_ROWID,
                crate::alloc::vec![],
                crate::alloc::vec![],
                crate::alloc::vec![],
                None,
            )),
            source: SourceId::new(id),
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

    #[test]
    fn aggregate_schema_does_not_publish_hidden_liveness_count() {
        let count = hir::Expr::Literal(ast::Literal::Numeric("0".to_string()));
        let aggregate = |published: Option<hir::Expr>| Aggregate {
            func: AggFunc::Count0,
            args: Vec::new(),
            distinct: false,
            filter: None,
            published,
        };
        let mut builder = DagBuilder::new();
        let input = builder.push(scan("t", 0)).unwrap();
        let root = builder
            .push(OpNode::Aggregate {
                input,
                group_exprs: vec![count.clone()],
                group_keys: vec![KeyColumn {
                    collation: CollationSeq::Binary,
                    ambiguous: false,
                }],
                aggregates: vec![aggregate(None), aggregate(Some(count.clone()))],
                multiset_keys: vec![None, None],
                scalar: false,
            })
            .unwrap();
        let dag = builder.finish(root).unwrap();

        let columns = &dag.root_schema().columns;
        assert_eq!(columns.len(), 3);
        assert!(columns[0].as_ref().is_some_and(|c| c.equivalent(&count)));
        assert!(columns[1].is_none());
        assert!(columns[2].as_ref().is_some_and(|c| c.equivalent(&count)));
    }
}
