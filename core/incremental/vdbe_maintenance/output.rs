use super::stream::{ArrangementHandle, DeltaIdentity, EphemeralDelta};
use crate::incremental::dag;
use crate::schema::BTreeTable;
use crate::sync::Arc;
use crate::{LimboError, Result};

/// The complete physical output contract of one DAG node.
///
/// Planning derives this once from the logical node and its already-planned
/// inputs. Bytecode emission consumes it verbatim; emitters must not infer an
/// identity from an operator shape or reconstruct a schema from physical
/// inputs.
#[derive(Debug, Clone)]
pub(super) struct NodeOutputContract {
    pub(super) schema: Arc<dag::StreamSchema>,
    /// Whether each logical binding's SQL rowid remains available after this
    /// operator. Transport identity is planned separately below.
    pub(super) binding_rowids: Arc<[bool]>,
    /// Identity emitted by the relational operator before an optional output
    /// arrangement.
    pub(super) emitted_identity: DeltaIdentity,
    /// Identity published on the node's DAG edge. An explicit arrangement
    /// replaces the producer identity with its own stable rowid.
    pub(super) published_identity: DeltaIdentity,
}

/// A delta-producing relation consumed by a maintenance operator.
///
/// This is derived exclusively from DAG edges. Emitters must not recover their
/// input by inspecting the original SELECT: doing so makes the DAG descriptive
/// rather than executable and reintroduces per-shape composition.
#[derive(Debug, Clone)]
pub(super) enum DeltaSource {
    BaseTable {
        table: Arc<BTreeTable>,
        /// Physical multiplicity column when this scan reads another
        /// materialized view. Transaction-delta cursors always expose their
        /// signed weight immediately after the logical columns.
        stored_weight_column: Option<usize>,
    },
    /// A z-set stream emitted by an upstream operator in this program.
    Ephemeral(EphemeralDelta),
}

impl DeltaSource {
    pub(super) fn identity(&self) -> DeltaIdentity {
        match self {
            Self::BaseTable { .. } => DeltaIdentity::BindingRowids(1),
            Self::Ephemeral(channel) => channel.identity,
        }
    }

    pub(super) fn binding_rowids(&self) -> Arc<[bool]> {
        match self {
            Self::BaseTable { table, .. } => vec![table.has_rowid].into(),
            Self::Ephemeral(channel) => channel
                .binding_rowid_columns
                .iter()
                .map(Option::is_some)
                .collect::<Vec<_>>()
                .into(),
        }
    }
}

/// The natural physical result emitted by one relational operator, before an
/// optional output arrangement changes the identity published on its DAG edge.
///
/// Construction validates the planner's emitted contract, so arrangement
/// publication never receives an unchecked operator result.
#[derive(Debug)]
pub(super) struct EmittedNodeOutput {
    delta: DeltaSource,
    native_arrangement: Option<ArrangementHandle>,
}

impl EmittedNodeOutput {
    pub(super) fn from_ephemeral(
        node_id: dag::NodeId,
        delta: EphemeralDelta,
        contract: &NodeOutputContract,
    ) -> Result<Self> {
        Self::new(node_id, DeltaSource::Ephemeral(delta), None, contract)
    }

    pub(super) fn new(
        node_id: dag::NodeId,
        delta: DeltaSource,
        native_arrangement: Option<ArrangementHandle>,
        contract: &NodeOutputContract,
    ) -> Result<Self> {
        validate_delta(
            node_id,
            "emitted",
            &delta,
            contract.emitted_identity,
            &contract.binding_rowids,
        )?;
        validate_arrangement(node_id, native_arrangement.as_ref(), &delta, contract)?;
        Ok(Self {
            delta,
            native_arrangement,
        })
    }

    pub(super) fn into_parts(self) -> (DeltaSource, Option<ArrangementHandle>) {
        (self.delta, self.native_arrangement)
    }
}

/// A validated physical DAG edge.
///
/// The complete planner-derived contract travels with the output, preventing a
/// downstream operator from pairing a stream with a separately reconstructed
/// or incorrectly indexed contract.
#[derive(Debug, Clone)]
pub(super) struct NodeOutput {
    delta: DeltaSource,
    arrangement: Option<ArrangementHandle>,
    contract: NodeOutputContract,
}

impl NodeOutput {
    pub(super) fn new(
        node_id: dag::NodeId,
        delta: DeltaSource,
        arrangement: Option<ArrangementHandle>,
        contract: &NodeOutputContract,
    ) -> Result<Self> {
        validate_delta(
            node_id,
            "published",
            &delta,
            contract.published_identity,
            &contract.binding_rowids,
        )?;
        validate_arrangement(node_id, arrangement.as_ref(), &delta, contract)?;
        Ok(Self {
            delta,
            arrangement,
            contract: contract.clone(),
        })
    }

    pub(super) fn delta(&self) -> &DeltaSource {
        &self.delta
    }

    pub(super) fn arrangement(&self) -> Option<&ArrangementHandle> {
        self.arrangement.as_ref()
    }

    pub(super) fn contract(&self) -> &NodeOutputContract {
        &self.contract
    }
}

fn validate_delta(
    node_id: dag::NodeId,
    phase: &str,
    delta: &DeltaSource,
    expected_identity: DeltaIdentity,
    expected_binding_rowids: &[bool],
) -> Result<()> {
    if delta.identity() != expected_identity {
        return Err(LimboError::InternalError(format!(
            "maintenance DAG node {node_id} {phase} {:?}, but its contract requires {expected_identity:?}",
            delta.identity()
        )));
    }
    if delta.binding_rowids().as_ref() != expected_binding_rowids {
        return Err(LimboError::InternalError(format!(
            "maintenance DAG node {node_id} {phase} the wrong binding-rowid provenance"
        )));
    }
    Ok(())
}

fn validate_arrangement(
    node_id: dag::NodeId,
    arrangement: Option<&ArrangementHandle>,
    delta: &DeltaSource,
    contract: &NodeOutputContract,
) -> Result<()> {
    let Some(arrangement) = arrangement else {
        return Ok(());
    };
    if arrangement.identity() != delta.identity() {
        return Err(LimboError::InternalError(format!(
            "maintenance DAG node {node_id} delta and arrangement expose different identities"
        )));
    }
    if arrangement.value_columns().len() != contract.schema.len() {
        return Err(LimboError::InternalError(format!(
            "maintenance DAG node {node_id} arrangement has the wrong logical width"
        )));
    }
    let arrangement_binding_rowids = arrangement
        .binding_rowid_columns()
        .iter()
        .map(Option::is_some)
        .collect::<Vec<_>>();
    if arrangement_binding_rowids.as_slice() != contract.binding_rowids.as_ref() {
        return Err(LimboError::InternalError(format!(
            "maintenance DAG node {node_id} arrangement exposes the wrong binding-rowid provenance"
        )));
    }
    Ok(())
}
