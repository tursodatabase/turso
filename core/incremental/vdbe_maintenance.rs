//! VDBE bytecode maintenance for materialized views.
//!
//! This is the execution half of the IVM engine rewrite: the DBSP circuit
//! remains the logical description of a view, but all row-level evaluation is
//! compiled to a VDBE program so that view maintenance shares the exact
//! expression, comparison, affinity, and NULL semantics of regular query
//! execution instead of re-implementing them.
//!
//! A maintenance program emits each node of the view's operator DAG once, in
//! topological order. Every node reads its inputs' deltas (rows of identity,
//! values and a signed weight) and writes its own delta to an ephemeral
//! table. The program ends by merging the root's delta into the view's btree,
//! whose records are the row values followed by the row's weight.
//!
//! A delta is an unordered z-set. Before a delta is merged into any state
//! (the view, aggregate state, set-operation counts, or an arrangement), it
//! is consolidated: equal rows are summed and rows whose weights cancel are
//! dropped (see `consolidate_delta`). So no merge depends on the order in which
//! the transaction captured its changes.
//!
//! Programs are built with [`ProgramBuilder::new_for_subprogram`], so they
//! emit no transaction instructions and their `Halt` does not commit: they
//! always run inside an already-open write transaction — at statement
//! completion (or before an in-statement view read), or inside the CREATE
//! MATERIALIZED VIEW statement for initial population.

use rustc_hash::FxHashMap;
use turso_parser::ast;

use crate::incremental::dag;
use crate::schema::Schema;
use crate::sync::Arc;
use crate::translate::emitter::Resolver;
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::{PreparedProgram, Program};
use crate::{Connection, LimboError, QueryMode, Result};

mod stream;

mod consolidate;
use consolidate::consolidate_delta;

mod binding;

mod sink;
use sink::{emit_terminal_delta, ViewSink};

mod output;
use output::NodeOutput;

mod source;
use source::{materialize_node_output, scan_node_output};

mod join;
use join::{emit_inner_join, emit_semi_or_anti_join, JoinContract};

mod aggregate;
use aggregate::emit_group_aggregate;

mod set_op;
use set_op::emit_set_op;

mod plan;
pub(crate) use plan::{plan_view, MaintenancePlan};

mod delta_cursor;
pub use delta_cursor::DeltaCursor;

mod linear;
use linear::{emit_alias, emit_filter, emit_pad, emit_project};

mod members;

mod arrangement;
use arrangement::publish_node_output;

/// What the maintenance program reads as its input relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum MaintenanceInput {
    /// The transaction's captured delta for the view's base table:
    /// rows are (rowid, base column values..., weight).
    TransactionDelta,
    /// A full scan of the base table with an implicit weight of +1 per row.
    /// Used for initial population at CREATE MATERIALIZED VIEW time.
    BaseTable,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MaintenanceProgramCacheKey {
    view_name: String,
    schema_version: u32,
    root_page: i64,
    num_columns: usize,
    input: MaintenanceInput,
}

/// Per-connection prepared maintenance programs, invalidated by the schema
/// cookie and normal prepared-statement context generation.
pub(crate) struct MaintenanceProgramCache {
    entries: crate::sync::RwLock<FxHashMap<MaintenanceProgramCacheKey, Arc<PreparedProgram>>>,
}

impl MaintenanceProgramCache {
    pub(crate) fn new() -> Self {
        Self {
            entries: crate::sync::RwLock::new(FxHashMap::default()),
        }
    }

    fn get(
        &self,
        key: &MaintenanceProgramCacheKey,
        connection: &Connection,
    ) -> Option<Arc<PreparedProgram>> {
        self.entries
            .read()
            .get(key)
            .filter(|program| program.is_compatible_with(connection))
            .cloned()
    }

    fn insert(&self, key: MaintenanceProgramCacheKey, program: Arc<PreparedProgram>) {
        let mut entries = self.entries.write();
        entries.retain(|existing, _| existing.schema_version == key.schema_version);
        entries.insert(key, program);
    }
}

pub(crate) fn compile_maintenance_program(
    view_name: &str,
    select: &ast::Select,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<Program> {
    let key = MaintenanceProgramCacheKey {
        view_name: view_name.to_string(),
        schema_version: schema.schema_version,
        root_page: view_root_page,
        num_columns: num_view_columns,
        input,
    };
    if let Some(prepared) = connection.maintenance_program_cache.get(&key, connection) {
        return Ok(prepared.bind(connection.clone()));
    }
    let plan = build_plan_for_connection(view_name, select, schema, connection)?;
    let program = compile_maintenance_plan(
        view_name,
        view_root_page,
        num_view_columns,
        input,
        &plan,
        schema,
        connection,
    )?;
    connection
        .maintenance_program_cache
        .insert(key, program.prepared().clone());
    Ok(program)
}

fn compile_maintenance_plan(
    view_name: &str,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    plan: &MaintenancePlan,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<Program> {
    let mut program = ProgramBuilder::new_for_subprogram(
        QueryMode::Normal,
        None,
        ProgramBuilderOpts {
            num_cursors: 13,
            approx_num_insns: 128 + 64 * num_view_columns,
            approx_num_labels: 48,
        },
        connection.mvcc_enabled(),
    );
    program.prologue();
    emit_plan_to_sink(
        &mut program,
        view_name,
        plan,
        input,
        &ViewSink {
            root_page: view_root_page,
            num_columns: num_view_columns,
        },
        schema,
        connection,
    )?;
    program.epilogue(schema);
    program.build(
        connection.clone(),
        false,
        "materialized view DAG maintenance",
    )
}

#[cfg(feature = "test_helper")]
impl Connection {
    /// For each column of materialized view `view_name`: the name of the
    /// collation under which maintenance chose the value the column shows
    /// among several values that compare equal, or `None` when the value is
    /// exact.
    ///
    /// SQL leaves that choice open: under NOCASE, a GROUP BY key or a MIN may
    /// show 'a' or 'A', and a fresh query shows whichever its plan meets
    /// first. A view shows the smallest under BINARY instead. A tool that
    /// checks a view against a fresh run of its definition may accept, in
    /// such a column only, any value equal to the expected one under the
    /// named collation.
    pub fn materialized_view_chosen_value_collations(
        self: &Arc<Self>,
        view_name: &str,
    ) -> Result<Vec<Option<String>>> {
        let schema = self.schema.read().clone();
        let view = schema.get_materialized_view(view_name).ok_or_else(|| {
            LimboError::InvalidArgument(format!("no such materialized view: {view_name}"))
        })?;
        let select = view.lock().select_stmt.clone();
        let plan = build_plan_for_connection(view_name, &select, &schema, self)?;
        Ok(plan
            .chosen_value_collations()
            .into_iter()
            .map(|collation| collation.map(|collation| collation.name()))
            .collect())
    }
}

/// Build the bound maintenance plan with a transient connection resolver.
fn build_plan_for_connection(
    view_name: &str,
    select: &ast::Select,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<MaintenancePlan> {
    let syms = connection.syms.read();
    let resolver = Resolver::new(
        schema,
        connection.database_schemas(),
        &connection.temp.database,
        connection.attached_databases(),
        &syms,
        connection.experimental_custom_types_enabled(),
        connection.get_dqs_dml().into(),
        Arc::new(crate::dialect::SqliteDialect),
        &None,
    );
    // Re-planning for maintenance must resolve the view's unqualified table
    // names the same way CREATE did: within the view's own database, never
    // through a TEMP table that happens to share a source table's name
    // (#9456). Materialized views always live in main.
    resolver.with_view_scope(crate::MAIN_DB_ID, view_name.to_string(), || {
        plan_view(view_name, select, &resolver, connection)
    })
}

/// Emit every node once in DAG topological order.
///
/// The output table is keyed by [`dag::NodeId`], making fan-out explicit:
/// downstream nodes consume the already-compiled [`NodeOutput`] of each
/// declared input rather than recursively re-emitting its subtree.
fn emit_plan_nodes(
    program: &mut ProgramBuilder,
    view_name: &str,
    plan: &MaintenancePlan,
    input: MaintenanceInput,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<NodeOutput> {
    let dag = plan.dag();
    let operator_states = plan.operator_states();
    let mut outputs: Vec<Option<NodeOutput>> = vec![None; dag.nodes.len()];
    for node_id in 0..dag.nodes.len() {
        let operator_state = operator_states.for_node(node_id)?;
        let output_contract = &operator_state.output;
        let output = match &dag.nodes[node_id] {
            dag::OpNode::Scan { .. } => scan_node_output(dag, node_id, output_contract, schema)?,
            dag::OpNode::Filter {
                input: upstream,
                predicate,
            } => {
                let upstream_id = *upstream;
                let upstream = outputs[upstream_id].as_ref().ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "filter DAG node {node_id} input {upstream} was not compiled"
                    ))
                })?;
                let upstream =
                    materialize_node_output(program, view_name, upstream_id, upstream, input)?;
                emit_filter(
                    program,
                    view_name,
                    node_id,
                    &upstream,
                    output_contract,
                    predicate,
                    schema,
                    connection,
                )?
            }
            dag::OpNode::Project {
                input: upstream,
                projections,
            } => {
                let upstream_id = *upstream;
                let upstream_output = outputs[upstream_id].as_ref().ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "project DAG node {node_id} input {upstream} was not compiled"
                    ))
                })?;
                let upstream = materialize_node_output(
                    program,
                    view_name,
                    upstream_id,
                    upstream_output,
                    input,
                )?;
                emit_project(
                    program,
                    view_name,
                    node_id,
                    &upstream,
                    projections,
                    output_contract,
                    schema,
                    connection,
                )?
            }
            dag::OpNode::Alias {
                input: upstream, ..
            } => {
                let upstream_id = *upstream;
                let upstream_output = outputs[upstream_id].as_ref().ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "alias DAG node {node_id} input {upstream} was not compiled"
                    ))
                })?;
                let upstream = materialize_node_output(
                    program,
                    view_name,
                    upstream_id,
                    upstream_output,
                    input,
                )?;
                emit_alias(program, view_name, node_id, &upstream, output_contract)?
            }
            dag::OpNode::Join { inputs, on, kind } => {
                let declared_input = |input_id: dag::NodeId| {
                    outputs[input_id].as_ref().ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "join DAG node {node_id} input {input_id} was not compiled"
                        ))
                    })
                };
                let declared_inputs = [declared_input(inputs[0])?, declared_input(inputs[1])?];
                let contract = JoinContract::from_outputs(declared_inputs, on)?;
                match kind {
                    dag::JoinKind::Inner => emit_inner_join(
                        program,
                        view_name,
                        node_id,
                        output_contract,
                        &contract,
                        input,
                        schema,
                        connection,
                    )?,
                    dag::JoinKind::Semi | dag::JoinKind::Anti => emit_semi_or_anti_join(
                        program,
                        view_name,
                        node_id,
                        *kind,
                        &contract,
                        output_contract,
                        input,
                        operator_state,
                        schema,
                        connection,
                    )?,
                }
            }
            dag::OpNode::Pad {
                input: upstream,
                before,
                ..
            } => {
                let upstream_id = *upstream;
                let upstream_output = outputs[upstream_id].as_ref().ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "pad DAG node {node_id} input {upstream} was not compiled"
                    ))
                })?;
                let upstream = materialize_node_output(
                    program,
                    view_name,
                    upstream_id,
                    upstream_output,
                    input,
                )?;
                emit_pad(
                    program,
                    view_name,
                    node_id,
                    &upstream,
                    before,
                    output_contract,
                )?
            }
            dag::OpNode::SetOp {
                inputs: set_inputs,
                operators,
                prefix_len,
                key_columns,
                ..
            } => {
                let mut channels = Vec::with_capacity(set_inputs.len());
                for upstream_id in set_inputs {
                    let upstream = outputs[*upstream_id].as_ref().ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "set-op DAG node {node_id} input {upstream_id} was not compiled"
                        ))
                    })?;
                    channels.push(materialize_node_output(
                        program,
                        view_name,
                        *upstream_id,
                        upstream,
                        input,
                    )?);
                }
                emit_set_op(
                    program,
                    view_name,
                    node_id,
                    &channels,
                    operators,
                    *prefix_len,
                    key_columns,
                    output_contract,
                    operator_state,
                    schema,
                )?
            }
            dag::OpNode::Aggregate {
                input: upstream,
                group_exprs,
                group_keys,
                aggregates,
                multiset_keys,
                scalar,
                ..
            } => {
                let upstream_id = *upstream;
                let upstream = outputs[upstream_id].as_ref().ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "aggregate DAG node {node_id} input {upstream} was not compiled"
                    ))
                })?;
                let upstream =
                    materialize_node_output(program, view_name, upstream_id, upstream, input)?;
                emit_group_aggregate(
                    program,
                    view_name,
                    node_id,
                    &upstream,
                    group_exprs,
                    group_keys,
                    aggregates,
                    multiset_keys,
                    *scalar,
                    input,
                    output_contract,
                    operator_state,
                    schema,
                    connection,
                )?
            }
        };
        let output = publish_node_output(
            program,
            view_name,
            node_id,
            output,
            input,
            operator_state,
            schema,
        )?;
        outputs[node_id] = Some(output);
    }

    outputs[dag.root].take().ok_or_else(|| {
        LimboError::InternalError(format!(
            "maintenance DAG root node {} was not compiled",
            dag.root
        ))
    })
}

fn emit_plan_to_sink(
    program: &mut ProgramBuilder,
    view_name: &str,
    plan: &MaintenancePlan,
    input: MaintenanceInput,
    sink: &ViewSink,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<()> {
    let node_output = emit_plan_nodes(program, view_name, plan, input, schema, connection)?;
    let root = plan.dag().root;
    let stream = materialize_node_output(program, view_name, root, &node_output, input)?;
    let stream = consolidate_delta(program, &format!("{view_name}_sink_input"), &stream)?;
    emit_terminal_delta(program, view_name, &stream, sink)
}
