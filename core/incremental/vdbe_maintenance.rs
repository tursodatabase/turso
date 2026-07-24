//! VDBE bytecode maintenance for materialized views.
//!
//! This is the execution half of the IVM engine rewrite: the DBSP circuit
//! remains the logical description of a view, but all row-level evaluation is
//! compiled to a VDBE program so that view maintenance shares the exact
//! expression, comparison, affinity, and NULL semantics of regular query
//! execution instead of re-implementing them.
//!
//! A maintenance program has the shape:
//!
//! ```text
//! OpenRead   c_in    (transaction delta, or the base table for population)
//! OpenWrite  c_view  (the view's btree; records are row values + weight)
//! Rewind c_in -> end
//! loop:
//!   w      := delta weight (Column c_in, n) or the literal +1 for population
//!   if NOT WHERE(row)  -> next     (three-valued logic via translate_condition_expr)
//!   rid    := RowId c_in
//!   out[i] := projection expressions (translate_expr)
//!   merge (rid, out, w) into the view btree:
//!     found:      new_w = cur_w + w
//!     not found:  new_w = w
//!     new_w > 0  -> Insert record(out.., new_w) (upsert at rid)
//!     new_w <= 0 -> Delete, when the row existed
//! next: Next c_in -> loop
//! end:  Halt
//! ```
//!
//! Programs are built with [`ProgramBuilder::new_for_subprogram`], so they
//! emit no transaction instructions and their `Halt` does not commit: they
//! always run inside an already-open write transaction — at statement
//! completion (or before an in-statement view read), or inside the CREATE
//! MATERIALIZED VIEW statement for initial population.
//!
//! Delta rows must be applied in capture order: an UPDATE is captured as
//! delete(old image) followed by insert(new image) under the same rowid, and
//! the rowid-keyed weight merge is only correct when the deletion lands
//! first. The [`DeltaCursor`] therefore iterates the captured `Delta`
//! verbatim, without consolidation.

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

mod binding;

mod sink;
use sink::{emit_terminal_delta, ViewSink};

mod output;
use output::NodeOutput;

mod source;
use source::{materialize_node_output, scan_node_output};

mod join;
use join::{emit_inner_join, emit_left_join, JoinContract};

mod aggregate;
use aggregate::emit_group_aggregate;

mod set_op;
use set_op::emit_set_op;

mod plan;
pub use plan::{plan_view, MaintenancePlan, OperatorStateCatalog};

mod delta_cursor;
pub use delta_cursor::DeltaCursor;

mod linear;
use linear::{emit_alias, emit_filter, emit_project};

mod arrangement;
use arrangement::publish_node_output;

/// What the maintenance program reads as its input relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MaintenanceInput {
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

#[allow(clippy::too_many_arguments)]
pub fn compile_maintenance_program(
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
    let program = compile_maintenance_program_uncached(
        view_name,
        select,
        view_root_page,
        num_view_columns,
        input,
        schema,
        connection,
    )?;
    connection
        .maintenance_program_cache
        .insert(key, program.prepared().clone());
    Ok(program)
}

#[allow(clippy::too_many_arguments)]
fn compile_maintenance_program_uncached(
    view_name: &str,
    select: &ast::Select,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<Program> {
    let plan = build_plan_for_connection(view_name, select, schema, connection)?;
    compile_maintenance_plan(
        view_name,
        view_root_page,
        num_view_columns,
        input,
        &plan,
        schema,
        connection,
    )
}

#[allow(clippy::too_many_arguments)]
fn compile_maintenance_plan(
    view_name: &str,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    plan: &MaintenancePlan,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<Program> {
    compile_generic_dag_program(
        view_name,
        &plan.dag,
        plan.dag.root,
        view_root_page,
        num_view_columns,
        input,
        &plan.operator_states,
        schema,
        connection,
    )
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
    );
    plan_view(view_name, select, &resolver, connection)
}

/// Emit every ancestor needed by `target` once, in DAG topological order.
///
/// The output table is keyed by [`dag::NodeId`], making fan-out explicit:
/// downstream nodes consume the already-compiled [`NodeOutput`] of each
/// declared input rather than recursively re-emitting its subtree.
#[allow(clippy::too_many_arguments)]
fn emit_declared_delta_nodes_to(
    program: &mut ProgramBuilder,
    view_name: &str,
    dag: &dag::MaintenanceDag,
    target: dag::NodeId,
    input: MaintenanceInput,
    operator_states: &OperatorStateCatalog,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<NodeOutput> {
    let mut required = vec![false; dag.nodes.len()];
    let mut pending = vec![target];
    while let Some(node_id) = pending.pop() {
        if std::mem::replace(&mut required[node_id], true) {
            continue;
        }
        pending.extend_from_slice(dag.nodes[node_id].inputs());
    }

    let mut outputs: Vec<Option<NodeOutput>> = vec![None; dag.nodes.len()];
    for node_id in 0..=target {
        if !required[node_id] {
            continue;
        }
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
                if *kind == dag::JoinKind::LeftOuter {
                    emit_left_join(
                        program,
                        view_name,
                        node_id,
                        &contract,
                        output_contract,
                        input,
                        operator_state,
                        schema,
                        connection,
                    )?
                } else {
                    emit_inner_join(
                        program,
                        view_name,
                        node_id,
                        output_contract,
                        &contract,
                        input,
                        schema,
                        connection,
                    )?
                }
            }
            dag::OpNode::SetOp {
                inputs: set_inputs,
                operators,
                prefix_len,
                key_collations,
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
                    key_collations,
                    output_contract,
                    operator_state,
                    schema,
                )?
            }
            dag::OpNode::Aggregate {
                input: upstream,
                group_exprs,
                group_collations,
                aggregates,
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
                    group_collations,
                    aggregates,
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

    outputs[target].take().ok_or_else(|| {
        LimboError::InternalError(format!(
            "maintenance DAG target node {target} was not compiled"
        ))
    })
}

#[allow(clippy::too_many_arguments)]
fn emit_generic_dag_to_sink(
    program: &mut ProgramBuilder,
    view_name: &str,
    dag: &dag::MaintenanceDag,
    root: dag::NodeId,
    input: MaintenanceInput,
    sink: &ViewSink,
    operator_states: &OperatorStateCatalog,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<()> {
    let node_output = emit_declared_delta_nodes_to(
        program,
        view_name,
        dag,
        root,
        input,
        operator_states,
        schema,
        connection,
    )?;
    let stream = materialize_node_output(program, view_name, root, &node_output, input)?;
    emit_terminal_delta(program, view_name, &stream, sink)
}

#[allow(clippy::too_many_arguments)]
fn compile_generic_dag_program(
    view_name: &str,
    dag: &dag::MaintenanceDag,
    root: dag::NodeId,
    view_root_page: i64,
    num_view_columns: usize,
    input: MaintenanceInput,
    operator_states: &OperatorStateCatalog,
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
    );
    program.prologue();
    emit_generic_dag_to_sink(
        &mut program,
        view_name,
        dag,
        root,
        input,
        &ViewSink {
            root_page: view_root_page,
            num_columns: num_view_columns,
        },
        operator_states,
        schema,
        connection,
    )?;
    program.epilogue(schema);
    program.build(
        connection.clone(),
        false,
        "materialized view generic DAG maintenance",
    )
}
