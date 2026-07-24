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
use crate::schema::{BTreeTable, Schema};
use crate::sync::Arc;
use crate::translate::emitter::Resolver;
use crate::translate::expr::{walk_expr_mut, WalkControl};
use crate::translate::plan::{
    ColumnUsedMask, IterationDirection, JoinedTable, Operation, Scan, TableReferences,
};
use crate::turso_assert;
use crate::vdbe::builder::{CursorType, ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::insn::{CmpInsFlags, InsertFlags, Insn, RegisterOrLiteral};
use crate::vdbe::{PreparedProgram, Program};
use crate::{Connection, LimboError, QueryMode, Result};
use turso_parser::ast::TableInternalId;

mod stream;
use stream::{
    open_ephemeral_delta, synthesized_view_table, ArrangementHandle, DeltaIdentity, EphemeralDelta,
    ViewSink,
};

mod join;
use join::{emit_join_deltas_to_ephemeral, emit_left_join_deltas_to_ephemeral, JoinContract};

mod aggregate;
use aggregate::emit_group_aggregate;

mod set_op;
use set_op::emit_set_op_to_ephemeral;

mod plan;
use plan::NodeOutputContract;
pub use plan::{plan_view, MaintenancePlan, OperatorStateCatalog};

mod delta_cursor;
pub use delta_cursor::DeltaCursor;

mod linear;
use linear::{emit_ephemeral_alias, emit_ephemeral_filter, emit_ephemeral_project};

mod arrangement;
use arrangement::{materialize_declared_output_arrangement, scan_node_output};

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

/// A delta-producing relation consumed by a maintenance operator.
///
/// This is derived exclusively from DAG edges. Emitters must not recover their
/// input by inspecting the original SELECT: doing so makes the DAG descriptive
/// rather than executable and reintroduces per-shape composition.
#[derive(Debug, Clone)]
enum DeltaSource {
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
    fn identity(&self) -> DeltaIdentity {
        match self {
            Self::BaseTable { .. } => DeltaIdentity::BindingRowids(1),
            Self::Ephemeral(channel) => channel.identity,
        }
    }

    fn binding_rowids(&self) -> Arc<[bool]> {
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

    #[allow(clippy::too_many_arguments)]
    fn materialize(
        &self,
        program: &mut ProgramBuilder,
        view_name: &str,
        source_node: dag::NodeId,
        source_contract: &NodeOutputContract,
        input: MaintenanceInput,
    ) -> Result<EphemeralDelta> {
        let Self::BaseTable {
            table,
            stored_weight_column,
            ..
        } = self
        else {
            let Self::Ephemeral(channel) = self else {
                unreachable!()
            };
            return Ok(channel.clone());
        };
        let channel = open_ephemeral_delta(
            program,
            &format!("{view_name}_scan_delta_{source_node}"),
            source_contract.schema.as_ref().clone(),
            source_contract.emitted_identity,
            source_contract.binding_rowids.clone(),
            false,
        );
        emit_base_scan_delta(
            program,
            view_name,
            table,
            *stored_weight_column,
            input,
            &channel,
        )?;
        Ok(channel)
    }
}

/// The physical result of compiling one DAG node.
///
/// `delta` is the node's transaction change stream. `arrangement` is the
/// maintained integral downstream stateful operators may probe. Linear nodes
/// deliberately have no arrangement: producing one requires an explicit
/// materialization operator rather than silently reopening an ancestor table.
#[derive(Debug, Clone)]
struct NodeOutput {
    delta: DeltaSource,
    arrangement: Option<ArrangementHandle>,
}

/// A [`JoinedTable`] scan entry for synthesized maintenance-program bindings.
/// Expressions are already planner-bound, so no SQL name resolution or
/// USING/NATURAL metadata is reconstructed here.
fn make_joined_table(
    table: &Arc<BTreeTable>,
    identifier: &str,
    id: TableInternalId,
) -> JoinedTable {
    JoinedTable {
        op: Operation::Scan(Scan::BTreeTable {
            iter_dir: IterationDirection::Forwards,
            index: None,
        }),
        table: crate::schema::Table::BTree(table.clone()),
        identifier: identifier.to_string(),
        internal_id: id,
        join_info: None,
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        database_id: 0,
        indexed: None,
    }
}

type BindingRemap = FxHashMap<TableInternalId, TableInternalId>;

fn stream_table_references(
    program: &mut ProgramBuilder,
    schema: &dag::StreamSchema,
) -> (TableReferences, BindingRemap) {
    let mut remap = FxHashMap::default();
    let tables = schema
        .bindings
        .iter()
        .map(|binding| {
            let phase_id = program.table_reference_counter.next();
            let previous = remap.insert(binding.logical_id, phase_id);
            turso_assert!(
                previous.is_none(),
                "a stream schema must expose each logical binding exactly once"
            );
            make_joined_table(&binding.table, &binding.identifier, phase_id)
        })
        .collect();
    (TableReferences::new(tables, vec![]), remap)
}

/// Retarget a planner-bound expression to one emitter phase's cursor
/// bindings. This is a mechanical id substitution: identifier spelling,
/// DQS, aliases, and rowid-name resolution never run a second time.
fn remap_bound_expr(expr: &ast::Expr, remap: &BindingRemap) -> Result<ast::Expr> {
    let mut bound = expr.clone();
    walk_expr_mut(&mut bound, &mut |node| {
        let logical_id = match node {
            ast::Expr::Column { table, .. } | ast::Expr::RowId { table, .. } => Some(table),
            ast::Expr::Id(_) | ast::Expr::Qualified(_, _) | ast::Expr::DoublyQualified(_, _, _) => {
                return Err(LimboError::InternalError(
                    "maintenance DAG contains an unresolved identifier".to_string(),
                ));
            }
            _ => None,
        };
        if let Some(logical_id) = logical_id {
            *logical_id = *remap.get(logical_id).ok_or_else(|| {
                LimboError::InternalError(
                    "maintenance expression references a binding outside its declared input"
                        .to_string(),
                )
            })?;
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(bound)
}

fn seed_ephemeral_stream_cache<'a>(
    program: &mut ProgramBuilder,
    channel: &EphemeralDelta,
    remap: &BindingRemap,
    resolver: &mut Resolver<'a>,
) -> Result<()> {
    resolver.enable_expr_to_reg_cache();
    for (column, stream_column) in channel.schema.columns.iter().enumerate() {
        let Some(expr) = &stream_column.expr else {
            continue;
        };
        let value_reg = program.alloc_register();
        program.emit_insn(Insn::Column {
            cursor_id: channel.cursor_id,
            column: channel.value_start + column,
            dest: value_reg,
            default: None,
        });
        let bound = remap_bound_expr(expr, remap)?;
        resolver.cache_expr_reg(std::borrow::Cow::Owned(bound), value_reg, false, None);
    }
    turso_assert!(
        channel.binding_rowid_columns.len() == channel.schema.bindings.len(),
        "rowid provenance must have one entry per stream binding",
        {
            "provenance_count": channel.binding_rowid_columns.len(),
            "binding_count": channel.schema.bindings.len()
        }
    );
    for (binding_rowid_column, binding) in channel
        .binding_rowid_columns
        .iter()
        .zip(channel.schema.bindings.iter())
    {
        let Some(binding_rowid_column) = binding_rowid_column else {
            continue;
        };
        let value_reg = program.alloc_register();
        program.emit_insn(Insn::Column {
            cursor_id: channel.cursor_id,
            column: *binding_rowid_column,
            dest: value_reg,
            default: None,
        });
        let phase_id = *remap.get(&binding.logical_id).ok_or_else(|| {
            LimboError::InternalError(
                "rowid stream identity has no phase-local binding".to_string(),
            )
        })?;
        let expr = ast::Expr::RowId {
            database: None,
            table: phase_id,
        };
        resolver.cache_expr_reg(std::borrow::Cow::Owned(expr), value_reg, false, None);
    }
    Ok(())
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
        let output_contract = operator_states.output_for_node(node_id)?;
        let output = match &dag.nodes[node_id] {
            dag::OpNode::Scan { .. } => scan_node_output(dag, node_id, schema)?,
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
                let upstream = upstream.delta.materialize(
                    program,
                    view_name,
                    upstream_id,
                    operator_states.output_for_node(upstream_id)?,
                    input,
                )?;
                NodeOutput {
                    delta: emit_ephemeral_filter(
                        program,
                        view_name,
                        node_id,
                        &upstream,
                        output_contract,
                        predicate,
                        schema,
                        connection,
                    )?,
                    arrangement: None,
                }
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
                let upstream = upstream_output.delta.materialize(
                    program,
                    view_name,
                    upstream_id,
                    operator_states.output_for_node(upstream_id)?,
                    input,
                )?;
                NodeOutput {
                    delta: emit_ephemeral_project(
                        program,
                        view_name,
                        node_id,
                        &upstream,
                        projections,
                        output_contract,
                        schema,
                        connection,
                    )?,
                    arrangement: None,
                }
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
                let upstream = upstream_output.delta.materialize(
                    program,
                    view_name,
                    upstream_id,
                    operator_states.output_for_node(upstream_id)?,
                    input,
                )?;
                NodeOutput {
                    delta: emit_ephemeral_alias(
                        program,
                        view_name,
                        node_id,
                        &upstream,
                        output_contract,
                    )?,
                    arrangement: None,
                }
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
                let input_schemas = [
                    operator_states.output_for_node(inputs[0])?.schema.clone(),
                    operator_states.output_for_node(inputs[1])?.schema.clone(),
                ];
                let contract = JoinContract::from_outputs(declared_inputs, input_schemas, on)?;
                if *kind == dag::JoinKind::LeftOuter {
                    NodeOutput {
                        delta: emit_left_join_deltas_to_ephemeral(
                            program,
                            view_name,
                            node_id,
                            &contract,
                            output_contract,
                            input,
                            operator_states.for_node(node_id)?,
                            schema,
                            connection,
                        )?,
                        arrangement: None,
                    }
                } else {
                    let syms = connection.syms.read();
                    let mut resolver = Resolver::new(
                        schema,
                        connection.database_schemas(),
                        &connection.temp.database,
                        connection.attached_databases(),
                        &syms,
                        connection.experimental_custom_types_enabled(),
                        connection.get_dqs_dml().into(),
                        Arc::new(crate::dialect::SqliteDialect),
                    );
                    NodeOutput {
                        delta: emit_join_deltas_to_ephemeral(
                            program,
                            &mut resolver,
                            view_name,
                            output_contract,
                            &contract,
                            input,
                        )?,
                        arrangement: None,
                    }
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
                    channels.push(upstream.delta.materialize(
                        program,
                        view_name,
                        *upstream_id,
                        operator_states.output_for_node(*upstream_id)?,
                        input,
                    )?);
                }
                NodeOutput {
                    delta: emit_set_op_to_ephemeral(
                        program,
                        view_name,
                        node_id,
                        &channels,
                        operators,
                        *prefix_len,
                        key_collations,
                        output_contract,
                        operator_states.for_node(node_id)?,
                        schema,
                    )?,
                    arrangement: None,
                }
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
                let upstream = upstream.delta.materialize(
                    program,
                    view_name,
                    upstream_id,
                    operator_states.output_for_node(upstream_id)?,
                    input,
                )?;
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
                    operator_states.for_node(node_id)?,
                    schema,
                    connection,
                )?
            }
        };
        let operator_state = operator_states.for_node(node_id)?;
        if output.delta.identity() != operator_state.output.emitted_identity {
            return Err(LimboError::InternalError(format!(
                "maintenance DAG node {node_id} emitted {:?}, but its declared identity is {:?}",
                output.delta.identity(),
                operator_state.output.emitted_identity
            )));
        }
        if output.delta.binding_rowids().as_ref() != operator_state.output.binding_rowids.as_ref() {
            return Err(LimboError::InternalError(format!(
                "maintenance DAG node {node_id} emitted the wrong binding-rowid provenance"
            )));
        }
        let output = materialize_declared_output_arrangement(
            program,
            view_name,
            node_id,
            output,
            input,
            operator_state,
            schema,
        )?;
        if output.delta.identity() != operator_state.output.published_identity {
            return Err(LimboError::InternalError(format!(
                "maintenance DAG node {node_id} published {:?}, but its edge contract is {:?}",
                output.delta.identity(),
                operator_state.output.published_identity
            )));
        }
        if output.delta.binding_rowids().as_ref() != operator_state.output.binding_rowids.as_ref() {
            return Err(LimboError::InternalError(format!(
                "maintenance DAG node {node_id} published the wrong binding-rowid provenance"
            )));
        }
        if let Some(arrangement) = &output.arrangement {
            let arrangement_binding_rowids = arrangement
                .binding_rowid_columns()
                .iter()
                .map(Option::is_some)
                .collect::<Vec<_>>();
            if arrangement_binding_rowids.as_slice()
                != operator_state.output.binding_rowids.as_ref()
            {
                return Err(LimboError::InternalError(format!(
                    "maintenance DAG node {node_id} arrangement exposes the wrong binding-rowid provenance"
                )));
            }
        }
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
    let stream = node_output.delta.materialize(
        program,
        view_name,
        root,
        operator_states.output_for_node(root)?,
        input,
    )?;
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

// Emit a Scan node into its declared ephemeral delta channel.
fn emit_base_scan_delta(
    program: &mut ProgramBuilder,
    view_name: &str,
    base_table: &Arc<BTreeTable>,
    stored_weight_column: Option<usize>,
    input: MaintenanceInput,
    output: &EphemeralDelta,
) -> Result<()> {
    turso_assert!(
        output.identity == DeltaIdentity::BindingRowids(1)
            && output.binding_rowid_columns.as_ref() == [Some(0)]
            && output.value_start == 1
            && output.width == base_table.columns().len()
            && output.weight_column == output.width + 1,
        "scan delta channel must contain rowid, base columns, and weight"
    );
    let input_cursor_id = match input {
        MaintenanceInput::TransactionDelta => {
            let cursor_id = program.alloc_cursor_id(CursorType::ViewDelta {
                view_name: view_name.to_string(),
                table: base_table.clone(),
            });
            program.emit_insn(Insn::OpenRead {
                cursor_id,
                root_page: 0,
                db: 0,
            });
            cursor_id
        }
        MaintenanceInput::BaseTable => {
            let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(base_table.clone()));
            program.emit_insn(Insn::OpenRead {
                cursor_id,
                root_page: base_table.root_page,
                db: 0,
            });
            cursor_id
        }
    };

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: input_cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);

    let record_start = program.alloc_registers(output.record_width());
    let weight_reg = record_start + output.weight_column;
    program.emit_insn(Insn::RowId {
        cursor_id: input_cursor_id,
        dest: record_start,
    });
    for column in 0..output.width {
        program.emit_column_or_rowid(
            input_cursor_id,
            column,
            record_start + output.value_start + column,
        );
    }
    match input {
        MaintenanceInput::TransactionDelta => {
            program.emit_insn(Insn::Column {
                cursor_id: input_cursor_id,
                column: base_table.columns().len(),
                dest: weight_reg,
                default: None,
            });
        }
        MaintenanceInput::BaseTable => {
            if let Some(column) = stored_weight_column {
                program.emit_insn(Insn::Column {
                    cursor_id: input_cursor_id,
                    column,
                    dest: weight_reg,
                    default: None,
                });
            } else {
                program.emit_int(1, weight_reg);
            }
        }
    }

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: record_start as u16,
        count: output.record_width() as u16,
        dest_reg: record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    let output_rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: output.cursor_id,
        rowid_reg: output_rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: output.cursor_id,
        key_reg: output_rowid_reg,
        record_reg,
        flag: InsertFlags::new().is_ephemeral_table_insert(),
        table_name: String::new(),
    });
    program.emit_insn(Insn::Next {
        cursor_id: input_cursor_id,
        pc_if_next: loop_label,
    });
    program.preassign_label_to_next_insn(end_label);
    Ok(())
}
/// Consume a fully evaluated one-identity delta stream at the maintenance
/// program boundary. Operator code owns expression evaluation; this adapter
/// only binds the root's standard `(identity, values, weight)` contract to a
/// persistent view.
fn emit_terminal_delta(
    program: &mut ProgramBuilder,
    view_name: &str,
    input: &EphemeralDelta,
    sink: &ViewSink,
) -> Result<()> {
    turso_assert!(
        matches!(
            input.identity,
            DeltaIdentity::BindingRowids(1) | DeltaIdentity::OperatorRowid
        ) && input.weight_column == input.value_start + input.width,
        "terminal delta streams require one stable operator identity"
    );
    let num_output_columns = sink.num_columns;
    turso_assert!(
        input.width == num_output_columns,
        "terminal delta width does not match its sink"
    );

    let table = Arc::new(synthesized_view_table(
        view_name,
        sink.root_page,
        num_output_columns,
    ));
    let view_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: view_cursor_id,
        root_page: RegisterOrLiteral::Literal(sink.root_page),
        db: 0,
    });

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let next_label = program.allocate_label();
    let row_start = program.alloc_registers(1 + num_output_columns + 1);
    let identity_reg = row_start;
    let values_start = row_start + 1;
    let weight_reg = values_start + num_output_columns;

    // Join phase decomposition can retract a cross-generation identity before
    // inserting it. Apply positive contributions first at the persistent
    // boundary so an unknown negative is never discarded before its matching
    // positive arrives.
    let pass_reg = input.requires_positive_first.then(|| {
        let pass_reg = program.alloc_register();
        program.emit_int(0, pass_reg);
        pass_reg
    });
    let pass_start_label = program.allocate_label();
    program.preassign_label_to_next_insn(pass_start_label);
    let pass_done_label = if pass_reg.is_some() {
        program.allocate_label()
    } else {
        end_label
    };
    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: pass_done_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: 0,
        dest: identity_reg,
        default: None,
    });
    for column in 0..num_output_columns {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column: input.value_start + column,
            dest: values_start + column,
            default: None,
        });
    }
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: input.weight_column,
        dest: weight_reg,
        default: None,
    });
    if let Some(pass_reg) = pass_reg {
        let negative_pass_label = program.allocate_label();
        let pass_ok_label = program.allocate_label();
        let zero_reg = program.alloc_register();
        program.emit_int(0, zero_reg);
        program.emit_insn(Insn::If {
            reg: pass_reg,
            target_pc: negative_pass_label,
            jump_if_null: false,
        });
        program.emit_insn(Insn::Lt {
            lhs: weight_reg,
            rhs: zero_reg,
            target_pc: next_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::Goto {
            target_pc: pass_ok_label,
        });
        program.preassign_label_to_next_insn(negative_pass_label);
        program.emit_insn(Insn::Gt {
            lhs: weight_reg,
            rhs: zero_reg,
            target_pc: next_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.preassign_label_to_next_insn(pass_ok_label);
    }

    {
        let not_found_label = program.allocate_label();
        let merge_label = program.allocate_label();
        let write_label = program.allocate_label();
        let delete_label = program.allocate_label();
        let current_weight_reg = program.alloc_register();
        let new_weight_reg = program.alloc_register();
        let found_reg = program.alloc_register();
        let zero_reg = program.alloc_register();
        program.emit_int(0, zero_reg);

        let view_key_reg = identity_reg;
        program.emit_insn(Insn::SeekRowid {
            cursor_id: view_cursor_id,
            src_reg: view_key_reg,
            target_pc: not_found_label,
        });
        program.emit_insn(Insn::Column {
            cursor_id: view_cursor_id,
            column: num_output_columns,
            dest: current_weight_reg,
            default: None,
        });
        program.emit_int(1, found_reg);
        program.emit_insn(Insn::Goto {
            target_pc: merge_label,
        });
        program.preassign_label_to_next_insn(not_found_label);
        program.emit_int(0, current_weight_reg);
        program.emit_int(0, found_reg);
        program.preassign_label_to_next_insn(merge_label);
        program.emit_insn(Insn::Add {
            lhs: weight_reg,
            rhs: current_weight_reg,
            dest: new_weight_reg,
        });
        program.emit_insn(Insn::Gt {
            lhs: new_weight_reg,
            rhs: zero_reg,
            target_pc: write_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::If {
            reg: found_reg,
            target_pc: delete_label,
            jump_if_null: false,
        });
        program.emit_insn(Insn::Goto {
            target_pc: next_label,
        });
        program.preassign_label_to_next_insn(delete_label);
        program.emit_insn(Insn::Delete {
            cursor_id: view_cursor_id,
            table_name: view_name.to_string(),
            is_part_of_update: true,
        });
        program.emit_insn(Insn::Goto {
            target_pc: next_label,
        });

        program.preassign_label_to_next_insn(write_label);
        // On the negative pass, retain the newer image installed by a
        // positive contribution instead of overwriting it with the
        // retracted old image.
        let values_ready_label = program.allocate_label();
        program.emit_insn(Insn::Gt {
            lhs: weight_reg,
            rhs: zero_reg,
            target_pc: values_ready_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        for column in 0..num_output_columns {
            program.emit_insn(Insn::Column {
                cursor_id: view_cursor_id,
                column,
                dest: values_start + column,
                default: None,
            });
        }
        program.preassign_label_to_next_insn(values_ready_label);
        program.emit_insn(Insn::Copy {
            src_reg: new_weight_reg,
            dst_reg: weight_reg,
            extra_amount: 0,
        });
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: values_start as u16,
            count: (num_output_columns + 1) as u16,
            dest_reg: record_reg as u16,
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: view_cursor_id,
            key_reg: view_key_reg,
            record_reg,
            flag: InsertFlags(
                InsertFlags::REQUIRE_SEEK
                    | InsertFlags::SKIP_LAST_ROWID
                    | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
            ),
            table_name: view_name.to_string(),
        });
    }

    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: input.cursor_id,
        pc_if_next: loop_label,
    });
    if let Some(pass_reg) = pass_reg {
        program.preassign_label_to_next_insn(pass_done_label);
        program.emit_insn(Insn::If {
            reg: pass_reg,
            target_pc: end_label,
            jump_if_null: false,
        });
        program.emit_int(1, pass_reg);
        program.emit_insn(Insn::Goto {
            target_pc: pass_start_label,
        });
    }
    program.preassign_label_to_next_insn(end_label);
    Ok(())
}
