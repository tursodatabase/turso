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
use crate::translate::expr::{
    translate_condition_expr, translate_expr_no_constant_opt, walk_expr_mut, ConditionMetadata,
    NoConstantOptReason, WalkControl,
};
use crate::translate::plan::{
    ColumnUsedMask, IterationDirection, JoinedTable, Operation, Scan, TableReferences,
};
use crate::turso_assert;
use crate::vdbe::builder::{CursorType, ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::insn::{CmpInsFlags, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral};
use crate::vdbe::{PreparedProgram, Program};
use crate::{Connection, LimboError, QueryMode, Result};
use turso_parser::ast::TableInternalId;

mod stream;
use stream::{
    base_arrangement, btree_arrangement, open_ephemeral_delta, synthesized_view_table,
    ArrangementHandle, ArrangementIdentityColumn, DeltaIdentity, EphemeralDelta, ViewSink,
};

mod join;
use join::{emit_join_deltas_to_ephemeral, emit_left_join_deltas_to_ephemeral, JoinContract};

mod aggregate;
use aggregate::emit_group_aggregate;

mod set_op;
use set_op::emit_set_op_to_ephemeral;

mod plan;
pub use plan::{plan_view, MaintenancePlan, OperatorStateCatalog};
use plan::{NodeOutputContract, OperatorStateDef};

mod delta_cursor;
pub use delta_cursor::DeltaCursor;

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

fn scan_node_output(
    dag: &dag::MaintenanceDag,
    node_id: dag::NodeId,
    schema: &Schema,
) -> Result<NodeOutput> {
    let dag::OpNode::Scan { table, .. } = &dag.nodes[node_id] else {
        return Err(LimboError::InternalError(format!(
            "maintenance DAG node {node_id} is not a scan"
        )));
    };
    let stored_weight_column = schema
        .is_materialized_view(&table.name)
        .then(|| table.columns().len());
    Ok(NodeOutput {
        delta: DeltaSource::BaseTable {
            table: table.clone(),
            stored_weight_column,
        },
        arrangement: Some(base_arrangement(table.clone(), stored_weight_column)),
    })
}

/// Integrate one node's declared delta into its persistent output arrangement.
///
/// Interior arrangements preserve the producer identity so later binary joins
/// can compose binding-rowid tuples without losing provenance. A terminal
/// arrangement publishes its own rowid to satisfy the materialized-view sink's
/// one-rowid contract.
#[allow(clippy::too_many_arguments)]
fn emit_output_arrangement(
    program: &mut ProgramBuilder,
    view_name: &str,
    input: &EphemeralDelta,
    output: &EphemeralDelta,
    arrangement_table_name: &str,
    schema: &Schema,
) -> Result<ArrangementHandle> {
    let identity_width = input.identity_width();
    if identity_width == 0 {
        return Err(LimboError::InternalError(
            "an arranged delta must carry a stable source identity".to_string(),
        ));
    }
    let extra_binding_rowid_width = input.value_start - identity_width;
    turso_assert!(
        (output.identity == input.identity || output.identity == DeltaIdentity::OperatorRowid)
            && output.width == input.width
            && output.binding_rowid_columns.len() == input.binding_rowid_columns.len()
            && output
                .binding_rowid_columns
                .iter()
                .map(Option::is_some)
                .eq(input.binding_rowid_columns.iter().map(Option::is_some))
            && input.weight_column == input.value_start + input.width
            && output.weight_column == output.value_start + output.width,
        "output arrangement must publish its planned identity, values, and weight"
    );

    let table = schema
        .get_btree_table(arrangement_table_name)
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "output arrangement {arrangement_table_name} of materialized view {view_name} not found"
            ))
        })?;
    let index = schema
        .get_indices(arrangement_table_name)
        .next()
        .cloned()
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "output arrangement {arrangement_table_name} of materialized view {view_name} has no index"
            ))
        })?;
    let expected_columns = identity_width + input.width + extra_binding_rowid_width + 1;
    if table.columns().len() != expected_columns {
        return Err(LimboError::InternalError(format!(
            "output arrangement {arrangement_table_name} has {} columns, expected {expected_columns}",
            table.columns().len()
        )));
    }

    let table_cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: table_cursor,
        root_page: RegisterOrLiteral::Literal(table.root_page),
        db: 0,
    });
    let index_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: index_cursor,
        root_page: RegisterOrLiteral::Literal(index.root_page),
        db: 0,
    });

    // The relational arrangement key is `(source identity, full row value)`.
    // A stable source identity can change values during an update, and the
    // old/new images must integrate independently.
    let key_width = identity_width + input.width;
    let lookup_start = program.alloc_registers(key_width + 1);
    let input_values_start = lookup_start + identity_width;
    let arrangement_rowid_reg = lookup_start + key_width;
    let contribution_reg = program.alloc_register();
    let current_mult_reg = program.alloc_register();
    let new_mult_reg = program.alloc_register();
    let zero_reg = program.alloc_register();
    let one_reg = program.alloc_register();
    let is_new_reg = program.alloc_register();
    let previous_rowid_reg = program.alloc_register();
    // [source identity..., persisted values..., binding rowids..., multiplicity].
    let state_record_start = program.alloc_registers(expected_columns);
    let state_binding_rowids_start = state_record_start + key_width;
    let state_mult_reg = state_binding_rowids_start + extra_binding_rowid_width;
    let state_record_reg = program.alloc_register();
    let index_record_reg = program.alloc_register();
    // [published identity..., binding rowids..., delta values..., delta weight].
    let output_record_start = program.alloc_registers(output.record_width());
    let output_values_start = output_record_start + output.value_start;
    let output_weight_reg = output_record_start + output.weight_column;
    let output_record_reg = program.alloc_register();
    let output_rowid_reg = program.alloc_register();

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let next_label = program.allocate_label();
    let found_label = program.allocate_label();
    let update_label = program.allocate_label();
    let write_state_label = program.allocate_label();
    let emit_label = program.allocate_label();
    let corrupt_label = program.allocate_label();

    program.emit_int(0, zero_reg);
    program.emit_int(1, one_reg);
    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    for column in 0..identity_width {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: lookup_start + column,
            default: None,
        });
    }
    for column in 0..input.width {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column: input.value_start + column,
            dest: input_values_start + column,
            default: None,
        });
    }
    for column in identity_width..input.value_start {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: state_binding_rowids_start + column - identity_width,
            default: None,
        });
    }
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: input.weight_column,
        dest: contribution_reg,
        default: None,
    });
    program.emit_insn(Insn::Eq {
        lhs: contribution_reg,
        rhs: zero_reg,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });

    program.emit_insn(Insn::Found {
        cursor_id: index_cursor,
        target_pc: found_label,
        record_reg: lookup_start,
        num_regs: key_width,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: table_cursor,
        rowid_reg: arrangement_rowid_reg,
        prev_largest_reg: previous_rowid_reg,
    });
    program.emit_insn(Insn::Copy {
        src_reg: contribution_reg,
        dst_reg: new_mult_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Copy {
        src_reg: one_reg,
        dst_reg: is_new_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Copy {
        src_reg: lookup_start,
        dst_reg: state_record_start,
        extra_amount: key_width - 1,
    });
    program.emit_insn(Insn::Goto {
        target_pc: write_state_label,
    });

    program.preassign_label_to_next_insn(found_label);
    program.emit_insn(Insn::Copy {
        src_reg: zero_reg,
        dst_reg: is_new_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::IdxRowId {
        cursor_id: index_cursor,
        dest: arrangement_rowid_reg,
    });
    program.emit_insn(Insn::SeekRowid {
        cursor_id: table_cursor,
        src_reg: arrangement_rowid_reg,
        target_pc: corrupt_label,
    });
    program.emit_insn(Insn::Column {
        cursor_id: table_cursor,
        column: key_width + extra_binding_rowid_width,
        dest: current_mult_reg,
        default: None,
    });
    program.emit_insn(Insn::Add {
        lhs: contribution_reg,
        rhs: current_mult_reg,
        dest: new_mult_reg,
    });
    program.emit_insn(Insn::Ne {
        lhs: new_mult_reg,
        rhs: zero_reg,
        target_pc: update_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::IdxDelete {
        start_reg: lookup_start,
        num_regs: key_width + 1,
        cursor_id: index_cursor,
        raise_error_if_no_matching_entry: true,
    });
    program.emit_insn(Insn::Delete {
        cursor_id: table_cursor,
        table_name: arrangement_table_name.to_string(),
        is_part_of_update: true,
    });
    program.emit_insn(Insn::Goto {
        target_pc: emit_label,
    });

    program.preassign_label_to_next_insn(update_label);
    program.emit_insn(Insn::Copy {
        src_reg: lookup_start,
        dst_reg: state_record_start,
        extra_amount: key_width - 1,
    });

    program.preassign_label_to_next_insn(write_state_label);
    program.emit_insn(Insn::Copy {
        src_reg: new_mult_reg,
        dst_reg: state_mult_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: state_record_start as u16,
        count: expected_columns as u16,
        dest_reg: state_record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: table_cursor,
        key_reg: arrangement_rowid_reg,
        record_reg: state_record_reg,
        flag: InsertFlags(
            InsertFlags::REQUIRE_SEEK
                | InsertFlags::SKIP_LAST_ROWID
                | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
        ),
        table_name: arrangement_table_name.to_string(),
    });
    // New rows need a primary-key index entry; replacing an existing table
    // row leaves its identity and index entry unchanged.
    program.emit_insn(Insn::Eq {
        lhs: is_new_reg,
        rhs: zero_reg,
        target_pc: emit_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: lookup_start as u16,
        count: (key_width + 1) as u16,
        dest_reg: index_record_reg as u16,
        index_name: Some(index.name.clone()),
        affinity_str: None,
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id: index_cursor,
        record_reg: index_record_reg,
        unpacked_start: Some(lookup_start),
        unpacked_count: Some((key_width + 1) as u16),
        flags: IdxInsertFlags::new(),
    });

    program.preassign_label_to_next_insn(emit_label);
    if output.identity == input.identity {
        program.emit_insn(Insn::Copy {
            src_reg: lookup_start,
            dst_reg: output_record_start,
            extra_amount: identity_width - 1,
        });
    } else {
        program.emit_insn(Insn::Copy {
            src_reg: arrangement_rowid_reg,
            dst_reg: output_record_start,
            extra_amount: 0,
        });
    }
    if input.width > 0 {
        program.emit_insn(Insn::Copy {
            src_reg: input_values_start,
            dst_reg: output_values_start,
            extra_amount: input.width - 1,
        });
    }
    for (input_column, output_column) in input
        .binding_rowid_columns
        .iter()
        .zip(output.binding_rowid_columns.iter())
    {
        match (input_column, output_column) {
            (None, None) => {}
            (Some(input_column), Some(output_column))
                if *output_column < output.identity_width() =>
            {
                turso_assert!(
                    output.identity == input.identity && input_column == output_column,
                    "published binding-rowid identity must preserve its source slot"
                );
            }
            (Some(input_column), Some(output_column)) => {
                let source_reg = if *input_column < identity_width {
                    lookup_start + *input_column
                } else {
                    state_binding_rowids_start + *input_column - identity_width
                };
                program.emit_insn(Insn::Copy {
                    src_reg: source_reg,
                    dst_reg: output_record_start + *output_column,
                    extra_amount: 0,
                });
            }
            _ => turso_assert!(
                false,
                "output arrangement must preserve binding-rowid availability"
            ),
        }
    }
    program.emit_insn(Insn::Copy {
        src_reg: contribution_reg,
        dst_reg: output_weight_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: output_record_start as u16,
        count: output.record_width() as u16,
        dest_reg: output_record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: output.cursor_id,
        rowid_reg: output_rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: output.cursor_id,
        key_reg: output_rowid_reg,
        record_reg: output_record_reg,
        flag: InsertFlags::new().is_ephemeral_table_insert(),
        table_name: String::new(),
    });

    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: input.cursor_id,
        pc_if_next: loop_label,
    });
    program.emit_insn(Insn::Goto {
        target_pc: end_label,
    });
    program.preassign_label_to_next_insn(corrupt_label);
    program.emit_insn(Insn::Halt {
        err_code: crate::error::SQLITE_ERROR,
        description: format!("materialized view {view_name} output arrangement is corrupted"),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(end_label);

    let value_columns = (identity_width..identity_width + input.width).collect();
    let binding_rowid_columns = input
        .binding_rowid_columns
        .iter()
        .map(|column| {
            column.map(|column| {
                ArrangementIdentityColumn::Column(if column < identity_width {
                    column
                } else {
                    key_width + column - identity_width
                })
            })
        })
        .collect();
    let (arrangement_identity, identity_columns) = if output.identity == input.identity {
        (
            input.identity,
            (0..identity_width)
                .map(ArrangementIdentityColumn::Column)
                .collect(),
        )
    } else {
        (
            DeltaIdentity::OperatorRowid,
            vec![ArrangementIdentityColumn::RowId],
        )
    };
    Ok(btree_arrangement(
        table,
        arrangement_identity,
        identity_columns,
        binding_rowid_columns,
        value_columns,
        Some(key_width + extra_binding_rowid_width),
    ))
}

#[allow(clippy::too_many_arguments)]
fn materialize_declared_output_arrangement(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    output: NodeOutput,
    maintenance_input: MaintenanceInput,
    operator_state: &OperatorStateDef,
    schema: &Schema,
) -> Result<NodeOutput> {
    let Some(arrangement_def) = &operator_state.arrangement_table else {
        return Ok(output);
    };
    let input = output.delta.materialize(
        program,
        view_name,
        node_id,
        &operator_state.output,
        maintenance_input,
    )?;
    let expected_identity = operator_state.output.emitted_identity;
    if input.identity != expected_identity {
        return Err(LimboError::InternalError(format!(
            "maintenance DAG node {node_id} emits {:?}, expected {:?}",
            input.identity, expected_identity
        )));
    }
    let arranged_output = open_ephemeral_delta(
        program,
        &format!("{view_name}_arranged_delta_{node_id}"),
        operator_state.output.schema.as_ref().clone(),
        operator_state.output.published_identity,
        operator_state.output.binding_rowids.clone(),
        input.requires_positive_first,
    );
    let arrangement = emit_output_arrangement(
        program,
        view_name,
        &input,
        &arranged_output,
        &arrangement_def.table_name,
        schema,
    )?;
    Ok(NodeOutput {
        delta: DeltaSource::Ephemeral(arranged_output),
        arrangement: Some(arrangement),
    })
}

#[allow(clippy::too_many_arguments)]
fn emit_ephemeral_filter(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    input: &EphemeralDelta,
    output_contract: &NodeOutputContract,
    predicate: &ast::Expr,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<DeltaSource> {
    let output = open_ephemeral_delta(
        program,
        &format!("{view_name}_filter_delta_{node_id}"),
        output_contract.schema.as_ref().clone(),
        output_contract.emitted_identity,
        output_contract.binding_rowids.clone(),
        input.requires_positive_first,
    );

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
    let (table_references, binding_remap) = stream_table_references(program, &input.schema);

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let next_label = program.allocate_label();
    let pass_label = program.allocate_label();
    turso_assert!(
        output.identity == input.identity
            && output.binding_rowid_columns == input.binding_rowid_columns
            && output.value_start == input.value_start
            && output.weight_column == input.weight_column,
        "filter output must preserve its complete input stream contract"
    );
    let record_start = program.alloc_registers(output.record_width());
    let record_reg = program.alloc_register();
    let rowid_reg = program.alloc_register();

    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    seed_ephemeral_stream_cache(program, input, &binding_remap, &mut resolver)?;
    let bound_predicate = remap_bound_expr(predicate, &binding_remap)?;
    translate_condition_expr(
        program,
        &table_references,
        &bound_predicate,
        ConditionMetadata {
            jump_if_condition_is_true: false,
            jump_target_when_true: pass_label,
            jump_target_when_false: next_label,
            jump_target_when_null: next_label,
        },
        &resolver,
    )?;
    program.preassign_label_to_next_insn(pass_label);

    for column in 0..input.weight_column {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: record_start + column,
            default: None,
        });
    }
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: input.weight_column,
        dest: record_start + output.weight_column,
        default: None,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: record_start as u16,
        count: output.record_width() as u16,
        dest_reg: record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: output.cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: output.cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new().is_ephemeral_table_insert(),
        table_name: String::new(),
    });
    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: input.cursor_id,
        pc_if_next: loop_label,
    });
    program.preassign_label_to_next_insn(end_label);
    drop(syms);
    Ok(DeltaSource::Ephemeral(output))
}

fn append_alias_values(
    program: &mut ProgramBuilder,
    input: &EphemeralDelta,
    output: &EphemeralDelta,
) -> Result<()> {
    if input.width != output.width
        || output.identity_width() != input.identity_width()
        || output.value_start != output.identity_width()
        || output.binding_rowid_columns.iter().any(Option::is_some)
    {
        return Err(LimboError::InternalError(
            "alias stream copy has incompatible input/output layouts".to_string(),
        ));
    }

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    let record_start = program.alloc_registers(output.record_width());
    let record_reg = program.alloc_register();
    let rowid_reg = program.alloc_register();

    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    for column in 0..output.identity_width() {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: record_start + column,
            default: None,
        });
    }
    for column in 0..input.width {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column: input.value_start + column,
            dest: record_start + output.value_start + column,
            default: None,
        });
    }
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: input.weight_column,
        dest: record_start + output.weight_column,
        default: None,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: record_start as u16,
        count: output.record_width() as u16,
        dest_reg: record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: output.cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: output.cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new().is_ephemeral_table_insert(),
        table_name: String::new(),
    });
    program.emit_insn(Insn::Next {
        cursor_id: input.cursor_id,
        pc_if_next: loop_label,
    });
    program.preassign_label_to_next_insn(end_label);
    Ok(())
}

fn emit_ephemeral_alias(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    input: &EphemeralDelta,
    output_contract: &NodeOutputContract,
) -> Result<DeltaSource> {
    let output = open_ephemeral_delta(
        program,
        &format!("{view_name}_alias_delta_{node_id}"),
        output_contract.schema.as_ref().clone(),
        output_contract.emitted_identity,
        output_contract.binding_rowids.clone(),
        input.requires_positive_first,
    );
    append_alias_values(program, input, &output)?;
    Ok(DeltaSource::Ephemeral(output))
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
                let output = open_ephemeral_delta(
                    program,
                    &format!("{view_name}_project_delta_{node_id}"),
                    output_contract.schema.as_ref().clone(),
                    output_contract.emitted_identity,
                    output_contract.binding_rowids.clone(),
                    upstream.requires_positive_first,
                );
                emit_ephemeral_project(
                    program,
                    &upstream,
                    projections,
                    &output,
                    schema,
                    connection,
                )?;
                NodeOutput {
                    delta: DeltaSource::Ephemeral(output),
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
                let output = open_ephemeral_delta(
                    program,
                    &format!("{view_name}_aggregate_delta_{node_id}"),
                    output_contract.schema.as_ref().clone(),
                    output_contract.emitted_identity,
                    output_contract.binding_rowids.clone(),
                    false,
                );
                let operator_state = operator_states.for_node(node_id)?;
                emit_group_aggregate(
                    program,
                    view_name,
                    &upstream,
                    group_exprs,
                    group_collations,
                    aggregates,
                    *scalar,
                    &output,
                    input,
                    operator_state,
                    schema,
                    connection,
                )?;
                let state_table_name = operator_state.state_table_name()?;
                let state_table = schema.get_btree_table(state_table_name).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "aggregate arrangement table {state_table_name} not found"
                    ))
                })?;
                let value_columns = (0..group_exprs.len() + aggregates.len()).collect();
                NodeOutput {
                    delta: DeltaSource::Ephemeral(output),
                    arrangement: Some(btree_arrangement(
                        state_table,
                        DeltaIdentity::OperatorRowid,
                        vec![ArrangementIdentityColumn::RowId],
                        vec![None; output_contract.schema.bindings.len()],
                        value_columns,
                        None,
                    )),
                }
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
#[allow(clippy::too_many_arguments)]
fn emit_ephemeral_project(
    program: &mut ProgramBuilder,
    input: &EphemeralDelta,
    projections: &[(ast::Expr, Option<String>)],
    output: &EphemeralDelta,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<()> {
    let num_output_columns = output.width;
    turso_assert!(
        projections.len() == num_output_columns,
        "projection output does not match its sink schema"
    );

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
    let (table_references, binding_remap) = stream_table_references(program, &input.schema);

    let end_label = program.allocate_label();
    let loop_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: input.cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(loop_label);
    seed_ephemeral_stream_cache(program, input, &binding_remap, &mut resolver)?;

    turso_assert!(
        output.identity == input.identity
            && output.binding_rowid_columns == input.binding_rowid_columns
            && output.value_start == input.value_start
            && output.weight_column == output.value_start + num_output_columns,
        "linear projection sink must preserve its input metadata contract"
    );
    let metadata_start = program.alloc_registers(output.value_start);
    for column in 0..input.value_start {
        program.emit_insn(Insn::Column {
            cursor_id: input.cursor_id,
            column,
            dest: metadata_start + column,
            default: None,
        });
    }
    let out_start = program.alloc_registers(num_output_columns + 1);
    let new_weight_reg = out_start + num_output_columns;
    for (index, (expr, _)) in projections.iter().enumerate() {
        let bound = remap_bound_expr(expr, &binding_remap)?;
        translate_expr_no_constant_opt(
            program,
            Some(&table_references),
            &bound,
            out_start + index,
            &resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
    }
    let weight_reg = program.alloc_register();
    program.emit_insn(Insn::Column {
        cursor_id: input.cursor_id,
        column: input.weight_column,
        dest: weight_reg,
        default: None,
    });

    debug_assert_eq!(metadata_start + output.value_start, out_start);
    program.emit_insn(Insn::Copy {
        src_reg: weight_reg,
        dst_reg: new_weight_reg,
        extra_amount: 0,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: metadata_start as u16,
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
        cursor_id: input.cursor_id,
        pc_if_next: loop_label,
    });
    program.preassign_label_to_next_insn(end_label);
    drop(syms);
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
