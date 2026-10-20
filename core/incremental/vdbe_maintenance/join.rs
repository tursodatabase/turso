use super::binding::{cache_stream_expr_reg, make_joined_table, remap_bound_expr, BindingRemap};
use super::output::{EmittedNodeOutput, NodeOutput, NodeOutputContract};
use super::plan::OperatorStateDef;
use super::source::materialize_node_output;
use super::stream::{
    emit_ephemeral_delta_row, open_ephemeral_delta, scratch_table, ArrangementHandle,
    ArrangementIdentityColumn, DeltaIdentity, EphemeralDelta,
};
use super::MaintenanceInput;
use crate::alloc::TursoIteratorExt;
use crate::incremental::dag;
use crate::schema::{Index, IndexColumn, Schema};
use crate::sync::Arc;
use crate::translate::collate::{get_expr_collation_ctx_with_symbols, CollationSeq};
use crate::translate::emitter::Resolver;
use crate::translate::expr::{
    comparison_affinity, expr_is_array, translate_condition_expr, translate_expr, ConditionMetadata,
};
use crate::translate::plan::TableReferences;
use crate::turso_assert;
use crate::vdbe::affinity::Affinity;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{
    to_u32, CmpInsFlags, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral,
};
use crate::vdbe::BranchOffset;
use crate::{Connection, LimboError, Result};
use std::num::NonZeroUsize;
use turso_parser::ast::{self, SortOrder};

const JOIN_ARITY: usize = 2;

/// One physical input edge of a binary join.
///
/// Keeping the delta, logical schema, and maintained arrangement together
/// prevents emitters from accidentally combining parallel slices belonging
/// to different inputs.
struct JoinInputContract {
    /// The input's changes in this transaction, copied into an ephemeral
    /// table so that a join can find a changed row again by its rowid.
    /// `None` while a new view is populated, which reads only arrangements.
    delta: Option<EphemeralDelta>,
    schema: Arc<dag::StreamSchema>,
    arrangement: ArrangementHandle,
}

impl JoinInputContract {
    fn delta(&self) -> Result<&EphemeralDelta> {
        self.delta.as_ref().ok_or_else(|| {
            LimboError::InternalError(
                "a join read the changes of its input while populating a view".to_string(),
            )
        })
    }
}

/// The complete physical contract of one join node.
pub(super) struct JoinContract {
    inputs: Vec<JoinInputContract>,
    on: Vec<ast::Expr>,
}

impl JoinContract {
    pub(super) fn from_outputs(
        program: &mut ProgramBuilder,
        view_name: &str,
        input_nodes: &[dag::NodeId],
        outputs: &[&NodeOutput],
        on: &[ast::Expr],
        input: MaintenanceInput,
    ) -> Result<Self> {
        turso_assert!(
            input_nodes.len() == outputs.len(),
            "a join needs one output per input node"
        );
        let inputs = input_nodes
            .iter()
            .zip(outputs)
            .map(|(input_node, output)| {
                let arrangement = output.arrangement().cloned().ok_or_else(|| {
                    LimboError::InternalError(
                        "join input has no materialized arrangement".to_string(),
                    )
                })?;
                let delta = match input {
                    MaintenanceInput::TransactionDelta => Some(materialize_node_output(
                        program,
                        view_name,
                        *input_node,
                        output,
                        input,
                    )?),
                    MaintenanceInput::BaseTable => None,
                };
                Ok(JoinInputContract {
                    delta,
                    schema: output.contract().schema.clone(),
                    arrangement,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            inputs,
            on: on.to_vec(),
        })
    }
}

/// Which relation a join phase reads for one side of the join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinSide {
    /// The transaction's captured delta for the table.
    Delta,
    /// The table's btree, which at maintenance time holds the post-change
    /// state.
    Btree,
}

/// The bilinear delta decomposition for one binary join:
/// `d(L ⋈ R) = dL ⋈ R_new + L_new ⋈ dR − dL ⋈ dR`.
///
/// Population is one all-arrangement phase with weight +1. Unlike the deleted
/// n-ary subset expansion, this has constant code size per join node.
fn binary_join_phases(input: MaintenanceInput) -> &'static [([JoinSide; JOIN_ARITY], bool)] {
    const POPULATION: &[([JoinSide; JOIN_ARITY], bool)] =
        &[([JoinSide::Btree, JoinSide::Btree], false)];
    const MAINTENANCE: &[([JoinSide; JOIN_ARITY], bool)] = &[
        ([JoinSide::Delta, JoinSide::Btree], false),
        ([JoinSide::Btree, JoinSide::Delta], false),
        ([JoinSide::Delta, JoinSide::Delta], true),
    ];
    match input {
        MaintenanceInput::BaseTable => POPULATION,
        MaintenanceInput::TransactionDelta => MAINTENANCE,
    }
}

/// Per-match emission hook for [`emit_join_phase`]: receives the program,
/// and the per-input physical cursor ids.
type JoinPhaseSink<'a> = dyn FnMut(&mut ProgramBuilder, &[usize; JOIN_ARITY]) -> Result<()> + 'a;

fn emit_arrangement_column_value(
    program: &mut ProgramBuilder,
    column: ArrangementIdentityColumn,
    cursor_id: usize,
    dest: usize,
) {
    match column {
        ArrangementIdentityColumn::RowId => {
            program.emit_insn(Insn::RowId { cursor_id, dest });
        }
        ArrangementIdentityColumn::Column(column) => {
            program.emit_insn(Insn::Column {
                cursor_id,
                column,
                dest,
                default: None,
            });
        }
    }
}

fn emit_arrangement_identity_value(
    program: &mut ProgramBuilder,
    arrangement: &ArrangementHandle,
    cursor_id: usize,
    identity_column: usize,
    dest: usize,
) {
    emit_arrangement_column_value(
        program,
        arrangement.identity_columns()[identity_column],
        cursor_id,
        dest,
    );
}

/// Read the SQL rowid of one binding of a join input. Returns false when the
/// input does not carry that rowid.
fn emit_join_input_binding_rowid(
    program: &mut ProgramBuilder,
    side: JoinSide,
    input: &JoinInputContract,
    cursor_id: usize,
    binding: usize,
    dest: usize,
) -> Result<bool> {
    let column = match side {
        JoinSide::Delta => input
            .delta()?
            .binding_rowid_columns
            .get(binding)
            .copied()
            .flatten()
            .map(ArrangementIdentityColumn::Column),
        JoinSide::Btree => input
            .arrangement
            .binding_rowid_columns()
            .get(binding)
            .copied()
            .flatten(),
    };
    let Some(column) = column else {
        return Ok(false);
    };
    emit_arrangement_column_value(program, column, cursor_id, dest);
    Ok(true)
}

fn emit_join_input_identity(
    program: &mut ProgramBuilder,
    side: JoinSide,
    input: &JoinInputContract,
    cursor_id: usize,
    dest: usize,
) -> Result<usize> {
    match side {
        JoinSide::Delta => {
            let width = input.delta()?.identity.width();
            for column in 0..width {
                program.emit_insn(Insn::Column {
                    cursor_id,
                    column,
                    dest: dest + column,
                    default: None,
                });
            }
            Ok(width)
        }
        JoinSide::Btree => {
            let width = input.arrangement.identity().width();
            for column in 0..width {
                emit_arrangement_identity_value(
                    program,
                    &input.arrangement,
                    cursor_id,
                    column,
                    dest + column,
                );
            }
            Ok(width)
        }
    }
}

fn join_input_identity(side: JoinSide, input: &JoinInputContract) -> Result<DeltaIdentity> {
    Ok(match side {
        JoinSide::Delta => input.delta()?.identity,
        JoinSide::Btree => input.arrangement.identity(),
    })
}

/// Read one logical column of a join input.
fn emit_join_input_value(
    program: &mut ProgramBuilder,
    side: JoinSide,
    input: &JoinInputContract,
    cursor_id: usize,
    logical_column: usize,
    dest: usize,
) -> Result<()> {
    match side {
        JoinSide::Delta => {
            program.emit_insn(Insn::Column {
                cursor_id,
                column: input.delta()?.value_start + logical_column,
                dest,
                default: None,
            });
        }
        JoinSide::Btree => program.emit_column_or_rowid(
            cursor_id,
            input.arrangement.value_columns()[logical_column],
            dest,
        ),
    }
    Ok(())
}

/// Emit one join delta phase: a loop over the outer input and, for each of
/// its rows, a loop over the rows of the inner input that can match it. At
/// the innermost level the ON predicates and WHERE are evaluated, the signed
/// weight product lands in `w_reg`, and `sink` emits the per-match work with
/// both cursors positioned (its slice maps input positions to cursor ids).
///
/// When the ON clause compares a column of each input with `=`, the inner
/// rows are found by key instead of by reading the whole inner input: see
/// [`choose_join_order`]. The full ON clause is still evaluated for every
/// row found this way, so a lookup may return extra rows but never misses
/// one.
#[allow(clippy::too_many_arguments)]
fn emit_join_phase(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    view_name: &str,
    schema: &Schema,
    contract: &JoinContract,
    sides: &[JoinSide; JOIN_ARITY],
    negate: bool,
    w_reg: usize,
    sink: &mut JoinPhaseSink<'_>,
) -> Result<()> {
    // A physical input may carry several SQL bindings after a previous binary
    // join. Bound expressions are retargeted to fresh phase ids and every
    // column is seeded from the one physical cursor declared by that edge.
    let mut binding_remap = BindingRemap::default();
    let mut phase_bindings = Vec::new();
    for input in &contract.inputs {
        for binding in &input.schema.bindings {
            let phase_id = program.table_reference_counter.next();
            let previous = binding_remap.insert(binding.logical_id, phase_id);
            turso_assert!(
                previous.is_none(),
                "a join phase must expose each logical binding exactly once"
            );
            phase_bindings.push((binding, phase_id));
        }
    }
    let table_references = TableReferences::new(
        phase_bindings
            .iter()
            .map(|(binding, phase_id)| {
                make_joined_table(&binding.table, &binding.identifier, *phase_id)
            })
            .collect(),
        vec![],
    );

    let cursor_ids = [
        open_join_input(program, contract, sides, 0)?,
        open_join_input(program, contract, sides, 1)?,
    ];
    let keys = find_join_keys(contract, &binding_remap, &table_references, resolver)?;
    let order = choose_join_order(schema, contract, sides, &keys);
    let (outer, inner) = (order.outer, order.inner);

    // An empty delta makes the whole phase empty; skip it before reading or
    // indexing an arrangement.
    let phase_done_label = program.allocate_label();
    for position in 0..JOIN_ARITY {
        if sides[position] == JoinSide::Delta {
            program.emit_insn(Insn::Rewind {
                cursor_id: cursor_ids[position],
                pc_if_empty: phase_done_label,
            });
        }
    }
    let lookup_cursor = match &order.access {
        InnerAccess::Scan | InnerAccess::Rowid { .. } => None,
        InnerAccess::Index { index, .. } => {
            let cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
            program.emit_insn(Insn::OpenRead {
                cursor_id,
                root_page: index.root_page,
                db: 0,
            });
            Some(cursor_id)
        }
        InnerAccess::TemporaryIndex => Some(emit_temporary_key_index(
            program,
            &contract.inputs[inner],
            sides[inner],
            inner,
            cursor_ids[inner],
            &keys,
        )?),
    };
    let missing_row_label = lookup_cursor.map(|_| program.allocate_label());

    // Bind every logical column to the physical arrangement/delta slot used
    // in this phase. This keeps expression translation independent of record
    // layout (aggregate arrangements can project away accumulator payloads,
    // while ephemeral deltas carry identity before their values).
    resolver.enable_expr_to_reg_cache();
    let mut weight_regs = Vec::new();
    let outer_loop = program.allocate_label();
    let outer_next = program.allocate_label();
    let inner_next = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: cursor_ids[outer],
        pc_if_empty: phase_done_label,
    });
    program.preassign_label_to_next_insn(outer_loop);
    emit_join_input_row(
        program,
        resolver,
        &contract.inputs[outer],
        sides[outer],
        cursor_ids[outer],
        &binding_remap,
        &table_references,
        &mut weight_regs,
    )?;
    let inner_loop = emit_inner_lookup(
        program,
        resolver,
        &table_references,
        &order,
        &keys,
        cursor_ids[inner],
        lookup_cursor,
        outer_next,
        missing_row_label,
    )?;
    emit_join_input_row(
        program,
        resolver,
        &contract.inputs[inner],
        sides[inner],
        cursor_ids[inner],
        &binding_remap,
        &table_references,
        &mut weight_regs,
    )?;

    // ON predicates and WHERE: a FALSE or NULL result means this combination
    // is not a join output.
    for condition in &contract.on {
        let bound = remap_bound_expr(condition, &binding_remap)?;
        let pred_true = program.allocate_label();
        translate_condition_expr(
            program,
            &table_references,
            &bound,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: pred_true,
                jump_target_when_false: inner_next,
                jump_target_when_null: inner_next,
            },
            resolver,
        )?;
        program.preassign_label_to_next_insn(pred_true);
    }

    // Signed weight: the product of the delta weights and arrangement
    // multiplicities times the phase sign.
    program.emit_int(if negate { -1 } else { 1 }, w_reg);
    for weight in &weight_regs {
        program.emit_insn(Insn::Multiply {
            lhs: w_reg,
            rhs: *weight,
            dest: w_reg,
        });
    }

    sink(program, &cursor_ids)?;

    program.preassign_label_to_next_insn(inner_next);
    match (inner_loop, lookup_cursor) {
        (Some(loop_label), Some(lookup_cursor)) => program.emit_insn(Insn::Next {
            cursor_id: lookup_cursor,
            pc_if_next: loop_label,
            fullscan: false,
            is_index: true,
        }),
        (Some(loop_label), None) => program.emit_insn(Insn::Next {
            cursor_id: cursor_ids[inner],
            pc_if_next: loop_label,
            fullscan: false,
            is_index: false,
        }),
        (None, _) => {}
    }
    program.preassign_label_to_next_insn(outer_next);
    program.emit_insn(Insn::Next {
        cursor_id: cursor_ids[outer],
        pc_if_next: outer_loop,
        fullscan: false,
        is_index: false,
    });
    if let Some(missing_row_label) = missing_row_label {
        program.emit_insn(Insn::Goto {
            target_pc: phase_done_label,
        });
        program.preassign_label_to_next_insn(missing_row_label);
        program.emit_insn(Insn::Halt {
            err_code: crate::error::SQLITE_ERROR,
            description: format!(
                "materialized view {view_name}: a join key lookup found a row that does not exist"
            ),
            on_error: None,
            description_reg: None,
        });
    }
    program.preassign_label_to_next_insn(phase_done_label);
    Ok(())
}

fn open_join_input(
    program: &mut ProgramBuilder,
    contract: &JoinContract,
    sides: &[JoinSide; JOIN_ARITY],
    position: usize,
) -> Result<usize> {
    let input = &contract.inputs[position];
    match sides[position] {
        JoinSide::Delta => Ok(input.delta()?.cursor_id),
        JoinSide::Btree => {
            let storage = input.arrangement.table();
            let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(storage.clone()));
            program.emit_insn(Insn::OpenRead {
                cursor_id,
                root_page: storage.root_page,
                db: 0,
            });
            Ok(cursor_id)
        }
    }
}

/// One column of a join input that a join key compares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinKeyColumn {
    /// A logical column of the input.
    Value(usize),
    /// The SQL rowid of one of the input's bindings.
    BindingRowid(usize),
}

/// An `a = b` conjunct of the ON clause where `a` is a column of one input
/// and `b` a column of the other.
struct JoinKey {
    /// The compared column of each input, in input order.
    columns: [JoinKeyColumn; JOIN_ARITY],
    /// The compared expression of each input, bound to this phase.
    operands: [ast::Expr; JOIN_ARITY],
    /// The affinity `=` applies to its operands before comparing them.
    affinity: Affinity,
    /// The collation `=` compares text with.
    collation: CollationSeq,
}

fn find_join_keys(
    contract: &JoinContract,
    binding_remap: &BindingRemap,
    table_references: &TableReferences,
    resolver: &Resolver,
) -> Result<Vec<JoinKey>> {
    let mut keys = Vec::new();
    for condition in &contract.on {
        let ast::Expr::Binary(lhs, ast::Operator::Equals, rhs) = condition else {
            continue;
        };
        let (Some((lhs_input, lhs_column)), Some((rhs_input, rhs_column))) = (
            join_key_column(contract, lhs),
            join_key_column(contract, rhs),
        ) else {
            continue;
        };
        if lhs_input == rhs_input {
            continue;
        }
        let lhs = remap_bound_expr(lhs, binding_remap)?;
        let rhs = remap_bound_expr(rhs, binding_remap)?;
        if expr_is_array(&lhs, Some(table_references))
            || expr_is_array(&rhs, Some(table_references))
        {
            continue;
        }
        // The same rules the ON condition itself is compiled with.
        let affinity = comparison_affinity(&lhs, &rhs, Some(table_references), Some(resolver));
        let collation = join_key_collation(&lhs, &rhs, table_references, resolver)?;
        let (columns, operands) = if lhs_input == 0 {
            ([lhs_column, rhs_column], [lhs, rhs])
        } else {
            ([rhs_column, lhs_column], [rhs, lhs])
        };
        keys.push(JoinKey {
            columns,
            operands,
            affinity,
            collation,
        });
    }
    Ok(keys)
}

/// The collation `lhs = rhs` compares text with in the compiled ON condition.
/// The operands are read from the expression cache, where a column carries
/// its declared collation (BINARY by default) and a rowid carries none; the
/// left operand's collation wins.
fn join_key_collation(
    lhs: &ast::Expr,
    rhs: &ast::Expr,
    table_references: &TableReferences,
    resolver: &Resolver,
) -> Result<CollationSeq> {
    let operand_collation = |expr: &ast::Expr| match expr {
        ast::Expr::RowId { .. } => Ok(None),
        _ => {
            get_expr_collation_ctx_with_symbols(expr, table_references, Some(resolver.symbol_table))
        }
    };
    Ok(match (operand_collation(lhs)?, operand_collation(rhs)?) {
        (Some((collation, _)), _) | (None, Some((collation, _))) => collation,
        (None, None) => CollationSeq::Binary,
    })
}

/// Which input an operand of `=` is a plain column of, if any.
fn join_key_column(contract: &JoinContract, expr: &ast::Expr) -> Option<(usize, JoinKeyColumn)> {
    contract
        .inputs
        .iter()
        .enumerate()
        .find_map(|(position, input)| match expr {
            ast::Expr::Column { .. } => input
                .schema
                .columns
                .iter()
                .position(|column| column.as_ref() == Some(expr))
                .map(|column| (position, JoinKeyColumn::Value(column))),
            ast::Expr::RowId { table, .. } => input
                .schema
                .bindings
                .iter()
                .position(|binding| binding.logical_id == *table)
                .filter(|binding| input.arrangement.binding_rowid_columns()[*binding].is_some())
                .map(|binding| (position, JoinKeyColumn::BindingRowid(binding))),
            _ => None,
        })
}

/// How a phase finds the rows of its inner input for one outer row.
enum InnerAccess {
    /// Read every row.
    Scan,
    /// Seek the inner arrangement's rowid with `keys[key]`.
    Rowid { key: usize },
    /// Seek a persistent index of the inner arrangement with `keys`, in the
    /// index's column order.
    Index { index: Arc<Index>, keys: Vec<usize> },
    /// Seek an index over all keys, built from the inner input at the start
    /// of the phase.
    TemporaryIndex,
}

struct JoinOrder {
    outer: usize,
    inner: usize,
    access: InnerAccess,
}

/// Choose which input to loop over and how to find the matching rows of the
/// other one.
///
/// - An arrangement with a usable rowid or index is looked up directly, as
///   SQLite's planner would.
/// - Otherwise a delta is looked up through a temporary index built over it.
///   The delta is usually small, so the phase reads the arrangement once
///   instead of once per changed row.
/// - Population joins two arrangements; the second one gets the temporary
///   index.
///
/// A join with no `=` between the inputs reads every pair of rows.
fn choose_join_order(
    schema: &Schema,
    contract: &JoinContract,
    sides: &[JoinSide; JOIN_ARITY],
    keys: &[JoinKey],
) -> JoinOrder {
    let scan_with_deltas_outside = || {
        let outer = if sides[1] == JoinSide::Delta && sides[0] == JoinSide::Btree {
            1
        } else {
            0
        };
        JoinOrder {
            outer,
            inner: 1 - outer,
            access: InnerAccess::Scan,
        }
    };
    if keys.is_empty() {
        return scan_with_deltas_outside();
    }
    for inner in [1, 0] {
        if sides[inner] != JoinSide::Btree {
            continue;
        }
        if let Some(access) = persistent_key_access(schema, &contract.inputs[inner], inner, keys) {
            return JoinOrder {
                outer: 1 - inner,
                inner,
                access,
            };
        }
    }
    let inner = [1, 0]
        .into_iter()
        .find(|&position| sides[position] == JoinSide::Delta)
        .unwrap_or(1);
    if sides[inner] == JoinSide::Btree && !contract.inputs[inner].arrangement.table().has_rowid {
        return scan_with_deltas_outside();
    }
    JoinOrder {
        outer: 1 - inner,
        inner,
        access: InnerAccess::TemporaryIndex,
    }
}

/// A rowid or index of the inner arrangement that finds every row the keys
/// match. An index column is usable only when it sorts values the way `=`
/// compares them: the same collation, and an affinity under which the
/// comparison does not convert the stored values (SQLite's
/// `sqlite3IndexAffinityOk`).
fn persistent_key_access(
    schema: &Schema,
    input: &JoinInputContract,
    inner: usize,
    keys: &[JoinKey],
) -> Option<InnerAccess> {
    let arrangement = &input.arrangement;
    let table = arrangement.table();
    if !table.has_rowid {
        return None;
    }
    let physical_column = |key: &JoinKey| match key.columns[inner] {
        JoinKeyColumn::Value(column) => Some(arrangement.value_columns()[column]),
        JoinKeyColumn::BindingRowid(_) => None,
    };
    for (position, key) in keys.iter().enumerate() {
        let is_rowid = match key.columns[inner] {
            JoinKeyColumn::BindingRowid(binding) => matches!(
                arrangement.binding_rowid_columns()[binding],
                Some(ArrangementIdentityColumn::RowId)
            ),
            JoinKeyColumn::Value(_) => {
                physical_column(key).is_some_and(|column| table.columns()[column].is_rowid_alias())
            }
        };
        if is_rowid {
            return Some(InnerAccess::Rowid { key: position });
        }
    }

    let mut best: Option<(Arc<Index>, Vec<usize>)> = None;
    for index in schema.get_indices(&table.name) {
        if index.where_clause.is_some() || index.index_method.is_some() || !index.has_rowid {
            continue;
        }
        let mut used = Vec::new();
        for index_column in &index.columns {
            if index_column.expr.is_some() || index_column.order != SortOrder::Asc {
                break;
            }
            let table_column = &table.columns()[index_column.pos_in_table];
            let index_collation = index_column
                .collation
                .unwrap_or_else(|| table_column.collation());
            let Some(position) = keys.iter().enumerate().position(|(position, key)| {
                !used.contains(&position)
                    && physical_column(key) == Some(index_column.pos_in_table)
                    && table_column.affinity().index_affinity_ok(key.affinity)
                    && same_collation(index_collation, key.collation)
            }) else {
                break;
            };
            used.push(position);
        }
        if !used.is_empty()
            && best
                .as_ref()
                .is_none_or(|(_, keys)| used.len() > keys.len())
        {
            best = Some((index.clone(), used));
        }
    }
    best.map(|(index, keys)| InnerAccess::Index { index, keys })
}

fn same_collation(a: CollationSeq, b: CollationSeq) -> bool {
    let normalize = |collation| match collation {
        CollationSeq::Unset => CollationSeq::Binary,
        other => other,
    };
    normalize(a) == normalize(b)
}

/// Build an index over all join keys of the inner input, holding each row's
/// key values and its rowid in the inner cursor. Keys are stored with the
/// affinity and collation `=` uses, and probes get the same affinity, so a
/// probe finds every row `=` would match.
fn emit_temporary_key_index(
    program: &mut ProgramBuilder,
    input: &JoinInputContract,
    side: JoinSide,
    inner: usize,
    inner_cursor: usize,
    keys: &[JoinKey],
) -> Result<usize> {
    let columns = keys
        .iter()
        .enumerate()
        .map(|(position, key)| IndexColumn {
            name: format!("k{position}"),
            order: SortOrder::Asc,
            nulls_order: None,
            pos_in_table: position,
            collation: Some(key.collation),
            default: None,
            expr: None,
        })
        .try_collect::<crate::alloc::Vec<_>>()?;
    let index = Arc::new(Index {
        name: format!("join_keys_{inner_cursor}"),
        table_name: input.arrangement.table().name.clone(),
        root_page: 0,
        columns,
        unique: false,
        ephemeral: true,
        has_rowid: true,
        where_clause: None,
        index_method: None,
        on_conflict: None,
    });
    let index_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
    program.emit_insn(Insn::OpenAutoindex {
        cursor_id: index_cursor,
    });

    let record_start = program.alloc_registers(keys.len() + 1);
    let record_reg = program.alloc_register();
    let build_loop = program.allocate_label();
    let build_done = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: inner_cursor,
        pc_if_empty: build_done,
    });
    program.preassign_label_to_next_insn(build_loop);
    for (position, key) in keys.iter().enumerate() {
        let dest = record_start + position;
        match key.columns[inner] {
            JoinKeyColumn::Value(column) => {
                emit_join_input_value(program, side, input, inner_cursor, column, dest)?
            }
            JoinKeyColumn::BindingRowid(binding) => turso_assert!(
                emit_join_input_binding_rowid(program, side, input, inner_cursor, binding, dest)?,
                "a join key reads a rowid its input does not carry"
            ),
        }
    }
    emit_key_affinity(program, keys.iter().map(|key| key.affinity), record_start);
    program.emit_insn(Insn::RowId {
        cursor_id: inner_cursor,
        dest: record_start + keys.len(),
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(record_start),
        count: to_u32(keys.len() + 1),
        dest_reg: to_u32(record_reg),
        index_name: Some(index.name.clone()),
        affinity_str: None,
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id: index_cursor,
        record_reg,
        unpacked_start: Some(record_start),
        unpacked_count: Some(to_u32(keys.len() + 1)),
        flags: IdxInsertFlags::new().use_seek(false),
    });
    program.emit_insn(Insn::Next {
        cursor_id: inner_cursor,
        pc_if_next: build_loop,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(build_done);
    Ok(index_cursor)
}

/// Read the weight and multiplicity of the current row of one input, and
/// make its columns and rowids available to expressions.
#[allow(clippy::too_many_arguments)]
fn emit_join_input_row(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    input: &JoinInputContract,
    side: JoinSide,
    cursor_id: usize,
    binding_remap: &BindingRemap,
    table_references: &TableReferences,
    weight_regs: &mut Vec<usize>,
) -> Result<()> {
    let weight_column = match side {
        JoinSide::Delta => Some(input.delta()?.weight_column),
        JoinSide::Btree => input.arrangement.count_column(),
    };
    if let Some(column) = weight_column {
        let weight_reg = program.alloc_register();
        program.emit_insn(Insn::Column {
            cursor_id,
            column,
            dest: weight_reg,
            default: None,
        });
        weight_regs.push(weight_reg);
    }
    for (logical_column, column) in input.schema.columns.iter().enumerate() {
        let Some(expr) = column else {
            continue;
        };
        let value_reg = program.alloc_register();
        emit_join_input_value(program, side, input, cursor_id, logical_column, value_reg)?;
        let bound = remap_bound_expr(expr, binding_remap)?;
        cache_stream_expr_reg(&mut *resolver, bound, value_reg, table_references)?;
    }
    for (binding_index, binding) in input.schema.bindings.iter().enumerate() {
        let rowid_reg = program.alloc_register();
        if !emit_join_input_binding_rowid(
            program,
            side,
            input,
            cursor_id,
            binding_index,
            rowid_reg,
        )? {
            continue;
        }
        let phase_id = *binding_remap.get(&binding.logical_id).ok_or_else(|| {
            LimboError::InternalError("join binding rowid has no phase-local binding".to_string())
        })?;
        resolver.cache_expr_reg(
            std::borrow::Cow::Owned(ast::Expr::RowId {
                database: None,
                table: phase_id,
            }),
            rowid_reg,
            false,
            None,
        );
    }
    Ok(())
}

/// Position the inner cursor on the first row that can match the current
/// outer row. Returns the label to loop back to for the next candidate, or
/// `None` when at most one row can match. Jumps to `outer_next` when no row
/// matches.
#[allow(clippy::too_many_arguments)]
fn emit_inner_lookup(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table_references: &TableReferences,
    order: &JoinOrder,
    keys: &[JoinKey],
    inner_cursor: usize,
    lookup_cursor: Option<usize>,
    outer_next: BranchOffset,
    missing_row_label: Option<BranchOffset>,
) -> Result<Option<BranchOffset>> {
    let probe_keys: Vec<usize> = match &order.access {
        InnerAccess::Scan => {
            program.emit_insn(Insn::Rewind {
                cursor_id: inner_cursor,
                pc_if_empty: outer_next,
            });
            let inner_loop = program.allocate_label();
            program.preassign_label_to_next_insn(inner_loop);
            return Ok(Some(inner_loop));
        }
        InnerAccess::Rowid { key } => {
            let probe_reg = program.alloc_register();
            translate_expr(
                program,
                Some(table_references),
                &keys[*key].operands[order.outer],
                probe_reg,
                resolver,
            )?;
            // A table seek compares the key with the rowid the way `=`
            // does, so a REAL key such as -9.223372036854776e18 finds rowid
            // -9223372036854775808. SeekRowid would not: it only converts
            // REALs that integer affinity converts, which excludes the
            // smallest and largest integers. Every phase must find the same
            // rows, or a delete would miss a row an insert added.
            program.emit_insn(Insn::SeekGE {
                is_index: false,
                cursor_id: inner_cursor,
                start_reg: probe_reg,
                num_regs: 1,
                target_pc: outer_next,
                eq_only: true,
                null_matching_mask: Default::default(),
            });
            return Ok(None);
        }
        InnerAccess::Index { keys, .. } => keys.clone(),
        InnerAccess::TemporaryIndex => (0..keys.len()).collect(),
    };
    let (Some(lookup_cursor), Some(missing_row_label)) = (lookup_cursor, missing_row_label) else {
        return Err(LimboError::InternalError(
            "a join key lookup has no index cursor".to_string(),
        ));
    };
    let probe_start = program.alloc_registers(probe_keys.len());
    for (position, key) in probe_keys.iter().enumerate() {
        translate_expr(
            program,
            Some(table_references),
            &keys[*key].operands[order.outer],
            probe_start + position,
            resolver,
        )?;
    }
    emit_key_affinity(
        program,
        probe_keys.iter().map(|key| keys[*key].affinity),
        probe_start,
    );
    program.emit_insn(Insn::SeekGE {
        is_index: true,
        cursor_id: lookup_cursor,
        start_reg: probe_start,
        num_regs: probe_keys.len(),
        target_pc: outer_next,
        eq_only: true,
        null_matching_mask: Default::default(),
    });
    let inner_loop = program.allocate_label();
    program.preassign_label_to_next_insn(inner_loop);
    program.emit_insn(Insn::IdxGT {
        cursor_id: lookup_cursor,
        start_reg: probe_start,
        num_regs: probe_keys.len(),
        target_pc: outer_next,
    });
    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::IdxRowId {
        cursor_id: lookup_cursor,
        dest: rowid_reg,
    });
    program.emit_insn(Insn::SeekRowid {
        cursor_id: inner_cursor,
        src_reg: rowid_reg,
        target_pc: missing_row_label,
    });
    Ok(Some(inner_loop))
}

/// Apply the comparison affinity of each key to consecutive registers.
fn emit_key_affinity(
    program: &mut ProgramBuilder,
    affinities: impl ExactSizeIterator<Item = Affinity>,
    start_reg: usize,
) {
    let Some(count) = NonZeroUsize::new(affinities.len()) else {
        return;
    };
    program.emit_insn(Insn::Affinity {
        start_reg,
        count,
        affinities: affinities.map(|affinity| affinity.aff_mask()).collect(),
    });
}

/// Emit a join's natural delta row into a typed ephemeral stream. The join
/// knows only its declared inputs and predicates; downstream group keys,
/// aggregate arguments, filters, and projections are evaluated by their own
/// operators from this stream.
///
/// The returned emitted output is validated against the node's planned
/// identity and binding namespace before the dispatcher can publish it.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_inner_join(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    output_contract: &NodeOutputContract,
    contract: &JoinContract,
    input: MaintenanceInput,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<EmittedNodeOutput> {
    let width = output_contract.schema.len();
    let natural_width = contract
        .inputs
        .iter()
        .map(|input| input.schema.len())
        .sum::<usize>();
    turso_assert!(
        width == natural_width,
        "join physical inputs must match the planned output schema"
    );

    let channel = open_ephemeral_delta(
        program,
        &format!("{view_name}_join_delta_{node_id}"),
        output_contract.schema.as_ref().clone(),
        output_contract.emitted_identity,
        output_contract.binding_rowids.clone(),
    )?;
    let eph_cursor_id = channel.cursor_id;

    // [transport identity..., binding rowids..., natural join row..., weight].
    let eph_rec_start = program.alloc_registers(channel.record_width());
    let eph_value_start = eph_rec_start + channel.value_start;
    let eph_weight_reg = eph_rec_start + channel.weight_column;
    let eph_rowid_reg = program.alloc_register();
    let eph_record_reg = program.alloc_register();

    let syms = connection.syms.read();
    let mut resolver = Resolver::new(
        schema,
        connection.database_schemas(),
        &connection.temp.database,
        connection.attached_databases(),
        connection.namespaces(),
        &syms,
        connection.experimental_custom_types_enabled(),
        connection.get_dqs_dml().into(),
        Arc::new(crate::dialect::SqliteDialect),
        &None,
    );

    let emit_eph_insert = |program: &mut ProgramBuilder| {
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(eph_rec_start),
            count: to_u32(channel.record_width()),
            dest_reg: to_u32(eph_record_reg),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::NewRowid {
            cursor: eph_cursor_id,
            rowid_reg: eph_rowid_reg,
            prev_largest_reg: 0,
        });
        program.emit_insn(Insn::Insert {
            cursor: eph_cursor_id,
            key_reg: eph_rowid_reg,
            record_reg: eph_record_reg,
            flag: InsertFlags::new().is_ephemeral_table_insert(),
            table_name: String::new(),
        });
    };

    for &(sides, negate) in binary_join_phases(input) {
        emit_join_phase(
            program,
            &mut resolver,
            view_name,
            schema,
            contract,
            &sides,
            negate,
            eph_weight_reg,
            &mut |program, cursors| {
                match channel.identity {
                    DeltaIdentity::BindingRowids(expected_width) => {
                        let mut destination = eph_rec_start;
                        for (position, input) in contract.inputs.iter().enumerate() {
                            turso_assert!(
                                matches!(
                                    join_input_identity(sides[position], input)?,
                                    DeltaIdentity::BindingRowids(_)
                                ),
                                "binding-rowid join output requires binding-rowid inputs"
                            );
                            destination += emit_join_input_identity(
                                program,
                                sides[position],
                                input,
                                cursors[position],
                                destination,
                            )?;
                        }
                        turso_assert!(
                            destination == eph_rec_start + expected_width,
                            "join binding-rowid output width must equal its input identities"
                        );
                    }
                    DeltaIdentity::OperatorKey(2) => {
                        for (position, input) in contract.inputs.iter().enumerate() {
                            let source_identity = join_input_identity(sides[position], input)?;
                            let source_start = program.alloc_registers(source_identity.width());
                            let width = emit_join_input_identity(
                                program,
                                sides[position],
                                input,
                                cursors[position],
                                source_start,
                            )?;
                            program.emit_insn(Insn::MakeRecord {
                                start_reg: to_u32(source_start),
                                count: to_u32(width),
                                dest_reg: to_u32(eph_rec_start + position),
                                index_name: None,
                                affinity_str: None,
                            });
                        }
                    }
                    DeltaIdentity::OperatorRowid | DeltaIdentity::OperatorKey(_) => {
                        return Err(LimboError::InternalError(
                            "binary inner join has an invalid planned identity".to_string(),
                        ));
                    }
                }
                let mut output_binding = 0;
                for (position, input) in contract.inputs.iter().enumerate() {
                    for input_binding in 0..input.schema.bindings.len() {
                        if let Some(output_column) = channel.binding_rowid_columns[output_binding] {
                            if output_column < channel.identity_width() {
                                turso_assert!(
                                    channel.identity
                                        == DeltaIdentity::BindingRowids(
                                            channel.schema.bindings.len()
                                        )
                                        && output_column == output_binding,
                                    "binding-rowid transport slots must follow binding order"
                                );
                            } else {
                                turso_assert!(
                                    emit_join_input_binding_rowid(
                                        program,
                                        sides[position],
                                        input,
                                        cursors[position],
                                        input_binding,
                                        eph_rec_start + output_column,
                                    )?,
                                    "planned join rowid provenance is unavailable from its input"
                                );
                            }
                        }
                        output_binding += 1;
                    }
                }
                turso_assert!(
                    output_binding == channel.schema.bindings.len(),
                    "join input bindings must match its output provenance contract"
                );
                let mut destination = eph_value_start;
                for (position, (input, cursor_id)) in
                    contract.inputs.iter().zip(cursors).enumerate()
                {
                    for column in 0..input.schema.len() {
                        emit_join_input_value(
                            program,
                            sides[position],
                            input,
                            *cursor_id,
                            column,
                            destination,
                        )?;
                        destination += 1;
                    }
                }
                turso_assert!(
                    destination == eph_value_start + width,
                    "join stream row does not match its declared schema"
                );
                emit_eph_insert(program);
                Ok(())
            },
        )?;
    }

    drop(syms);
    EmittedNodeOutput::from_ephemeral(node_id, channel, output_contract)
}

/// Emit a semi or anti join: the rows of the first input that some (semi) or
/// no (anti) row of the second input matches under the ON predicates.
///
/// For each row of the first input, a hidden table stored under the row's
/// rowid keeps:
///
/// - `present`: the row's multiplicity, the integral of the first input's
///   delta;
/// - `matches`: its number of ON matches, the integral of the bilinear join
///   delta `dA ⋈ B + A ⋈ dB − dA ⋈ dB`;
/// - `published`: the weight the row has in the output, which is `present`
///   when the match test passes (`matches > 0` for semi, `matches == 0` for
///   anti) and zero otherwise, and the image published with it.
///
/// One run first sums the changes to both counts per row in a scratch table
/// (the consolidated delta), then visits each changed row once: it adds the
/// sums to the stored counts and emits the change of the row's output
/// weight. Summing first makes the result independent of the order of the
/// delta rows, and visiting each row once means every decision reads the
/// row's final counts. A row inserted and deleted by the same statement
/// therefore never reaches the output.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_semi_or_anti_join(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    kind: dag::JoinKind,
    contract: &JoinContract,
    output_contract: &NodeOutputContract,
    input: MaintenanceInput,
    operator_state: &OperatorStateDef,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<EmittedNodeOutput> {
    let keeps_matched_rows = match kind {
        dag::JoinKind::Semi => true,
        dag::JoinKind::Anti => false,
        dag::JoinKind::Inner => {
            return Err(LimboError::InternalError(
                "an inner join has no per-row match state".to_string(),
            ))
        }
    };
    let rows = &contract.inputs[0];
    let output = open_ephemeral_delta(
        program,
        &format!("{view_name}_semi_join_delta_{node_id}"),
        output_contract.schema.as_ref().clone(),
        output_contract.emitted_identity,
        output_contract.binding_rowids.clone(),
    )?;
    if output.width != rows.schema.len()
        || output.identity_width() != 1
        || !matches!(
            rows.arrangement.identity_columns(),
            [ArrangementIdentityColumn::RowId]
        )
    {
        return Err(LimboError::InternalError(
            "semi or anti join rows must be identified by the rowid of their arrangement"
                .to_string(),
        ));
    }

    let state_name = operator_state.match_counts_table_name()?.to_string();
    let state_table = schema.get_btree_table(&state_name).ok_or_else(|| {
        LimboError::InternalError(format!(
            "semi or anti join state {state_name} of materialized view {view_name} not found"
        ))
    })?;

    let syms = connection.syms.read();
    let mut resolver = Resolver::new(
        schema,
        connection.database_schemas(),
        &connection.temp.database,
        connection.attached_databases(),
        connection.namespaces(),
        &syms,
        connection.experimental_custom_types_enabled(),
        connection.get_dqs_dml().into(),
        Arc::new(crate::dialect::SqliteDialect),
        &None,
    );

    let state_cursor = program.alloc_cursor_id(CursorType::BTreeTable(state_table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: state_cursor,
        root_page: RegisterOrLiteral::Literal(state_table.root_page),
        db: 0,
    });
    let rows_table = rows.arrangement.table();
    let rows_cursor = program.alloc_cursor_id(CursorType::BTreeTable(rows_table.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: rows_cursor,
        root_page: rows_table.root_page,
        db: 0,
    });
    // Per changed row, keyed by its rowid: (present change, matches change,
    // whether the row's own delta was non-empty).
    let changes_table = Arc::new(scratch_table(&format!("{state_name}_changes"), 3)?);
    let changes_cursor = program.alloc_cursor_id(CursorType::BTreeTable(changes_table));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: changes_cursor,
        is_table: true,
    });

    let zero_reg = program.alloc_register();
    program.emit_int(0, zero_reg);
    let row_reg = program.alloc_register();
    let present_change_reg = program.alloc_register();
    let matches_change_reg = program.alloc_register();
    let return_reg = program.alloc_register();
    let sums_start = program.alloc_registers(3);
    let sums_record_reg = program.alloc_register();
    let add_change_label = program.allocate_label();
    let corrupt_label = program.allocate_label();
    let start_label = program.allocate_label();
    let end_label = program.allocate_label();

    program.emit_insn(Insn::Goto {
        target_pc: start_label,
    });
    emit_add_change_subroutine(
        program,
        add_change_label,
        changes_cursor,
        row_reg,
        present_change_reg,
        matches_change_reg,
        sums_start,
        sums_record_reg,
        zero_reg,
        return_reg,
    );
    program.preassign_label_to_next_insn(start_label);

    // The first input's own delta changes `present`. Population reads the
    // whole arrangement instead.
    let (presence_side, presence_cursor) = match input {
        MaintenanceInput::TransactionDelta => (JoinSide::Delta, rows.delta()?.cursor_id),
        MaintenanceInput::BaseTable => {
            let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(rows_table.clone()));
            program.emit_insn(Insn::OpenRead {
                cursor_id,
                root_page: rows_table.root_page,
                db: 0,
            });
            (JoinSide::Btree, cursor_id)
        }
    };
    let presence_done = program.allocate_label();
    let presence_loop = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: presence_cursor,
        pc_if_empty: presence_done,
    });
    program.preassign_label_to_next_insn(presence_loop);
    match (presence_side, rows.arrangement.count_column()) {
        (JoinSide::Delta, _) => program.emit_insn(Insn::Column {
            cursor_id: presence_cursor,
            column: rows.delta()?.weight_column,
            dest: present_change_reg,
            default: None,
        }),
        (JoinSide::Btree, Some(count_column)) => program.emit_insn(Insn::Column {
            cursor_id: presence_cursor,
            column: count_column,
            dest: present_change_reg,
            default: None,
        }),
        (JoinSide::Btree, None) => program.emit_int(1, present_change_reg),
    }
    emit_join_input_identity(program, presence_side, rows, presence_cursor, row_reg)?;
    program.emit_int(0, matches_change_reg);
    program.emit_insn(Insn::Gosub {
        target_pc: add_change_label,
        return_reg,
    });
    program.emit_insn(Insn::Next {
        cursor_id: presence_cursor,
        pc_if_next: presence_loop,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(presence_done);

    // Each changed ON match changes `matches` of its first-input row.
    let match_weight_reg = program.alloc_register();
    for &(sides, negate) in binary_join_phases(input) {
        emit_join_phase(
            program,
            &mut resolver,
            view_name,
            schema,
            contract,
            &sides,
            negate,
            match_weight_reg,
            &mut |program, cursors| {
                emit_join_input_identity(program, sides[0], rows, cursors[0], row_reg)?;
                program.emit_int(0, present_change_reg);
                program.emit_insn(Insn::Copy {
                    src_reg: match_weight_reg,
                    dst_reg: matches_change_reg,
                    extra_amount: 0,
                });
                program.emit_insn(Insn::Gosub {
                    target_pc: add_change_label,
                    return_reg,
                });
                Ok(())
            },
        )?;
    }

    // Visit each changed row once.
    // State record: [present, matches, published, output image...], where the
    // image is the row's values followed by its available binding rowids.
    let image_width = output.width
        + output
            .binding_rowid_columns
            .iter()
            .filter(|column| column.is_some())
            .count();
    let state_width = 3 + image_width;
    let state_start = program.alloc_registers(state_width);
    let present_reg = state_start;
    let matches_reg = state_start + 1;
    let published_reg = state_start + 2;
    let image_start = state_start + 3;
    let image_binding_rowid_regs = {
        let mut next = image_start + output.width;
        output
            .binding_rowid_columns
            .iter()
            .map(|column| {
                column.map(|_| {
                    next += 1;
                    next - 1
                })
            })
            .collect::<Vec<_>>()
    };
    let own_delta_reg = sums_start + 2;
    let new_weight_reg = program.alloc_register();
    let retract_weight_reg = program.alloc_register();
    let state_record_reg = program.alloc_register();

    let visit_loop = program.allocate_label();
    let visit_next = program.allocate_label();
    let visit_done = program.allocate_label();
    let new_row = program.allocate_label();
    let counted = program.allocate_label();
    let has_rows = program.allocate_label();
    let decided = program.allocate_label();
    let publish = program.allocate_label();
    let add_new_image = program.allocate_label();
    let store = program.allocate_label();
    let delete_state = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: changes_cursor,
        pc_if_empty: visit_done,
    });
    program.preassign_label_to_next_insn(visit_loop);
    program.emit_insn(Insn::RowId {
        cursor_id: changes_cursor,
        dest: row_reg,
    });
    for column in 0..3 {
        program.emit_insn(Insn::Column {
            cursor_id: changes_cursor,
            column,
            dest: sums_start + column,
            default: None,
        });
    }
    program.emit_insn(Insn::NotExists {
        cursor: state_cursor,
        rowid_reg: row_reg,
        target_pc: new_row,
    });
    for column in 0..state_width {
        program.emit_insn(Insn::Column {
            cursor_id: state_cursor,
            column,
            dest: state_start + column,
            default: None,
        });
    }
    program.emit_insn(Insn::Goto { target_pc: counted });
    program.preassign_label_to_next_insn(new_row);
    for reg in [present_reg, matches_reg, published_reg] {
        program.emit_int(0, reg);
    }
    if image_width > 0 {
        program.emit_insn(Insn::Null {
            dest: image_start,
            dest_end: Some(image_start + image_width - 1),
        });
    }
    program.preassign_label_to_next_insn(counted);
    program.emit_insn(Insn::Add {
        lhs: present_reg,
        rhs: sums_start,
        dest: present_reg,
    });
    program.emit_insn(Insn::Add {
        lhs: matches_reg,
        rhs: sums_start + 1,
        dest: matches_reg,
    });
    // Counts are integrals of z-sets that can only hold existing rows and
    // pairs: neither is negative, and a row that is gone has no matches.
    for reg in [present_reg, matches_reg] {
        program.emit_insn(Insn::Lt {
            lhs: reg,
            rhs: zero_reg,
            target_pc: corrupt_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
    }
    program.emit_insn(Insn::If {
        reg: present_reg,
        target_pc: has_rows,
        jump_if_null: false,
    });
    program.emit_insn(Insn::If {
        reg: matches_reg,
        target_pc: corrupt_label,
        jump_if_null: false,
    });
    program.preassign_label_to_next_insn(has_rows);
    program.emit_int(0, new_weight_reg);
    if keeps_matched_rows {
        program.emit_insn(Insn::IfNot {
            reg: matches_reg,
            target_pc: decided,
            jump_if_null: false,
        });
    } else {
        program.emit_insn(Insn::If {
            reg: matches_reg,
            target_pc: decided,
            jump_if_null: false,
        });
    }
    program.emit_insn(Insn::Copy {
        src_reg: present_reg,
        dst_reg: new_weight_reg,
        extra_amount: 0,
    });
    program.preassign_label_to_next_insn(decided);
    // Republish when the weight changes, or when the row's own delta may have
    // changed its values while it stays in the output.
    program.emit_insn(Insn::Ne {
        lhs: new_weight_reg,
        rhs: published_reg,
        target_pc: publish,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::IfNot {
        reg: own_delta_reg,
        target_pc: store,
        jump_if_null: false,
    });
    program.emit_insn(Insn::IfNot {
        reg: new_weight_reg,
        target_pc: store,
        jump_if_null: false,
    });
    program.preassign_label_to_next_insn(publish);
    program.emit_insn(Insn::IfNot {
        reg: published_reg,
        target_pc: add_new_image,
        jump_if_null: false,
    });
    program.emit_insn(Insn::Subtract {
        lhs: zero_reg,
        rhs: published_reg,
        dest: retract_weight_reg,
    });
    emit_ephemeral_delta_row(
        program,
        &output,
        row_reg,
        &image_binding_rowid_regs,
        image_start,
        retract_weight_reg,
    );
    program.preassign_label_to_next_insn(add_new_image);
    program.emit_insn(Insn::IfNot {
        reg: new_weight_reg,
        target_pc: store,
        jump_if_null: false,
    });
    // The arrangement is already in its post-statement state.
    program.emit_insn(Insn::SeekRowid {
        cursor_id: rows_cursor,
        src_reg: row_reg,
        target_pc: corrupt_label,
    });
    for (column, physical_column) in rows.arrangement.value_columns().iter().enumerate() {
        program.emit_column_or_rowid(rows_cursor, *physical_column, image_start + column);
    }
    for (binding, reg) in image_binding_rowid_regs.iter().enumerate() {
        if let Some(reg) = reg {
            turso_assert!(
                emit_join_input_binding_rowid(
                    program,
                    JoinSide::Btree,
                    rows,
                    rows_cursor,
                    binding,
                    *reg,
                )?,
                "semi or anti join output rowid provenance is unavailable from its input"
            );
        }
    }
    emit_ephemeral_delta_row(
        program,
        &output,
        row_reg,
        &image_binding_rowid_regs,
        image_start,
        new_weight_reg,
    );
    program.preassign_label_to_next_insn(store);
    program.emit_insn(Insn::Copy {
        src_reg: new_weight_reg,
        dst_reg: published_reg,
        extra_amount: 0,
    });
    // `present` is zero only when `matches` is zero too (checked above).
    program.emit_insn(Insn::IfNot {
        reg: present_reg,
        target_pc: delete_state,
        jump_if_null: false,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(state_start),
        count: to_u32(state_width),
        dest_reg: to_u32(state_record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: state_cursor,
        key_reg: row_reg,
        record_reg: state_record_reg,
        flag: InsertFlags(
            InsertFlags::REQUIRE_SEEK
                | InsertFlags::SKIP_LAST_ROWID
                | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
        ),
        table_name: state_name.clone(),
    });
    program.emit_insn(Insn::Goto {
        target_pc: visit_next,
    });
    program.preassign_label_to_next_insn(delete_state);
    program.emit_insn(Insn::NotExists {
        cursor: state_cursor,
        rowid_reg: row_reg,
        target_pc: visit_next,
    });
    program.emit_insn(Insn::Delete {
        cursor_id: state_cursor,
        table_name: state_name,
        is_part_of_update: true,
    });
    program.preassign_label_to_next_insn(visit_next);
    program.emit_insn(Insn::Next {
        cursor_id: changes_cursor,
        pc_if_next: visit_loop,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(visit_done);

    program.emit_insn(Insn::Goto {
        target_pc: end_label,
    });
    program.preassign_label_to_next_insn(corrupt_label);
    program.emit_insn(Insn::Halt {
        err_code: crate::error::SQLITE_ERROR,
        description: format!("materialized view {view_name} semi or anti join state is corrupted"),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(end_label);
    drop(syms);
    EmittedNodeOutput::from_ephemeral(node_id, output, output_contract)
}

/// Emit the subroutine that adds one change to a row's sums in the scratch
/// table: `(row_reg, present_change_reg, matches_change_reg)` in, sums
/// `(present change, matches change, own delta seen)` stored under `row_reg`.
#[allow(clippy::too_many_arguments)]
fn emit_add_change_subroutine(
    program: &mut ProgramBuilder,
    label: crate::vdbe::BranchOffset,
    changes_cursor: usize,
    row_reg: usize,
    present_change_reg: usize,
    matches_change_reg: usize,
    sums_start: usize,
    sums_record_reg: usize,
    zero_reg: usize,
    return_reg: usize,
) {
    let first_change = program.allocate_label();
    let add = program.allocate_label();
    let write = program.allocate_label();
    program.preassign_label_to_next_insn(label);
    program.emit_insn(Insn::NotExists {
        cursor: changes_cursor,
        rowid_reg: row_reg,
        target_pc: first_change,
    });
    for column in 0..3 {
        program.emit_insn(Insn::Column {
            cursor_id: changes_cursor,
            column,
            dest: sums_start + column,
            default: None,
        });
    }
    program.emit_insn(Insn::Goto { target_pc: add });
    program.preassign_label_to_next_insn(first_change);
    for column in 0..3 {
        program.emit_int(0, sums_start + column);
    }
    program.preassign_label_to_next_insn(add);
    program.emit_insn(Insn::Add {
        lhs: sums_start,
        rhs: present_change_reg,
        dest: sums_start,
    });
    program.emit_insn(Insn::Add {
        lhs: sums_start + 1,
        rhs: matches_change_reg,
        dest: sums_start + 1,
    });
    program.emit_insn(Insn::Eq {
        lhs: present_change_reg,
        rhs: zero_reg,
        target_pc: write,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_int(1, sums_start + 2);
    program.preassign_label_to_next_insn(write);
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(sums_start),
        count: 3,
        dest_reg: to_u32(sums_record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: changes_cursor,
        key_reg: row_reg,
        record_reg: sums_record_reg,
        flag: InsertFlags::new()
            .require_seek()
            .is_ephemeral_table_insert(),
        table_name: String::new(),
    });
    program.emit_insn(Insn::Return {
        return_reg,
        can_fallthrough: false,
    });
}

#[cfg(test)]
mod tests {
    use super::super::{compile_maintenance_program, MaintenanceInput};
    use crate::sync::Arc;
    use crate::vdbe::builder::CursorType;
    use crate::vdbe::insn::Insn;
    use crate::vdbe::Program;
    use crate::{ConnectOpts, Connection, Database, DatabaseOpts, OpenFlags, SqliteDialect};

    /// A join on an INTEGER PRIMARY KEY and an indexed column never reads
    /// either table in full: each changed row, and each row of the other
    /// table while populating, finds its matches by rowid or through the
    /// index.
    #[test]
    fn key_join_never_reads_a_whole_table() {
        let conn = open_views_db("join-key-lookup.db");
        conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("CREATE TABLE child(pid INT, v INT)").unwrap();
        conn.execute("CREATE INDEX child_pid ON child(pid)")
            .unwrap();
        conn.execute(
            "CREATE MATERIALIZED VIEW v AS \
             SELECT c.v, p.name FROM child c JOIN parent p ON c.pid = p.id",
        )
        .unwrap();

        let maintenance = compile_view_program(&conn, "v", MaintenanceInput::TransactionDelta);
        assert_eq!(full_reads_of(&maintenance, "parent"), 0);
        assert_eq!(full_reads_of(&maintenance, "child"), 0);
        assert!(rowid_lookups_of(&maintenance, "parent") > 0);
        assert!(rowid_lookups_of(&maintenance, "child") > 0);

        let population = compile_view_program(&conn, "v", MaintenanceInput::BaseTable);
        assert_eq!(full_reads_of(&population, "parent"), 0);
        assert!(rowid_lookups_of(&population, "parent") > 0);
    }

    fn open_views_db(name: &str) -> Arc<Connection> {
        #[allow(clippy::arc_with_non_send_sync)]
        let io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            name,
            OpenFlags::default(),
            DatabaseOpts::new().with_views(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        db.connect(ConnectOpts::new()).unwrap()
    }

    fn compile_view_program(
        conn: &Arc<Connection>,
        view_name: &str,
        input: MaintenanceInput,
    ) -> Program {
        let schema = conn.schema.read().clone();
        let view = schema.get_materialized_view(view_name).unwrap();
        let (select, root_page, num_columns) = {
            let view = view.lock();
            (
                view.select_stmt.clone(),
                view.get_root_page(),
                view.column_schema.flat_columns().unwrap().len(),
            )
        };
        compile_maintenance_program(
            view_name,
            &select,
            root_page,
            num_columns,
            input,
            &schema,
            conn,
        )
        .unwrap()
    }

    fn full_reads_of(program: &Program, table_name: &str) -> usize {
        program
            .insns
            .iter()
            .filter(|(insn, _)| {
                matches!(insn, Insn::Rewind { cursor_id, .. } if reads_table(program, *cursor_id, table_name))
            })
            .count()
    }

    fn rowid_lookups_of(program: &Program, table_name: &str) -> usize {
        program
            .insns
            .iter()
            .filter(|(insn, _)| match insn {
                Insn::SeekRowid { cursor_id, .. }
                | Insn::SeekGE {
                    cursor_id,
                    is_index: false,
                    ..
                } => reads_table(program, *cursor_id, table_name),
                _ => false,
            })
            .count()
    }

    fn reads_table(program: &Program, cursor_id: usize, table_name: &str) -> bool {
        matches!(
            &program.cursor_ref[cursor_id].1,
            CursorType::BTreeTable(table) if table.name == table_name
        )
    }
}
