use super::*;

const JOIN_ARITY: usize = 2;

/// One physical input edge of a binary join.
///
/// Keeping the delta, logical schema, and maintained arrangement together
/// prevents emitters from accidentally combining parallel slices belonging
/// to different inputs.
struct JoinInputContract {
    delta: DeltaSource,
    schema: Arc<dag::StreamSchema>,
    arrangement: ArrangementHandle,
}

/// The complete physical contract of one binary join node.
pub(super) struct JoinContract {
    inputs: [JoinInputContract; JOIN_ARITY],
    on: Vec<ast::Expr>,
}

impl JoinContract {
    pub(super) fn from_outputs(
        outputs: [&NodeOutput; JOIN_ARITY],
        schemas: [Arc<dag::StreamSchema>; JOIN_ARITY],
        on: &[ast::Expr],
    ) -> Result<Self> {
        let input = |position: usize| -> Result<JoinInputContract> {
            let output = outputs[position];
            let schema = schemas[position].clone();
            let arrangement = output.arrangement.clone().ok_or_else(|| {
                LimboError::InternalError("join input has no materialized arrangement".to_string())
            })?;
            if arrangement.value_columns().len() != schema.len() {
                return Err(LimboError::InternalError(
                    "join arrangement does not match its logical input width".to_string(),
                ));
            }
            if arrangement.binding_rowid_columns().len() != schema.bindings.len() {
                return Err(LimboError::InternalError(
                    "join arrangement does not match its logical binding provenance".to_string(),
                ));
            }
            if arrangement.identity() != output.delta.identity() {
                return Err(LimboError::InternalError(
                    "join delta and arrangement expose different identities".to_string(),
                ));
            }
            Ok(JoinInputContract {
                delta: output.delta.clone(),
                schema,
                arrangement,
            })
        };
        Ok(Self {
            inputs: [input(0)?, input(1)?],
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

fn emit_join_input_binding_rowid(
    program: &mut ProgramBuilder,
    side: JoinSide,
    delta: &DeltaSource,
    arrangement: &ArrangementHandle,
    cursor_id: usize,
    binding: usize,
    dest: usize,
) -> bool {
    match (side, delta) {
        (JoinSide::Delta, DeltaSource::BaseTable { table, .. }) => {
            turso_assert!(
                binding == 0,
                "a base-table delta exposes exactly one logical binding"
            );
            if !table.has_rowid {
                return false;
            }
            program.emit_insn(Insn::RowId { cursor_id, dest });
            true
        }
        (JoinSide::Delta, DeltaSource::Ephemeral(channel)) => {
            let Some(column) = channel
                .binding_rowid_columns
                .get(binding)
                .copied()
                .flatten()
            else {
                return false;
            };
            program.emit_insn(Insn::Column {
                cursor_id,
                column,
                dest,
                default: None,
            });
            true
        }
        (JoinSide::Btree, _) => {
            let Some(column) = arrangement
                .binding_rowid_columns()
                .get(binding)
                .copied()
                .flatten()
            else {
                return false;
            };
            emit_arrangement_column_value(program, column, cursor_id, dest);
            true
        }
    }
}

fn emit_join_input_identity(
    program: &mut ProgramBuilder,
    side: JoinSide,
    delta: &DeltaSource,
    arrangement: &ArrangementHandle,
    cursor_id: usize,
    dest: usize,
) -> usize {
    let identity = match side {
        JoinSide::Delta => delta.identity(),
        JoinSide::Btree => arrangement.identity(),
    };
    match (side, delta) {
        (JoinSide::Delta, DeltaSource::BaseTable { .. }) => {
            turso_assert!(
                identity == DeltaIdentity::BindingRowids(1),
                "base-table delta identity must be its rowid"
            );
            program.emit_insn(Insn::RowId { cursor_id, dest });
        }
        (JoinSide::Delta, DeltaSource::Ephemeral(channel)) => {
            for column in 0..identity.width() {
                program.emit_insn(Insn::Column {
                    cursor_id: channel.cursor_id,
                    column,
                    dest: dest + column,
                    default: None,
                });
            }
        }
        (JoinSide::Btree, _) => {
            for column in 0..identity.width() {
                emit_arrangement_identity_value(
                    program,
                    arrangement,
                    cursor_id,
                    column,
                    dest + column,
                );
            }
        }
    }
    identity.width()
}

/// Emit one join delta phase: nested loops over every joined table — reading
/// the transaction delta for tables in the phase's subset and the post-state
/// btree for the rest — with the delta tables outermost so the probe scans
/// run on the btree side. At the innermost level the ON predicates and WHERE
/// are evaluated, the signed weight product lands in `w_reg`, and `sink`
/// emits the per-match work with all cursors positioned (its slice maps
/// table positions to cursor ids).
#[allow(clippy::too_many_arguments)]
fn emit_join_phase(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    view_name: &str,
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

    let mut open_input = |position: usize| -> Result<usize> {
        let input = &contract.inputs[position];
        let storage = input.arrangement.table();
        match sides[position] {
            JoinSide::Delta => match &input.delta {
                DeltaSource::BaseTable {
                    table: delta_table, ..
                } => {
                    turso_assert!(
                        input.schema.len() == delta_table.columns().len()
                            && (Arc::ptr_eq(storage, delta_table)
                                || storage.root_page == delta_table.root_page),
                        "join delta and arrangement must describe the same relation"
                    );
                    let cursor_id = program.alloc_cursor_id(CursorType::ViewDelta {
                        view_name: view_name.to_string(),
                        table: delta_table.clone(),
                    });
                    program.emit_insn(Insn::OpenRead {
                        cursor_id,
                        root_page: 0,
                        db: 0,
                    });
                    Ok(cursor_id)
                }
                DeltaSource::Ephemeral(channel) => Ok(channel.cursor_id),
            },
            JoinSide::Btree => {
                let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(storage.clone()));
                program.emit_insn(Insn::OpenRead {
                    cursor_id,
                    root_page: storage.root_page,
                    db: 0,
                });
                Ok(cursor_id)
            }
        }
    };
    let cursor_ids = [open_input(0)?, open_input(1)?];

    // Nest delta tables outermost.
    let mut order = [0, 1];
    order.sort_by_key(|&i| sides[i] == JoinSide::Btree);

    let phase_done_label = program.allocate_label();
    let loop_labels = std::array::from_fn::<_, JOIN_ARITY, _>(|_| program.allocate_label());
    let next_labels = std::array::from_fn::<_, JOIN_ARITY, _>(|_| program.allocate_label());
    let delta_w_regs = std::array::from_fn::<_, JOIN_ARITY, _>(|position| {
        (sides[position] == JoinSide::Delta).then(|| program.alloc_register())
    });
    let arrangement_count_regs = std::array::from_fn::<_, JOIN_ARITY, _>(|position| {
        (sides[position] == JoinSide::Btree
            && contract.inputs[position]
                .arrangement
                .count_column()
                .is_some())
        .then(|| program.alloc_register())
    });

    for (depth, &pos) in order.iter().enumerate() {
        program.emit_insn(Insn::Rewind {
            cursor_id: cursor_ids[pos],
            pc_if_empty: if depth == 0 {
                phase_done_label
            } else {
                next_labels[depth - 1]
            },
        });
        program.preassign_label_to_next_insn(loop_labels[depth]);
        if let Some(w) = delta_w_regs[pos] {
            let weight_column = match &contract.inputs[pos].delta {
                DeltaSource::BaseTable { .. } => contract.inputs[pos].schema.len(),
                DeltaSource::Ephemeral(channel) => channel.weight_column,
            };
            program.emit_insn(Insn::Column {
                cursor_id: cursor_ids[pos],
                column: weight_column,
                dest: w,
                default: None,
            });
        }
        if let (Some(count_reg), Some(count_column)) = (
            arrangement_count_regs[pos],
            contract.inputs[pos].arrangement.count_column(),
        ) {
            program.emit_insn(Insn::Column {
                cursor_id: cursor_ids[pos],
                column: count_column,
                dest: count_reg,
                default: None,
            });
        }
    }

    // Bind every logical column to the physical arrangement/delta slot used
    // in this phase. This keeps expression translation independent of record
    // layout (aggregate arrangements can project away accumulator payloads,
    // while ephemeral deltas carry identity before their values).
    resolver.enable_expr_to_reg_cache();
    for pos in 0..JOIN_ARITY {
        let input = &contract.inputs[pos];
        for (logical_column, column) in input.schema.columns.iter().enumerate() {
            let Some(expr) = &column.expr else {
                continue;
            };
            let physical_column = match (&sides[pos], &input.delta) {
                (JoinSide::Delta, DeltaSource::Ephemeral(channel)) => {
                    channel.value_start + logical_column
                }
                (JoinSide::Delta, DeltaSource::BaseTable { .. }) => logical_column,
                (JoinSide::Btree, _) => input.arrangement.value_columns()[logical_column],
            };
            let value_reg = program.alloc_register();
            match (&sides[pos], &input.delta) {
                (JoinSide::Delta, DeltaSource::Ephemeral(_)) => {
                    program.emit_insn(Insn::Column {
                        cursor_id: cursor_ids[pos],
                        column: physical_column,
                        dest: value_reg,
                        default: None,
                    });
                }
                _ => program.emit_column_or_rowid(cursor_ids[pos], physical_column, value_reg),
            }
            let bound = remap_bound_expr(expr, &binding_remap)?;
            resolver.cache_expr_reg(std::borrow::Cow::Owned(bound), value_reg, false, None);
        }
        for (binding_index, binding) in input.schema.bindings.iter().enumerate() {
            let rowid_reg = program.alloc_register();
            if !emit_join_input_binding_rowid(
                program,
                sides[pos],
                &input.delta,
                &input.arrangement,
                cursor_ids[pos],
                binding_index,
                rowid_reg,
            ) {
                continue;
            }
            let phase_id = *binding_remap.get(&binding.logical_id).ok_or_else(|| {
                LimboError::InternalError(
                    "join binding rowid has no phase-local binding".to_string(),
                )
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
    }

    // ON predicates and WHERE: a FALSE or NULL result means this combination
    // is not a join output.
    let innermost_next = next_labels[JOIN_ARITY - 1];
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
                jump_target_when_false: innermost_next,
                jump_target_when_null: innermost_next,
            },
            resolver,
        )?;
        program.preassign_label_to_next_insn(pred_true);
    }

    // Signed weight: the product of the delta weights times the phase sign.
    program.emit_int(if negate { -1 } else { 1 }, w_reg);
    for w in delta_w_regs.iter().flatten() {
        program.emit_insn(Insn::Multiply {
            lhs: w_reg,
            rhs: *w,
            dest: w_reg,
        });
    }
    for count in arrangement_count_regs.iter().flatten() {
        program.emit_insn(Insn::Multiply {
            lhs: w_reg,
            rhs: *count,
            dest: w_reg,
        });
    }

    sink(program, &cursor_ids)?;

    for depth in (0..JOIN_ARITY).rev() {
        program.preassign_label_to_next_insn(next_labels[depth]);
        program.emit_insn(Insn::Next {
            cursor_id: cursor_ids[order[depth]],
            pc_if_next: loop_labels[depth],
        });
    }
    program.preassign_label_to_next_insn(phase_done_label);
    Ok(())
}

/// Emit a join's natural delta row into a typed ephemeral stream. The join
/// knows only its declared inputs and predicates; downstream group keys,
/// aggregate arguments, filters, and projections are evaluated by their own
/// operators from this stream.
///
/// Returns the ephemeral cursor and its row layout. The cursor's
/// [`dag::StreamSchema`] owns the logical binding namespace used by the
/// aggregate consumer.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_join_deltas_to_ephemeral(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    view_name: &str,
    output_contract: &NodeOutputContract,
    contract: &JoinContract,
    input: MaintenanceInput,
) -> Result<DeltaSource> {
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
        &format!("{view_name}_joined_delta"),
        output_contract.schema.as_ref().clone(),
        output_contract.emitted_identity,
        output_contract.binding_rowids.clone(),
        true,
    );
    let eph_cursor_id = channel.cursor_id;

    // [transport identity..., binding rowids..., natural join row..., weight].
    let eph_rec_start = program.alloc_registers(channel.record_width());
    let eph_value_start = eph_rec_start + channel.value_start;
    let eph_weight_reg = eph_rec_start + channel.weight_column;
    let eph_rowid_reg = program.alloc_register();
    let eph_record_reg = program.alloc_register();

    let emit_eph_insert = |program: &mut ProgramBuilder| {
        program.emit_insn(Insn::MakeRecord {
            start_reg: eph_rec_start as u16,
            count: channel.record_width() as u16,
            dest_reg: eph_record_reg as u16,
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
            resolver,
            view_name,
            contract,
            &sides,
            negate,
            eph_weight_reg,
            &mut |program, cursors| {
                match channel.identity {
                    DeltaIdentity::BindingRowids(expected_width) => {
                        let mut destination = eph_rec_start;
                        for (position, input) in contract.inputs.iter().enumerate() {
                            let source_identity = match sides[position] {
                                JoinSide::Delta => input.delta.identity(),
                                JoinSide::Btree => input.arrangement.identity(),
                            };
                            turso_assert!(
                                matches!(source_identity, DeltaIdentity::BindingRowids(_)),
                                "binding-rowid join output requires binding-rowid inputs"
                            );
                            destination += emit_join_input_identity(
                                program,
                                sides[position],
                                &input.delta,
                                &input.arrangement,
                                cursors[position],
                                destination,
                            );
                        }
                        turso_assert!(
                            destination == eph_rec_start + expected_width,
                            "join binding-rowid output width must equal its input identities"
                        );
                    }
                    DeltaIdentity::OperatorKey(2) => {
                        for (position, input) in contract.inputs.iter().enumerate() {
                            let source_identity = match sides[position] {
                                JoinSide::Delta => input.delta.identity(),
                                JoinSide::Btree => input.arrangement.identity(),
                            };
                            let source_start = program.alloc_registers(source_identity.width());
                            let width = emit_join_input_identity(
                                program,
                                sides[position],
                                &input.delta,
                                &input.arrangement,
                                cursors[position],
                                source_start,
                            );
                            program.emit_insn(Insn::MakeRecord {
                                start_reg: source_start as u16,
                                count: width as u16,
                                dest_reg: (eph_rec_start + position) as u16,
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
                                        &input.delta,
                                        &input.arrangement,
                                        cursors[position],
                                        input_binding,
                                        eph_rec_start + output_column,
                                    ),
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
                        match (&sides[position], &input.delta) {
                            (JoinSide::Delta, DeltaSource::Ephemeral(channel)) => {
                                program.emit_insn(Insn::Column {
                                    cursor_id: *cursor_id,
                                    column: channel.value_start + column,
                                    dest: destination,
                                    default: None,
                                });
                            }
                            (JoinSide::Btree, _) => program.emit_column_or_rowid(
                                *cursor_id,
                                input.arrangement.value_columns()[column],
                                destination,
                            ),
                            (JoinSide::Delta, DeltaSource::BaseTable { .. }) => {
                                program.emit_column_or_rowid(*cursor_id, column, destination);
                            }
                        }
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

    Ok(DeltaSource::Ephemeral(channel))
}

#[allow(clippy::too_many_arguments)]
fn emit_operator_keyed_ephemeral_row(
    program: &mut ProgramBuilder,
    output: &EphemeralDelta,
    kind: i64,
    source_identity_start: usize,
    source_identity_width: usize,
    binding_rowid_regs: &[Option<usize>],
    values_start: usize,
    weight_reg: usize,
) {
    turso_assert!(
        output.identity == DeltaIdentity::OperatorKey(2)
            && source_identity_width > 0
            && binding_rowid_regs.len() == output.binding_rowid_columns.len()
            && output.weight_column == output.value_start + output.width,
        "operator-keyed delta rows carry kind, packed source identity, values, and weight"
    );
    let record_start = program.alloc_registers(output.record_width());
    program.emit_int(kind, record_start);
    program.emit_insn(Insn::MakeRecord {
        start_reg: source_identity_start as u16,
        count: source_identity_width as u16,
        dest_reg: (record_start + 1) as u16,
        index_name: None,
        affinity_str: None,
    });
    for (column, source_reg) in output.binding_rowid_columns.iter().zip(binding_rowid_regs) {
        match (column, source_reg) {
            (Some(column), Some(source_reg)) => program.emit_insn(Insn::Copy {
                src_reg: *source_reg,
                dst_reg: record_start + *column,
                extra_amount: 0,
            }),
            (None, None) => {}
            _ => turso_assert!(
                false,
                "operator-keyed delta row does not satisfy its provenance contract"
            ),
        }
    }
    if output.width > 0 {
        program.emit_insn(Insn::Copy {
            src_reg: values_start,
            dst_reg: record_start + output.value_start,
            extra_amount: output.width - 1,
        });
    }
    program.emit_insn(Insn::Copy {
        src_reg: weight_reg,
        dst_reg: record_start + output.weight_column,
        extra_amount: 0,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: record_start as u16,
        count: output.record_width() as u16,
        dest_reg: record_reg as u16,
        index_name: None,
        affinity_str: None,
    });
    let rowid_reg = program.alloc_register();
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
}

fn emit_natural_join_values(
    program: &mut ProgramBuilder,
    contract: &JoinContract,
    cursors: &[usize; JOIN_ARITY],
    values_start: usize,
) -> usize {
    let mut destination = values_start;
    for (input, cursor) in contract.inputs.iter().zip(cursors) {
        for column in 0..input.schema.len() {
            program.emit_column_or_rowid(
                *cursor,
                input.arrangement.value_columns()[column],
                destination,
            );
            destination += 1;
        }
    }
    destination
}

/// Emit a two-input LEFT JOIN as one typed z-set stream.
///
/// Matched rows use `(0, packed(left identity, right identity))`; padded rows
/// use `(1, packed(aux rowid))`. The auxiliary table persists the padded
/// natural row so a departing left row can publish its old image. An explicit
/// output arrangement then normalizes both key forms to one operator rowid
/// before any downstream consumer sees them.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_left_join_deltas_to_ephemeral(
    program: &mut ProgramBuilder,
    view_name: &str,
    node_id: dag::NodeId,
    contract: &JoinContract,
    output_contract: &NodeOutputContract,
    input: MaintenanceInput,
    operator_state: &OperatorStateDef,
    schema: &Schema,
    connection: &Arc<Connection>,
) -> Result<DeltaSource> {
    let output = open_ephemeral_delta(
        program,
        &format!("{view_name}_left_join_delta_{node_id}"),
        output_contract.schema.as_ref().clone(),
        output_contract.emitted_identity,
        output_contract.binding_rowids.clone(),
        true,
    );
    let natural_width = contract
        .inputs
        .iter()
        .map(|input| input.schema.len())
        .sum::<usize>();
    turso_assert!(
        output.width == natural_width,
        "LEFT JOIN output stream must contain its natural row"
    );

    let aux_name = operator_state.auxiliary_table_name()?.to_string();
    let aux_table = schema.get_btree_table(&aux_name).ok_or_else(|| {
        LimboError::InternalError(format!(
            "aux table {aux_name} of materialized view {view_name} not found"
        ))
    })?;
    let aux_index = schema
        .get_indices(&aux_name)
        .next()
        .cloned()
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "aux table {aux_name} of materialized view {view_name} has no index"
            ))
        })?;

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

    let zero_reg = program.alloc_register();
    let one_reg = program.alloc_register();
    let prev_rowid_scratch = program.alloc_register();
    let corrupt_label = program.allocate_label();
    let main_start_label = program.allocate_label();
    let end_label = program.allocate_label();
    let pair_identity_start = program.alloc_registers(JOIN_ARITY);
    let matched_values_start = program.alloc_registers(natural_width);
    let matched_weight_reg = program.alloc_register();
    let binding_rowid_width = output
        .binding_rowid_columns
        .iter()
        .filter(|column| column.is_some())
        .count();
    let binding_rowid_registers = |start| {
        let mut next = start;
        output
            .binding_rowid_columns
            .iter()
            .map(|column| {
                column.map(|_| {
                    let register = next;
                    next += 1;
                    register
                })
            })
            .collect::<Vec<_>>()
    };
    let matched_binding_rowids_start = program.alloc_registers(binding_rowid_width);
    let matched_binding_rowid_regs = binding_rowid_registers(matched_binding_rowids_start);

    let mut open_arrangement = |position: usize| {
        let table = contract.inputs[position].arrangement.table();
        let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
        program.emit_insn(Insn::OpenRead {
            cursor_id,
            root_page: table.root_page,
            db: 0,
        });
        cursor_id
    };
    let pad_cursors = [open_arrangement(0), open_arrangement(1)];
    let outer = LeftJoinAux::open(
        program,
        aux_name,
        &aux_table,
        &aux_index,
        natural_width + binding_rowid_width,
        zero_reg,
        prev_rowid_scratch,
        corrupt_label,
    );
    let padded_values_start = outer.content_start;
    let padded_binding_rowids_start = padded_values_start + natural_width;
    let padded_binding_rowid_regs = binding_rowid_registers(padded_binding_rowids_start);

    program.emit_int(0, zero_reg);
    program.emit_int(1, one_reg);
    program.emit_insn(Insn::Goto {
        target_pc: main_start_label,
    });

    let mut emit_padded_add = |program: &mut ProgramBuilder,
                               pad_cursors: &[usize; JOIN_ARITY],
                               srid_reg: usize|
     -> Result<()> {
        let end = emit_natural_join_values(program, contract, pad_cursors, padded_values_start);
        turso_assert!(
            end == padded_values_start + natural_width,
            "padded LEFT JOIN row does not match its output schema"
        );
        let mut output_binding = 0;
        for (position, input) in contract.inputs.iter().enumerate() {
            for input_binding in 0..input.schema.bindings.len() {
                if let Some(dest) = padded_binding_rowid_regs[output_binding] {
                    if position == 0 {
                        turso_assert!(
                            emit_join_input_binding_rowid(
                                program,
                                JoinSide::Btree,
                                &input.delta,
                                &input.arrangement,
                                pad_cursors[position],
                                input_binding,
                                dest,
                            ),
                            "LEFT JOIN padded row requires its left rowid provenance"
                        );
                    } else {
                        program.emit_insn(Insn::Null {
                            dest,
                            dest_end: None,
                        });
                    }
                }
                output_binding += 1;
            }
        }
        turso_assert!(
            output_binding == output.schema.bindings.len(),
            "LEFT JOIN padded row provenance must match its output bindings"
        );
        emit_operator_keyed_ephemeral_row(
            program,
            &output,
            1,
            srid_reg,
            1,
            &padded_binding_rowid_regs,
            padded_values_start,
            one_reg,
        );
        Ok(())
    };
    let mut emit_padded_remove = |program: &mut ProgramBuilder, srid_reg: usize| -> Result<()> {
        let negative_one_reg = program.alloc_register();
        program.emit_int(-1, negative_one_reg);
        emit_operator_keyed_ephemeral_row(
            program,
            &output,
            1,
            srid_reg,
            1,
            &padded_binding_rowid_regs,
            padded_values_start,
            negative_one_reg,
        );
        Ok(())
    };
    outer.emit_subroutine(
        program,
        &pad_cursors,
        &mut emit_padded_add,
        &mut emit_padded_remove,
    )?;

    program.preassign_label_to_next_insn(main_start_label);
    outer.emit_presence_pass(
        program,
        input,
        &contract.inputs[0].delta,
        &contract.inputs[0].arrangement,
        contract.inputs[0].schema.len(),
        view_name,
    )?;

    for &(sides, negate) in binary_join_phases(input) {
        emit_join_phase(
            program,
            &mut resolver,
            view_name,
            contract,
            &sides,
            negate,
            matched_weight_reg,
            &mut |program, cursors| {
                for (position, (input, cursor)) in contract.inputs.iter().zip(cursors).enumerate() {
                    match (&sides[position], &input.delta) {
                        (JoinSide::Delta, DeltaSource::Ephemeral(channel)) => {
                            turso_assert!(
                                channel.identity == DeltaIdentity::OperatorRowid,
                                "arranged LEFT JOIN delta must carry its arrangement rowid"
                            );
                            program.emit_insn(Insn::Column {
                                cursor_id: *cursor,
                                column: 0,
                                dest: pair_identity_start + position,
                                default: None,
                            });
                        }
                        _ => program.emit_insn(Insn::RowId {
                            cursor_id: *cursor,
                            dest: pair_identity_start + position,
                        }),
                    }
                }
                outer.emit_count_match(program, pair_identity_start, matched_weight_reg);
                let mut destination = matched_values_start;
                for (position, (input, cursor)) in contract.inputs.iter().zip(cursors).enumerate() {
                    for column in 0..input.schema.len() {
                        match (&sides[position], &input.delta) {
                            (JoinSide::Delta, DeltaSource::Ephemeral(channel)) => {
                                program.emit_insn(Insn::Column {
                                    cursor_id: *cursor,
                                    column: channel.value_start + column,
                                    dest: destination,
                                    default: None,
                                });
                            }
                            (JoinSide::Btree, _) => program.emit_column_or_rowid(
                                *cursor,
                                input.arrangement.value_columns()[column],
                                destination,
                            ),
                            (JoinSide::Delta, DeltaSource::BaseTable { .. }) => {
                                program.emit_column_or_rowid(*cursor, column, destination);
                            }
                        }
                        destination += 1;
                    }
                }
                turso_assert!(
                    destination == matched_values_start + natural_width,
                    "matched LEFT JOIN row does not match its output schema"
                );
                let mut output_binding = 0;
                for (position, input) in contract.inputs.iter().enumerate() {
                    for input_binding in 0..input.schema.bindings.len() {
                        if let Some(dest) = matched_binding_rowid_regs[output_binding] {
                            turso_assert!(
                                emit_join_input_binding_rowid(
                                    program,
                                    sides[position],
                                    &input.delta,
                                    &input.arrangement,
                                    cursors[position],
                                    input_binding,
                                    dest,
                                ),
                                "matched LEFT JOIN row requires its planned rowid provenance"
                            );
                        }
                        output_binding += 1;
                    }
                }
                turso_assert!(
                    output_binding == output.schema.bindings.len(),
                    "matched LEFT JOIN provenance must match its output bindings"
                );
                emit_operator_keyed_ephemeral_row(
                    program,
                    &output,
                    0,
                    pair_identity_start,
                    JOIN_ARITY,
                    &matched_binding_rowid_regs,
                    matched_values_start,
                    matched_weight_reg,
                );
                Ok(())
            },
        )?;
    }

    program.emit_insn(Insn::Goto {
        target_pc: end_label,
    });
    program.preassign_label_to_next_insn(corrupt_label);
    program.emit_insn(Insn::Halt {
        err_code: crate::error::SQLITE_ERROR,
        description: format!("materialized view {view_name} state is corrupted"),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(end_label);
    drop(syms);
    Ok(DeltaSource::Ephemeral(output))
}

/// Consumer hook that materializes one NULL-padded output row: given the
/// padded-image cursors (left seeked, right on its null row), their table
/// references, and the aux row's rowid identifying the padded row.
type PaddedAddFn<'a> =
    dyn FnMut(&mut ProgramBuilder, &[usize; JOIN_ARITY], usize) -> Result<()> + 'a;
/// Consumer hook that retracts one NULL-padded output row, given the aux
/// row's rowid.
type PaddedRemoveFn<'a> = dyn FnMut(&mut ProgramBuilder, usize) -> Result<()> + 'a;

/// Per-left-row NULL-padded bookkeeping for a two-table LEFT (or swapped
/// RIGHT) join view.
///
/// A LEFT join's output is the inner matches plus one NULL-padded row for each
/// left row with no ON-match. The inner rows are a plain function of the
/// current inputs (the delta phases reconstruct them, retractions included,
/// from the base-table deltas), but a padded row exists only *because* its
/// left row currently has zero matches — it must appear when the left row
/// arrives unmatched and disappear the moment a match arrives, the left row
/// leaves. That is anti-join maintenance, and it needs state: this auxiliary
/// table keeps, per left row, its signed presence, its count of ON-matches,
/// and whether its padded output row is currently live.
///
/// The subroutine applies a `(l_rid, dPresent, dMatches)` delta to that state
/// and, on a transition to or from "present with zero matches", invokes the
/// consumer's `emit_padded_add` / `padded_remove` to materialize or retract
/// the one padded output row. WHERE is a separate DAG filter. Presence deltas
/// come from the left-input pass ([`Self::emit_presence_pass`]); match deltas
/// come from each inner join phase ([`Self::emit_count_match`]).
struct LeftJoinAux {
    aux_name: String,
    aux_index_name: String,
    aux_cursor_id: usize,
    aux_index_cursor_id: usize,
    /// Set by the caller before `Gosub(merge_label)`: `[l_rid, aux_rowid]`.
    akey_start: usize,
    /// The aux row's rowid slot (`akey_start + 1`), doubling as the padded
    /// output row's identity for the consumer's add/remove.
    srid_reg: usize,
    /// Set by the caller before the gosub: the signed presence delta.
    dp_reg: usize,
    /// Set by the caller before the gosub: the signed match-count delta.
    dm_reg: usize,
    return_reg: usize,
    merge_label: BranchOffset,
    // Working registers, private to the subroutine.
    rec_start: usize,
    present_reg: usize,
    matches_reg: usize,
    padded_reg: usize,
    fresh_reg: usize,
    padded_new_reg: usize,
    state_record_reg: usize,
    index_record_reg: usize,
    /// Optional padded-output payload stored in the aux row after `padded`:
    /// its first register and width. A consumer that must retract a padded
    /// row it cannot reproject after the left row is gone writes the payload
    /// here on add and reads it back on remove.
    content_start: usize,
    content_width: usize,
    // Shared scratch owned by the caller.
    zero_reg: usize,
    prev_rowid_scratch: usize,
    corrupt_label: BranchOffset,
}

impl LeftJoinAux {
    /// Open the aux table and its index and allocate the subroutine's
    /// registers. `zero_reg`, `prev_rowid_scratch`, and `corrupt_label` are
    /// shared with the caller's other subroutines.
    #[allow(clippy::too_many_arguments)]
    fn open(
        program: &mut ProgramBuilder,
        aux_name: String,
        aux_table: &Arc<BTreeTable>,
        aux_index: &Arc<crate::schema::Index>,
        content_width: usize,
        zero_reg: usize,
        prev_rowid_scratch: usize,
        corrupt_label: BranchOffset,
    ) -> Self {
        let aux_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(aux_table.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: aux_cursor_id,
            root_page: RegisterOrLiteral::Literal(aux_table.root_page),
            db: 0,
        });
        let aux_index_cursor_id =
            program.alloc_cursor_id(CursorType::BTreeIndex(aux_index.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: aux_index_cursor_id,
            root_page: RegisterOrLiteral::Literal(aux_index.root_page),
            db: 0,
        });

        // [l_rid, aux_rowid], contiguous for Found/IdxInsert/IdxDelete.
        let akey_start = program.alloc_registers(2);
        let srid_reg = akey_start + 1;
        // [l_rid, present, matches, padded, payload..], the aux row image.
        let rec_start = program.alloc_registers(4 + content_width);
        let present_reg = rec_start + 1;
        let matches_reg = rec_start + 2;
        let padded_reg = rec_start + 3;
        let content_start = rec_start + 4;

        Self {
            aux_name,
            aux_index_name: aux_index.name.clone(),
            aux_cursor_id,
            aux_index_cursor_id,
            akey_start,
            srid_reg,
            dp_reg: program.alloc_register(),
            dm_reg: program.alloc_register(),
            return_reg: program.alloc_register(),
            merge_label: program.allocate_label(),
            rec_start,
            present_reg,
            matches_reg,
            padded_reg,
            fresh_reg: program.alloc_register(),
            padded_new_reg: program.alloc_register(),
            state_record_reg: program.alloc_register(),
            index_record_reg: program.alloc_register(),
            content_start,
            content_width,
            zero_reg,
            prev_rowid_scratch,
            corrupt_label,
        }
    }

    /// Emit the aux-merge subroutine (reached by `Gosub(self.merge_label)`).
    /// `pad_cursors` describe the padded image (left cursor plus a NULL-row
    /// right cursor). The hooks materialize and retract its one output row.
    fn emit_subroutine(
        &self,
        program: &mut ProgramBuilder,
        pad_cursors: &[usize; JOIN_ARITY],
        emit_padded_add: &mut PaddedAddFn<'_>,
        emit_padded_remove: &mut PaddedRemoveFn<'_>,
    ) -> Result<()> {
        program.preassign_label_to_next_insn(self.merge_label);

        let af_found = program.allocate_label();
        let af_have_row = program.allocate_label();
        let af_transition = program.allocate_label();
        let af_padwrite = program.allocate_label();
        let af_store = program.allocate_label();
        let af_write = program.allocate_label();
        let af_ret = program.allocate_label();

        program.emit_insn(Insn::Found {
            cursor_id: self.aux_index_cursor_id,
            target_pc: af_found,
            record_reg: self.akey_start,
            num_regs: 1,
        });
        // Fresh left row (or a retraction arriving first: recorded with its
        // signs).
        program.emit_int(1, self.fresh_reg);
        program.emit_insn(Insn::NewRowid {
            cursor: self.aux_cursor_id,
            rowid_reg: self.srid_reg,
            prev_largest_reg: self.prev_rowid_scratch,
        });
        program.emit_insn(Insn::Copy {
            src_reg: self.akey_start,
            dst_reg: self.rec_start,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Copy {
            src_reg: self.dp_reg,
            dst_reg: self.present_reg,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Copy {
            src_reg: self.dm_reg,
            dst_reg: self.matches_reg,
            extra_amount: 0,
        });
        program.emit_int(0, self.padded_reg);
        // The payload is undefined until the row first becomes padded; NULL it
        // so a persisted-but-never-padded aux row carries defined columns.
        if self.content_width > 0 {
            program.emit_insn(Insn::Null {
                dest: self.content_start,
                dest_end: Some(self.content_start + self.content_width - 1),
            });
        }
        program.emit_insn(Insn::Goto {
            target_pc: af_have_row,
        });

        program.preassign_label_to_next_insn(af_found);
        program.emit_int(0, self.fresh_reg);
        program.emit_insn(Insn::IdxRowId {
            cursor_id: self.aux_index_cursor_id,
            dest: self.srid_reg,
        });
        program.emit_insn(Insn::SeekRowid {
            cursor_id: self.aux_cursor_id,
            src_reg: self.srid_reg,
            target_pc: self.corrupt_label,
        });
        for (column, dest) in [
            (1, self.present_reg),
            (2, self.matches_reg),
            (3, self.padded_reg),
        ] {
            program.emit_insn(Insn::Column {
                cursor_id: self.aux_cursor_id,
                column,
                dest,
                default: None,
            });
        }
        // Load the stored padded payload so a remove can emit the departing
        // row's old image without repositioning cursors.
        for i in 0..self.content_width {
            program.emit_insn(Insn::Column {
                cursor_id: self.aux_cursor_id,
                column: 4 + i,
                dest: self.content_start + i,
                default: None,
            });
        }
        program.emit_insn(Insn::Copy {
            src_reg: self.akey_start,
            dst_reg: self.rec_start,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Add {
            lhs: self.dp_reg,
            rhs: self.present_reg,
            dest: self.present_reg,
        });
        program.emit_insn(Insn::Add {
            lhs: self.dm_reg,
            rhs: self.matches_reg,
            dest: self.matches_reg,
        });

        program.preassign_label_to_next_insn(af_have_row);
        program.emit_int(0, self.padded_new_reg);
        program.emit_insn(Insn::Le {
            lhs: self.present_reg,
            rhs: self.zero_reg,
            target_pc: af_transition,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::If {
            reg: self.matches_reg,
            target_pc: af_transition,
            jump_if_null: false,
        });
        // Candidate padded row: position the left cursor on the row (present,
        // so the post-state btree has it), put the right cursor on its null
        // row, and let WHERE decide over that image.
        program.emit_insn(Insn::SeekRowid {
            cursor_id: pad_cursors[0],
            src_reg: self.akey_start,
            target_pc: self.corrupt_label,
        });
        program.emit_insn(Insn::NullRow {
            cursor_id: pad_cursors[1],
        });
        program.emit_int(1, self.padded_new_reg);
        program.preassign_label_to_next_insn(af_transition);
        program.emit_insn(Insn::Eq {
            lhs: self.padded_reg,
            rhs: self.padded_new_reg,
            target_pc: af_store,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        program.emit_insn(Insn::If {
            reg: self.padded_new_reg,
            target_pc: af_padwrite,
            jump_if_null: false,
        });
        // Padded before, not now: retract the padded output row.
        emit_padded_remove(program, self.srid_reg)?;
        program.emit_insn(Insn::Goto {
            target_pc: af_store,
        });

        // Not padded before, padded now: materialize it. The only path that
        // sets `padded_new` positioned the pad cursors, so the consumer reads
        // the left row with NULLs on the right.
        program.preassign_label_to_next_insn(af_padwrite);
        emit_padded_add(program, pad_cursors, self.srid_reg)?;

        program.preassign_label_to_next_insn(af_store);
        // Persist the padded bit the transitions just enacted.
        program.emit_insn(Insn::Copy {
            src_reg: self.padded_new_reg,
            dst_reg: self.padded_reg,
            extra_amount: 0,
        });
        // Delete the aux row once everything it tracks is zero; otherwise
        // (re)write it.
        program.emit_insn(Insn::If {
            reg: self.present_reg,
            target_pc: af_write,
            jump_if_null: false,
        });
        program.emit_insn(Insn::If {
            reg: self.matches_reg,
            target_pc: af_write,
            jump_if_null: false,
        });
        program.emit_insn(Insn::If {
            reg: self.fresh_reg,
            target_pc: af_ret,
            jump_if_null: false,
        });
        program.emit_insn(Insn::IdxDelete {
            start_reg: self.akey_start,
            num_regs: 2,
            cursor_id: self.aux_index_cursor_id,
            raise_error_if_no_matching_entry: true,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: self.aux_cursor_id,
            table_name: self.aux_name.clone(),
            is_part_of_update: true,
        });
        program.emit_insn(Insn::Goto { target_pc: af_ret });

        program.preassign_label_to_next_insn(af_write);
        program.emit_insn(Insn::MakeRecord {
            start_reg: self.rec_start as u16,
            count: (4 + self.content_width) as u16,
            dest_reg: self.state_record_reg as u16,
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: self.aux_cursor_id,
            key_reg: self.srid_reg,
            record_reg: self.state_record_reg,
            flag: InsertFlags(
                InsertFlags::REQUIRE_SEEK
                    | InsertFlags::SKIP_LAST_ROWID
                    | InsertFlags::SKIP_STATEMENT_CHANGE_COUNT,
            ),
            table_name: self.aux_name.clone(),
        });
        let af_skip_index = program.allocate_label();
        program.emit_insn(Insn::IfNot {
            reg: self.fresh_reg,
            target_pc: af_skip_index,
            jump_if_null: true,
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: self.akey_start as u16,
            count: 2,
            dest_reg: self.index_record_reg as u16,
            index_name: Some(self.aux_index_name.clone()),
            affinity_str: None,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: self.aux_index_cursor_id,
            record_reg: self.index_record_reg,
            unpacked_start: Some(self.akey_start),
            unpacked_count: Some(2),
            flags: IdxInsertFlags::new(),
        });
        program.preassign_label_to_next_insn(af_skip_index);
        program.preassign_label_to_next_insn(af_ret);
        program.emit_insn(Insn::Return {
            return_reg: self.return_reg,
            can_fallthrough: false,
        });
        Ok(())
    }

    /// Emit the left-input presence pass: iterate the left input (transaction
    /// delta or, during population, the full left btree) and drive a presence
    /// delta through the subroutine for each left row. A new unmatched left
    /// row reaches no join phase, so this is where it first appears.
    fn emit_presence_pass(
        &self,
        program: &mut ProgramBuilder,
        input: MaintenanceInput,
        delta: &DeltaSource,
        arrangement: &ArrangementHandle,
        logical_width: usize,
        view_name: &str,
    ) -> Result<()> {
        let input_cursor_id = match input {
            MaintenanceInput::TransactionDelta => match delta {
                DeltaSource::BaseTable {
                    table: delta_table, ..
                } => {
                    turso_assert!(
                        arrangement.table().root_page == delta_table.root_page,
                        "outer-join presence delta and arrangement must describe the same relation"
                    );
                    let cursor_id = program.alloc_cursor_id(CursorType::ViewDelta {
                        view_name: view_name.to_string(),
                        table: delta_table.clone(),
                    });
                    program.emit_insn(Insn::OpenRead {
                        cursor_id,
                        root_page: 0,
                        db: 0,
                    });
                    cursor_id
                }
                DeltaSource::Ephemeral(channel) => channel.cursor_id,
            },
            MaintenanceInput::BaseTable => {
                let table = arrangement.table();
                let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
                program.emit_insn(Insn::OpenRead {
                    cursor_id,
                    root_page: table.root_page,
                    db: 0,
                });
                cursor_id
            }
        };
        let pass_done = program.allocate_label();
        let pass_loop = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: input_cursor_id,
            pc_if_empty: pass_done,
        });
        program.preassign_label_to_next_insn(pass_loop);
        match input {
            MaintenanceInput::TransactionDelta => {
                let weight_column = match delta {
                    DeltaSource::BaseTable { .. } => logical_width,
                    DeltaSource::Ephemeral(channel) => channel.weight_column,
                };
                program.emit_insn(Insn::Column {
                    cursor_id: input_cursor_id,
                    column: weight_column,
                    dest: self.dp_reg,
                    default: None,
                });
            }
            MaintenanceInput::BaseTable => {
                if let Some(column) = arrangement.count_column() {
                    program.emit_insn(Insn::Column {
                        cursor_id: input_cursor_id,
                        column,
                        dest: self.dp_reg,
                        default: None,
                    });
                } else {
                    program.emit_int(1, self.dp_reg);
                }
            }
        }
        match (input, delta) {
            (MaintenanceInput::TransactionDelta, DeltaSource::Ephemeral(channel)) => {
                turso_assert!(
                    channel.identity == DeltaIdentity::OperatorRowid,
                    "arranged LEFT JOIN input must carry its arrangement rowid"
                );
                program.emit_insn(Insn::Column {
                    cursor_id: input_cursor_id,
                    column: 0,
                    dest: self.akey_start,
                    default: None,
                });
            }
            _ => program.emit_insn(Insn::RowId {
                cursor_id: input_cursor_id,
                dest: self.akey_start,
            }),
        }
        program.emit_int(0, self.dm_reg);
        program.emit_insn(Insn::Gosub {
            target_pc: self.merge_label,
            return_reg: self.return_reg,
        });
        program.emit_insn(Insn::Next {
            cursor_id: input_cursor_id,
            pc_if_next: pass_loop,
        });
        program.preassign_label_to_next_insn(pass_done);
        Ok(())
    }

    /// Emit a match-count delta for the left row at `left_rowid_reg`, weighted
    /// by the current phase's signed weight. Called from each inner join phase
    /// so every ON match adjusts its left row's count, whether or not WHERE
    /// lets the inner row through.
    fn emit_count_match(
        &self,
        program: &mut ProgramBuilder,
        left_rowid_reg: usize,
        weight_reg: usize,
    ) {
        program.emit_insn(Insn::Copy {
            src_reg: left_rowid_reg,
            dst_reg: self.akey_start,
            extra_amount: 0,
        });
        program.emit_int(0, self.dp_reg);
        program.emit_insn(Insn::Copy {
            src_reg: weight_reg,
            dst_reg: self.dm_reg,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Gosub {
            target_pc: self.merge_label,
            return_reg: self.return_reg,
        });
    }
}
