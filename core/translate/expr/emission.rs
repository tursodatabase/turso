use super::*;
use crate::alloc::TursoIteratorExt;
use crate::translate::plan::{NonFromClauseSubquery, QueryDestination, SubqueryState};
use crate::translate::plan_expr::{walk_plan_expr, PlanColumnRef, PlanExpr, PlanWalkControl};
use crate::turso_assert_eq;

/// Emit literal values - shared between regular and RETURNING expression evaluation
pub fn emit_literal(
    program: &mut ProgramBuilder,
    literal: &ast::Literal,
    target_register: usize,
) -> Result<usize> {
    match literal {
        ast::Literal::Numeric(val) => {
            match parse_numeric_literal(val)? {
                Value::Numeric(Numeric::Integer(int_value)) => {
                    program.emit_insn(Insn::Integer {
                        value: int_value,
                        dest: target_register,
                    });
                }
                Value::Numeric(Numeric::Float(real_value)) => {
                    program.emit_insn(Insn::Real {
                        value: real_value.into(),
                        dest: target_register,
                    });
                }
                _ => unreachable!(),
            }
            Ok(target_register)
        }
        ast::Literal::String(s) => {
            program.emit_insn(Insn::String8 {
                value: sanitize_string(s),
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::Blob(s) => {
            let bytes = ast::blob_literal_hex(s)
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| {
                    // We assume that sqlite3-parser has already validated that
                    // the input is valid hex string, thus unwrap is safe.
                    let hex_byte = std::str::from_utf8(pair).unwrap();
                    u8::from_str_radix(hex_byte, 16).unwrap()
                })
                .try_collect()?;
            program.emit_insn(Insn::Blob {
                value: bytes,
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::Keyword(_) => {
            crate::bail_parse_error!("Keyword in WHERE clause is not supported")
        }
        ast::Literal::Null => {
            program.emit_insn(Insn::Null {
                dest: target_register,
                dest_end: None,
            });
            Ok(target_register)
        }
        ast::Literal::True => {
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::False => {
            program.emit_insn(Insn::Integer {
                value: 0,
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::CurrentDate => {
            program.emit_insn(Insn::String8 {
                value: datetime::exec_date::<&[_; 0], std::slice::Iter<'_, Value>, &Value>(&[])
                    .to_string(),
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::CurrentTime => {
            program.emit_insn(Insn::String8 {
                value: datetime::exec_time::<&[_; 0], std::slice::Iter<'_, Value>, &Value>(&[])
                    .to_string(),
                dest: target_register,
            });
            Ok(target_register)
        }
        ast::Literal::CurrentTimestamp => {
            program.emit_insn(
                Insn::String8 {
                    value: datetime::exec_datetime_full::<
                        &[_; 0],
                        std::slice::Iter<'_, Value>,
                        &Value,
                    >(&[])
                    .to_string(),
                    dest: target_register,
                },
            );
            Ok(target_register)
        }
    }
}

/// Emit a function call instruction with pre-allocated argument registers
/// This is shared between different function call contexts
pub fn emit_function_call(
    program: &mut ProgramBuilder,
    func_ctx: FuncCtx,
    arg_registers: &[usize],
    target_register: usize,
) -> Result<()> {
    let start_reg = if arg_registers.is_empty() {
        target_register // If no arguments, use target register as start
    } else {
        arg_registers[0] // Use first argument register as start
    };

    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg,
        dest: target_register,
        func: func_ctx,
    });

    Ok(())
}

/// Context for buffering RETURNING results into an ephemeral table
/// instead of yielding them immediately via ResultRow.
/// When used, the DML loop buffers each result row into the ephemeral table,
/// and a scan-back loop after the DML loop yields them to the caller.
pub struct ReturningBufferCtx {
    /// Cursor ID of the ephemeral table to buffer results into
    pub cursor_id: usize,
    /// Frozen semantic metadata used to present buffered values at ResultRow.
    pub result_columns: Vec<ResultSetColumn>,
}

/// Emit the scan-back loop that reads all buffered RETURNING rows from the
/// ephemeral table and yields them via ResultRow. Called after all DML is complete.
pub(crate) fn emit_returning_scan_back(
    program: &mut ProgramBuilder,
    buf: &ReturningBufferCtx,
    table_references: &TableReferences,
    resolver: &Resolver<'_>,
) -> Result<()> {
    let end_label = program.allocate_label();
    let scan_start = program.allocate_label();
    let num_columns = buf.result_columns.len();

    program.emit_insn(Insn::Rewind {
        cursor_id: buf.cursor_id,
        pc_if_empty: end_label,
    });
    program.preassign_label_to_next_insn(scan_start);

    let result_start_reg = program.alloc_registers(num_columns);
    for i in 0..num_columns {
        program.emit_insn(Insn::Column {
            cursor_id: buf.cursor_id,
            column: i,
            dest: result_start_reg + i,
            default: None,
        });
    }
    crate::translate::result_row::emit_result_columns_to_destination(
        program,
        &QueryDestination::ResultRows,
        result_start_reg,
        &buf.result_columns,
        table_references,
        resolver,
    )?;
    program.emit_insn(Insn::Next {
        cursor_id: buf.cursor_id,
        pc_if_next: scan_start,
    });
    program.preassign_label_to_next_insn(end_label);
    Ok(())
}

/// Emit bytecode to evaluate RETURNING expressions and produce result rows.
/// RETURNING result expressions are otherwise evaluated as normal, but the columns of the target table
/// are added to [Resolver::plan_expr_to_reg_cache], meaning a reference to e.g tbl.col will effectively
/// refer to a register where the OLD/NEW value of tbl.col is stored after an INSERT/UPDATE/DELETE.
///
/// When `returning_buffer` is `Some`, the results are buffered into an ephemeral table
/// instead of being yielded immediately. A subsequent call to `emit_returning_scan_back`
/// will drain the buffer and yield the rows to the caller.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_returning_results<'a>(
    program: &mut ProgramBuilder,
    table_references: &TableReferences,
    result_columns: &[ResultSetColumn],
    reg_columns_start: usize,
    rowid_reg: usize,
    resolver: &mut Resolver<'a>,
    returning_buffer: Option<&ReturningBufferCtx>,
    layout: &ColumnLayout,
) -> Result<()> {
    if result_columns.is_empty() {
        return Ok(());
    }

    let cache_state = seed_returning_row_image_in_cache(
        table_references,
        result_columns,
        &[],
        reg_columns_start,
        rowid_reg,
        resolver,
        layout,
    )?;

    let result = (|| {
        let result_start_reg = program.alloc_registers(result_columns.len());

        for (i, result_column) in result_columns.iter().enumerate() {
            let reg = result_start_reg + i;
            translate_plan_expr_no_constant_opt(
                program,
                Some(table_references),
                &result_column.expr,
                reg,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
        }

        if let Some(buf) = returning_buffer {
            turso_assert_eq!(
                buf.result_columns.len(),
                result_columns.len(),
                "RETURNING buffer metadata must match the buffered row"
            );
            // Keep canonical expression values in the internal table. Display
            // conversion runs only while scanning rows back to the caller.
            let record_reg = program.alloc_register();
            let eph_rowid_reg = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: crate::vdbe::insn::to_u32(result_start_reg),
                count: crate::vdbe::insn::to_u32(result_columns.len()),
                dest_reg: crate::vdbe::insn::to_u32(record_reg),
                index_name: None,
                affinity_str: None,
            });
            program.emit_insn(Insn::NewRowid {
                cursor: buf.cursor_id,
                rowid_reg: eph_rowid_reg,
                prev_largest_reg: 0,
            });
            program.emit_insn(Insn::Insert {
                cursor: buf.cursor_id,
                key_reg: eph_rowid_reg,
                record_reg,
                flag: InsertFlags::new().is_ephemeral_table_insert(),
                table_name: String::new(),
            });
        } else {
            crate::translate::result_row::emit_result_columns_to_destination(
                program,
                &QueryDestination::ResultRows,
                result_start_reg,
                result_columns,
                table_references,
                resolver,
            )?;
        }

        Ok(())
    })();

    restore_returning_row_image_in_cache(resolver, cache_state);
    result
}

pub(crate) struct ReturningRowImageCacheState {
    plan_cache_len: usize,
    cache_enabled: bool,
}

pub(crate) fn seed_returning_row_image_in_cache<'a>(
    table_references: &TableReferences,
    result_columns: &[ResultSetColumn],
    post_write_subqueries: &[NonFromClauseSubquery],
    reg_columns_start: usize,
    rowid_reg: usize,
    resolver: &mut Resolver<'a>,
    layout: &ColumnLayout,
) -> Result<ReturningRowImageCacheState> {
    turso_assert!(
        table_references.joined_tables().len() == 1,
        "RETURNING is only used with INSERT, UPDATE, or DELETE statements, which target a single table"
    );
    let table = table_references.joined_tables().first().unwrap();
    let target_source = table.internal_id;

    let plan_cache_len = resolver.plan_expr_to_reg_cache.len();
    let cache_enabled = resolver.expr_to_reg_cache_enabled;
    resolver.enable_expr_to_reg_cache();

    let mut reads_rowid = false;
    let mut referenced_columns = Vec::<PlanColumnRef>::new();
    for result_column in result_columns {
        walk_plan_expr(&result_column.expr, &mut |expr| {
            match expr {
                PlanExpr::Column(column) if column.source == target_source => {
                    if !referenced_columns.iter().any(|cached| {
                        cached.source == column.source && cached.column == column.column
                    }) {
                        referenced_columns.push(column.clone());
                    }
                }
                PlanExpr::MergedColumn(column) if column.right.source == target_source => {
                    if !referenced_columns.iter().any(|cached| {
                        cached.source == column.right.source && cached.column == column.right.column
                    }) {
                        referenced_columns.push(column.right.clone());
                    }
                }
                PlanExpr::RowId(source) if *source == target_source => reads_rowid = true,
                _ => {}
            }
            Ok(PlanWalkControl::Continue)
        })?;
    }

    // A subquery expression only carries its semantic ID. Its correlated
    // column reads live in the still-unevaluated child plan, so include those
    // exact dependencies before that child is emitted against the post-write
    // target cursor.
    for subquery in post_write_subqueries
        .iter()
        .filter(|subquery| subquery.is_post_write_returning())
    {
        let SubqueryState::Unevaluated {
            plan: Some(subquery_plan),
        } = &subquery.state
        else {
            continue;
        };
        let dependency = subquery_plan.source_row_dependency(target_source)?;
        reads_rowid |= dependency.rowid;
        for column in dependency.columns {
            if !referenced_columns
                .iter()
                .any(|cached| cached.source == column.source && cached.column == column.column)
            {
                referenced_columns.push(column);
            }
        }
    }

    if reads_rowid {
        resolver.cache_plan_scalar_expr_reg(
            PlanExpr::RowId(target_source),
            rowid_reg,
            false,
            &(),
        )?;
    }

    for column in referenced_columns {
        let raw_reg = if column.rowid_alias {
            rowid_reg
        } else {
            reg_columns_start + layout.to_reg_offset(column.column)
        };
        let needs_decode =
            column.type_fact.declared.as_ref().is_some_and(|declared| {
                declared.array_dimensions == 0 && declared.custom().is_some()
            });
        resolver.cache_plan_scalar_expr_reg(
            PlanExpr::Column(column),
            raw_reg,
            needs_decode,
            &(),
        )?;
    }

    Ok(ReturningRowImageCacheState {
        plan_cache_len,
        cache_enabled,
    })
}

pub(crate) fn restore_returning_row_image_in_cache(
    resolver: &mut Resolver<'_>,
    state: ReturningRowImageCacheState,
) {
    resolver
        .plan_expr_to_reg_cache
        .truncate(state.plan_cache_len);
    resolver.expr_to_reg_cache_enabled = state.cache_enabled;
}
