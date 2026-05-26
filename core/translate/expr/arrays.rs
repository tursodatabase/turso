use super::*;

/// Maximum number of array elements supported in the per-element transform loop.
/// Limited by the fixed register block allocated at compile time.
const MAX_ARRAY_LOOP_ELEMENTS: usize = 1024;

/// Emit a per-element transform loop on an array blob in `reg`.
/// Extracts each element, applies `transform_expr` via emit_type_expr,
/// stores results into contiguous registers, then rebuilds the blob with
/// MakeArrayDynamic. O(N) instead of O(N²) ArraySetElement per iteration.
#[turso_macros::trace_stack]
pub(super) fn emit_array_element_loop(
    program: &mut ProgramBuilder,
    reg: usize,
    transform_expr: &ast::Expr,
    col: &Column,
    type_def: &TypeDef,
    resolver: &Resolver,
) -> Result<()> {
    let reg_len = program.alloc_register();
    let reg_idx = program.alloc_register();
    let reg_elem = program.alloc_register();
    // Reserve a contiguous block for transformed elements.
    // At runtime, we only use registers[elem_base..elem_base+len].
    let elem_base = program.alloc_registers(MAX_ARRAY_LOOP_ELEMENTS);

    program.emit_insn(Insn::ArrayLength { reg, dest: reg_len });

    // Guard: halt if the array exceeds the register block size.
    let max_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: MAX_ARRAY_LOOP_ELEMENTS as i64,
        dest: max_reg,
    });
    let ok_label = program.allocate_label();
    program.emit_insn(Insn::Le {
        lhs: reg_len,
        rhs: max_reg,
        target_pc: ok_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::Halt {
        err_code: SQLITE_CONSTRAINT,
        description: format!(
            "array exceeds maximum element count for custom type transform ({MAX_ARRAY_LOOP_ELEMENTS})"
        ),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(ok_label);
    // reg_idx is the 1-based array index for ArrayElement (PG convention)
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: reg_idx,
    });
    // reg_offset is the 0-based offset for RegCopyOffset into the register block
    let reg_offset = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: reg_offset,
    });

    let loop_start = program.offset();
    let loop_end_label = program.allocate_label();

    program.emit_insn(Insn::Gt {
        lhs: reg_idx,
        rhs: reg_len,
        target_pc: loop_end_label,
        flags: CmpInsFlags::default(),
        collation: None,
    });

    // Extract element from record blob (1-based index)
    program.emit_insn(Insn::ArrayElement {
        array_reg: reg,
        index_reg: reg_idx,
        dest: reg_elem,
    });

    // Apply per-element transform expression
    emit_type_expr(
        program,
        transform_expr,
        reg_elem,
        reg_elem,
        col,
        type_def,
        resolver,
    )?;

    // Store transformed element into contiguous register block at 0-based offset
    program.emit_insn(Insn::RegCopyOffset {
        src: reg_elem,
        base: elem_base,
        offset_reg: reg_offset,
    });

    program.emit_insn(Insn::AddImm {
        register: reg_idx,
        value: 1,
    });
    program.emit_insn(Insn::AddImm {
        register: reg_offset,
        value: 1,
    });
    program.emit_insn(Insn::Goto {
        target_pc: loop_start,
    });

    program.preassign_label_to_next_insn(loop_end_label);

    // Rebuild the array blob from the contiguous register block in one pass
    program.emit_insn(Insn::MakeArrayDynamic {
        start_reg: elem_base,
        count_reg: reg_len,
        dest: reg,
    });

    Ok(())
}

/// Emit bytecode to encode an array value: parse JSON text input, validate/coerce
/// elements, and serialize to a native record-format BLOB.
/// For custom element types with encode expressions, a per-element bytecode loop
/// normalizes input to blob, applies encode per element, then rebuilds the blob.
#[turso_macros::trace_stack]
pub(super) fn emit_array_encode(
    program: &mut ProgramBuilder,
    reg: usize,
    col: &Column,
    resolver: &Resolver,
    table_name: &str,
) -> Result<()> {
    if let Some(type_def) = resolver.schema().get_type_def_unchecked(&col.ty_str) {
        if let Some(encode_expr) = type_def.encode() {
            // Normalize input (text or blob) to blob with ANY affinity first
            program.emit_insn(Insn::ArrayEncode {
                reg,
                element_affinity: Affinity::Blob,
                element_type: "ANY".into(),
                table_name: table_name.into(),
                col_name: col.name.as_deref().unwrap_or("").into(),
            });

            emit_array_element_loop(program, reg, encode_expr, col, type_def, resolver)?;
        }
    }

    // ArrayEncode: parse JSON text → validate/coerce → serialize to record blob.
    // For multi-dimensional arrays (e.g. INTEGER[][]), the outer array's elements
    // are themselves arrays (blobs), so we use ANY/Blob for validation.
    // Only 1-dimensional arrays validate elements against the declared base type.
    let is_any = col.ty_str.eq_ignore_ascii_case("ANY");
    let is_multidim = col.array_dimensions() > 1;
    let col_name = col.name.as_deref().unwrap_or("");
    let element_affinity = if is_any || is_multidim {
        Affinity::Blob
    } else {
        Affinity::affinity(&col.ty_str)
    };
    let element_type = if is_any || is_multidim {
        "ANY".into()
    } else {
        col.ty_str.to_uppercase().into()
    };
    program.emit_insn(Insn::ArrayEncode {
        reg,
        element_affinity,
        element_type,
        table_name: table_name.into(),
        col_name: col_name.into(),
    });
    Ok(())
}

/// Emit bytecode to decode an array value: convert record-format BLOB to JSON text.
/// For base element types, this is a single ArrayDecode instruction.
/// For custom element types with decode expressions, a per-element loop
/// extracts elements via ArrayElement, applies decode, then rebuilds the blob.
#[turso_macros::trace_stack]
pub(crate) fn emit_array_decode(
    program: &mut ProgramBuilder,
    reg: usize,
    col: &Column,
    resolver: &Resolver,
) -> Result<()> {
    if let Some(type_def) = resolver.schema().get_type_def_unchecked(&col.ty_str) {
        if let Some(decode_expr) = type_def.decode() {
            emit_array_element_loop(program, reg, decode_expr, col, type_def, resolver)?;
        }
    }

    // Convert record blob to JSON text for display
    program.emit_insn(Insn::ArrayDecode { reg });
    Ok(())
}

/// Emit encode expressions for columns with custom types in a contiguous register range.
/// Used by INSERT, UPDATE, and UPSERT paths to encode values before TypeCheck.
///
/// If `only_columns` is `Some`, only encode columns whose index is in the set.
/// This is needed for UPDATE/UPSERT where non-SET columns are already encoded
/// (read from disk), and re-encoding them would corrupt data.
#[turso_macros::trace_stack]
pub(crate) fn emit_custom_type_encode_columns(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    columns: &[Column],
    start_reg: usize,
    only_columns: Option<&ColumnMask>,
    table_name: &str,
    layout: &ColumnLayout,
) -> Result<()> {
    for (i, col) in columns.iter().enumerate() {
        if let Some(filter) = only_columns {
            if !filter.get(i) {
                continue;
            }
        }

        let reg = layout.to_register(start_reg, i);

        // Handle array columns: encode input (text or blob) -> record blob for storage
        if col.is_array() {
            let skip_label = program.allocate_label();
            program.emit_insn(Insn::IsNull {
                reg,
                target_pc: skip_label,
            });
            emit_array_encode(program, reg, col, resolver, table_name)?;
            program.preassign_label_to_next_insn(skip_label);
            continue;
        }

        let type_name = &col.ty_str;
        if type_name.is_empty() {
            continue;
        }
        let Ok(Some(resolved)) = resolver.schema().resolve_type_unchecked(type_name) else {
            continue;
        };

        // Check if any type in the chain has not_null — if so, don't skip NULLs
        let any_not_null = resolved.chain.iter().any(|td| td.not_null);

        let skip_label = if !any_not_null {
            // Skip NULL values: jump over encode if NULL
            let label = program.allocate_label();
            program.emit_insn(Insn::IsNull {
                reg,
                target_pc: label,
            });
            Some(label)
        } else {
            None
        };

        // Apply encode for each type in the chain (child first, then parent)
        for td in &resolved.chain {
            if let Some(encode_expr) = td.encode() {
                emit_type_expr(program, encode_expr, reg, reg, col, td, resolver)?;
            }
        }

        if let Some(label) = skip_label {
            program.preassign_label_to_next_insn(label);
        }
    }
    Ok(())
}

/// Emit decode expressions for columns with custom types in a contiguous register range.
/// Used by the UPSERT path to decode values that were read from disk (encoded) so that
/// WHERE/SET expressions in DO UPDATE see user-facing values.
///
/// If `only_columns` is `Some`, only decode columns whose index is in the set.
#[turso_macros::trace_stack]
pub(crate) fn emit_custom_type_decode_columns(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    columns: &[Column],
    start_reg: usize,
    only_columns: Option<&ColumnMask>,
    layout: &ColumnLayout,
) -> Result<()> {
    for (i, col) in columns.iter().enumerate() {
        if let Some(filter) = only_columns {
            if !filter.get(i) {
                continue;
            }
        }

        let reg = layout.to_register(start_reg, i);

        // Handle array columns: decode record blob -> JSON text for display
        if col.is_array() {
            let skip_label = program.allocate_label();
            program.emit_insn(Insn::IsNull {
                reg,
                target_pc: skip_label,
            });
            emit_array_decode(program, reg, col, resolver)?;
            program.preassign_label_to_next_insn(skip_label);
            continue;
        }

        let type_name = &col.ty_str;
        if type_name.is_empty() {
            continue;
        }
        let Ok(Some(resolved)) = resolver.schema().resolve_type_unchecked(type_name) else {
            continue;
        };

        // Skip NULL values: jump over decode if NULL
        let skip_label = program.allocate_label();
        program.emit_insn(Insn::IsNull {
            reg,
            target_pc: skip_label,
        });

        // Apply decode in reverse order (parent/ancestor first, then child)
        for td in resolved.chain.iter().rev() {
            if let Some(decode_expr) = td.decode() {
                emit_type_expr(program, decode_expr, reg, reg, col, td, resolver)?;
            }
        }

        program.preassign_label_to_next_insn(skip_label);
    }
    Ok(())
}
