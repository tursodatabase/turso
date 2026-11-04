use super::*;
use crate::function::{AccumulatorFunc, AggFunc};
use crate::types::StrictType;
use crate::vdbe::insn::{AggStepData, ArrayEncodeData, ArrayEncodeTarget};

/// Emit a per-element transform loop on an array blob in `reg`.
/// Extracts each element, applies `transform_element` to the register holding
/// it, and collects the results with array_agg's accumulator, which builds the
/// new blob once at the end, so the loop takes O(N) time for any N.
/// A NULL array stays NULL, and an empty array stays empty.
pub(super) fn emit_array_element_loop(
    program: &mut ProgramBuilder,
    reg: usize,
    transform_element: impl FnOnce(&mut ProgramBuilder, usize) -> Result<()>,
) -> Result<()> {
    let done_label = program.allocate_label();
    program.emit_insn(Insn::IsNull {
        reg,
        target_pc: done_label,
    });

    let reg_len = program.alloc_register();
    let reg_idx = program.alloc_register();
    let reg_elem = program.alloc_register();
    let reg_acc = program.alloc_register();

    program.emit_insn(Insn::ArrayLength { reg, dest: reg_len });
    // ArrayLength is NULL for a value that is not an array. An array column
    // and each level of an array of arrays hold only arrays and NULLs, so
    // this is a damaged value; stop instead of looping with no end.
    let is_array_label = program.allocate_label();
    program.emit_insn(Insn::NotNull {
        reg: reg_len,
        target_pc: is_array_label,
    });
    program.emit_insn(Insn::Halt {
        constraint_info: None,
        err_code: SQLITE_ERROR,
        description: "expected an array, but the value is not an array".to_string(),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(is_array_label);
    // array_agg of no elements is NULL, not an empty array.
    program.emit_insn(Insn::IfNot {
        reg: reg_len,
        target_pc: done_label,
        jump_if_null: false,
    });
    // reg_idx is the 1-based array index for ArrayElement (PG convention)
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: reg_idx,
    });
    // A NULL accumulator starts a new array_agg.
    program.emit_insn(Insn::Null {
        dest: reg_acc,
        dest_end: None,
    });

    let loop_start = program.allocate_label();
    let loop_end_label = program.allocate_label();
    program.preassign_label_to_next_insn(loop_start);

    program.emit_insn(Insn::Gt {
        lhs: reg_idx,
        rhs: reg_len,
        target_pc: loop_end_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });

    // Extract element from record blob (1-based index)
    program.emit_insn(Insn::ArrayElement {
        array_reg: reg,
        index_reg: reg_idx,
        dest: reg_elem,
    });

    transform_element(program, reg_elem)?;

    let array_agg = AccumulatorFunc::Agg(AggFunc::ArrayAgg);
    program.emit_insn(Insn::AggStep {
        data: Box::new(AggStepData {
            acc_reg: reg_acc,
            col: reg_elem,
            delimiter: 0,
            func: array_agg.clone(),
            collation: CollationSeq::BINARY,
        }),
    });

    program.emit_insn(Insn::AddImm {
        register: reg_idx,
        value: 1,
    });
    program.emit_insn(Insn::Goto {
        target_pc: loop_start,
    });

    program.preassign_label_to_next_insn(loop_end_label);
    program.emit_insn(Insn::AggFinal {
        register: reg_acc,
        func: array_agg,
    });
    program.emit_insn(Insn::Copy {
        src_reg: reg_acc,
        dst_reg: reg,
        extra_amount: 0,
    });

    program.preassign_label_to_next_insn(done_label);
    Ok(())
}

/// What an array leaf loop accepts in place of an array blob.
#[derive(Clone, Copy)]
pub(crate) enum ArrayInput<'a> {
    /// Only array blobs, such as an array read from a column.
    Blob,
    /// Text such as `'{1,2}'`, parsed into an array. Any other value is an
    /// error that names `target`.
    Text(&'a ArrayEncodeTarget),
    /// Text parsed into an array when it is one. Any other value is left as it
    /// is, so that an index key such as `'x'` matches no row, as in a scan.
    TextIfArray,
}

/// Runs `transform_leaf` on the register holding each leaf element of the
/// array in `reg`: each element of a one-dimensional array, and each element
/// of each inner array of an array of arrays (`dimensions` > 1). Each level
/// first turns text into an array blob as `input` says.
pub(crate) fn emit_array_leaf_loop(
    program: &mut ProgramBuilder,
    reg: usize,
    dimensions: u32,
    input: ArrayInput,
    transform_leaf: impl FnOnce(&mut ProgramBuilder, usize) -> Result<()>,
) -> Result<()> {
    let parse = |program: &mut ProgramBuilder, target: ArrayEncodeTarget| {
        program.emit_insn(Insn::ArrayEncode {
            data: Box::new(ArrayEncodeData {
                reg,
                element_type: "ANY".into(),
                element_kind: StrictType::Any,
                dimensions,
                target,
            }),
        });
    };
    let not_an_array_label = match input {
        ArrayInput::Blob => None,
        ArrayInput::Text(target) => {
            parse(program, target.clone());
            None
        }
        ArrayInput::TextIfArray => {
            // array_length is NULL for a value that is not an array.
            let length_reg = program.alloc_register();
            program.emit_insn(Insn::ArrayLength {
                reg,
                dest: length_reg,
            });
            let label = program.allocate_label();
            program.emit_insn(Insn::IsNull {
                reg: length_reg,
                target_pc: label,
            });
            parse(program, ArrayEncodeTarget::IndexKey);
            Some(label)
        }
    };
    emit_array_element_loop(program, reg, |program, element_reg| {
        if dimensions > 1 {
            emit_array_leaf_loop(program, element_reg, dimensions - 1, input, transform_leaf)
        } else {
            transform_leaf(program, element_reg)
        }
    })?;
    if let Some(label) = not_an_array_label {
        program.preassign_label_to_next_insn(label);
    }
    Ok(())
}

/// Emit bytecode that turns the array value in `reg` into what an array column
/// `col` stores: parse text input, run the element type's ENCODE and COERCE on
/// each leaf element, check each element against the type's storage type, and
/// serialize to a native record-format BLOB.
pub(super) fn emit_array_encode(
    program: &mut ProgramBuilder,
    reg: usize,
    col: &Column,
    resolver: &Resolver,
    table_name: &str,
) -> Result<()> {
    let Some(col_name) = col.name.as_deref() else {
        crate::bail_parse_error!("array column of table {table_name} has no name");
    };
    let target = ArrayEncodeTarget::Column {
        table_name: table_name.into(),
        col_name: col_name.into(),
    };
    let resolved = resolve_array_element_type(col, resolver)?;
    emit_array_encode_as(program, reg, col, resolved.as_ref(), resolver, target)
}

/// [emit_array_encode] for elements of the custom type `resolved`, or of the
/// plain type `col.ty_str` when it is None, building an array for `target`.
pub(super) fn emit_array_encode_as(
    program: &mut ProgramBuilder,
    reg: usize,
    col: &Column,
    resolved: Option<&crate::schema::ResolvedType>,
    resolver: &Resolver,
    target: ArrayEncodeTarget,
) -> Result<()> {
    if let Some(resolved) = resolved {
        if resolved.chain.iter().any(|td| td.has_encode_or_coerce()) {
            emit_array_leaf_loop(
                program,
                reg,
                col.array_dimensions(),
                ArrayInput::Text(&target),
                |program, leaf_reg| {
                    emit_store_value(program, resolved, leaf_reg, &col.ty_params, resolver)
                },
            )?;
        }
    }

    // ArrayEncode: parse text → convert elements → serialize to record blob.
    // A custom type's elements hold what its ENCODE returns, which is checked
    // against the type's BASE, as for a column of the type.
    let element_type: Arc<str> = resolved
        .map_or(col.ty_str.as_str(), |resolved| resolved.primitive.as_str())
        .to_uppercase()
        .into();
    // check_array_element_type refuses other names before code is emitted.
    let element_kind = StrictType::named(&element_type).ok_or_else(|| {
        LimboError::InternalError(format!(
            "array elements of {target} have type {element_type}, which is not INTEGER, REAL, TEXT, BLOB or ANY"
        ))
    })?;
    program.emit_insn(Insn::ArrayEncode {
        data: Box::new(ArrayEncodeData {
            reg,
            element_type,
            element_kind,
            dimensions: col.array_dimensions(),
            target,
        }),
    });
    Ok(())
}

/// Emit bytecode that turns the array an array column `col` stores, in `reg`,
/// into the array it reads as: the element type's DECODE runs on each leaf
/// element. The array stays a record-format BLOB.
pub(crate) fn emit_array_decode(
    program: &mut ProgramBuilder,
    reg: usize,
    col: &Column,
    resolver: &Resolver,
) -> Result<()> {
    let Some(resolved) = resolve_array_element_type(col, resolver)? else {
        return Ok(());
    };
    if resolved.decode_returns_value_unchanged() {
        return Ok(());
    }
    emit_array_leaf_loop(
        program,
        reg,
        col.array_dimensions(),
        ArrayInput::Blob,
        |program, leaf_reg| emit_decode(program, &resolved.chain, leaf_reg, resolver),
    )
}

/// The custom type of the elements of array column `col`, or None when they
/// are of a plain type such as INTEGER, which has no ENCODE or DECODE.
/// Fails for an element type that is neither.
fn resolve_array_element_type(
    col: &Column,
    resolver: &Resolver,
) -> Result<Option<crate::schema::ResolvedType>> {
    match resolver.schema().resolve_type_unchecked(&col.ty_str)? {
        Some(resolved) => Ok(Some(resolved)),
        None if crate::util::is_builtin_strict_type(&col.ty_str) => Ok(None),
        None => crate::bail_parse_error!(
            "unknown datatype for array column {}: \"{}\"",
            col.name.as_deref().unwrap_or(""),
            col.ty_str
        ),
    }
}

/// Emit ENCODE and COERCE for columns with custom types in a contiguous register range.
/// Used by INSERT, UPDATE, and UPSERT paths to encode values before TypeCheck.
///
/// If `only_columns` is `Some`, only encode columns whose index is in the set.
/// This is needed for UPDATE/UPSERT where non-SET columns are already encoded
/// (read from disk), and re-encoding them would corrupt data.
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
            emit_array_encode(program, reg, col, resolver, table_name)?;
            continue;
        }

        let type_name = &col.ty_str;
        if type_name.is_empty() {
            continue;
        }
        // A plain type has no ENCODE; TypeCheck checks its values.
        let Some(resolved) = resolver.schema().resolve_type_unchecked(type_name)? else {
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

        emit_store_value(program, &resolved, reg, &col.ty_params, resolver)?;

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

        if col.is_array() {
            emit_array_decode(program, reg, col, resolver)?;
            continue;
        }

        let type_name = &col.ty_str;
        if type_name.is_empty() {
            continue;
        }
        if let Ok(Some(resolved)) = resolver.schema().resolve_type_unchecked(type_name) {
            emit_decode(program, &resolved.chain, reg, resolver)?;
        }
    }
    Ok(())
}
