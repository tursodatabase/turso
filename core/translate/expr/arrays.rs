use super::*;
use crate::translate::plan::JoinedTable;

#[derive(Clone, Copy)]
enum PlanColumnTransform {
    Encode,
    Decode,
    DecodeForReencode,
}

#[allow(clippy::too_many_arguments)]
fn emit_plan_source_column_transforms(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    source: &JoinedTable,
    start_reg: usize,
    only_columns: Option<&ColumnMask>,
    layout: &ColumnLayout,
    transform: PlanColumnTransform,
    resolver: &Resolver<'_>,
) -> Result<()> {
    let column_count = source.table.columns().len();
    let column_programs = &source.read_programs.column_type_programs;
    if column_programs.len() != column_count {
        return Err(LimboError::InternalError(format!(
            "source {} has {} column type program slots for {column_count} columns",
            source.internal_id,
            column_programs.len()
        )));
    }
    if layout.column_count() != column_count {
        return Err(LimboError::InternalError(format!(
            "source {} has a {}-column register layout for {column_count} columns",
            source.internal_id,
            layout.column_count()
        )));
    }

    for (column, programs) in column_programs.iter().enumerate() {
        if only_columns.is_some_and(|mask| !mask.get(column)) {
            continue;
        }
        let Some(programs) = programs else {
            continue;
        };
        let register = layout.to_register(start_reg, column);
        match transform {
            PlanColumnTransform::Encode => emit_plan_column_value_encode(
                program,
                referenced_tables,
                programs,
                register,
                resolver,
            )?,
            PlanColumnTransform::Decode => emit_plan_column_value_decode(
                program,
                referenced_tables,
                programs,
                register,
                resolver,
            )?,
            PlanColumnTransform::DecodeForReencode => emit_plan_column_value_decode_for_reencode(
                program,
                referenced_tables,
                programs,
                register,
                resolver,
            )?,
        }
    }
    Ok(())
}

/// Encode the selected columns of one planned row image for storage.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_plan_source_encode_columns(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    source: &JoinedTable,
    start_reg: usize,
    only_columns: Option<&ColumnMask>,
    layout: &ColumnLayout,
    resolver: &Resolver<'_>,
) -> Result<()> {
    emit_plan_source_column_transforms(
        program,
        referenced_tables,
        source,
        start_reg,
        only_columns,
        layout,
        PlanColumnTransform::Encode,
        resolver,
    )
}

/// Decode stored scalar columns in one planned row image for expression use.
/// Arrays deliberately remain in their record representation.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_plan_source_decode_columns(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    source: &JoinedTable,
    start_reg: usize,
    only_columns: Option<&ColumnMask>,
    layout: &ColumnLayout,
    resolver: &Resolver<'_>,
) -> Result<()> {
    emit_plan_source_column_transforms(
        program,
        referenced_tables,
        source,
        start_reg,
        only_columns,
        layout,
        PlanColumnTransform::Decode,
        resolver,
    )
}

/// Decode a stored row before encoding the whole row again. Array element
/// decoders run, but the array itself stays in record form for its encoder.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_plan_source_decode_columns_for_reencode(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    source: &JoinedTable,
    start_reg: usize,
    only_columns: Option<&ColumnMask>,
    layout: &ColumnLayout,
    resolver: &Resolver<'_>,
) -> Result<()> {
    emit_plan_source_column_transforms(
        program,
        referenced_tables,
        source,
        start_reg,
        only_columns,
        layout,
        PlanColumnTransform::DecodeForReencode,
        resolver,
    )
}
