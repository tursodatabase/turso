use std::sync::Arc;

use turso_sqlite3_parser::ast::{
    DistinctNames, Expr, InsertBody, OneSelect, QualifiedName, ResolveType, ResultColumn, With,
};

use crate::error::{SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_PRIMARYKEY};
use crate::schema::{self, IndexColumn, Table};
use crate::translate::emitter::{emit_cdc_insns, emit_cdc_patch_record, OperationMode};
use crate::translate::expr::{
    emit_returning_results, process_returning_clause, ReturningValueRegisters,
};
use crate::translate::plan::TableReferences;
use crate::translate::planner::ROWID;
use crate::util::normalize_ident;
use crate::vdbe::builder::ProgramBuilderOpts;
use crate::vdbe::insn::{IdxInsertFlags, InsertFlags, RegisterOrLiteral};
use crate::vdbe::BranchOffset;
use crate::{
    schema::{Column, Schema},
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::Insn,
    },
};
use crate::{Result, SymbolTable, VirtualTable};

use super::emitter::Resolver;
use super::expr::{translate_expr, translate_expr_no_constant_opt, NoConstantOptReason};
use super::optimizer::rewrite_expr;
use super::plan::QueryDestination;
use super::select::translate_select;

struct TempTableCtx {
    cursor_id: usize,
    loop_start_label: BranchOffset,
    loop_end_label: BranchOffset,
}

#[allow(clippy::too_many_arguments)]
pub fn translate_insert(
    schema: &Schema,
    with: Option<With>,
    on_conflict: Option<ResolveType>,
    tbl_name: QualifiedName,
    columns: Option<DistinctNames>,
    mut body: InsertBody,
    mut returning: Option<Vec<ResultColumn>>,
    syms: &SymbolTable,
    mut program: ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<ProgramBuilder> {
    let opts = ProgramBuilderOpts {
        num_cursors: 1,
        approx_num_insns: 30,
        approx_num_labels: 5,
    };
    program.extend(&opts);
    if with.is_some() {
        crate::bail_parse_error!("WITH clause is not supported");
    }
    if on_conflict.is_some() {
        crate::bail_parse_error!("ON CONFLICT clause is not supported");
    }

    if schema.table_has_indexes(&tbl_name.name.to_string()) && !schema.indexes_enabled() {
        // Let's disable altering a table with indices altogether instead of checking column by
        // column to be extra safe.
        crate::bail_parse_error!(
            "INSERT to table with indexes is disabled. Omit the `--experimental-indexes=false` flag to enable this feature."
        );
    }
    let table_name = &tbl_name.name;
    let table = match schema.get_table(table_name.as_str()) {
        Some(table) => table,
        None => crate::bail_parse_error!("no such table: {}", table_name),
    };

    let resolver = Resolver::new(schema, syms);

    if let Some(virtual_table) = &table.virtual_table() {
        program = translate_virtual_table_insert(
            program,
            virtual_table.clone(),
            columns,
            body,
            on_conflict,
            &resolver,
        )?;
        return Ok(program);
    }

    let Some(btree_table) = table.btree() else {
        crate::bail_parse_error!("no such table: {}", table_name);
    };
    if !btree_table.has_rowid {
        crate::bail_parse_error!("INSERT into WITHOUT ROWID table is not supported");
    }

    let root_page = btree_table.root_page;

    let mut values: Option<Vec<Expr>> = None;
    let inserting_multiple_rows = match &mut body {
        InsertBody::Select(select, _) => match select.body.select.as_mut() {
            // TODO see how to avoid clone
            OneSelect::Values(values_expr) if values_expr.len() <= 1 => {
                if values_expr.is_empty() {
                    crate::bail_parse_error!("no values to insert");
                }
                let mut param_idx = 1;
                for expr in values_expr.iter_mut().flat_map(|v| v.iter_mut()) {
                    rewrite_expr(expr, &mut param_idx)?;
                }
                values = values_expr.pop();
                false
            }
            _ => true,
        },
        InsertBody::DefaultValues => false,
    };

    let halt_label = program.allocate_label();
    let loop_start_label = program.allocate_label();

    let cdc_table = program.capture_data_changes_mode().table();
    let cdc_table = if let Some(cdc_table) = cdc_table {
        if table.get_name() != cdc_table {
            let Some(turso_cdc_table) = schema.get_table(cdc_table) else {
                crate::bail_parse_error!("no such table: {}", cdc_table);
            };
            let Some(cdc_btree) = turso_cdc_table.btree().clone() else {
                crate::bail_parse_error!("no such table: {}", cdc_table);
            };
            Some((
                program.alloc_cursor_id(CursorType::BTreeTable(cdc_btree.clone())),
                cdc_btree,
            ))
        } else {
            None
        }
    } else {
        None
    };

    // Process RETURNING clause using shared module
    let (result_columns, _) = if let Some(returning) = &mut returning {
        process_returning_clause(
            returning,
            &table,
            table_name.as_str(),
            &mut program,
            connection,
        )?
    } else {
        (vec![], TableReferences::new(vec![], vec![]))
    };

    // Set up the program to return result columns if RETURNING is specified
    if !result_columns.is_empty() {
        program.result_columns = result_columns.clone();
    }

    let mut yield_reg_opt = None;
    let mut temp_table_ctx = None;
    let (num_values, cursor_id) = match body {
        // TODO: upsert
        InsertBody::Select(select, _) => {
            // Simple Common case of INSERT INTO <table> VALUES (...)
            if matches!(select.body.select.as_ref(),  OneSelect::Values(values) if values.len() <= 1)
            {
                (
                    values.as_ref().unwrap().len(),
                    program.alloc_cursor_id(CursorType::BTreeTable(btree_table.clone())),
                )
            } else {
                // Multiple rows - use coroutine for value population
                let yield_reg = program.alloc_register();
                let jump_on_definition_label = program.allocate_label();
                let start_offset_label = program.allocate_label();
                program.emit_insn(Insn::InitCoroutine {
                    yield_reg,
                    jump_on_definition: jump_on_definition_label,
                    start_offset: start_offset_label,
                });

                program.preassign_label_to_next_insn(start_offset_label);

                let query_destination = QueryDestination::CoroutineYield {
                    yield_reg,
                    coroutine_implementation_start: halt_label,
                };
                program.incr_nesting();
                let result = translate_select(
                    schema,
                    *select,
                    syms,
                    program,
                    query_destination,
                    connection,
                )?;
                program = result.program;
                program.decr_nesting();

                program.emit_insn(Insn::EndCoroutine { yield_reg });
                program.preassign_label_to_next_insn(jump_on_definition_label);

                let cursor_id =
                    program.alloc_cursor_id(CursorType::BTreeTable(btree_table.clone()));

                // From SQLite
                /* Set useTempTable to TRUE if the result of the SELECT statement
                 ** should be written into a temporary table (template 4).  Set to
                 ** FALSE if each output row of the SELECT can be written directly into
                 ** the destination table (template 3).
                 **
                 ** A temp table must be used if the table being updated is also one
                 ** of the tables being read by the SELECT statement.  Also use a
                 ** temp table in the case of row triggers.
                 */
                if program.is_table_open(&table) {
                    let temp_cursor_id =
                        program.alloc_cursor_id(CursorType::BTreeTable(btree_table.clone()));
                    temp_table_ctx = Some(TempTableCtx {
                        cursor_id: temp_cursor_id,
                        loop_start_label: program.allocate_label(),
                        loop_end_label: program.allocate_label(),
                    });

                    program.emit_insn(Insn::OpenEphemeral {
                        cursor_id: temp_cursor_id,
                        is_table: true,
                    });

                    // Main loop
                    // FIXME: rollback is not implemented. E.g. if you insert 2 rows and one fails to unique constraint violation,
                    // the other row will still be inserted.
                    program.preassign_label_to_next_insn(loop_start_label);

                    let yield_label = program.allocate_label();

                    program.emit_insn(Insn::Yield {
                        yield_reg,
                        end_offset: yield_label,
                    });
                    let record_reg = program.alloc_register();
                    program.emit_insn(Insn::MakeRecord {
                        start_reg: yield_reg + 1,
                        count: result.num_result_cols,
                        dest_reg: record_reg,
                        index_name: None,
                    });

                    let rowid_reg = program.alloc_register();
                    program.emit_insn(Insn::NewRowid {
                        cursor: temp_cursor_id,
                        rowid_reg,
                        prev_largest_reg: 0,
                    });

                    program.emit_insn(Insn::Insert {
                        cursor: temp_cursor_id,
                        key_reg: rowid_reg,
                        record_reg,
                        // since we are not doing an Insn::NewRowid or an Insn::NotExists here, we need to seek to ensure the insertion happens in the correct place.
                        flag: InsertFlags::new().require_seek(),
                        table_name: "".to_string(),
                    });

                    // loop back
                    program.emit_insn(Insn::Goto {
                        target_pc: loop_start_label,
                    });

                    program.preassign_label_to_next_insn(yield_label);

                    program.emit_insn(Insn::OpenWrite {
                        cursor_id,
                        root_page: RegisterOrLiteral::Literal(root_page),
                        db: 0,
                    });
                } else {
                    program.emit_insn(Insn::OpenWrite {
                        cursor_id,
                        root_page: RegisterOrLiteral::Literal(root_page),
                        db: 0,
                    });

                    // Main loop
                    // FIXME: rollback is not implemented. E.g. if you insert 2 rows and one fails to unique constraint violation,
                    // the other row will still be inserted.
                    program.preassign_label_to_next_insn(loop_start_label);
                    program.emit_insn(Insn::Yield {
                        yield_reg,
                        end_offset: halt_label,
                    });
                }

                yield_reg_opt = Some(yield_reg);
                (result.num_result_cols, cursor_id)
            }
        }
        InsertBody::DefaultValues => (
            0,
            program.alloc_cursor_id(CursorType::BTreeTable(btree_table.clone())),
        ),
    };

    // allocate cursor id's for each btree index cursor we'll need to populate the indexes
    // (idx name, root_page, idx cursor id)
    let idx_cursors = schema
        .get_indices(table_name.as_str())
        .iter()
        .map(|idx| {
            (
                &idx.name,
                idx.root_page,
                program.alloc_cursor_id(CursorType::BTreeIndex(idx.clone())),
            )
        })
        .collect::<Vec<(&String, usize, usize)>>();

    let column_mappings = resolve_columns_for_insert(&table, &columns, num_values)?;
    let has_user_provided_rowid = column_mappings
        .iter()
        .any(|x| x.value_index.is_some() && x.column.is_rowid_alias);

    // allocate a register for each column in the table. if not provided by user, they will simply be set as null.
    // allocate an extra register for rowid regardless of whether user provided a rowid alias column.
    let num_cols = btree_table.columns.len();
    let rowid_and_columns_start_register = program.alloc_registers(num_cols + 1);
    let columns_start_register = rowid_and_columns_start_register + 1;

    let record_register = program.alloc_register();

    if inserting_multiple_rows {
        if let Some(ref temp_table_ctx) = temp_table_ctx {
            // Rewind loop to read from ephemeral table
            program.emit_insn(Insn::Rewind {
                cursor_id: temp_table_ctx.cursor_id,
                pc_if_empty: temp_table_ctx.loop_end_label,
            });
            program.preassign_label_to_next_insn(temp_table_ctx.loop_start_label);
        }
        populate_columns_multiple_rows(
            &mut program,
            &column_mappings,
            rowid_and_columns_start_register,
            yield_reg_opt.unwrap() + 1,
            &resolver,
            &temp_table_ctx,
        )?;
    } else {
        // Single row - populate registers directly
        program.emit_insn(Insn::OpenWrite {
            cursor_id,
            root_page: RegisterOrLiteral::Literal(root_page),
            db: 0,
        });

        populate_column_registers(
            &mut program,
            &values.unwrap(),
            &column_mappings,
            rowid_and_columns_start_register,
            &resolver,
        )?;
    }
    // Open turso_cdc table btree for writing if necessary
    if let Some((cdc_cursor_id, cdc_btree)) = &cdc_table {
        program.emit_insn(Insn::OpenWrite {
            cursor_id: *cdc_cursor_id,
            root_page: cdc_btree.root_page.into(),
            db: 0,
        });
    }

    // Open all the index btrees for writing
    for idx_cursor in idx_cursors.iter() {
        program.emit_insn(Insn::OpenWrite {
            cursor_id: idx_cursor.2,
            root_page: idx_cursor.1.into(),
            db: 0,
        });
    }
    // Common record insertion logic for both single and multiple rows
    let check_rowid_is_integer_label = if has_user_provided_rowid {
        Some(program.allocate_label())
    } else {
        None
    };
    if has_user_provided_rowid {
        program.emit_insn(Insn::NotNull {
            reg: rowid_and_columns_start_register,
            target_pc: check_rowid_is_integer_label.unwrap(),
        });
    }

    // Create new rowid if a) not provided by user or b) provided by user but is NULL
    program.emit_insn(Insn::NewRowid {
        cursor: cursor_id,
        rowid_reg: rowid_and_columns_start_register,
        prev_largest_reg: 0,
    });

    if let Some(must_be_int_label) = check_rowid_is_integer_label {
        program.resolve_label(must_be_int_label, program.offset());
        // If the user provided a rowid, it must be an integer.
        program.emit_insn(Insn::MustBeInt {
            reg: rowid_and_columns_start_register,
        });
    }

    // Check uniqueness constraint for rowid if it was provided by user.
    // When the DB allocates it there are no need for separate uniqueness checks.
    if has_user_provided_rowid {
        let make_record_label = program.allocate_label();
        program.emit_insn(Insn::NotExists {
            cursor: cursor_id,
            rowid_reg: rowid_and_columns_start_register,
            target_pc: make_record_label,
        });
        let rowid_column = column_mappings
            .iter()
            .find(|x| x.value_index.is_some() && x.column.is_rowid_alias)
            .expect("rowid alias column must be provided");
        let rowid_column_name = rowid_column.column.name.as_deref().unwrap_or(ROWID);
        program.emit_insn(Insn::Halt {
            err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
            description: format!("{}.{}", table_name.as_str(), rowid_column_name),
        });
        program.preassign_label_to_next_insn(make_record_label);
    }

    match table.btree() {
        Some(t) if t.is_strict => {
            program.emit_insn(Insn::TypeCheck {
                start_reg: columns_start_register,
                count: num_cols,
                check_generated: true,
                table_reference: Arc::clone(&t),
            });
        }
        _ => (),
    }

    let index_col_mappings = resolve_indicies_for_insert(schema, table.as_ref(), &column_mappings)?;
    for index_col_mapping in index_col_mappings {
        // find which cursor we opened earlier for this index
        let idx_cursor_id = idx_cursors
            .iter()
            .find(|(name, _, _)| *name == &index_col_mapping.idx_name)
            .map(|(_, _, c_id)| *c_id)
            .expect("no cursor found for index");

        let num_cols = index_col_mapping.columns.len();
        // allocate scratch registers for the index columns plus rowid
        let idx_start_reg = program.alloc_registers(num_cols + 1);

        // copy each index column from the table's column registers into these scratch regs
        for (i, col) in index_col_mapping.columns.iter().enumerate() {
            // copy from the table's column register over to the index's scratch register

            program.emit_insn(Insn::Copy {
                src_reg: rowid_and_columns_start_register + col.0,
                dst_reg: idx_start_reg + i,
                extra_amount: 0,
            });
        }
        // last register is the rowid
        program.emit_insn(Insn::Copy {
            src_reg: rowid_and_columns_start_register,
            dst_reg: idx_start_reg + num_cols,
            extra_amount: 0,
        });

        let index = schema
            .get_index(table_name.as_str(), &index_col_mapping.idx_name)
            .expect("index should be present");

        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: idx_start_reg,
            count: num_cols + 1,
            dest_reg: record_reg,
            index_name: Some(index_col_mapping.idx_name),
        });

        if index.unique {
            let label_idx_insert = program.allocate_label();
            program.emit_insn(Insn::NoConflict {
                cursor_id: idx_cursor_id,
                target_pc: label_idx_insert,
                record_reg: idx_start_reg,
                num_regs: num_cols,
            });
            let column_names = index_col_mapping.columns.iter().enumerate().fold(
                String::with_capacity(50),
                |mut accum, (idx, (index, _))| {
                    if idx > 0 {
                        accum.push_str(", ");
                    }

                    accum.push_str(&btree_table.name);
                    accum.push('.');

                    let name = column_mappings
                        .get(*index)
                        .unwrap()
                        .column
                        .name
                        .as_ref()
                        .expect("column name is None");
                    accum.push_str(name);

                    accum
                },
            );

            program.emit_insn(Insn::Halt {
                err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                description: column_names,
            });

            program.resolve_label(label_idx_insert, program.offset());
        }

        // now do the actual index insertion using the unpacked registers
        program.emit_insn(Insn::IdxInsert {
            cursor_id: idx_cursor_id,
            record_reg,
            unpacked_start: Some(idx_start_reg), // TODO: enable optimization
            unpacked_count: Some((num_cols + 1) as u16),
            // TODO: figure out how to determine whether or not we need to seek prior to insert.
            flags: IdxInsertFlags::new(),
        });
    }

    for (i, col) in column_mappings
        .iter()
        .enumerate()
        .filter(|(_, col)| col.column.notnull)
    {
        // if this is rowid alias - turso-db will emit NULL as a column value and always use rowid for the row as a column value
        if col.column.is_rowid_alias {
            continue;
        }
        let target_reg = i + rowid_and_columns_start_register;
        program.emit_insn(Insn::HaltIfNull {
            target_reg,
            err_code: SQLITE_CONSTRAINT_NOTNULL,
            description: format!(
                "{}.{}",
                table_name,
                col.column
                    .name
                    .as_ref()
                    .expect("Column name must be present")
            ),
        });
    }
    // Create and insert the record
    program.emit_insn(Insn::MakeRecord {
        start_reg: columns_start_register,
        count: num_cols,
        dest_reg: record_register,
        index_name: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: cursor_id,
        key_reg: rowid_and_columns_start_register,
        record_reg: record_register,
        flag: InsertFlags::new(),
        table_name: table_name.to_string(),
    });

    // Emit update in the CDC table if necessary (after the INSERT updated the table)
    if let Some((cdc_cursor_id, _)) = &cdc_table {
        let cdc_has_after = program.capture_data_changes_mode().has_after();
        let after_record_reg = if cdc_has_after {
            Some(emit_cdc_patch_record(
                &mut program,
                &table,
                columns_start_register,
                record_register,
                rowid_and_columns_start_register,
            ))
        } else {
            None
        };
        emit_cdc_insns(
            &mut program,
            &resolver,
            OperationMode::INSERT,
            *cdc_cursor_id,
            rowid_and_columns_start_register,
            None,
            after_record_reg,
            table_name.as_str(),
        )?;
    }

    // Emit RETURNING results if specified
    if !result_columns.is_empty() {
        let value_registers = ReturningValueRegisters {
            rowid_register: rowid_and_columns_start_register,
            columns_start_register,
            num_columns: table.columns().len(),
        };

        emit_returning_results(&mut program, &result_columns, &value_registers)?;
    }

    if inserting_multiple_rows {
        if let Some(temp_table_ctx) = temp_table_ctx {
            program.emit_insn(Insn::Next {
                cursor_id: temp_table_ctx.cursor_id,
                pc_if_next: temp_table_ctx.loop_start_label,
            });
            program.preassign_label_to_next_insn(temp_table_ctx.loop_end_label);

            program.emit_insn(Insn::Close {
                cursor_id: temp_table_ctx.cursor_id,
            });
        } else {
            // For multiple rows which not require a temp table, loop back
            program.emit_insn(Insn::Goto {
                target_pc: loop_start_label,
            });
        }
    }

    program.resolve_label(halt_label, program.offset());

    Ok(program)
}

#[derive(Debug)]
/// Represents how a column should be populated during an INSERT.
/// Contains both the column definition and optionally the index into the VALUES tuple.
struct ColumnMapping<'a> {
    /// Reference to the column definition from the table schema
    column: &'a Column,
    /// If Some(i), use the i-th value from the VALUES tuple
    /// If None, use NULL (column was not specified in INSERT statement)
    value_index: Option<usize>,
    /// The default value for the column, if defined
    default_value: Option<&'a Expr>,
}

pub const ROWID_COLUMN: &Column = &Column {
    name: None,
    ty: schema::Type::Integer,
    ty_str: String::new(),
    primary_key: true,
    is_rowid_alias: true,
    notnull: true,
    default: None,
    unique: false,
    collation: None,
    hidden: false,
};

/// Resolves how each column in a table should be populated during an INSERT.
/// Returns a Vec of ColumnMapping, one for each column in the table's schema.
///
/// For each column, specifies:
/// 1. The column definition (type, constraints, etc)
/// 2. Where to get the value from:
///    - Some(i) -> use i-th value from the VALUES tuple
///    - None -> use NULL (column wasn't specified in INSERT)
///
/// Two cases are handled:
/// 1. No column list specified (INSERT INTO t VALUES ...):
///    - Values are assigned to columns in table definition order
///    - If fewer values than columns, remaining columns map to None
/// 2. Column list specified (INSERT INTO t (col1, col3) VALUES ...):
///    - Named columns map to their corresponding value index
///    - Unspecified columns map to None
fn resolve_columns_for_insert<'a>(
    table: &'a Table,
    columns: &Option<DistinctNames>,
    num_values: usize,
) -> Result<Vec<ColumnMapping<'a>>> {
    let table_columns = table.columns();

    // +1 for rowid implicit ROWID column
    let mut column_mappings = Vec::with_capacity(table_columns.len() + 1);
    column_mappings.push(ColumnMapping {
        column: ROWID_COLUMN,
        value_index: None,
        default_value: None,
    });
    for column in table_columns {
        column_mappings.push(ColumnMapping {
            column,
            value_index: None,
            default_value: column.default.as_ref(),
        });
    }

    if columns.is_none() {
        // Case 1: No columns specified - map values to columns in order
        let mut value_idx = 0;
        for (i, col) in table_columns.iter().enumerate() {
            column_mappings[i + 1].value_index = if col.hidden { None } else { Some(value_idx) };
            if !col.hidden {
                value_idx += 1;
            }
        }

        if num_values != value_idx {
            crate::bail_parse_error!(
                "table {} has {} columns but {} values were supplied",
                &table.get_name(),
                value_idx,
                num_values
            );
        }
    } else {
        // Case 2: Columns specified - map named columns to their values
        // Map each named column to its value index
        for (value_index, column_name) in columns.as_ref().unwrap().iter().enumerate() {
            let column_name = normalize_ident(column_name.as_str());
            let table_index = if column_name == ROWID {
                Some(0)
            } else {
                column_mappings.iter().position(|c| {
                    c.column
                        .name
                        .as_ref()
                        .is_some_and(|name| name.eq_ignore_ascii_case(&column_name))
                })
            };

            let Some(table_index) = table_index else {
                crate::bail_parse_error!(
                    "table {} has no column named {}",
                    &table.get_name(),
                    column_name
                );
            };

            column_mappings[table_index].value_index = Some(value_index);
        }
    }

    Ok(column_mappings)
}

/// Represents how a column in an index should be populated during an INSERT.
/// Similar to ColumnMapping above but includes the index name, as well as multiple
/// possible value indices for each.
#[derive(Debug, Default)]
struct IndexColMapping {
    idx_name: String,
    columns: Vec<(usize, IndexColumn)>,
    value_indicies: Vec<Option<usize>>,
}

impl IndexColMapping {
    fn new(name: String) -> Self {
        IndexColMapping {
            idx_name: name,
            ..Default::default()
        }
    }
}

/// Example:
/// Table 'test': (a, b, c);
/// Index 'idx': test(a, b);
///________________________________
/// Insert (a, c): (2, 3)
/// Record: (2, NULL, 3)
/// IndexColMapping: (a, b) = (2, NULL)
fn resolve_indicies_for_insert(
    schema: &Schema,
    table: &Table,
    columns: &[ColumnMapping<'_>],
) -> Result<Vec<IndexColMapping>> {
    let mut index_col_mappings = Vec::new();
    // Iterate over all indices for this table
    for index in schema.get_indices(table.get_name()) {
        let mut idx_map = IndexColMapping::new(index.name.clone());
        // For each column in the index (in the order defined by the index),
        // try to find the corresponding column in the insert’s column mapping.
        for idx_col in &index.columns {
            let target_name = normalize_ident(idx_col.name.as_str());
            if let Some((i, col_mapping)) = columns.iter().enumerate().find(|(_, mapping)| {
                mapping
                    .column
                    .name
                    .as_ref()
                    .is_some_and(|name| name.eq_ignore_ascii_case(&target_name))
            }) {
                idx_map.columns.push((i, idx_col.clone()));
                idx_map.value_indicies.push(col_mapping.value_index);
            } else {
                return Err(crate::LimboError::ParseError(format!(
                    "Column {} not found in index {}",
                    target_name, index.name
                )));
            }
        }
        // Add the mapping if at least one column was found.
        if !idx_map.columns.is_empty() {
            index_col_mappings.push(idx_map);
        }
    }
    Ok(index_col_mappings)
}

fn populate_columns_multiple_rows(
    program: &mut ProgramBuilder,
    column_mappings: &[ColumnMapping], // columns in order of table definition
    column_registers_start: usize,
    yield_reg: usize,
    resolver: &Resolver,
    temp_table_ctx: &Option<TempTableCtx>,
) -> Result<()> {
    // In case when both rowid and rowid-alias column provided in the query - turso-db overwrite rowid with **latest** value from the list
    // As we iterate by column in natural order of their definition in scheme,
    // we need to track last value_index we wrote to the rowid and overwrite rowid register only if new value_index is greater
    let mut last_rowid_explicit_value = None;
    for (i, mapping) in column_mappings.iter().enumerate() {
        let column_register = column_registers_start + i;

        if let Some(value_index) = mapping.value_index {
            let write_directly_to_rowid_reg = mapping.column.is_rowid_alias;
            let write_reg = if write_directly_to_rowid_reg {
                if last_rowid_explicit_value.is_some_and(|x| x > value_index) {
                    continue;
                }
                last_rowid_explicit_value = Some(value_index);
                column_registers_start // rowid always the first register in the array for insertion record
            } else {
                column_register
            };
            if let Some(temp_table_ctx) = temp_table_ctx {
                program.emit_column(temp_table_ctx.cursor_id, value_index, write_reg);
            } else {
                program.emit_insn(Insn::Copy {
                    src_reg: yield_reg + value_index,
                    dst_reg: write_reg,
                    extra_amount: 0,
                });
            }
            // write_reg can be euqal to column_register if column list explicitly mention "rowid"
            if write_reg != column_register {
                program.emit_null(column_register, None);
                program.mark_last_insn_constant();
            }
        } else if mapping.column.is_rowid_alias {
            program.emit_insn(Insn::SoftNull {
                reg: column_register,
            });
        } else if let Some(default_expr) = mapping.default_value {
            translate_expr(program, None, default_expr, column_register, resolver)?;
        } else {
            // Column was not specified as has no DEFAULT - use NULL if it is nullable, otherwise error
            // Rowid alias columns can be NULL because we will autogenerate a rowid in that case.
            let is_nullable = !mapping.column.primary_key || mapping.column.is_rowid_alias;
            if is_nullable {
                program.emit_insn(Insn::Null {
                    dest: column_register,
                    dest_end: None,
                });
            } else {
                crate::bail_parse_error!(
                    "column {} is not nullable",
                    mapping.column.name.as_ref().expect("column name is None")
                );
            }
        }
    }
    Ok(())
}

/// Populates the column registers with values for a single row
#[allow(clippy::too_many_arguments)]
fn populate_column_registers(
    program: &mut ProgramBuilder,
    value: &[Expr],
    column_mappings: &[ColumnMapping],
    column_registers_start: usize,
    resolver: &Resolver,
) -> Result<()> {
    // In case when both rowid and rowid-alias column provided in the query - turso-db overwrite rowid with **latest** value from the list
    // As we iterate by column in natural order of their definition in scheme,
    // we need to track last value_index we wrote to the rowid and overwrite rowid register only if new value_index is greater
    let mut last_rowid_explicit_value = None;
    for (i, mapping) in column_mappings.iter().enumerate() {
        let column_register = column_registers_start + i;

        // Column has a value in the VALUES tuple
        if let Some(value_index) = mapping.value_index {
            // When inserting a single row, SQLite writes the value provided for the rowid alias column (INTEGER PRIMARY KEY)
            // directly into the rowid register and writes a NULL into the rowid alias column.
            let write_directly_to_rowid_reg = mapping.column.is_rowid_alias;
            let write_reg = if write_directly_to_rowid_reg {
                if last_rowid_explicit_value.is_some_and(|x| x > value_index) {
                    continue;
                }
                last_rowid_explicit_value = Some(value_index);
                column_registers_start // rowid always the first register in the array for insertion record
            } else {
                column_register
            };

            translate_expr_no_constant_opt(
                program,
                None,
                value.get(value_index).expect("value index out of bounds"),
                write_reg,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
            // write_reg can be euqal to column_register if column list explicitly mention "rowid"
            if write_reg != column_register {
                program.emit_null(column_register, None);
                program.mark_last_insn_constant();
            }
        } else if mapping.column.hidden {
            program.emit_insn(Insn::Null {
                dest: column_register,
                dest_end: None,
            });
            program.mark_last_insn_constant();
        } else if let Some(default_expr) = mapping.default_value {
            translate_expr_no_constant_opt(
                program,
                None,
                default_expr,
                column_register,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
        } else {
            // Column was not specified as has no DEFAULT - use NULL if it is nullable, otherwise error
            // Rowid alias columns can be NULL because we will autogenerate a rowid in that case.
            let is_nullable = !mapping.column.primary_key || mapping.column.is_rowid_alias;
            if is_nullable {
                program.emit_insn(Insn::Null {
                    dest: column_register,
                    dest_end: None,
                });
                program.mark_last_insn_constant();
            } else {
                crate::bail_parse_error!(
                    "column {} is not nullable",
                    mapping.column.name.as_ref().expect("column name is None")
                );
            }
        }
    }
    Ok(())
}

// TODO: comeback here later to apply the same improvements on select
fn translate_virtual_table_insert(
    mut program: ProgramBuilder,
    virtual_table: Arc<VirtualTable>,
    columns: Option<DistinctNames>,
    mut body: InsertBody,
    on_conflict: Option<ResolveType>,
    resolver: &Resolver,
) -> Result<ProgramBuilder> {
    if virtual_table.readonly() {
        crate::bail_constraint_error!("Table is read-only: {}", virtual_table.name);
    }
    let (num_values, value) = match &mut body {
        InsertBody::Select(select, None) => match select.body.select.as_mut() {
            OneSelect::Values(values) => (values[0].len(), values.pop().unwrap()),
            _ => crate::bail_parse_error!("Virtual tables only support VALUES clause in INSERT"),
        },
        InsertBody::DefaultValues => (0, vec![]),
        _ => crate::bail_parse_error!("Unsupported INSERT body for virtual tables"),
    };
    let table = Table::Virtual(virtual_table.clone());
    let column_mappings = resolve_columns_for_insert(&table, &columns, num_values)?;
    let registers_start = program.alloc_registers(column_mappings.len() + 1);

    /* *
     * Inserts for virtual tables are done in a single step.
     * argv[0] = (NULL for insert)
     * argv[1] = (rowid for insert - NULL in most cases)
     * argv[2..] = column values
     * */

    program.emit_insn(Insn::Null {
        dest: registers_start,
        dest_end: None,
    });

    populate_column_registers(
        &mut program,
        &value,
        &column_mappings,
        registers_start + 1,
        resolver,
    )?;
    let conflict_action = on_conflict.as_ref().map(|c| c.bit_value()).unwrap_or(0) as u16;

    let cursor_id = program.alloc_cursor_id(CursorType::VirtualTable(virtual_table.clone()));

    program.emit_insn(Insn::VUpdate {
        cursor_id,
        arg_count: column_mappings.len() + 1,
        start_reg: registers_start,
        conflict_action,
    });

    let halt_label = program.allocate_label();
    program.resolve_label(halt_label, program.offset());

    Ok(program)
}
