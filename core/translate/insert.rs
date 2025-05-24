use std::ops::Deref;
use std::rc::Rc;

use limbo_sqlite3_parser::ast::{
    DistinctNames, Expr, InsertBody, OneSelect, QualifiedName, ResolveType, ResultColumn, With,
};

use crate::error::SQLITE_CONSTRAINT_PRIMARYKEY;
use crate::schema::{IndexColumn, Table};
use crate::util::normalize_ident;
use crate::vdbe::builder::{ProgramBuilderOpts, QueryMode};
use crate::vdbe::insn::{IdxInsertFlags, RegisterOrLiteral};
use crate::{
    schema::{Column, Schema},
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::Insn,
    },
};
use crate::{Result, SymbolTable, VirtualTable};

use super::emitter::Resolver;
use super::expr::{translate_expr_no_constant_opt, NoConstantOptReason};
use super::optimizer::rewrite_expr;

#[allow(clippy::too_many_arguments)]
pub fn translate_insert(
    query_mode: QueryMode,
    schema: &Schema,
    with: &Option<With>,
    on_conflict: &Option<ResolveType>,
    tbl_name: &QualifiedName,
    columns: &Option<DistinctNames>,
    body: &mut InsertBody,
    _returning: &Option<Vec<ResultColumn>>,
    syms: &SymbolTable,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    let opts = ProgramBuilderOpts {
        query_mode,
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

    let table_name = &tbl_name.name;
    let table = match schema.get_table(table_name.0.as_str()) {
        Some(table) => table,
        None => crate::bail_corrupt_error!("Parse error: no such table: {}", table_name),
    };

    let resolver = Resolver::new(syms);

    if let Some(virtual_table) = &table.virtual_table() {
        translate_virtual_table_insert(
            &mut program,
            virtual_table.clone(),
            columns,
            body,
            on_conflict,
            &resolver,
        )?;
        program.epilogue(super::emitter::TransactionMode::Write);
        return Ok(program);
    }

    let Some(btree_table) = table.btree() else {
        crate::bail_corrupt_error!("Parse error: no such table: {}", table_name);
    };
    if !btree_table.has_rowid {
        crate::bail_parse_error!("INSERT into WITHOUT ROWID table is not supported");
    }

    let cursor_id = program.alloc_cursor_id(
        Some(table_name.0.clone()),
        CursorType::BTreeTable(btree_table.clone()),
    );
    // allocate cursor id's for each btree index cursor we'll need to populate the indexes
    // (idx name, root_page, idx cursor id)
    let idx_cursors = schema
        .get_indices(&table_name.0)
        .iter()
        .map(|idx| {
            (
                &idx.name,
                idx.root_page,
                program.alloc_cursor_id(
                    Some(table_name.0.clone()),
                    CursorType::BTreeIndex(idx.clone()),
                ),
            )
        })
        .collect::<Vec<(&String, usize, usize)>>();
    let root_page = btree_table.root_page;
    let values = match body {
        InsertBody::Select(ref mut select, _) => match select.body.select.as_mut() {
            OneSelect::Values(ref mut values) => values,
            _ => todo!(),
        },
        InsertBody::DefaultValues => &mut vec![vec![]],
    };
    let mut param_idx = 1;
    for expr in values.iter_mut().flat_map(|v| v.iter_mut()) {
        rewrite_expr(expr, &mut param_idx)?;
    }

    let column_mappings = resolve_columns_for_insert(&table, columns, values)?;
    let index_col_mappings = resolve_indicies_for_insert(schema, table.as_ref(), &column_mappings)?;
    // Check if rowid was provided (through INTEGER PRIMARY KEY as a rowid alias)
    let rowid_alias_index = btree_table.columns.iter().position(|c| c.is_rowid_alias);
    let has_user_provided_rowid = {
        assert_eq!(column_mappings.len(), btree_table.columns.len());
        if let Some(index) = rowid_alias_index {
            column_mappings[index].value_index.is_some()
        } else {
            false
        }
    };

    // allocate a register for each column in the table. if not provided by user, they will simply be set as null.
    // allocate an extra register for rowid regardless of whether user provided a rowid alias column.
    let num_cols = btree_table.columns.len();
    let rowid_reg = program.alloc_registers(num_cols + 1);
    let column_registers_start = rowid_reg + 1;
    let rowid_alias_reg = {
        if has_user_provided_rowid {
            Some(column_registers_start + rowid_alias_index.unwrap())
        } else {
            None
        }
    };

    let record_register = program.alloc_register();
    let halt_label = program.allocate_label();
    let loop_start_label = program.allocate_label();

    let inserting_multiple_rows = values.len() > 1;

    // Multiple rows - use coroutine for value population
    if inserting_multiple_rows {
        let yield_reg = program.alloc_register();
        let jump_on_definition_label = program.allocate_label();
        let start_offset_label = program.allocate_label();
        program.emit_insn(Insn::InitCoroutine {
            yield_reg,
            jump_on_definition: jump_on_definition_label,
            start_offset: start_offset_label,
        });

        program.preassign_label_to_next_insn(start_offset_label);

        for value in values.iter() {
            populate_column_registers(
                &mut program,
                value,
                &column_mappings,
                column_registers_start,
                true,
                rowid_reg,
                &resolver,
            )?;
            program.emit_insn(Insn::Yield {
                yield_reg,
                end_offset: halt_label,
            });
        }
        program.emit_insn(Insn::EndCoroutine { yield_reg });
        program.preassign_label_to_next_insn(jump_on_definition_label);

        program.emit_insn(Insn::OpenWrite {
            cursor_id,
            root_page: RegisterOrLiteral::Literal(root_page),
            name: table_name.0.clone(),
        });

        // Main loop
        // FIXME: rollback is not implemented. E.g. if you insert 2 rows and one fails to unique constraint violation,
        // the other row will still be inserted.
        program.resolve_label(loop_start_label, program.offset());
        program.emit_insn(Insn::Yield {
            yield_reg,
            end_offset: halt_label,
        });
    } else {
        // Single row - populate registers directly
        program.emit_insn(Insn::OpenWrite {
            cursor_id,
            root_page: RegisterOrLiteral::Literal(root_page),
            name: table_name.0.clone(),
        });

        populate_column_registers(
            &mut program,
            &values[0],
            &column_mappings,
            column_registers_start,
            false,
            rowid_reg,
            &resolver,
        )?;
    }
    // Open all the index btrees for writing
    for idx_cursor in idx_cursors.iter() {
        program.emit_insn(Insn::OpenWrite {
            cursor_id: idx_cursor.2,
            root_page: idx_cursor.1.into(),
            name: idx_cursor.0.clone(),
        });
    }
    // Common record insertion logic for both single and multiple rows
    let check_rowid_is_integer_label = rowid_alias_reg.and(Some(program.allocate_label()));
    if let Some(reg) = rowid_alias_reg {
        // for the row record, the rowid alias column (INTEGER PRIMARY KEY) is always set to NULL
        // and its value is copied to the rowid register. in the case where a single row is inserted,
        // the value is written directly to the rowid register (see populate_column_registers()).
        // again, not sure why this only happens in the single row case, but let's mimic sqlite.
        // in the single row case we save a Copy instruction, but in the multiple rows case we do
        // it here in the loop.
        if inserting_multiple_rows {
            program.emit_insn(Insn::Copy {
                src_reg: reg,
                dst_reg: rowid_reg,
                amount: 0, // TODO: rename 'amount' to something else; amount==0 means 1
            });
            // for the row record, the rowid alias column is always set to NULL
            program.emit_insn(Insn::SoftNull { reg });
        }
        // the user provided rowid value might itself be NULL. If it is, we create a new rowid on the next instruction.
        program.emit_insn(Insn::NotNull {
            reg: rowid_reg,
            target_pc: check_rowid_is_integer_label.unwrap(),
        });
    }

    // Create new rowid if a) not provided by user or b) provided by user but is NULL
    program.emit_insn(Insn::NewRowid {
        cursor: cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });

    if let Some(must_be_int_label) = check_rowid_is_integer_label {
        program.resolve_label(must_be_int_label, program.offset());
        // If the user provided a rowid, it must be an integer.
        program.emit_insn(Insn::MustBeInt { reg: rowid_reg });
    }

    // Check uniqueness constraint for rowid if it was provided by user.
    // When the DB allocates it there are no need for separate uniqueness checks.
    if has_user_provided_rowid {
        let make_record_label = program.allocate_label();
        program.emit_insn(Insn::NotExists {
            cursor: cursor_id,
            rowid_reg,
            target_pc: make_record_label,
        });
        let rowid_column_name = if let Some(index) = rowid_alias_index {
            btree_table
                .columns
                .get(index)
                .unwrap()
                .name
                .as_ref()
                .expect("column name is None")
        } else {
            "rowid"
        };

        program.emit_insn(Insn::Halt {
            err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
            description: format!("{}.{}", table_name.0, rowid_column_name),
        });
        program.preassign_label_to_next_insn(make_record_label);
    }

    match table.btree() {
        Some(t) if t.is_strict => {
            program.emit_insn(Insn::TypeCheck {
                start_reg: column_registers_start,
                count: num_cols,
                check_generated: true,
                table_reference: Rc::clone(&t),
            });
        }
        _ => (),
    }

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
                src_reg: column_registers_start + col.0,
                dst_reg: idx_start_reg + i,
                amount: 0,
            });
        }
        // last register is the rowid
        program.emit_insn(Insn::Copy {
            src_reg: rowid_reg,
            dst_reg: idx_start_reg + num_cols,
            amount: 0,
        });

        let index = schema
            .get_index(&table_name.0, &index_col_mapping.idx_name)
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

                    let name = btree_table
                        .columns
                        .get(*index)
                        .unwrap()
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

    // Create and insert the record
    program.emit_insn(Insn::MakeRecord {
        start_reg: column_registers_start,
        count: num_cols,
        dest_reg: record_register,
        index_name: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: cursor_id,
        key_reg: rowid_reg,
        record_reg: record_register,
        flag: 0,
        table_name: table_name.to_string(),
    });

    if inserting_multiple_rows {
        // For multiple rows, loop back
        program.emit_insn(Insn::Goto {
            target_pc: loop_start_label,
        });
    }

    program.resolve_label(halt_label, program.offset());
    program.epilogue(super::emitter::TransactionMode::Write);

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
    values: &[Vec<Expr>],
) -> Result<Vec<ColumnMapping<'a>>> {
    if values.is_empty() {
        crate::bail_parse_error!("no values to insert");
    }

    let table_columns = &table.columns();
    // Case 1: No columns specified - map values to columns in order
    if columns.is_none() {
        let num_values = values[0].len();
        if num_values > table_columns.len() {
            crate::bail_parse_error!(
                "table {} has {} columns but {} values were supplied",
                &table.get_name(),
                table_columns.len(),
                num_values
            );
        }

        // Verify all value tuples have same length
        for value in values.iter().skip(1) {
            if value.len() != num_values {
                crate::bail_parse_error!("all VALUES must have the same number of terms");
            }
        }

        // Map each column to either its corresponding value index or None
        return Ok(table_columns
            .iter()
            .enumerate()
            .map(|(i, col)| ColumnMapping {
                column: col,
                value_index: if i < num_values { Some(i) } else { None },
                default_value: col.default.as_ref(),
            })
            .collect());
    }

    // Case 2: Columns specified - map named columns to their values
    let mut mappings: Vec<_> = table_columns
        .iter()
        .map(|col| ColumnMapping {
            column: col,
            value_index: None,
            default_value: col.default.as_ref(),
        })
        .collect();

    // Map each named column to its value index
    for (value_index, column_name) in columns.as_ref().unwrap().iter().enumerate() {
        let column_name = normalize_ident(column_name.0.as_str());
        let table_index = table_columns.iter().position(|c| {
            c.name
                .as_ref()
                .map_or(false, |name| name.eq_ignore_ascii_case(&column_name))
        });

        let Some(table_index) = table_index else {
            crate::bail_parse_error!(
                "table {} has no column named {}",
                &table.get_name(),
                column_name
            );
        };

        mappings[table_index].value_index = Some(value_index);
    }

    Ok(mappings)
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
                    .map_or(false, |name| name.eq_ignore_ascii_case(&target_name))
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

/// Populates the column registers with values for a single row
#[allow(clippy::too_many_arguments)]
fn populate_column_registers(
    program: &mut ProgramBuilder,
    value: &[Expr],
    column_mappings: &[ColumnMapping],
    column_registers_start: usize,
    inserting_multiple_rows: bool,
    rowid_reg: usize,
    resolver: &Resolver,
) -> Result<()> {
    for (i, mapping) in column_mappings.iter().enumerate() {
        let target_reg = column_registers_start + i;

        // Column has a value in the VALUES tuple
        if let Some(value_index) = mapping.value_index {
            // When inserting a single row, SQLite writes the value provided for the rowid alias column (INTEGER PRIMARY KEY)
            // directly into the rowid register and writes a NULL into the rowid alias column. Not sure why this only happens
            // in the single row case, but let's copy it.
            let write_directly_to_rowid_reg =
                mapping.column.is_rowid_alias && !inserting_multiple_rows;
            let reg = if write_directly_to_rowid_reg {
                rowid_reg
            } else {
                target_reg
            };
            translate_expr_no_constant_opt(
                program,
                None,
                value.get(value_index).expect("value index out of bounds"),
                reg,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
            if write_directly_to_rowid_reg {
                program.emit_insn(Insn::SoftNull { reg: target_reg });
            }
        } else if let Some(default_expr) = mapping.default_value {
            translate_expr_no_constant_opt(
                program,
                None,
                default_expr,
                target_reg,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
        } else {
            // Column was not specified as has no DEFAULT - use NULL if it is nullable, otherwise error
            // Rowid alias columns can be NULL because we will autogenerate a rowid in that case.
            let is_nullable = !mapping.column.primary_key || mapping.column.is_rowid_alias;
            if is_nullable {
                program.emit_insn(Insn::Null {
                    dest: target_reg,
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

fn translate_virtual_table_insert(
    program: &mut ProgramBuilder,
    virtual_table: Rc<VirtualTable>,
    columns: &Option<DistinctNames>,
    body: &InsertBody,
    on_conflict: &Option<ResolveType>,
    resolver: &Resolver,
) -> Result<()> {
    let values = match body {
        InsertBody::Select(select, None) => match &select.body.select.deref() {
            OneSelect::Values(values) => values,
            _ => crate::bail_parse_error!("Virtual tables only support VALUES clause in INSERT"),
        },
        InsertBody::DefaultValues => &vec![],
        _ => crate::bail_parse_error!("Unsupported INSERT body for virtual tables"),
    };
    let table = Table::Virtual(virtual_table.clone());
    let column_mappings = resolve_columns_for_insert(&table, columns, values)?;
    let registers_start = program.alloc_registers(2);

    /* *
     * Inserts for virtual tables are done in a single step.
     * argv[0] = (NULL for insert)
     * argv[1] = (NULL for insert)
     * argv[2..] = column values
     * */

    program.emit_insn(Insn::Null {
        dest: registers_start,
        dest_end: Some(registers_start + 1),
    });

    let values_reg = program.alloc_registers(column_mappings.len());
    populate_column_registers(
        program,
        &values[0],
        &column_mappings,
        values_reg,
        false,
        registers_start,
        resolver,
    )?;
    let conflict_action = on_conflict.as_ref().map(|c| c.bit_value()).unwrap_or(0) as u16;

    let cursor_id = program.alloc_cursor_id(
        Some(virtual_table.name.clone()),
        CursorType::VirtualTable(virtual_table.clone()),
    );

    program.emit_insn(Insn::VUpdate {
        cursor_id,
        arg_count: column_mappings.len() + 2,
        start_reg: registers_start,
        conflict_action,
    });

    let halt_label = program.allocate_label();
    program.resolve_label(halt_label, program.offset());

    Ok(())
}
