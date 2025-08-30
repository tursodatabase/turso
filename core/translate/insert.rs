use std::sync::Arc;

use turso_parser::ast::{
    self, Expr, InsertBody, OneSelect, QualifiedName, ResolveType, ResultColumn, With,
};

use crate::error::{SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_PRIMARYKEY};
use crate::schema::{self, Table};
use crate::translate::emitter::{
    emit_cdc_insns, emit_cdc_patch_record, prepare_cdc_if_necessary, OperationMode,
};
use crate::translate::expr::{
    emit_returning_results, process_returning_clause, ReturningValueRegisters,
};
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
    columns: Vec<ast::Name>,
    mut body: InsertBody,
    mut returning: Vec<ResultColumn>,
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

    let mut values: Option<Vec<Box<Expr>>> = None;
    let inserting_multiple_rows = match &mut body {
        InsertBody::Select(select, _) => match &mut select.body.select {
            // TODO see how to avoid clone
            OneSelect::Values(values_expr) if values_expr.len() <= 1 => {
                if values_expr.is_empty() {
                    crate::bail_parse_error!("no values to insert");
                }
                let mut param_idx = 1;
                for expr in values_expr.iter_mut().flat_map(|v| v.iter_mut()) {
                    match expr.as_mut() {
                        Expr::Id(name) => {
                            if name.is_double_quoted() {
                                *expr =
                                    Expr::Literal(ast::Literal::String(name.to_string())).into();
                            } else {
                                // an INSERT INTO ... VALUES (...) cannot reference columns
                                crate::bail_parse_error!("no such column: {name}");
                            }
                        }
                        Expr::Qualified(first_name, second_name) => {
                            // an INSERT INTO ... VALUES (...) cannot reference columns
                            crate::bail_parse_error!("no such column: {first_name}.{second_name}");
                        }
                        _ => {}
                    }
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

    let cdc_table = prepare_cdc_if_necessary(&mut program, schema, table.get_name())?;

    // Process RETURNING clause using shared module
    let (result_columns, _) = process_returning_clause(
        &mut returning,
        &table,
        table_name.as_str(),
        &mut program,
        connection,
    )?;

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
            if matches!(&select.body.select, OneSelect::Values(values) if values.len() <= 1) {
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
                let result =
                    translate_select(schema, select, syms, program, query_destination, connection)?;
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

    let insertion = build_insertion(&mut program, &table, &columns, num_values)?;

    if inserting_multiple_rows {
        translate_rows_multiple(
            &mut program,
            &insertion,
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

        translate_rows_single(&mut program, &values.unwrap(), &insertion, &resolver)?;
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
    let has_user_provided_rowid = insertion.key.is_provided_by_user();
    let check_rowid_is_integer_label = if has_user_provided_rowid {
        Some(program.allocate_label())
    } else {
        None
    };
    if has_user_provided_rowid {
        program.emit_insn(Insn::NotNull {
            reg: insertion.key_register(),
            target_pc: check_rowid_is_integer_label.unwrap(),
        });
    }

    // Create new rowid if a) not provided by user or b) provided by user but is NULL
    program.emit_insn(Insn::NewRowid {
        cursor: cursor_id,
        rowid_reg: insertion.key_register(),
        prev_largest_reg: 0,
    });

    if let Some(must_be_int_label) = check_rowid_is_integer_label {
        program.resolve_label(must_be_int_label, program.offset());
        // If the user provided a rowid, it must be an integer.
        program.emit_insn(Insn::MustBeInt {
            reg: insertion.key_register(),
        });
    }

    // Check uniqueness constraint for rowid if it was provided by user.
    // When the DB allocates it there are no need for separate uniqueness checks.
    if has_user_provided_rowid {
        let make_record_label = program.allocate_label();
        program.emit_insn(Insn::NotExists {
            cursor: cursor_id,
            rowid_reg: insertion.key_register(),
            target_pc: make_record_label,
        });
        let rowid_column_name = insertion.key.column_name();
        let mut description =
            String::with_capacity(table_name.as_str().len() + rowid_column_name.len() + 2);
        description.push_str(table_name.as_str());
        description.push('.');
        description.push_str(rowid_column_name);
        program.emit_insn(Insn::Halt {
            err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
            description,
        });
        program.preassign_label_to_next_insn(make_record_label);
    }

    match table.btree() {
        Some(t) if t.is_strict => {
            program.emit_insn(Insn::TypeCheck {
                start_reg: insertion.first_col_register(),
                count: insertion.col_mappings.len(),
                check_generated: true,
                table_reference: Arc::clone(&t),
            });
        }
        _ => (),
    }

    for index in schema.get_indices(table_name.as_str()) {
        let column_mappings = index
            .columns
            .iter()
            .map(|idx_col| insertion.get_col_mapping_by_name(&idx_col.name));
        // find which cursor we opened earlier for this index
        let idx_cursor_id = idx_cursors
            .iter()
            .find(|(name, _, _)| *name == &index.name)
            .map(|(_, _, c_id)| *c_id)
            .expect("no cursor found for index");

        let num_cols = index.columns.len();
        // allocate scratch registers for the index columns plus rowid
        let idx_start_reg = program.alloc_registers(num_cols + 1);

        // copy each index column from the table's column registers into these scratch regs
        for (i, column_mapping) in column_mappings.clone().enumerate() {
            // copy from the table's column register over to the index's scratch register
            program.emit_insn(Insn::Copy {
                src_reg: column_mapping.register,
                dst_reg: idx_start_reg + i,
                extra_amount: 0,
            });
        }
        // last register is the rowid
        program.emit_insn(Insn::Copy {
            src_reg: insertion.key_register(),
            dst_reg: idx_start_reg + num_cols,
            extra_amount: 0,
        });

        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: idx_start_reg,
            count: num_cols + 1,
            dest_reg: record_reg,
            index_name: Some(index.name.clone()),
        });

        if index.unique {
            let label_idx_insert = program.allocate_label();
            program.emit_insn(Insn::NoConflict {
                cursor_id: idx_cursor_id,
                target_pc: label_idx_insert,
                record_reg: idx_start_reg,
                num_regs: num_cols,
            });
            let column_names = index.columns.iter().enumerate().fold(
                String::with_capacity(50),
                |mut accum, (idx, column)| {
                    if idx > 0 {
                        accum.push_str(", ");
                    }
                    accum.push_str(&index.name);
                    accum.push('.');
                    accum.push_str(&column.name);
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
            flags: IdxInsertFlags::new().nchange(true),
        });
    }

    for column_mapping in insertion
        .col_mappings
        .iter()
        .filter(|column_mapping| column_mapping.column.notnull)
    {
        // if this is rowid alias - turso-db will emit NULL as a column value and always use rowid for the row as a column value
        if column_mapping.column.is_rowid_alias {
            continue;
        }
        program.emit_insn(Insn::HaltIfNull {
            target_reg: column_mapping.register,
            err_code: SQLITE_CONSTRAINT_NOTNULL,
            description: {
                let mut description = String::with_capacity(
                    table_name.as_str().len()
                        + column_mapping
                            .column
                            .name
                            .as_ref()
                            .expect("Column name must be present")
                            .len()
                        + 2,
                );
                description.push_str(table_name.as_str());
                description.push('.');
                description.push_str(
                    column_mapping
                        .column
                        .name
                        .as_ref()
                        .expect("Column name must be present"),
                );
                description
            },
        });
    }
    // Create and insert the record
    program.emit_insn(Insn::MakeRecord {
        start_reg: insertion.first_col_register(),
        count: insertion.col_mappings.len(),
        dest_reg: insertion.record_register(),
        index_name: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: cursor_id,
        key_reg: insertion.key_register(),
        record_reg: insertion.record_register(),
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
                insertion.first_col_register(),
                insertion.record_register(),
                insertion.key_register(),
            ))
        } else {
            None
        };
        emit_cdc_insns(
            &mut program,
            &resolver,
            OperationMode::INSERT,
            *cdc_cursor_id,
            insertion.key_register(),
            None,
            after_record_reg,
            None,
            table_name.as_str(),
        )?;
    }

    // Emit RETURNING results if specified
    if !result_columns.is_empty() {
        let value_registers = ReturningValueRegisters {
            rowid_register: insertion.key_register(),
            columns_start_register: insertion.first_col_register(),
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

/// Represents how a table should be populated during an INSERT.
#[derive(Debug)]
struct Insertion<'a> {
    /// The integer key ("rowid") provided to the VDBE.
    key: InsertionKey<'a>,
    /// The column values that will be fed to the MakeRecord instruction to insert the row.
    /// If the table has a rowid alias column, it will also be included in this record,
    /// but a NULL will be stored for it.
    col_mappings: Vec<ColMapping<'a>>,
    /// The register that will contain the record built using the MakeRecord instruction.
    record_reg: usize,
}

impl<'a> Insertion<'a> {
    /// Return the register that contains the rowid.
    pub fn key_register(&self) -> usize {
        self.key.register()
    }

    /// Return the first register of the values that used to build the record
    /// for the main table insert.
    pub fn first_col_register(&self) -> usize {
        self.col_mappings
            .first()
            .expect("columns must be present")
            .register
    }

    /// Return the register that contains the record built using the MakeRecord instruction.
    pub fn record_register(&self) -> usize {
        self.record_reg
    }

    /// Returns the column mapping for a given column name.
    pub fn get_col_mapping_by_name(&self, name: &str) -> &ColMapping<'a> {
        if let InsertionKey::RowidAlias(mapping) = &self.key {
            // If the key is a rowid alias, a NULL is emitted as the column value,
            // so we need to return the key mapping instead so that the non-NULL rowid is used
            // for the index insert.
            if mapping
                .column
                .name
                .as_ref()
                .is_some_and(|n| n.eq_ignore_ascii_case(name))
            {
                return mapping;
            }
        }
        self.col_mappings
            .iter()
            .find(|col| {
                col.column
                    .name
                    .as_ref()
                    .is_some_and(|n| n.eq_ignore_ascii_case(name))
            })
            .unwrap_or_else(|| panic!("column {name} not found in insertion"))
    }
}

#[derive(Debug)]
enum InsertionKey<'a> {
    /// Rowid is not provided by user and will be autogenerated.
    Autogenerated { register: usize },
    /// Rowid is provided via the 'rowid' keyword.
    LiteralRowid {
        value_index: Option<usize>,
        register: usize,
    },
    /// Rowid is provided via a rowid alias column.
    RowidAlias(ColMapping<'a>),
}

impl InsertionKey<'_> {
    fn register(&self) -> usize {
        match self {
            InsertionKey::Autogenerated { register } => *register,
            InsertionKey::LiteralRowid { register, .. } => *register,
            InsertionKey::RowidAlias(x) => x.register,
        }
    }
    fn is_provided_by_user(&self) -> bool {
        !matches!(self, InsertionKey::Autogenerated { .. })
    }

    fn column_name(&self) -> &str {
        match self {
            InsertionKey::RowidAlias(x) => x
                .column
                .name
                .as_ref()
                .expect("rowid alias column must be present")
                .as_str(),
            InsertionKey::LiteralRowid { .. } => ROWID,
            InsertionKey::Autogenerated { .. } => ROWID,
        }
    }
}

/// Represents how a column in a table should be populated during an INSERT.
/// In a vector of [ColMapping], the index of a given [ColMapping] is
/// the position of the column in the table.
#[derive(Debug)]
struct ColMapping<'a> {
    /// Column definition
    column: &'a Column,
    /// Index of the value to use from a tuple in the insert statement.
    /// This is needed because the values in the insert statement are not necessarily
    /// in the same order as the columns in the table, nor do they necessarily contain
    /// all of the columns in the table.
    /// If None, a NULL will be emitted for the column, unless it has a default value.
    /// A NULL rowid alias column's value will be autogenerated.
    value_index: Option<usize>,
    /// Register where the value will be stored for insertion into the table.
    register: usize,
}

/// Resolves how each column in a table should be populated during an INSERT.
/// Returns an [Insertion] struct that contains the key and record for the insertion.
fn build_insertion<'a>(
    program: &mut ProgramBuilder,
    table: &'a Table,
    columns: &'a [ast::Name],
    num_values: usize,
) -> Result<Insertion<'a>> {
    let table_columns = table.columns();
    let rowid_register = program.alloc_register();
    let mut insertion_key = InsertionKey::Autogenerated {
        register: rowid_register,
    };
    let mut column_mappings = table
        .columns()
        .iter()
        .map(|c| ColMapping {
            column: c,
            value_index: None,
            register: program.alloc_register(),
        })
        .collect::<Vec<_>>();

    if columns.is_empty() {
        // Case 1: No columns specified - map values to columns in order
        if num_values != table_columns.iter().filter(|c| !c.hidden).count() {
            crate::bail_parse_error!(
                "table {} has {} columns but {} values were supplied",
                &table.get_name(),
                table_columns.len(),
                num_values
            );
        }
        let mut value_idx = 0;
        for (i, col) in table_columns.iter().enumerate() {
            if col.hidden {
                // Hidden columns are not taken into account.
                continue;
            }
            if col.is_rowid_alias {
                insertion_key = InsertionKey::RowidAlias(ColMapping {
                    column: col,
                    value_index: Some(value_idx),
                    register: rowid_register,
                });
            } else {
                column_mappings[i].value_index = Some(value_idx);
            }
            value_idx += 1;
        }
    } else {
        // Case 2: Columns specified - map named columns to their values
        // Map each named column to its value index
        for (value_index, column_name) in columns.iter().enumerate() {
            let column_name = normalize_ident(column_name.as_str());
            if let Some((idx_in_table, col_in_table)) = table.get_column_by_name(&column_name) {
                // Named column
                if col_in_table.is_rowid_alias {
                    insertion_key = InsertionKey::RowidAlias(ColMapping {
                        column: col_in_table,
                        value_index: Some(value_index),
                        register: rowid_register,
                    });
                } else {
                    column_mappings[idx_in_table].value_index = Some(value_index);
                }
            } else if column_name == ROWID {
                // Explicit use of the 'rowid' keyword
                if let Some(col_in_table) = table.columns().iter().find(|c| c.is_rowid_alias) {
                    insertion_key = InsertionKey::RowidAlias(ColMapping {
                        column: col_in_table,
                        value_index: Some(value_index),
                        register: rowid_register,
                    });
                } else {
                    insertion_key = InsertionKey::LiteralRowid {
                        value_index: Some(value_index),
                        register: rowid_register,
                    };
                }
            } else {
                crate::bail_parse_error!(
                    "table {} has no column named {}",
                    &table.get_name(),
                    column_name
                );
            }
        }
    }

    Ok(Insertion {
        key: insertion_key,
        col_mappings: column_mappings,
        record_reg: program.alloc_register(),
    })
}

/// Populates the column registers with values for multiple rows.
/// This is used for INSERT INTO <table> VALUES (...), (...), ... or INSERT INTO <table> SELECT ...
/// which use either a coroutine or an ephemeral table as the value source.
fn translate_rows_multiple<'short, 'long: 'short>(
    program: &mut ProgramBuilder,
    insertion: &'short Insertion<'long>,
    yield_reg: usize,
    resolver: &Resolver,
    temp_table_ctx: &Option<TempTableCtx>,
) -> Result<()> {
    if let Some(ref temp_table_ctx) = temp_table_ctx {
        // Rewind loop to read from ephemeral table
        program.emit_insn(Insn::Rewind {
            cursor_id: temp_table_ctx.cursor_id,
            pc_if_empty: temp_table_ctx.loop_end_label,
        });
        program.preassign_label_to_next_insn(temp_table_ctx.loop_start_label);
    }
    let translate_value_fn =
        |prg: &mut ProgramBuilder, value_index: usize, column_register: usize| {
            if let Some(temp_table_ctx) = temp_table_ctx {
                prg.emit_column_or_rowid(temp_table_ctx.cursor_id, value_index, column_register);
            } else {
                prg.emit_insn(Insn::Copy {
                    src_reg: yield_reg + value_index,
                    dst_reg: column_register,
                    extra_amount: 0,
                });
            }
            Ok(())
        };
    translate_rows_base(program, insertion, translate_value_fn, resolver)
}
/// Populates the column registers with values for a single row
#[allow(clippy::too_many_arguments)]
fn translate_rows_single(
    program: &mut ProgramBuilder,
    value: &[Box<Expr>],
    insertion: &Insertion,
    resolver: &Resolver,
) -> Result<()> {
    let translate_value_fn =
        |prg: &mut ProgramBuilder, value_index: usize, column_register: usize| -> Result<()> {
            translate_expr_no_constant_opt(
                prg,
                None,
                value.get(value_index).unwrap_or_else(|| {
                    panic!("value index out of bounds: {value_index} for value: {value:?}")
                }),
                column_register,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
            Ok(())
        };
    translate_rows_base(program, insertion, translate_value_fn, resolver)
}

/// Translate the key and the columns of the insertion.
/// This function is called by both [translate_rows_single] and [translate_rows_multiple],
/// each providing a different [translate_value_fn] implementation, because for multiple rows
/// we need to emit the values in a loop, from either an ephemeral table or a coroutine,
/// whereas for the single row the translation happens in a single pass without looping.
fn translate_rows_base<'short, 'long: 'short>(
    program: &mut ProgramBuilder,
    insertion: &'short Insertion<'long>,
    mut translate_value_fn: impl FnMut(&mut ProgramBuilder, usize, usize) -> Result<()>,
    resolver: &Resolver,
) -> Result<()> {
    translate_key(program, insertion, &mut translate_value_fn, resolver)?;
    for col in insertion.col_mappings.iter() {
        translate_column(
            program,
            col.column,
            col.register,
            col.value_index,
            &mut translate_value_fn,
            resolver,
        )?;
    }

    Ok(())
}

/// Translate the [InsertionKey].
fn translate_key(
    program: &mut ProgramBuilder,
    insertion: &Insertion,
    mut translate_value_fn: impl FnMut(&mut ProgramBuilder, usize, usize) -> Result<()>,
    resolver: &Resolver,
) -> Result<()> {
    match &insertion.key {
        InsertionKey::RowidAlias(rowid_alias_column) => translate_column(
            program,
            rowid_alias_column.column,
            rowid_alias_column.register,
            rowid_alias_column.value_index,
            &mut translate_value_fn,
            resolver,
        ),
        InsertionKey::LiteralRowid {
            value_index,
            register,
        } => translate_column(
            program,
            ROWID_COLUMN,
            *register,
            *value_index,
            &mut translate_value_fn,
            resolver,
        ),
        InsertionKey::Autogenerated { .. } => Ok(()), // will be populated later
    }
}

fn translate_column(
    program: &mut ProgramBuilder,
    column: &Column,
    column_register: usize,
    value_index: Option<usize>,
    translate_value_fn: &mut impl FnMut(&mut ProgramBuilder, usize, usize) -> Result<()>,
    resolver: &Resolver,
) -> Result<()> {
    if let Some(value_index) = value_index {
        translate_value_fn(program, value_index, column_register)?;
    } else if column.is_rowid_alias {
        // Although a non-NULL integer key is used for the insertion key,
        // the rowid alias column is emitted as NULL.
        program.emit_insn(Insn::SoftNull {
            reg: column_register,
        });
    } else if column.hidden {
        // Emit NULL for not-explicitly-mentioned hidden columns, even ignoring DEFAULT.
        program.emit_insn(Insn::Null {
            dest: column_register,
            dest_end: None,
        });
    } else if let Some(default_expr) = column.default.as_ref() {
        translate_expr(program, None, default_expr, column_register, resolver)?;
    } else {
        let nullable = !column.notnull && !column.primary_key && !column.unique;
        if !nullable {
            crate::bail_parse_error!(
                "column {} is not nullable",
                column
                    .name
                    .as_ref()
                    .expect("column name must be present")
                    .as_str()
            );
        }
        program.emit_insn(Insn::Null {
            dest: column_register,
            dest_end: None,
        });
    }
    Ok(())
}

// TODO: comeback here later to apply the same improvements on select
fn translate_virtual_table_insert(
    mut program: ProgramBuilder,
    virtual_table: Arc<VirtualTable>,
    columns: Vec<ast::Name>,
    mut body: InsertBody,
    on_conflict: Option<ResolveType>,
    resolver: &Resolver,
) -> Result<ProgramBuilder> {
    if virtual_table.readonly() {
        crate::bail_constraint_error!("Table is read-only: {}", virtual_table.name);
    }
    let (num_values, value) = match &mut body {
        InsertBody::Select(select, None) => match &mut select.body.select {
            OneSelect::Values(values) => (values[0].len(), values.pop().unwrap()),
            _ => crate::bail_parse_error!("Virtual tables only support VALUES clause in INSERT"),
        },
        InsertBody::DefaultValues => (0, vec![]),
        _ => crate::bail_parse_error!("Unsupported INSERT body for virtual tables"),
    };
    let table = Table::Virtual(virtual_table.clone());
    /* *
     * Inserts for virtual tables are done in a single step.
     * argv[0] = (NULL for insert)
     */
    let registers_start = program.alloc_register();
    program.emit_insn(Insn::Null {
        dest: registers_start,
        dest_end: None,
    });
    /* *
     * argv[1] = (rowid for insert - NULL in most cases)
     * argv[2..] = column values
     * */
    let insertion = build_insertion(&mut program, &table, &columns, num_values)?;

    translate_rows_single(&mut program, &value, &insertion, resolver)?;
    let conflict_action = on_conflict.as_ref().map(|c| c.bit_value()).unwrap_or(0) as u16;

    let cursor_id = program.alloc_cursor_id(CursorType::VirtualTable(virtual_table.clone()));

    program.emit_insn(Insn::VUpdate {
        cursor_id,
        arg_count: insertion.col_mappings.len() + 2, // +1 for NULL, +1 for rowid
        start_reg: registers_start,
        conflict_action,
    });

    let halt_label = program.allocate_label();
    program.resolve_label(halt_label, program.offset());

    Ok(program)
}
