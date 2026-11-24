//! Bytecode that stores a seeded table on its first write, and that writes
//! the rows of a seeded table.

use crate::dialect::seeded::{
    unwritable_index_reason, unwritable_table_reason, RecordSeedVersion, SeedRowColumn, StoredForm,
    SEED_SCAN,
};
use crate::dialect::CmdContext;
use crate::error::{SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_PRIMARYKEY, SQLITE_ERROR};
use crate::namespace::NamespaceId;
use crate::schema::{BTreeTable, Index, Table, TURSO_SEED_VERSIONS_TABLE_NAME};
use crate::storage::pager::CreateBTreeFlags;
use crate::sync::Arc;
use crate::translate::collate::CollationSeq;
use crate::translate::insert::format_unique_violation_desc;
use crate::translate::schema::{emit_schema_cookie, emit_schema_entry, SchemaEntryType};
use crate::translate::ProgramBuilder;
use crate::util::escape_sql_string_literal;
use crate::vdbe::builder::CursorType;
use crate::vdbe::insn::{
    to_u32, CmpInsFlags, ConstraintInfo, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral,
};
use crate::{bail_parse_error, LimboError, Result, MAIN_DB_ID};

/// A seeded table that a statement writes, with a write cursor on the table
/// and on each of its indexes. Core writes the indexes, so that the order of
/// the index and table writes is the order of an INSERT.
#[derive(Clone)]
pub struct SeededTableWriter {
    table: Arc<BTreeTable>,
    cursor_id: usize,
    /// Each index with its cursor, in the order in which an insert writes
    /// them.
    indexes: Vec<(Arc<Index>, usize)>,
    /// The column and the range of the seed rows. None for a seeded table
    /// of core.
    seed_rows: Option<SeedRowColumn>,
}

/// Which rule a row that the writer inserts follows.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RowOwner {
    /// A seed row, which the fill copies. It must be inside the seed row
    /// range.
    Seed,
    /// A row that the catalog writes. It must be outside the seed row
    /// range, so that a refill never deletes it.
    Catalog,
}

/// Opens the seeded table `name` of the main database for writing. When the
/// schema of the statement has the table stored, it opens the table and its
/// indexes. Otherwise it creates the table and its indexes, writes their
/// `sqlite_schema` rows, makes the schema read them, and copies the seed
/// rows into the table.
pub(crate) fn open_seeded_table_for_write(
    ctx: &CmdContext<'_>,
    program: &mut ProgramBuilder,
    name: &str,
) -> Result<SeededTableWriter> {
    let connection = ctx.connection;
    let dialect = connection.db.dialect();
    let Some(form) = connection
        .db
        .seeded_tables
        .stored_form(dialect.as_ref(), name)?
    else {
        return Err(LimboError::InternalError(format!(
            "no seeded table is named {name}"
        )));
    };
    let resolver = ctx.resolver;
    let schema_cookie = resolver.with_schema(MAIN_DB_ID, |schema| schema.schema_version);
    program.begin_write_on_database(MAIN_DB_ID, schema_cookie)?;
    let (in_schema, name_is_used) = resolver.with_schema(MAIN_DB_ID, |schema| {
        (
            schema.get_table(NamespaceId::ROOT, &form.name),
            schema
                .get_object_type(NamespaceId::ROOT, &form.name)
                .is_some(),
        )
    });
    match in_schema {
        Some(table) => match table.as_ref() {
            Table::BTree(btree) => open_stored_table(ctx, program, form, btree.clone()),
            _ if connection
                .db
                .seeded_tables
                .is_read_only_table(&form.name, &table) =>
            {
                store_seeded_table(ctx, program, form)
            }
            _ => bail_parse_error!(
                "cannot store seeded table {}: another table has the name",
                form.name
            ),
        },
        None if name_is_used => bail_parse_error!(
            "cannot store seeded table {}: another object has the name",
            form.name
        ),
        // A seeded table of core has no read-only table.
        None => store_seeded_table(ctx, program, form),
    }
}

/// Opens the stored table and the indexes that the schema gives it.
fn open_stored_table(
    ctx: &CmdContext<'_>,
    program: &mut ProgramBuilder,
    form: &StoredForm,
    table: Arc<BTreeTable>,
) -> Result<SeededTableWriter> {
    // A stored table of the seeded name can come from a database that
    // another dialect wrote.
    let same_columns = table.columns().len() == form.table.columns().len()
        && table
            .columns()
            .iter()
            .zip(form.table.columns())
            .all(|(stored, seeded)| match (&stored.name, &seeded.name) {
                (Some(stored), Some(seeded)) => stored.eq_ignore_ascii_case(seeded),
                _ => false,
            });
    if !same_columns {
        bail_parse_error!(
            "cannot write seeded table {}: the stored table has other columns",
            form.name
        );
    }
    if let Some(reason) = unwritable_table_reason(&table) {
        bail_parse_error!("cannot write seeded table {}: {reason}", form.name);
    }
    let indexes: Vec<Arc<Index>> = ctx.resolver.with_schema(MAIN_DB_ID, |schema| {
        schema
            .get_indices(NamespaceId::ROOT, &form.name)
            .cloned()
            .collect()
    });
    if let Some(reason) = indexes
        .iter()
        .find_map(|index| unwritable_index_reason(index))
    {
        bail_parse_error!("cannot write seeded table {}: {reason}", form.name);
    }
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: RegisterOrLiteral::Literal(table.root_page),
        db: MAIN_DB_ID,
    });
    let mut index_cursors = Vec::with_capacity(indexes.len());
    for index in indexes {
        let index_cursor_id = program.alloc_cursor_index(None, &index)?;
        program.emit_insn(Insn::OpenWrite {
            cursor_id: index_cursor_id,
            root_page: RegisterOrLiteral::Literal(index.root_page),
            db: MAIN_DB_ID,
        });
        index_cursors.push((index, index_cursor_id));
    }
    Ok(SeededTableWriter {
        table,
        cursor_id,
        indexes: index_cursors,
        seed_rows: form.seed_rows.clone(),
    })
}

/// Creates the B-trees of the table and of its indexes, writes their
/// `sqlite_schema` rows, and makes the schema read them. The schema then has
/// the stored table instead of the read-only table.
///
/// `SetCookie` precedes `ParseSchema` for the reason that
/// [`super::emit_create_internal_table`] gives.
fn store_seeded_table(
    ctx: &CmdContext<'_>,
    program: &mut ProgramBuilder,
    form: &StoredForm,
) -> Result<SeededTableWriter> {
    // The stored table would lose the hidden columns, and queries that
    // read them would fail after the first write.
    if !form.seeded_table.hidden_columns.is_empty() {
        return Err(LimboError::InternalError(format!(
            "cannot store seeded table {}: the stored table cannot have its hidden columns",
            form.name
        )));
    }
    let resolver = ctx.resolver;
    let table_root_reg = program.alloc_register();
    program.emit_insn(Insn::CreateBtree {
        db: MAIN_DB_ID,
        root: table_root_reg,
        flags: CreateBTreeFlags::new_table(),
    });
    let mut index_root_regs = Vec::with_capacity(form.index_rows.len());
    for _ in &form.index_rows {
        let root_reg = program.alloc_register();
        program.emit_insn(Insn::CreateBtree {
            db: MAIN_DB_ID,
            root: root_reg,
            flags: CreateBTreeFlags::new_index(),
        });
        index_root_regs.push(root_reg);
    }

    let sqlite_schema_cursor_id = ctx.main_sqlite_schema_cursor(program);
    emit_schema_entry(
        program,
        resolver,
        sqlite_schema_cursor_id,
        None,
        SchemaEntryType::Table,
        &form.name,
        &form.name,
        table_root_reg,
        Some(form.seeded_table.create_sql.to_string()),
    )?;
    for (row, root_reg) in form.index_rows.iter().zip(&index_root_regs) {
        emit_schema_entry(
            program,
            resolver,
            sqlite_schema_cursor_id,
            None,
            SchemaEntryType::Index,
            &row.name,
            &form.name,
            *root_reg,
            row.sql.map(str::to_string),
        )?;
    }
    emit_schema_cookie(resolver, program);
    program.emit_insn(Insn::ParseSchema {
        db: MAIN_DB_ID,
        where_clause: Some(format!(
            "tbl_name = '{}' AND type != 'trigger'",
            escape_sql_string_literal(&form.name)
        )),
        trigger_target_database_id: None,
    });

    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(form.table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: RegisterOrLiteral::Register(table_root_reg),
        db: MAIN_DB_ID,
    });
    let mut index_cursors = Vec::with_capacity(form.indexes.len());
    for index in &form.indexes {
        let position = form
            .index_rows
            .iter()
            .position(|row| row.name == index.name)
            .expect("each index has a row");
        let index_cursor_id = program.alloc_cursor_index(None, index)?;
        program.emit_insn(Insn::OpenWrite {
            cursor_id: index_cursor_id,
            root_page: RegisterOrLiteral::Register(index_root_regs[position]),
            db: MAIN_DB_ID,
        });
        index_cursors.push((index.clone(), index_cursor_id));
    }
    let writer = SeededTableWriter {
        table: form.table.clone(),
        cursor_id,
        indexes: index_cursors,
        seed_rows: form.seed_rows.clone(),
    };
    if !form.seeded_table.seed.is_empty() {
        emit_fill(program, form, &writer);
    }
    // A seeded table of core has no read-only table and no version: its
    // seed is empty and stays empty.
    if form.read_only.is_some() {
        emit_record_seed_version(ctx, program, form)?;
    }
    Ok(writer)
}

/// Copies the seed rows into the new table, as
/// `INSERT INTO t SELECT rowid, * FROM <read-only table>` does: a scan of the
/// read-only table, and an insert of each row with its rowid.
fn emit_fill(program: &mut ProgramBuilder, form: &StoredForm, writer: &SeededTableWriter) {
    let read_only = form
        .read_only
        .as_ref()
        .and_then(|table| table.virtual_table())
        .expect("a seed with rows has a read-only table");
    let column_count = form.table.columns().len();
    let cursor_id = program.alloc_cursor_id(CursorType::VirtualTable(read_only));
    program.emit_insn(Insn::VOpen { cursor_id });
    let done = program.allocate_label();
    let next_row = program.allocate_label();
    let args_reg = program.alloc_register();
    program.emit_insn(Insn::VFilter {
        cursor_id,
        pc_if_empty: done,
        arg_count: 0,
        args_reg,
        idx_str: None,
        idx_num: SEED_SCAN,
    });
    program.preassign_label_to_next_insn(next_row);
    let values_reg = program.alloc_registers(column_count);
    for column in 0..column_count {
        program.emit_insn(Insn::VColumn {
            cursor_id,
            column,
            dest: values_reg + column,
        });
    }
    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::RowId {
        cursor_id,
        dest: rowid_reg,
    });
    writer.emit_insert_row(program, values_reg, Some(rowid_reg), RowOwner::Seed);
    program.emit_insn(Insn::VNext {
        cursor_id,
        pc_if_next: next_row,
    });
    program.preassign_label_to_next_insn(done);
}

/// Writes the row of `form` in the table of seed versions, with the version
/// and the seed row range of the seed in the binary, and records them in
/// the schema. The row replaces the row that the table had, if it had one.
fn emit_record_seed_version(
    ctx: &CmdContext<'_>,
    program: &mut ProgramBuilder,
    form: &StoredForm,
) -> Result<()> {
    let Some(seed) = form.seed_of_binary() else {
        unreachable!("only a seeded table of a catalog gets a seed version");
    };
    let versions = ctx.open_seeded_table_for_write(program, TURSO_SEED_VERSIONS_TABLE_NAME)?;
    let cursor_id = versions.cursor_id();
    let row_reg = program.alloc_registers(5);
    program.emit_insn(Insn::String8 {
        value: form.name.clone(),
        dest: row_reg,
    });
    program.emit_insn(Insn::Integer {
        value: i64::from(seed.version),
        dest: row_reg + 1,
    });
    program.emit_insn(Insn::String8 {
        value: seed.column.clone(),
        dest: row_reg + 2,
    });
    program.emit_insn(Insn::Integer {
        value: seed.range.start,
        dest: row_reg + 3,
    });
    program.emit_insn(Insn::Integer {
        value: seed.range.end,
        dest: row_reg + 4,
    });

    // The table has a row for the name when the seeded table was stored
    // before, and then dropped.
    let done = program.allocate_label();
    let next_row = program.allocate_label();
    let skip = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id,
        pc_if_empty: done,
    });
    program.preassign_label_to_next_insn(next_row);
    let name_reg = program.alloc_register();
    program.emit_column_or_rowid(cursor_id, 0, name_reg);
    program.emit_insn(Insn::Ne {
        lhs: name_reg,
        rhs: row_reg,
        target_pc: skip,
        flags: CmpInsFlags::default().jump_if_null(),
        collation: CollationSeq::for_internal_values(),
    });
    versions.emit_delete(program);
    program.preassign_label_to_next_insn(skip);
    program.emit_insn(Insn::Next {
        cursor_id,
        pc_if_next: next_row,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(done);

    versions.emit_insert(program, row_reg);
    program.emit_insn(Insn::CatalogApply {
        db: MAIN_DB_ID,
        apply: Arc::new(RecordSeedVersion {
            name: form.name.clone(),
            seed,
        }),
        value_regs: Vec::new(),
    });
    Ok(())
}

impl SeededTableWriter {
    /// The table, with its columns in the order of the stored rows.
    pub fn table(&self) -> &BTreeTable {
        &self.table
    }

    /// The write cursor on the table.
    pub fn cursor_id(&self) -> usize {
        self.cursor_id
    }

    /// Emits the insert of one row. `values_start_reg` is the first of one
    /// register for each column, in column order. The value of the
    /// `INTEGER PRIMARY KEY` column is the rowid; NULL gives a new rowid. A
    /// row that breaks a NOT NULL or UNIQUE constraint fails the statement,
    /// and so does a row inside the [`SeedRowRange`] of the table, which only
    /// seed rows may have. The insert does not count as a change and does
    /// not set `last_insert_rowid()`.
    ///
    /// [`SeedRowRange`]: crate::dialect::SeedRowRange
    pub fn emit_insert(&self, program: &mut ProgramBuilder, values_start_reg: usize) {
        self.emit_insert_row(program, values_start_reg, None, RowOwner::Catalog);
    }

    /// Emits the delete of the row at the table cursor, and of its index
    /// entries. The delete does not count as a change.
    pub fn emit_delete(&self, program: &mut ProgramBuilder) {
        if !self.indexes.is_empty() {
            let rowid_reg = program.alloc_register();
            program.emit_insn(Insn::RowId {
                cursor_id: self.cursor_id,
                dest: rowid_reg,
            });
            let rowid_column = self.rowid_column();
            for (index, index_cursor_id) in &self.indexes {
                let key_reg = program.alloc_registers(index.columns.len() + 1);
                for (position, column) in index.columns.iter().enumerate() {
                    if Some(column.pos_in_table) == rowid_column {
                        program.emit_insn(Insn::Copy {
                            src_reg: rowid_reg,
                            dst_reg: key_reg + position,
                            extra_amount: 0,
                        });
                    } else {
                        program.emit_column_or_rowid(
                            self.cursor_id,
                            column.pos_in_table,
                            key_reg + position,
                        );
                    }
                }
                program.emit_insn(Insn::Copy {
                    src_reg: rowid_reg,
                    dst_reg: key_reg + index.columns.len(),
                    extra_amount: 0,
                });
                program.emit_insn(Insn::IdxDelete {
                    start_reg: key_reg,
                    num_regs: index.columns.len() + 1,
                    cursor_id: *index_cursor_id,
                    raise_error_if_no_matching_entry: true,
                });
            }
        }
        // A catalog row is not a change that the user made. The flag keeps
        // the delete out of the change counters.
        program.emit_insn(Insn::Delete {
            cursor_id: self.cursor_id,
            table_name: self.table.name.clone(),
            is_part_of_update: true,
        });
    }

    fn rowid_column(&self) -> Option<usize> {
        self.table
            .get_rowid_alias_column()
            .map(|(position, _)| position)
    }

    /// Emits the insert of one row, as [`Self::emit_insert`] does. When
    /// `given_rowid_reg` is given, it has the rowid of the row. `owner` says
    /// on which side of the seed row range the row must be.
    fn emit_insert_row(
        &self,
        program: &mut ProgramBuilder,
        values_start_reg: usize,
        given_rowid_reg: Option<usize>,
        owner: RowOwner,
    ) {
        let table = &self.table;
        let columns = table.columns();
        let column_count = columns.len();
        let rowid_column = self.rowid_column();
        let rowid_reg = program.alloc_register();
        let record_reg = program.alloc_registers(column_count);
        program.emit_insn(Insn::Copy {
            src_reg: values_start_reg,
            dst_reg: record_reg,
            extra_amount: column_count - 1,
        });

        // The rowid: the given one, the value of the INTEGER PRIMARY KEY
        // column, or a new one when that value is NULL.
        let rowid_can_conflict = given_rowid_reg.is_some() || rowid_column.is_some();
        match (given_rowid_reg, rowid_column) {
            (Some(given_rowid_reg), _) => {
                program.emit_insn(Insn::Copy {
                    src_reg: given_rowid_reg,
                    dst_reg: rowid_reg,
                    extra_amount: 0,
                });
            }
            (None, Some(rowid_column)) => {
                program.emit_insn(Insn::Copy {
                    src_reg: record_reg + rowid_column,
                    dst_reg: rowid_reg,
                    extra_amount: 0,
                });
                let given = program.allocate_label();
                let known = program.allocate_label();
                program.emit_insn(Insn::NotNull {
                    reg: rowid_reg,
                    target_pc: given,
                });
                program.emit_insn(Insn::NewRowid {
                    cursor: self.cursor_id,
                    rowid_reg,
                    prev_largest_reg: 0,
                });
                program.emit_insn(Insn::Goto { target_pc: known });
                program.preassign_label_to_next_insn(given);
                program.emit_insn(Insn::MustBeInt {
                    reg: rowid_reg,
                    target_pc: None,
                });
                program.preassign_label_to_next_insn(known);
            }
            (None, None) => {
                program.emit_insn(Insn::NewRowid {
                    cursor: self.cursor_id,
                    rowid_reg,
                    prev_largest_reg: 0,
                });
            }
        }
        // As SQLite does, the record has NULL for the INTEGER PRIMARY KEY
        // column, whose value is the rowid.
        if let Some(rowid_column) = rowid_column {
            program.emit_insn(Insn::SoftNull {
                reg: record_reg + rowid_column,
            });
        }

        for (position, column) in columns.iter().enumerate() {
            if !column.notnull() || Some(position) == rowid_column {
                continue;
            }
            let column_name = column.name.clone().unwrap_or_default();
            program.emit_insn(Insn::HaltIfNull {
                constraint_info: Some(Box::new(ConstraintInfo::not_null(
                    &table.name,
                    column_name.clone(),
                ))),
                target_reg: record_reg + position,
                err_code: SQLITE_CONSTRAINT_NOTNULL,
                description: format!("{}.{column_name}", table.name),
                on_error: None,
            });
        }
        let affinities: String = columns
            .iter()
            .map(|column| column.affinity().aff_mask())
            .collect();
        program.emit_insn(Insn::Affinity {
            start_reg: record_reg,
            count: std::num::NonZeroUsize::new(column_count).expect("a seeded table has columns"),
            affinities,
        });

        if rowid_can_conflict {
            let free = program.allocate_label();
            program.emit_insn(Insn::NotExists {
                cursor: self.cursor_id,
                rowid_reg,
                target_pc: free,
            });
            let rowid_name = rowid_column
                .and_then(|position| columns[position].name.clone())
                .unwrap_or_else(|| "rowid".to_string());
            program.emit_insn(Insn::Halt {
                constraint_info: Some(Box::new(ConstraintInfo::primary_key(
                    &table.name,
                    table.primary_key_constraint_name.clone(),
                    vec![rowid_name.clone()],
                    vec![rowid_reg],
                ))),
                err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                description: format!("{}.{rowid_name}", table.name),
                on_error: None,
                description_reg: None,
            });
            program.preassign_label_to_next_insn(free);
        }
        self.emit_seed_row_range_check(program, record_reg, rowid_reg, owner);

        for (index, index_cursor_id) in self.indexes.iter().filter(|(index, _)| index.unique) {
            let key_reg = self.emit_index_key(program, index, record_reg, rowid_reg);
            let no_conflict = program.allocate_label();
            program.emit_insn(Insn::NoConflict {
                cursor_id: *index_cursor_id,
                target_pc: no_conflict,
                record_reg: key_reg,
                num_regs: index.columns.len(),
            });
            let constraint_info = ConstraintInfo::unique_index(
                table,
                index,
                (0..index.columns.len()).map(|i| key_reg + i).collect(),
            );
            let err_code = constraint_info.kind.sqlite_extended_result_code();
            program.emit_insn(Insn::Halt {
                constraint_info: Some(Box::new(constraint_info)),
                err_code,
                description: format_unique_violation_desc(table, index),
                on_error: None,
                description_reg: None,
            });
            program.preassign_label_to_next_insn(no_conflict);
        }

        let table_record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(record_reg),
            count: to_u32(column_count),
            dest_reg: to_u32(table_record_reg),
            index_name: None,
            affinity_str: None,
        });
        for (index, index_cursor_id) in &self.indexes {
            let key_reg = self.emit_index_key(program, index, record_reg, rowid_reg);
            let index_record_reg = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(key_reg),
                count: to_u32(index.columns.len() + 1),
                dest_reg: to_u32(index_record_reg),
                index_name: Some(index.name.clone()),
                affinity_str: None,
            });
            program.emit_insn(Insn::IdxInsert {
                cursor_id: *index_cursor_id,
                record_reg: index_record_reg,
                unpacked_start: Some(key_reg),
                unpacked_count: Some(to_u32(index.columns.len() + 1)),
                flags: IdxInsertFlags::new(),
            });
        }
        // A catalog row is not a change that the user made.
        program.emit_insn(Insn::Insert {
            cursor: self.cursor_id,
            key_reg: rowid_reg,
            record_reg: table_record_reg,
            flag: InsertFlags::new()
                .skip_last_rowid()
                .skip_all_change_counts(),
            table_name: table.name.clone(),
        });
    }

    /// Emits the check that the row in `record_reg`, whose rowid is in
    /// `rowid_reg`, is on the side of the seed row range that `owner` says.
    /// A row on the other side halts the statement with an error. A NULL
    /// value is outside the range.
    fn emit_seed_row_range_check(
        &self,
        program: &mut ProgramBuilder,
        record_reg: usize,
        rowid_reg: usize,
        owner: RowOwner,
    ) {
        let Some(seed_rows) = &self.seed_rows else {
            return;
        };
        let table = &self.table.name;
        let column = self.table.columns()[seed_rows.position]
            .name
            .clone()
            .unwrap_or_default();
        // The record has NULL for the INTEGER PRIMARY KEY column, whose value
        // is the rowid.
        let value_reg = if Some(seed_rows.position) == self.rowid_column() {
            rowid_reg
        } else {
            record_reg + seed_rows.position
        };
        let bounds_reg = program.alloc_registers(2);
        program.emit_insn(Insn::Integer {
            value: seed_rows.range.start,
            dest: bounds_reg,
        });
        program.emit_insn(Insn::Integer {
            value: seed_rows.range.end,
            dest: bounds_reg + 1,
        });
        let outside = program.allocate_label();
        program.emit_insn(Insn::IsNull {
            reg: value_reg,
            target_pc: outside,
        });
        program.emit_insn(Insn::Lt {
            lhs: value_reg,
            rhs: bounds_reg,
            target_pc: outside,
            flags: CmpInsFlags::default(),
            collation: CollationSeq::for_internal_values(),
        });
        program.emit_insn(Insn::Ge {
            lhs: value_reg,
            rhs: bounds_reg + 1,
            target_pc: outside,
            flags: CmpInsFlags::default(),
            collation: CollationSeq::for_internal_values(),
        });
        let (start, end) = (seed_rows.range.start, seed_rows.range.end);
        match owner {
            RowOwner::Catalog => {
                program.emit_insn(Insn::Halt {
                    constraint_info: None,
                    err_code: SQLITE_ERROR,
                    description: format!(
                        "cannot write a row of seeded table {table}: a row whose {column} \
                         is at least {start} and less than {end} is a seed row"
                    ),
                    on_error: None,
                    description_reg: None,
                });
                program.preassign_label_to_next_insn(outside);
            }
            RowOwner::Seed => {
                let inside = program.allocate_label();
                program.emit_insn(Insn::Goto { target_pc: inside });
                program.preassign_label_to_next_insn(outside);
                program.emit_insn(Insn::Halt {
                    constraint_info: None,
                    err_code: SQLITE_ERROR,
                    description: format!(
                        "a seed row of seeded table {table} is outside its seed row range: \
                         {column} must be at least {start} and less than {end}"
                    ),
                    on_error: None,
                    description_reg: None,
                });
                program.preassign_label_to_next_insn(inside);
            }
        }
    }

    /// Copies the key of `index` for the row in `record_reg` to new
    /// registers: the indexed columns, then the rowid. Returns the first
    /// register.
    fn emit_index_key(
        &self,
        program: &mut ProgramBuilder,
        index: &Index,
        record_reg: usize,
        rowid_reg: usize,
    ) -> usize {
        let rowid_column = self.rowid_column();
        let key_reg = program.alloc_registers(index.columns.len() + 1);
        for (position, column) in index.columns.iter().enumerate() {
            let src_reg = if Some(column.pos_in_table) == rowid_column {
                rowid_reg
            } else {
                record_reg + column.pos_in_table
            };
            program.emit_insn(Insn::Copy {
                src_reg,
                dst_reg: key_reg + position,
                extra_amount: 0,
            });
        }
        program.emit_insn(Insn::Copy {
            src_reg: rowid_reg,
            dst_reg: key_reg + index.columns.len(),
            extra_amount: 0,
        });
        key_reg
    }
}
