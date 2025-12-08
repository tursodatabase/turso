use std::sync::Arc;

use crate::{
    bail_parse_error,
    schema::{BTreeTable, Index},
    storage::pager::CreateBTreeFlags,
    translate::{
        emitter::Resolver,
        schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID},
    },
    util::normalize_ident,
    vdbe::{
        affinity::Affinity,
        builder::{CursorType, ProgramBuilder},
        insn::{Cookie, Insn, RegisterOrLiteral},
    },
    Result,
};
use turso_parser::ast;

pub fn translate_analyze(
    target_opt: Option<ast::QualifiedName>,
    resolver: &Resolver,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    enum AnalyzeTarget {
        Table { table: Arc<BTreeTable> },
        Index { table: Arc<BTreeTable> },
    }

    let (target_table, target_index) = match target_opt {
        Some(target) => {
            let normalized = normalize_ident(target.name.as_str());
            if let Some(table) = resolver.schema.get_btree_table(&normalized) {
                (
                    AnalyzeTarget::Table {
                        table: table.clone(),
                    },
                    None,
                )
            } else {
                // Try to find an index by this name.
                let mut found: Option<(Arc<BTreeTable>, Arc<Index>)> = None;
                for (table_name, indexes) in resolver.schema.indexes.iter() {
                    if let Some(index) = indexes
                        .iter()
                        .find(|idx| idx.name.eq_ignore_ascii_case(&normalized))
                    {
                        if let Some(table) = resolver.schema.get_btree_table(table_name) {
                            found = Some((table, index.clone()));
                            break;
                        }
                    }
                }
                let Some((table, index)) = found else {
                    bail_parse_error!("no such table or index: {}", target.name);
                };
                (
                    AnalyzeTarget::Index {
                        table: table.clone(),
                    },
                    Some(index),
                )
            }
        }
        None => bail_parse_error!("ANALYZE with no target is not supported"),
    };

    let target_table = match &target_table {
        AnalyzeTarget::Table { table } => table.clone(),
        AnalyzeTarget::Index { table, .. } => table.clone(),
    };

    // This is emitted early because SQLite does, and thus generated VDBE matches a bit closer.
    let null_reg = program.alloc_register();
    program.emit_insn(Insn::Null {
        dest: null_reg,
        dest_end: None,
    });

    // After preparing/creating sqlite_stat1, we need to OpenWrite it, and how we acquire
    // the necessary BTreeTable for cursor creation and root page for the instruction changes
    // depending on which path we take.
    let sqlite_stat1_btreetable: Arc<BTreeTable>;
    let sqlite_stat1_source: RegisterOrLiteral<_>;

    if let Some(sqlite_stat1) = resolver.schema.get_btree_table("sqlite_stat1") {
        sqlite_stat1_btreetable = sqlite_stat1.clone();
        sqlite_stat1_source = RegisterOrLiteral::Literal(sqlite_stat1.root_page);
        // sqlite_stat1 already exists, so we need to remove any rows
        // corresponding to the stats for the table which we're about to
        // ANALYZE. SQLite implements this as a full table scan over sqlite_stat1
        // deleting any rows where the first column (table_name) is the targeted table.
        let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_stat1.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id,
            root_page: RegisterOrLiteral::Literal(sqlite_stat1.root_page),
            db: 0,
        });
        let after_loop = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id,
            pc_if_empty: after_loop,
        });
        let loophead = program.allocate_label();
        program.preassign_label_to_next_insn(loophead);
        let column_reg = program.alloc_register();
        program.emit_insn(Insn::Column {
            cursor_id,
            column: 0,
            dest: column_reg,
            default: None,
        });
        let tablename_reg = program.alloc_register();
        program.emit_insn(Insn::String8 {
            value: target_table.name.to_string(),
            dest: tablename_reg,
        });
        program.mark_last_insn_constant();
        // FIXME: The SQLite instruction says p4=BINARY-8 and p5=81.  Neither are currently supported in Turso.
        program.emit_insn(Insn::Ne {
            lhs: column_reg,
            rhs: tablename_reg,
            target_pc: after_loop,
            flags: Default::default(),
            collation: None,
        });
        let rowid_reg = program.alloc_register();
        program.emit_insn(Insn::RowId {
            cursor_id,
            dest: rowid_reg,
        });
        program.emit_insn(Insn::Delete {
            cursor_id,
            table_name: "sqlite_stat1".to_string(),
            is_part_of_update: false,
        });
        program.emit_insn(Insn::Next {
            cursor_id,
            pc_if_next: loophead,
        });
        program.preassign_label_to_next_insn(after_loop);
    } else {
        // FIXME: Emit ReadCookie 0 3 2
        // FIXME: Emit If 3 +2 0
        // FIXME: Emit SetCookie 0 2 4
        // FIXME: Emit SetCookie 0 5 1

        // See the large comment in schema.rs:translate_create_table about
        // deviating from SQLite codegen, as the same deviation is being done
        // here.

        // TODO: this code half-copies translate_create_table, because there's
        // no way to get the table_root_reg back out, and it's needed for later
        // codegen to open the table we just created.  It's worth a future
        // refactoring to remove the duplication one the rest of ANALYZE is
        // implemented.
        let table_root_reg = program.alloc_register();
        program.emit_insn(Insn::CreateBtree {
            db: 0,
            root: table_root_reg,
            flags: CreateBTreeFlags::new_table(),
        });
        let sql = "CREATE TABLE sqlite_stat1(tbl,idx,stat)";
        // The root_page==0 is false, but we don't rely on it, and there's no
        // way to initialize it with a correct value.
        sqlite_stat1_btreetable = Arc::new(BTreeTable::from_sql(sql, 0)?);
        sqlite_stat1_source = RegisterOrLiteral::Register(table_root_reg);

        let table = resolver.schema.get_btree_table(SQLITE_TABLEID).unwrap();
        let sqlite_schema_cursor_id =
            program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: sqlite_schema_cursor_id,
            root_page: 1i64.into(),
            db: 0,
        });

        // Add the table entry to sqlite_schema
        emit_schema_entry(
            &mut program,
            resolver,
            sqlite_schema_cursor_id,
            None,
            SchemaEntryType::Table,
            "sqlite_stat1",
            "sqlite_stat1",
            table_root_reg,
            Some(sql.to_string()),
        )?;

        let parse_schema_where_clause =
            "tbl_name = 'sqlite_stat1' AND type != 'trigger'".to_string();
        program.emit_insn(Insn::ParseSchema {
            db: sqlite_schema_cursor_id,
            where_clause: Some(parse_schema_where_clause),
        });

        // Bump schema cookie so subsequent statements reparse schema.
        program.emit_insn(Insn::SetCookie {
            db: 0,
            cookie: Cookie::SchemaVersion,
            value: resolver.schema.schema_version as i32 + 1,
            p5: 0,
        });
    };

    if !target_table.has_rowid {
        bail_parse_error!("ANALYZE on tables without rowid is not supported");
    }

    // Count the number of rows in the target table, and insert it into sqlite_stat1.
    let sqlite_stat1 = sqlite_stat1_btreetable;
    let stat_cursor = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_stat1.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: stat_cursor,
        root_page: sqlite_stat1_source,
        db: 0,
    });

    // Remove existing stat rows for this target before inserting fresh ones.
    let rewind_done = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: stat_cursor,
        pc_if_empty: rewind_done,
    });
    let loop_start = program.allocate_label();
    program.preassign_label_to_next_insn(loop_start);

    let tbl_col_reg = program.alloc_register();
    program.emit_insn(Insn::Column {
        cursor_id: stat_cursor,
        column: 0,
        dest: tbl_col_reg,
        default: None,
    });
    let target_tbl_reg = program.alloc_register();
    program.emit_insn(Insn::String8 {
        value: target_table.name.to_string(),
        dest: target_tbl_reg,
    });
    program.mark_last_insn_constant();

    let skip_label = program.allocate_label();
    program.emit_insn(Insn::Ne {
        lhs: tbl_col_reg,
        rhs: target_tbl_reg,
        target_pc: skip_label,
        flags: Default::default(),
        collation: None,
    });

    if let Some(idx) = target_index.clone() {
        let idx_col_reg = program.alloc_register();
        program.emit_insn(Insn::Column {
            cursor_id: stat_cursor,
            column: 1,
            dest: idx_col_reg,
            default: None,
        });
        let target_idx_reg = program.alloc_register();
        program.emit_insn(Insn::String8 {
            value: idx.name.to_string(),
            dest: target_idx_reg,
        });
        program.mark_last_insn_constant();
        let continue_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: idx_col_reg,
            rhs: target_idx_reg,
            target_pc: continue_label,
            flags: Default::default(),
            collation: None,
        });
        let rowid_reg = program.alloc_register();
        program.emit_insn(Insn::RowId {
            cursor_id: stat_cursor,
            dest: rowid_reg,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: stat_cursor,
            table_name: "sqlite_stat1".to_string(),
            is_part_of_update: false,
        });
        program.emit_insn(Insn::Next {
            cursor_id: stat_cursor,
            pc_if_next: loop_start,
        });
        program.preassign_label_to_next_insn(continue_label);
    } else {
        let rowid_reg = program.alloc_register();
        program.emit_insn(Insn::RowId {
            cursor_id: stat_cursor,
            dest: rowid_reg,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: stat_cursor,
            table_name: "sqlite_stat1".to_string(),
            is_part_of_update: false,
        });
        program.emit_insn(Insn::Next {
            cursor_id: stat_cursor,
            pc_if_next: loop_start,
        });
    }

    program.preassign_label_to_next_insn(skip_label);
    program.emit_insn(Insn::Next {
        cursor_id: stat_cursor,
        pc_if_next: loop_start,
    });
    program.preassign_label_to_next_insn(rewind_done);

    let target_cursor = program.alloc_cursor_id(CursorType::BTreeTable(target_table.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: target_cursor,
        root_page: target_table.root_page,
        db: 0,
    });
    let rowid_reg = program.alloc_register();
    let tablename_reg = program.alloc_register();
    let indexname_reg = program.alloc_register();
    let stat_text_reg = program.alloc_register();
    let record_reg = program.alloc_register();
    let count_reg = program.alloc_register();
    program.emit_insn(Insn::String8 {
        value: target_table.name.to_string(),
        dest: tablename_reg,
    });
    program.mark_last_insn_constant();
    program.emit_insn(Insn::Count {
        cursor_id: target_cursor,
        target_reg: count_reg,
        exact: true,
    });
    let after_insert = program.allocate_label();
    program.emit_insn(Insn::IfNot {
        reg: count_reg,
        target_pc: after_insert,
        jump_if_null: false,
    });
    program.emit_insn(Insn::Null {
        dest: indexname_reg,
        dest_end: None,
    });
    // stat = CAST(count AS TEXT)
    program.emit_insn(Insn::Copy {
        src_reg: count_reg,
        dst_reg: stat_text_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Cast {
        reg: stat_text_reg,
        affinity: Affinity::Text,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: tablename_reg,
        count: 3,
        dest_reg: record_reg,
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: stat_cursor,
        rowid_reg,
        prev_largest_reg: 0,
    });
    // FIXME: SQLite sets OPFLAG_APPEND on the insert, but that's not supported in turso right now.
    // SQLite doesn't emit the table name, but like... why not?
    program.emit_insn(Insn::Insert {
        cursor: stat_cursor,
        key_reg: rowid_reg,
        record_reg,
        flag: Default::default(),
        table_name: "sqlite_stat1".to_string(),
    });
    program.preassign_label_to_next_insn(after_insert);
    // Emit index stats for this table (or for a single index target).
    let indexes: Vec<Arc<Index>> = match target_index {
        Some(idx) => vec![idx],
        None => resolver
            .schema
            .get_indices(&target_table.name)
            .filter(|idx| idx.index_method.is_none()) // skip virtual/custom for now
            .cloned()
            .collect(),
    };

    if !indexes.is_empty() {
        let space_reg = program.alloc_register();
        program.emit_insn(Insn::String8 {
            value: " ".to_string(),
            dest: space_reg,
        });
        program.mark_last_insn_constant();

        for index in indexes {
            let idx_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
            program.emit_insn(Insn::OpenRead {
                cursor_id: idx_cursor,
                root_page: index.root_page,
                db: 0,
            });

            let idx_count_reg = program.alloc_register();
            program.emit_insn(Insn::Count {
                cursor_id: idx_cursor,
                target_reg: idx_count_reg,
                exact: true,
            });

            let idx_tablename_reg = program.alloc_register();
            let idx_name_reg = program.alloc_register();
            let idx_stat_reg = program.alloc_register();
            let idx_record_reg = program.alloc_register();

            program.emit_insn(Insn::String8 {
                value: target_table.name.to_string(),
                dest: idx_tablename_reg,
            });
            program.mark_last_insn_constant();
            program.emit_insn(Insn::String8 {
                value: index.name.to_string(),
                dest: idx_name_reg,
            });
            program.mark_last_insn_constant();

            // idx_stat_reg starts as CAST(count AS TEXT)
            program.emit_insn(Insn::Copy {
                src_reg: idx_count_reg,
                dst_reg: idx_stat_reg,
                extra_amount: 0,
            });
            program.emit_insn(Insn::Cast {
                reg: idx_stat_reg,
                affinity: Affinity::Text,
            });

            // Append one entry per indexed column; naive fallback uses the same count value.
            for _ in index.columns.iter() {
                let part_reg = program.alloc_register();
                program.emit_insn(Insn::Copy {
                    src_reg: idx_count_reg,
                    dst_reg: part_reg,
                    extra_amount: 0,
                });
                program.emit_insn(Insn::Cast {
                    reg: part_reg,
                    affinity: Affinity::Text,
                });
                program.emit_insn(Insn::Concat {
                    lhs: idx_stat_reg,
                    rhs: space_reg,
                    dest: idx_stat_reg,
                });
                program.emit_insn(Insn::Concat {
                    lhs: idx_stat_reg,
                    rhs: part_reg,
                    dest: idx_stat_reg,
                });
            }

            program.emit_insn(Insn::MakeRecord {
                start_reg: idx_tablename_reg,
                count: 3,
                dest_reg: idx_record_reg,
                index_name: None,
                affinity_str: None,
            });
            let idx_rowid_reg = program.alloc_register();
            program.emit_insn(Insn::NewRowid {
                cursor: stat_cursor,
                rowid_reg: idx_rowid_reg,
                prev_largest_reg: 0,
            });
            program.emit_insn(Insn::Insert {
                cursor: stat_cursor,
                key_reg: idx_rowid_reg,
                record_reg: idx_record_reg,
                flag: Default::default(),
                table_name: "sqlite_stat1".to_string(),
            });
        }
    }

    // FIXME: Emit LoadAnalysis
    // FIXME: Emit Expire
    Ok(program)
}
