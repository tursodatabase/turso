use std::sync::Arc;

use turso_parser::ast;

use crate::{
    bail_parse_error,
    schema::BTreeTable,
    storage::pager::CreateBTreeFlags,
    translate::{
        emitter::Resolver,
        schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID},
    },
    util::normalize_ident,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{Insn, RegisterOrLiteral},
    },
    Result,
};

pub fn translate_analyze(
    target_opt: Option<ast::QualifiedName>,
    resolver: &Resolver,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    let Some(target) = target_opt else {
        bail_parse_error!("ANALYZE with no target is not supported");
    };
    let normalized = normalize_ident(target.name.as_str());
    let Some(target_schema) = resolver.schema.get_table(&normalized) else {
        bail_parse_error!("ANALYZE <schema_name> is not supported");
    };
    let Some(target_btree) = target_schema.btree() else {
        bail_parse_error!("ANALYZE on index is not supported");
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
        // sqlite_stat1 already exists, so we need to remove the row
        // corresponding to the stats for the table which we're about to
        // ANALYZE. SQLite implements this as a full table scan over
        // sqlite_stat1 deleting any rows where the first column (table_name)
        // is the targeted table.
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
            value: target_schema.get_name().to_string(),
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
        //FIXME: Emit SetCookie?
        let parse_schema_where_clause =
            "tbl_name = 'sqlite_stat1' AND type != 'trigger'".to_string();
        program.emit_insn(Insn::ParseSchema {
            db: sqlite_schema_cursor_id,
            where_clause: Some(parse_schema_where_clause),
        });
    };

    if target_schema.columns().iter().any(|c| c.primary_key) {
        bail_parse_error!("ANALYZE on tables with primary key is not supported");
    }
    if !target_btree.has_rowid {
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
    let target_cursor = program.alloc_cursor_id(CursorType::BTreeTable(target_btree.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: target_cursor,
        root_page: target_btree.root_page,
        db: 0,
    });
    let rowid_reg = program.alloc_register();
    let record_reg = program.alloc_register();
    let tablename_reg = program.alloc_register();
    let indexname_reg = program.alloc_register();
    let count_reg = program.alloc_register();
    program.emit_insn(Insn::String8 {
        value: target_schema.get_name().to_string(),
        dest: tablename_reg,
    });
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
    // FIXME: Emit LoadAnalysis
    // FIXME: Emit Expire
    Ok(program)
}
