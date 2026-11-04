//! Bytecode for the statements that enable and disable row-level security,
//! and for keeping the stored tables with row-level security up to date when
//! a table is renamed or dropped.

use crate::{
    bail_parse_error,
    namespace::{NamespaceId, NamespaceRef},
    security::policy::{PolicyCatalog, CREATE_ROW_SECURITY_TABLE_SQL, ROW_SECURITY_TABLE_NAME},
    translate::{
        collate::CollationSeq,
        emitter::Resolver,
        schema::{emit_create_internal_table, emit_schema_cookie},
    },
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{to_u32, CmpInsFlags, InsertFlags, Insn, RegisterOrLiteral},
    },
    Result, MAIN_DB_ID,
};

/// ALTER TABLE ... ENABLE ROW LEVEL SECURITY, or DISABLE when `enabled` is
/// false. Enabling it for a table that already has it, or disabling it for a
/// table that does not, changes nothing.
pub fn translate_set_row_security(
    namespace: NamespaceRef,
    table_name: &str,
    enabled: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    if !resolver.catalog.dialect.supports_roles() {
        bail_parse_error!("row-level security is not supported");
    }
    if namespace.database_id != MAIN_DB_ID {
        bail_parse_error!("row-level security is only supported for tables in the main database");
    }
    if resolver
        .schema()
        .policies
        .is_enabled(namespace.id, table_name)
        == enabled
    {
        return Ok(());
    }
    let rows = RowSecurityRows::open(resolver, program)?;
    let stored_name = PolicyCatalog::stored_table_name(namespace.id, table_name);
    if enabled {
        rows.emit_insert(program, &stored_name);
    } else {
        rows.emit_delete(program, &stored_name);
    }
    emit_catalog_update(program, resolver, namespace.id, table_name, enabled);
    Ok(())
}

/// Moves the row-level security of a renamed table to its new name.
pub fn emit_rename_table(
    namespace: NamespaceRef,
    old_name: &str,
    new_name: &str,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    if !has_row_security(namespace, old_name, resolver) {
        return Ok(());
    }
    let rows = RowSecurityRows::open(resolver, program)?;
    rows.emit_delete(
        program,
        &PolicyCatalog::stored_table_name(namespace.id, old_name),
    );
    rows.emit_insert(
        program,
        &PolicyCatalog::stored_table_name(namespace.id, new_name),
    );
    emit_catalog_update(program, resolver, namespace.id, old_name, false);
    emit_catalog_update(program, resolver, namespace.id, new_name, true);
    Ok(())
}

/// Removes the row-level security of a dropped table, so that a new table
/// with the same name does not start with it.
pub fn emit_drop_table(
    namespace: NamespaceRef,
    table_name: &str,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    if !has_row_security(namespace, table_name, resolver) {
        return Ok(());
    }
    let rows = RowSecurityRows::open(resolver, program)?;
    rows.emit_delete(
        program,
        &PolicyCatalog::stored_table_name(namespace.id, table_name),
    );
    emit_catalog_update(program, resolver, namespace.id, table_name, false);
    Ok(())
}

fn has_row_security(namespace: NamespaceRef, table_name: &str, resolver: &Resolver) -> bool {
    namespace.database_id == MAIN_DB_ID
        && resolver
            .schema()
            .policies
            .is_enabled(namespace.id, table_name)
}

/// Bumps the schema cookie and then changes the in-memory catalog. The cookie
/// goes first because writing it is what fails when the transaction cannot
/// change the schema, and a failed statement must not leave the in-memory
/// catalog changed.
fn emit_catalog_update(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    namespace: NamespaceId,
    table_name: &str,
    enabled: bool,
) {
    emit_schema_cookie(resolver, program);
    program.emit_insn(Insn::SetRowSecurity {
        db: MAIN_DB_ID,
        namespace,
        table_name: table_name.to_string(),
        enabled,
    });
}

/// A write cursor on `__turso_internal_row_security`, which is created on
/// first use.
struct RowSecurityRows {
    cursor_id: usize,
}

impl RowSecurityRows {
    fn open(resolver: &Resolver, program: &mut ProgramBuilder) -> Result<Self> {
        let (table, root_page) = match resolver
            .schema()
            .get_btree_table(NamespaceId::ROOT, ROW_SECURITY_TABLE_NAME)
        {
            Some(table) => {
                let root_page = RegisterOrLiteral::Literal(table.root_page);
                (table, root_page)
            }
            None => {
                let created = emit_create_internal_table(
                    program,
                    resolver,
                    ROW_SECURITY_TABLE_NAME,
                    CREATE_ROW_SECURITY_TABLE_SQL,
                    &[],
                )?;
                (created.table, created.root_page)
            }
        };
        let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
        program.emit_insn(Insn::OpenWrite {
            cursor_id,
            root_page,
            db: MAIN_DB_ID,
        });
        Ok(Self { cursor_id })
    }

    fn emit_insert(&self, program: &mut ProgramBuilder, table_name: &str) {
        let rowid_reg = program.alloc_register();
        program.emit_insn(Insn::NewRowid {
            cursor: self.cursor_id,
            rowid_reg,
            prev_largest_reg: 0,
        });
        let table_name_reg = program.emit_string8_new_reg(table_name.to_string());
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(table_name_reg),
            count: to_u32(1),
            dest_reg: to_u32(record_reg),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Insert {
            cursor: self.cursor_id,
            key_reg: rowid_reg,
            record_reg,
            flag: InsertFlags::new().skip_last_rowid(),
            table_name: ROW_SECURITY_TABLE_NAME.to_string(),
        });
    }

    /// Deletes every row of the table stored as `table_name`.
    fn emit_delete(&self, program: &mut ProgramBuilder, table_name: &str) {
        let done = program.allocate_label();
        let loop_start = program.allocate_label();
        let next = program.allocate_label();
        let table_name_reg = program.emit_string8_new_reg(table_name.to_string());
        let column_reg = program.alloc_register();
        program.emit_insn(Insn::Rewind {
            cursor_id: self.cursor_id,
            pc_if_empty: done,
        });
        program.preassign_label_to_next_insn(loop_start);
        program.emit_column_or_rowid(self.cursor_id, 0, column_reg);
        program.emit_insn(Insn::Ne {
            lhs: column_reg,
            rhs: table_name_reg,
            target_pc: next,
            flags: CmpInsFlags::default(),
            collation: CollationSeq::for_internal_values(),
        });
        program.emit_insn(Insn::Delete {
            cursor_id: self.cursor_id,
            table_name: ROW_SECURITY_TABLE_NAME.to_string(),
            is_part_of_update: false,
        });
        program.preassign_label_to_next_insn(next);
        program.emit_insn(Insn::Next {
            cursor_id: self.cursor_id,
            pc_if_next: loop_start,
            fullscan: false,
            is_index: false,
        });
        program.preassign_label_to_next_insn(done);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use turso_parser::ast;

    use crate::dialect::EngineDialect;
    use crate::{Database, MemoryIO};

    #[test]
    fn enabling_row_security_fails_under_a_dialect_without_roles() {
        let db = Database::open_file(
            Arc::new(MemoryIO::new()),
            ":memory:",
            Arc::new(EngineDialect),
        )
        .unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();
        conn.execute("CREATE TABLE t (x)").unwrap();
        let enable = ast::Cmd::Stmt(ast::Stmt::AlterTable(ast::AlterTable {
            name: ast::QualifiedName::single(ast::Name::exact("t".to_string())),
            body: ast::AlterTableBody::EnableRowSecurity,
        }));

        let error = conn
            .prepare_translated_cmd(enable, "ALTER TABLE t ENABLE ROW LEVEL SECURITY")
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("row-level security is not supported"),
            "{error}"
        );
    }
}
