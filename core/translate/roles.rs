//! Bytecode for role statements, and the privilege checks that run when a
//! statement is prepared.
//!
//! Privileges are checked when a statement is prepared, against the role that
//! is current at that time. Changing the role makes prepared statements
//! prepare again, so a statement always runs with the privileges of the
//! current role.
//!
//! Ownership and `GRANT` do not exist yet, so a role that is not a superuser
//! may not write to any table, use a sequence or change the schema. It may
//! read tables, which row-level security limits, and switch roles.

use crate::namespace::NamespaceId;
use turso_parser::ast;

use crate::security::permission::{Action, Object, PermissionDenied};
use crate::security::roles::{
    CREATE_ROLES_NAME_INDEX_SQL, CREATE_ROLES_TABLE_SQL, ROLES_NAME_INDEX_NAME, ROLES_TABLE_NAME,
};
use crate::translate::emitter::Resolver;
use crate::translate::schema::{emit_create_internal_table, emit_schema_cookie};
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{to_u32, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral};
use crate::{bail_parse_error, LimboError, Result, MAIN_DB_ID};

const MAIN_SCHEMA_NAME: &str = "main";

/// Fails if a role that is not a superuser may not run `stmt` at all. Uses of
/// sequences and PRAGMAs inside the statement are checked when they are
/// compiled, by [check_sequence_change] and [check_pragma_function], so that
/// the views a query reads are checked too.
pub fn check_statement_privileges(stmt: &ast::Stmt) -> Result<()> {
    let denial = match stmt {
        ast::Stmt::Select(_) => return Ok(()),
        ast::Stmt::Insert { tbl_name, .. } | ast::Stmt::Delete { tbl_name, .. } => {
            write_denial(tbl_name)
        }
        ast::Stmt::Update(update) => write_denial(&update.tbl_name),
        ast::Stmt::Begin { .. }
        | ast::Stmt::Commit { .. }
        | ast::Stmt::Rollback { .. }
        | ast::Stmt::Savepoint { .. }
        | ast::Stmt::Release { .. } => return Ok(()),
        ast::Stmt::CreateTable {
            temporary: true, ..
        }
        | ast::Stmt::CreateView {
            temporary: true, ..
        }
        | ast::Stmt::CreateTrigger {
            temporary: true, ..
        } => PermissionDenied::privilege(
            Action::CreateTemporary,
            Object::Database(MAIN_SCHEMA_NAME.to_string()),
        ),
        ast::Stmt::CreateTable { tbl_name, .. } => create_in_schema_denial(tbl_name),
        ast::Stmt::CreateView { view_name, .. }
        | ast::Stmt::CreateMaterializedView { view_name, .. } => create_in_schema_denial(view_name),
        ast::Stmt::CreateVirtualTable(create) => create_in_schema_denial(&create.tbl_name),
        ast::Stmt::CreateSequence { seq_name, .. } => create_in_schema_denial(seq_name),
        ast::Stmt::CreateType { .. } | ast::Stmt::CreateDomain { .. } => {
            PermissionDenied::privilege(
                Action::Create,
                Object::Schema(MAIN_SCHEMA_NAME.to_string()),
            )
        }
        ast::Stmt::CreateIndex { tbl_name, .. } => {
            PermissionDenied::owner(Action::Alter, Object::Table(tbl_name.as_str().to_string()))
        }
        ast::Stmt::CreateTrigger { tbl_name, .. } => PermissionDenied::privilege(
            Action::CreateTrigger,
            Object::Table(tbl_name.name.as_str().to_string()),
        ),
        ast::Stmt::AlterTable(alter) => PermissionDenied::owner(
            Action::Alter,
            Object::Table(alter.name.name.as_str().to_string()),
        ),
        ast::Stmt::DropTable { tbl_name, .. } => PermissionDenied::owner(
            Action::Drop,
            Object::Table(tbl_name.name.as_str().to_string()),
        ),
        ast::Stmt::DropIndex { idx_name, .. } => PermissionDenied::owner(
            Action::Drop,
            Object::Index(idx_name.name.as_str().to_string()),
        ),
        ast::Stmt::DropView { view_name, .. } => PermissionDenied::owner(
            Action::Drop,
            Object::View(view_name.name.as_str().to_string()),
        ),
        ast::Stmt::DropTrigger { trigger_name, .. } => PermissionDenied::owner(
            Action::Drop,
            Object::Trigger(trigger_name.name.as_str().to_string()),
        ),
        ast::Stmt::DropType { type_name, .. } => {
            PermissionDenied::owner(Action::Drop, Object::Type(type_name.to_string()))
        }
        ast::Stmt::DropDomain { domain_name, .. } => {
            PermissionDenied::owner(Action::Drop, Object::Type(domain_name.to_string()))
        }
        ast::Stmt::DropSequence { seq_name, .. } => PermissionDenied::owner(
            Action::Drop,
            Object::Sequence(seq_name.name.as_str().to_string()),
        ),
        ast::Stmt::CreateSchema { .. } => database_denial(Action::Create),
        ast::Stmt::DropSchema { name } => {
            PermissionDenied::owner(Action::Drop, Object::Schema(name.as_str().to_string()))
        }
        ast::Stmt::CreateRole { role_name } => {
            PermissionDenied::privilege(Action::Create, Object::Role(role_name.clone()))
        }
        ast::Stmt::Pragma {
            name,
            body: Some(_),
        } => PermissionDenied::privilege(
            Action::Write,
            Object::Setting(name.name.as_str().to_string()),
        ),
        ast::Stmt::Pragma { name, body: None } => PermissionDenied::privilege(
            Action::Read,
            Object::Setting(name.name.as_str().to_string()),
        ),
        ast::Stmt::Analyze { .. } => database_denial(Action::Analyze),
        ast::Stmt::Vacuum { .. } => database_denial(Action::Vacuum),
        ast::Stmt::Reindex { .. } => database_denial(Action::Reindex),
        ast::Stmt::Optimize { .. } => database_denial(Action::Optimize),
        ast::Stmt::Attach { .. } => database_denial(Action::Attach),
        ast::Stmt::Detach { .. } => database_denial(Action::Detach),
    };
    Err(permission_denied(denial))
}

fn write_denial(tbl_name: &ast::QualifiedName) -> PermissionDenied {
    PermissionDenied::privilege(
        Action::Write,
        Object::Table(tbl_name.name.as_str().to_string()),
    )
}

/// Fails if the statement is compiled for a role that is not a superuser,
/// which may not change `sequence` with `nextval()` or `setval()`.
pub(crate) fn check_sequence_change(resolver: &Resolver, sequence: &str) -> Result<()> {
    if !resolver.role_is_not_superuser {
        return Ok(());
    }
    Err(permission_denied(PermissionDenied::privilege(
        Action::Write,
        Object::Sequence(sequence.to_string()),
    )))
}

/// Fails if the statement is compiled for a role that is not a superuser,
/// which may not read a table-valued PRAGMA function. The function runs
/// `pragma_name` as a statement of the engine's own, which is not checked, so
/// the PRAGMA is checked here as if it was run directly.
pub(crate) fn check_pragma_function(resolver: &Resolver, pragma_name: &str) -> Result<()> {
    if !resolver.role_is_not_superuser {
        return Ok(());
    }
    Err(permission_denied(PermissionDenied::privilege(
        Action::Read,
        Object::Setting(pragma_name.to_string()),
    )))
}

fn create_in_schema_denial(name: &ast::QualifiedName) -> PermissionDenied {
    let schema = name
        .namespace
        .as_ref()
        .map_or(MAIN_SCHEMA_NAME, |namespace| namespace.as_str());
    PermissionDenied::privilege(Action::Create, Object::Schema(schema.to_string()))
}

fn database_denial(action: Action) -> PermissionDenied {
    PermissionDenied::privilege(action, Object::Database(MAIN_SCHEMA_NAME.to_string()))
}

fn permission_denied(denial: PermissionDenied) -> LimboError {
    LimboError::PermissionDenied(Box::new(denial))
}

/// Creates a role that is not a superuser and cannot log in.
pub fn translate_create_role(
    role_name: &str,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    if !resolver.catalog.dialect.supports_roles() {
        bail_parse_error!("CREATE ROLE is not supported");
    }
    if resolver.schema().roles.get_by_name(role_name).is_some() {
        bail_parse_error!("role \"{role_name}\" already exists");
    }
    let superuser = false;

    let existing = resolver
        .schema()
        .get_btree_table(NamespaceId::ROOT, ROLES_TABLE_NAME);
    let (roles_table, roles_root_page, (name_index, name_index_root_page)) = match existing {
        Some(table) => {
            let name_index = resolver
                .schema()
                .get_indices(NamespaceId::ROOT, ROLES_TABLE_NAME)
                .find(|index| index.name == ROLES_NAME_INDEX_NAME)
                .cloned()
                .ok_or_else(|| {
                    LimboError::Corrupt(format!("{ROLES_TABLE_NAME} has no name index"))
                })?;
            emit_schema_cookie(resolver, program);
            let root_page = RegisterOrLiteral::Literal(table.root_page);
            let name_index_root_page = RegisterOrLiteral::Literal(name_index.root_page);
            (table, root_page, (name_index, name_index_root_page))
        }
        // Creating the table bumps the schema cookie too.
        None => {
            let mut created = emit_create_internal_table(
                program,
                resolver,
                ROLES_TABLE_NAME,
                CREATE_ROLES_TABLE_SQL,
                &[CREATE_ROLES_NAME_INDEX_SQL],
            )?;
            let name_index = created.indexes.pop().expect("one index was created");
            (created.table, created.root_page, name_index)
        }
    };
    let roles_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(roles_table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: roles_cursor_id,
        root_page: roles_root_page,
        db: MAIN_DB_ID,
    });
    let name_index_cursor_id = program.alloc_cursor_index(None, &name_index)?;
    program.emit_insn(Insn::OpenWrite {
        cursor_id: name_index_cursor_id,
        root_page: name_index_root_page,
        db: MAIN_DB_ID,
    });

    let id_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: roles_cursor_id,
        rowid_reg: id_reg,
        prev_largest_reg: 0,
    });
    let first_column_reg = program.alloc_registers(3);
    program.emit_insn(Insn::Null {
        dest: first_column_reg,
        dest_end: None,
    });
    program.emit_insn(Insn::String8 {
        value: role_name.to_string(),
        dest: first_column_reg + 1,
    });
    program.emit_insn(Insn::Integer {
        value: superuser as i64,
        dest: first_column_reg + 2,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(first_column_reg),
        count: to_u32(3),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: roles_cursor_id,
        key_reg: id_reg,
        record_reg,
        flag: InsertFlags::new().skip_last_rowid(),
        table_name: ROLES_TABLE_NAME.to_string(),
    });
    emit_role_name_index_entry(
        program,
        name_index_cursor_id,
        &name_index.name,
        role_name,
        id_reg,
    );

    program.emit_insn(Insn::AddRole {
        db: MAIN_DB_ID,
        id_reg,
        name: role_name.to_string(),
        superuser,
    });
    Ok(())
}

/// Adds the role's name to the name index, failing if another role has the
/// name. The in-memory catalog has already rejected a name it knows, so this
/// only fails if the stored table has a role the catalog does not.
fn emit_role_name_index_entry(
    program: &mut ProgramBuilder,
    cursor_id: usize,
    index_name: &str,
    role_name: &str,
    id_reg: usize,
) {
    let key_reg = program.alloc_registers(2);
    program.emit_insn(Insn::String8 {
        value: role_name.to_string(),
        dest: key_reg,
    });
    program.emit_insn(Insn::Copy {
        src_reg: id_reg,
        dst_reg: key_reg + 1,
        extra_amount: 0,
    });
    let no_conflict = program.allocate_label();
    program.emit_insn(Insn::NoConflict {
        cursor_id,
        target_pc: no_conflict,
        record_reg: key_reg,
        num_regs: 1,
    });
    program.emit_insn(Insn::Halt {
        constraint_info: None,
        err_code: crate::error::SQLITE_CONSTRAINT_UNIQUE,
        description: format!("role \"{role_name}\" already exists"),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(no_conflict);
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(key_reg),
        count: to_u32(2),
        dest_reg: to_u32(record_reg),
        index_name: Some(index_name.to_string()),
        affinity_str: None,
    });
    program.emit_insn(Insn::IdxInsert {
        cursor_id,
        record_reg,
        unpacked_start: Some(key_reg),
        unpacked_count: Some(2),
        flags: IdxInsertFlags::new(),
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use turso_parser::ast;

    use crate::{Connection, Database, MemoryIO, SqliteDialect};

    #[test]
    fn create_role_fails_under_a_dialect_without_roles() {
        let db = Database::open_file(
            Arc::new(MemoryIO::new()),
            ":memory:",
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();

        let error = create_role(&conn, "alice").unwrap_err();

        assert!(
            error.to_string().contains("CREATE ROLE is not supported"),
            "{error}"
        );
    }

    fn create_role(conn: &Arc<Connection>, name: &str) -> crate::Result<()> {
        let create_role = ast::Cmd::Stmt(ast::Stmt::CreateRole {
            role_name: name.to_string(),
        });
        conn.prepare_translated_cmd(create_role, &format!("CREATE ROLE {name}"))?
            .run_ignore_rows()
    }
}
