//! Bytecode for role statements, and the privilege checks that run when a
//! statement is prepared.
//!
//! Privileges are checked when a statement is prepared, against the role that
//! is current at that time. Changing the role makes prepared statements
//! prepare again, so a statement always runs with the privileges of the
//! current role.
//!
//! Ownership and `GRANT` do not exist yet, so a role that is not a superuser
//! has no privileges on any database object. It may still run statements that
//! touch no database object, such as `SELECT 1`, switch roles, and read the
//! catalog: the schema table, the types table and the role table.
//!
//! Only statements from the user are checked. Statements that the engine
//! prepares for itself run SQL that the engine wrote, such as reads of
//! sequence state or table statistics to load the schema. The one such
//! statement whose rows reach the user, the PRAGMA that a PRAGMA table
//! function runs, is checked when the function is compiled.

use crate::namespace::NamespaceId;
use turso_ext::VTabKind;
use turso_parser::ast;

use crate::schema::{Schema, SEQ_BACKING_TABLE_PREFIX};
use crate::security::permission::{Action, Object, PermissionDenied};
use crate::security::roles::{
    CREATE_ROLES_NAME_INDEX_SQL, CREATE_ROLES_TABLE_SQL, ROLES_NAME_INDEX_NAME, ROLES_TABLE_NAME,
};
use crate::translate::emitter::Resolver;
use crate::translate::schema::{emit_create_internal_table, emit_schema_cookie};
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{to_u32, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral};
use crate::vtab::VirtualTableType;
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

/// Fails if the compiled program reads or writes a database object that the
/// current role has no privileges on.
pub fn check_storage_access(program: &ProgramBuilder, resolver: &Resolver) -> Result<()> {
    match storage_denial(program, resolver) {
        None => Ok(()),
        Some(denial) => Err(permission_denied(denial)),
    }
}

/// Returns what the compiled program reads or writes that the current role
/// has no privileges on. Every access to stored data goes through one of the
/// instructions checked here, so a statement cannot reach an object without
/// being checked. A trigger or foreign-key action is compiled into a
/// subprogram, whose denial the subprogram's compiler records in `program`
/// and which is returned after the program's own accesses pass.
pub fn storage_denial(program: &ProgramBuilder, resolver: &Resolver) -> Option<PermissionDenied> {
    if !resolver.role_is_not_superuser {
        return None;
    }
    if let Some(view_name) = program.referenced_views.first() {
        return Some(PermissionDenied::privilege(
            Action::Read,
            Object::View(view_name.clone()),
        ));
    }
    if let Some(virtual_table) = program.written_virtual_tables.first() {
        return Some(PermissionDenied::privilege(
            Action::Write,
            Object::Table(virtual_table.name.clone()),
        ));
    }
    for (insn, _) in &program.insns {
        let (action, db, root_page) = match insn {
            Insn::OpenRead { db, root_page, .. } => (Action::Read, *db, Some(*root_page)),
            Insn::OpenWrite {
                db,
                root_page: RegisterOrLiteral::Literal(root_page),
                ..
            } => (Action::Write, *db, Some(*root_page)),
            Insn::OpenWrite { db, .. } => (Action::Write, *db, None),
            Insn::ClearBtree { db, root, .. } | Insn::Destroy { db, root, .. } => {
                (Action::Write, *db, Some(*root))
            }
            Insn::CreateBtree { db, .. } => (Action::Write, *db, None),
            _ => continue,
        };
        let object = root_page
            .and_then(|root_page| {
                resolver.with_schema(db, |schema| object_with_root_page(schema, root_page))
            })
            .unwrap_or(Object::Storage);
        if action == Action::Read && is_catalog_table(&object) {
            continue;
        }
        return Some(PermissionDenied::privilege(action, object));
    }
    for virtual_table in &program.opened_virtual_tables {
        let readable = match &virtual_table.vtab_type {
            VirtualTableType::Internal(table) => table.read().readable_without_privileges(),
            VirtualTableType::External(table) => table.readable_without_privileges(),
            VirtualTableType::Pragma(_) => false,
        };
        if !readable {
            return Some(match virtual_table.kind {
                VTabKind::TableValuedFunction => PermissionDenied::privilege(
                    Action::Execute,
                    Object::Function(virtual_table.name.clone()),
                ),
                VTabKind::VirtualTable => PermissionDenied::privilege(
                    Action::Read,
                    Object::Table(virtual_table.name.clone()),
                ),
            });
        }
    }
    program.subprogram_denial.clone()
}

fn permission_denied(denial: PermissionDenied) -> LimboError {
    LimboError::PermissionDenied(Box::new(denial))
}

/// Returns the object stored at `root_page`. An index is reported as the
/// table it belongs to.
fn object_with_root_page(schema: &Schema, root_page: i64) -> Option<Object> {
    let (namespace, table_name) = schema.namespace_ids().find_map(|namespace| {
        schema.tables(namespace).find_map(|table| {
            let btree = table.btree()?;
            let has_root_page = btree.root_page == root_page
                || schema
                    .get_indices(namespace, &btree.name)
                    .any(|index| index.root_page == root_page);
            has_root_page.then(|| (namespace, btree.name.clone()))
        })
    })?;
    if let Some(sequence_name) = table_name.strip_prefix(SEQ_BACKING_TABLE_PREFIX) {
        let (_, sequence_name) = schema.demangle_name(sequence_name);
        return Some(Object::Sequence(sequence_name.to_string()));
    }
    if schema.is_materialized_view(namespace, &table_name) {
        return Some(Object::MaterializedView(table_name));
    }
    Some(Object::Table(table_name))
}

fn is_catalog_table(object: &Object) -> bool {
    matches!(
        object,
        Object::Table(name)
            if [
                crate::schema::SCHEMA_TABLE_NAME,
                crate::schema::TURSO_TYPES_TABLE_NAME,
                ROLES_TABLE_NAME,
            ]
            .contains(&name.as_str())
    )
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
    let first_column_reg = program.alloc_registers(4);
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
    let dropped = false;
    program.emit_insn(Insn::Integer {
        value: dropped as i64,
        dest: first_column_reg + 3,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(first_column_reg),
        count: to_u32(4),
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

    use turso_ext::{
        ConstraintInfo, IndexInfo, OrderByInfo, ResultCode, VTabCursor, VTabKind, VTabModule,
        VTabModuleDerive, VTable,
    };

    use crate::native_ext::{TableUpdate, VirtualTable, VirtualTableCursor, VirtualTableModule};
    use crate::security::permission::{Action, Object, PermissionDenied};
    use crate::security::test_support::{
        become_new_role, connect_with_roles, connect_with_roles_and, SqliteWithRoles,
    };
    use crate::types::IOResultOr;
    use crate::{
        Connection, Database, IOResult, LimboError, MemoryIO, OpenOptions, SqliteDialect, Value,
    };

    #[test]
    fn create_role_stores_a_role_that_is_not_dropped() {
        let conn = connect_with_roles();

        become_new_role(&conn, "alice");

        conn.set_current_role(crate::security::roles::RoleId::SUPERUSER);
        let mut stmt = conn
            .prepare("SELECT id, name, superuser, dropped FROM __turso_internal_roles")
            .unwrap();
        assert_eq!(
            stmt.run_collect_rows().unwrap(),
            vec![vec![
                Value::from_i64(1),
                Value::build_text("alice"),
                Value::from_i64(0),
                Value::from_i64(0),
            ]]
        );
    }

    #[test]
    fn create_role_fails_under_a_dialect_without_roles() {
        let db = Database::open_file(
            Arc::new(MemoryIO::new()),
            ":memory:",
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db
            .connect(crate::ConnectOpts::new(crate::SessionRole::Superuser))
            .unwrap();

        let error = create_role(&conn, "alice").unwrap_err();

        assert!(
            error.to_string().contains("CREATE ROLE is not supported"),
            "{error}"
        );
    }

    #[test]
    fn role_without_privileges_may_use_only_virtual_tables_that_allow_it() {
        let conn = connect_with_roles();
        become_new_role(&conn, "alice");

        #[cfg(feature = "cli_only")]
        {
            let dbpage = conn.execute("SELECT * FROM sqlite_dbpage").unwrap_err();
            assert_eq!(
                denial(dbpage),
                PermissionDenied::privilege(
                    Action::Execute,
                    Object::Function("sqlite_dbpage".to_string())
                )
            );
        }
        #[cfg(feature = "json")]
        conn.execute("SELECT * FROM json_each('[1, 2]')").unwrap();
    }

    #[test]
    fn role_without_privileges_may_read_native_virtual_tables_that_allow_it() {
        let options = OpenOptions::new(Arc::new(SqliteWithRoles))
            .native_module(
                "open_rows",
                VTabKind::VirtualTable,
                EmptyModule { readable: true },
            )
            .native_module(
                "closed_rows",
                VTabKind::VirtualTable,
                EmptyModule { readable: false },
            );
        let conn = connect_with_roles_and(options);
        conn.execute("CREATE VIRTUAL TABLE open_table USING open_rows")
            .unwrap();
        conn.execute("CREATE VIRTUAL TABLE closed_table USING closed_rows")
            .unwrap();
        become_new_role(&conn, "alice");

        conn.execute("SELECT * FROM open_table").unwrap();
        let closed = conn.execute("SELECT * FROM closed_table").unwrap_err();
        assert_eq!(
            denial(closed),
            PermissionDenied::privilege(Action::Read, Object::Table("closed_table".to_string()))
        );
    }

    #[test]
    fn role_without_privileges_may_use_only_native_table_valued_functions_that_allow_it() {
        let options = OpenOptions::new(Arc::new(SqliteWithRoles))
            .native_module(
                "open_rows",
                VTabKind::TableValuedFunction,
                EmptyModule { readable: true },
            )
            .native_module(
                "closed_rows",
                VTabKind::TableValuedFunction,
                EmptyModule { readable: false },
            );
        let conn = connect_with_roles_and(options);
        become_new_role(&conn, "alice");

        conn.execute("SELECT * FROM open_rows()").unwrap();
        let closed = conn.execute("SELECT * FROM closed_rows()").unwrap_err();
        assert_eq!(
            denial(closed),
            PermissionDenied::privilege(
                Action::Execute,
                Object::Function("closed_rows".to_string())
            )
        );
    }

    #[test]
    fn role_without_privileges_may_use_only_extension_table_valued_functions_that_allow_it() {
        // A table-valued function registered through a connection is added
        // to the database's schema, which the next connection starts from.
        let registering_conn = connect_with_roles();
        registering_conn.register_static_extension(|api| unsafe {
            OpenExtensionRows::register_OpenExtensionRows(api);
            ClosedExtensionRows::register_ClosedExtensionRows(api);
        });
        let conn = registering_conn
            .db
            .connect(crate::ConnectOpts::new(crate::SessionRole::Superuser))
            .unwrap();
        become_new_role(&conn, "alice");

        conn.execute("SELECT * FROM open_extension_rows()").unwrap();
        #[cfg(feature = "series")]
        conn.execute("SELECT * FROM generate_series(1, 3)").unwrap();
        let closed = conn
            .execute("SELECT * FROM closed_extension_rows()")
            .unwrap_err();
        assert_eq!(
            denial(closed),
            PermissionDenied::privilege(
                Action::Execute,
                Object::Function("closed_extension_rows".to_string())
            )
        );
    }

    #[test]
    fn storage_access_in_trigger_and_foreign_key_subprograms_is_denied() {
        let conn = connect_with_roles();
        conn.execute("PRAGMA foreign_keys = ON").unwrap();
        conn.execute("CREATE TABLE secret (x)").unwrap();
        conn.execute("CREATE TABLE log (x)").unwrap();
        conn.execute(
            "CREATE TRIGGER read_secret AFTER INSERT ON log BEGIN SELECT x FROM secret; END",
        )
        .unwrap();
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("CREATE TABLE child (pid REFERENCES parent(id) ON DELETE CASCADE)")
            .unwrap();
        become_new_role(&conn, "alice");

        assert_eq!(
            subprogram_denial(&conn, "INSERT INTO log VALUES (1)"),
            Some(PermissionDenied::privilege(
                Action::Read,
                Object::Table("secret".to_string())
            ))
        );
        assert_eq!(
            subprogram_denial(&conn, "DELETE FROM parent"),
            Some(PermissionDenied::privilege(
                Action::Write,
                Object::Table("child".to_string())
            ))
        );
        assert_eq!(
            subprogram_denial(&conn, "INSERT INTO secret VALUES (1)"),
            None
        );
    }

    /// Compiles `sql` as `translate()` does, up to the privilege checks, and
    /// returns the denial recorded from the statement's subprograms. The
    /// statement's own accesses are denied first, so the subprograms can only
    /// be checked this way until privileges can be granted.
    fn subprogram_denial(conn: &Arc<Connection>, sql: &str) -> Option<PermissionDenied> {
        use crate::translate::emitter::Resolver;
        use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode};

        let (Some(ast::Cmd::Stmt(stmt)), _) = conn.dialect().parse(sql).unwrap() else {
            panic!("{sql} is not a statement");
        };
        let schema = conn.schema.read().clone();
        let syms = conn.syms.read();
        let qualifiers = conn.qualifiers(&schema);
        let mut resolver = Resolver::new(
            &schema,
            conn.database_schemas(),
            &conn.temp.database,
            conn.attached_databases(),
            &qualifiers,
            &syms,
            conn.experimental_custom_types_enabled(),
            conn.get_dqs_dml().into(),
            conn.dialect(),
            None,
        );
        resolver.role_is_not_superuser = true;
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(1, 32, 2));
        program.set_mvcc_enabled(conn.mvcc_enabled());
        program.prologue();
        crate::translate::translate_inner(stmt, &mut resolver, &mut program, conn, sql).unwrap();
        program.subprogram_denial
    }

    #[test]
    fn role_without_privileges_cannot_change_virtual_tables_it_may_read() {
        let options = OpenOptions::new(Arc::new(SqliteWithRoles)).native_module(
            "open_rows",
            VTabKind::VirtualTable,
            EmptyModule { readable: true },
        );
        let conn = connect_with_roles_and(options);
        conn.execute("CREATE VIRTUAL TABLE open_table USING open_rows")
            .unwrap();
        become_new_role(&conn, "alice");

        for sql in [
            "INSERT INTO open_table VALUES (1)",
            "UPDATE open_table SET value = 2",
            "DELETE FROM open_table",
        ] {
            let error = conn.execute(sql).unwrap_err();
            assert_eq!(
                denial(error),
                PermissionDenied::privilege(Action::Write, Object::Table("open_table".to_string())),
                "{sql}"
            );
        }
    }

    #[test]
    fn role_without_privileges_cannot_read_currval() {
        let conn = connect_with_roles();
        conn.execute("CREATE SEQUENCE s").unwrap();
        conn.execute("SELECT nextval('s')").unwrap();
        become_new_role(&conn, "alice");

        let error = conn.execute("SELECT currval('s')").unwrap_err();

        assert_eq!(
            denial(error),
            PermissionDenied::privilege(Action::Use, Object::Sequence("s".to_string()))
        );
    }

    #[test]
    fn role_without_privileges_cannot_read_the_sequence_watermark() {
        let conn = connect_with_roles();
        conn.execute("CREATE SEQUENCE s").unwrap();
        become_new_role(&conn, "alice");

        let error = conn
            .execute("SELECT sequence_watermark_experimental('s')")
            .unwrap_err();

        assert_eq!(
            denial(error),
            PermissionDenied::privilege(Action::Use, Object::Sequence("s".to_string()))
        );
    }

    fn denial(error: LimboError) -> PermissionDenied {
        match error {
            LimboError::PermissionDenied(denial) => *denial,
            other => panic!("expected a permission error, got: {other}"),
        }
    }

    fn create_role(conn: &Arc<Connection>, name: &str) -> crate::Result<()> {
        let create_role = ast::Cmd::Stmt(ast::Stmt::CreateRole {
            role_name: name.to_string(),
        });
        conn.prepare_translated_cmd(create_role, &format!("CREATE ROLE {name}"))?
            .run_ignore_rows()
    }

    #[derive(Debug)]
    struct EmptyModule {
        readable: bool,
    }

    impl VirtualTableModule for EmptyModule {
        type Table = EmptyTable;

        fn schema(&self, _args: &[Value]) -> crate::Result<String> {
            Ok("CREATE TABLE x(value INTEGER)".to_string())
        }

        fn create(&self, _args: &[Value]) -> crate::Result<Self::Table> {
            Ok(EmptyTable {
                readable: self.readable,
            })
        }
    }

    #[derive(Debug)]
    struct EmptyTable {
        readable: bool,
    }

    impl VirtualTable for EmptyTable {
        type Cursor = EmptyCursor;

        fn open(&self, _connection: Arc<Connection>) -> crate::Result<Self::Cursor> {
            Ok(EmptyCursor)
        }

        fn best_index(
            &self,
            _constraints: &[ConstraintInfo],
            _order_by: &[OrderByInfo],
        ) -> Result<IndexInfo, ResultCode> {
            Ok(IndexInfo::default())
        }

        fn readonly(&self) -> bool {
            false
        }

        fn readable_without_privileges(&self) -> bool {
            self.readable
        }

        fn create_update(&mut self, _args: Vec<Value>) -> crate::Result<Box<dyn TableUpdate>> {
            Ok(Box::new(IgnoredUpdate))
        }
    }

    struct IgnoredUpdate;

    impl TableUpdate for IgnoredUpdate {
        fn step(&mut self) -> IOResultOr<Option<i64>> {
            Ok(IOResult::Done(None))
        }
    }

    struct EmptyCursor;

    impl VirtualTableCursor for EmptyCursor {
        fn filter(
            &mut self,
            _args: &[Value],
            _idx_str: Option<&str>,
            _idx_num: i32,
        ) -> IOResultOr<bool> {
            Ok(IOResult::Done(false))
        }

        fn next(&mut self) -> IOResultOr<bool> {
            Ok(IOResult::Done(false))
        }

        fn column(&mut self, _column: usize) -> IOResultOr<Value> {
            unreachable!("an empty table has no rows")
        }

        fn rowid(&self) -> i64 {
            unreachable!("an empty table has no rows")
        }
    }

    /// A table-valued function of a C extension that says a role without
    /// privileges may read it.
    #[derive(Debug, Default, VTabModuleDerive)]
    struct OpenExtensionRows;

    impl VTabModule for OpenExtensionRows {
        type Table = ExtensionTable;
        const NAME: &'static str = "open_extension_rows";
        const VTAB_KIND: VTabKind = VTabKind::TableValuedFunction;
        const READABLE_WITHOUT_PRIVILEGES: bool = true;

        fn create(_args: &[turso_ext::Value]) -> Result<(String, Self::Table), ResultCode> {
            Ok(("CREATE TABLE x(value INTEGER)".to_string(), ExtensionTable))
        }
    }

    /// A table-valued function of a C extension that keeps the default, like
    /// one written before roles existed.
    #[derive(Debug, Default, VTabModuleDerive)]
    struct ClosedExtensionRows;

    impl VTabModule for ClosedExtensionRows {
        type Table = ExtensionTable;
        const NAME: &'static str = "closed_extension_rows";
        const VTAB_KIND: VTabKind = VTabKind::TableValuedFunction;

        fn create(_args: &[turso_ext::Value]) -> Result<(String, Self::Table), ResultCode> {
            Ok(("CREATE TABLE x(value INTEGER)".to_string(), ExtensionTable))
        }
    }

    struct ExtensionTable;

    impl VTable for ExtensionTable {
        type Cursor = ExtensionCursor;
        type Error = ResultCode;

        fn open(
            &self,
            _conn: Option<Arc<turso_ext::Connection>>,
        ) -> Result<Self::Cursor, Self::Error> {
            Ok(ExtensionCursor)
        }
    }

    struct ExtensionCursor;

    impl VTabCursor for ExtensionCursor {
        type Error = ResultCode;

        fn filter(
            &mut self,
            _args: &[turso_ext::Value],
            _idx_info: Option<(&str, i32)>,
        ) -> ResultCode {
            ResultCode::OK
        }

        fn rowid(&self) -> i64 {
            0
        }

        fn column(&self, _idx: u32) -> Result<turso_ext::Value, Self::Error> {
            Ok(turso_ext::Value::null())
        }

        fn eof(&self) -> bool {
            true
        }

        fn next(&mut self) -> ResultCode {
            ResultCode::EOF
        }
    }
}
