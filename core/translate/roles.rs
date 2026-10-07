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
//! touch no database object, such as `SELECT 1`, and switch roles.

use std::sync::Arc;

use turso_ext::VTabKind;
use turso_parser::ast;

use crate::schema::{BTreeTable, Schema, SEQ_BACKING_TABLE_PREFIX};
use crate::security::roles::{CREATE_ROLES_TABLE_SQL, ROLES_TABLE_NAME};
use crate::storage::pager::CreateBTreeFlags;
use crate::translate::emitter::Resolver;
use crate::translate::schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID};
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{to_u32, Cookie, InsertFlags, Insn, RegisterOrLiteral};
use crate::vtab::VirtualTableType;
use crate::{bail_parse_error, Connection, LimboError, Result, MAIN_DB_ID};

/// Fails if the current role may not run `stmt` at all. Statements that pass
/// are checked again by [`check_storage_access`] once they are compiled.
pub fn check_statement_privileges(
    stmt: &ast::Stmt,
    resolver: &Resolver,
    connection: &Connection,
) -> Result<()> {
    if resolver
        .schema()
        .roles
        .is_superuser(connection.current_role())
    {
        return Ok(());
    }
    let denial = match stmt {
        ast::Stmt::Select(_)
        | ast::Stmt::Insert { .. }
        | ast::Stmt::Update(_)
        | ast::Stmt::Delete { .. }
        | ast::Stmt::Begin { .. }
        | ast::Stmt::Commit { .. }
        | ast::Stmt::Rollback { .. }
        | ast::Stmt::Savepoint { .. }
        | ast::Stmt::Release { .. }
        | ast::Stmt::SetRole { .. } => return Ok(()),
        ast::Stmt::CreateTable {
            temporary: true, ..
        }
        | ast::Stmt::CreateView {
            temporary: true, ..
        }
        | ast::Stmt::CreateTrigger {
            temporary: true, ..
        } => "permission denied to create temporary objects".to_string(),
        ast::Stmt::CreateTable { tbl_name, .. } => schema_denial(tbl_name),
        ast::Stmt::CreateView { view_name, .. }
        | ast::Stmt::CreateMaterializedView { view_name, .. } => schema_denial(view_name),
        ast::Stmt::CreateVirtualTable(create) => schema_denial(&create.tbl_name),
        ast::Stmt::CreateSequence { seq_name, .. } => schema_denial(seq_name),
        ast::Stmt::CreateType { .. } | ast::Stmt::CreateDomain { .. } => {
            "permission denied for schema public".to_string()
        }
        ast::Stmt::CreateIndex { tbl_name, .. } => {
            format!("must be owner of table {}", tbl_name.as_str())
        }
        ast::Stmt::CreateTrigger { tbl_name, .. } => {
            format!("permission denied for table {}", tbl_name.name.as_str())
        }
        ast::Stmt::AlterTable(alter) => {
            format!("must be owner of table {}", alter.name.name.as_str())
        }
        ast::Stmt::DropTable { tbl_name, .. } => {
            format!("must be owner of table {}", tbl_name.name.as_str())
        }
        ast::Stmt::DropIndex { idx_name, .. } => {
            format!("must be owner of index {}", idx_name.name.as_str())
        }
        ast::Stmt::DropView { view_name, .. } => {
            format!("must be owner of view {}", view_name.name.as_str())
        }
        ast::Stmt::DropTrigger { trigger_name, .. } => {
            format!("must be owner of trigger {}", trigger_name.name.as_str())
        }
        ast::Stmt::DropType { type_name, .. } => format!("must be owner of type {type_name}"),
        ast::Stmt::DropDomain { domain_name, .. } => {
            format!("must be owner of type {domain_name}")
        }
        ast::Stmt::DropSequence { seq_name, .. } => {
            format!("must be owner of sequence {}", seq_name.name.as_str())
        }
        ast::Stmt::CreateRole { .. } => "permission denied to create role".to_string(),
        ast::Stmt::Pragma {
            name,
            body: Some(_),
        } => format!(
            "permission denied to set parameter \"{}\"",
            name.name.as_str()
        ),
        ast::Stmt::Pragma { name, body: None } => {
            format!("permission denied to examine \"{}\"", name.name.as_str())
        }
        ast::Stmt::Analyze { .. } => "permission denied to run ANALYZE".to_string(),
        ast::Stmt::Vacuum { .. } => "permission denied to run VACUUM".to_string(),
        ast::Stmt::Reindex { .. } => "permission denied to run REINDEX".to_string(),
        ast::Stmt::Optimize { .. } => "permission denied to run OPTIMIZE".to_string(),
        ast::Stmt::Attach { .. } => "permission denied to attach a database".to_string(),
        ast::Stmt::Detach { .. } => "permission denied to detach a database".to_string(),
    };
    Err(LimboError::PermissionDenied(denial))
}

fn schema_denial(name: &ast::QualifiedName) -> String {
    let schema = match &name.db_name {
        Some(db_name) if db_name.as_str() != "main" => db_name.as_str(),
        _ => "public",
    };
    format!("permission denied for schema {schema}")
}

/// Fails if the compiled program reads or writes a database object that the
/// current role has no privileges on. Every access to stored data goes
/// through one of the instructions checked here, so a statement cannot reach
/// an object without being checked.
pub fn check_storage_access(
    program: &ProgramBuilder,
    resolver: &Resolver,
    connection: &Connection,
) -> Result<()> {
    if resolver
        .schema()
        .roles
        .is_superuser(connection.current_role())
    {
        return Ok(());
    }
    if let Some(view_name) = program.referenced_views.first() {
        return Err(LimboError::PermissionDenied(format!(
            "permission denied for view {view_name}"
        )));
    }
    for (insn, _) in &program.insns {
        let (db, root_page) = match insn {
            Insn::OpenRead { db, root_page, .. } => (*db, Some(*root_page)),
            Insn::OpenWrite {
                db,
                root_page: RegisterOrLiteral::Literal(root_page),
                ..
            } => (*db, Some(*root_page)),
            Insn::OpenWrite { db, .. } => (*db, None),
            Insn::ClearBtree { db, root, .. } | Insn::Destroy { db, root, .. } => {
                (*db, Some(*root))
            }
            Insn::CreateBtree { db, .. } => (*db, None),
            _ => continue,
        };
        let object = root_page.and_then(|root_page| {
            resolver.with_schema(db, |schema| object_with_root_page(schema, root_page))
        });
        let denial = match object {
            Some(object) => format!("permission denied for {object}"),
            None => "permission denied to access database storage".to_string(),
        };
        return Err(LimboError::PermissionDenied(denial));
    }
    for virtual_table in &program.opened_virtual_tables {
        let readable = match &virtual_table.vtab_type {
            VirtualTableType::Internal(table) => table.read().readable_without_privileges(),
            VirtualTableType::External(_) => virtual_table.kind == VTabKind::TableValuedFunction,
            VirtualTableType::Pragma(_) => false,
        };
        if !readable {
            let object = match virtual_table.kind {
                VTabKind::TableValuedFunction => "function",
                VTabKind::VirtualTable => "table",
            };
            return Err(LimboError::PermissionDenied(format!(
                "permission denied for {object} {}",
                virtual_table.name
            )));
        }
    }
    Ok(())
}

/// Describes the object stored at `root_page` the way PostgreSQL names it in
/// a permission error, for example `table t` or `sequence s`. An index is
/// described by the table it belongs to.
fn object_with_root_page(schema: &Schema, root_page: i64) -> Option<String> {
    let table_name = schema
        .tables
        .values()
        .filter_map(|table| table.btree())
        .find(|table| table.root_page == root_page)
        .map(|table| table.name.clone())
        .or_else(|| {
            schema
                .indexes
                .values()
                .flatten()
                .find(|index| index.root_page == root_page)
                .map(|index| index.table_name.clone())
        })?;
    if let Some(sequence_name) = table_name.strip_prefix(SEQ_BACKING_TABLE_PREFIX) {
        return Some(format!("sequence {sequence_name}"));
    }
    if schema.materialized_view_names.contains(&table_name) {
        return Some(format!("materialized view {table_name}"));
    }
    Some(format!("table {table_name}"))
}

/// Switches the connection to `role_name`, or back to the session role when
/// `role_name` is `None`. The role is looked up when the statement runs. The
/// read transaction makes the statement prepare again if another connection
/// changed the roles since it was prepared.
pub fn translate_set_role(role_name: Option<String>, program: &mut ProgramBuilder) -> Result<()> {
    program.begin_read_operation()?;
    program.emit_insn(Insn::SetRole { role_name });
    Ok(())
}

/// Creates a role that is not a superuser and cannot log in.
pub fn translate_create_role(
    role_name: &str,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    if resolver.schema().roles.get_by_name(role_name).is_some() {
        bail_parse_error!("role \"{role_name}\" already exists");
    }
    let superuser = false;
    let can_login = false;

    program.emit_insn(Insn::SetCookie {
        db: MAIN_DB_ID,
        cookie: Cookie::SchemaVersion,
        value: (resolver.schema().schema_version + 1) as i32,
        p5: 0,
    });

    let (roles_table, roles_root_page) = emit_roles_table_if_missing(resolver, program)?;
    let roles_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(roles_table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: roles_cursor_id,
        root_page: roles_root_page,
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
    program.emit_insn(Insn::Integer {
        value: can_login as i64,
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
        flag: InsertFlags::new(),
        table_name: ROLES_TABLE_NAME.to_string(),
    });

    program.emit_insn(Insn::AddRole {
        db: MAIN_DB_ID,
        id_reg,
        name: role_name.to_string(),
        superuser,
        can_login,
    });
    Ok(())
}

/// Returns the roles table and its root page. If the table does not exist
/// yet, emits bytecode that creates it and returns the register that will
/// hold its root page.
fn emit_roles_table_if_missing(
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<(Arc<BTreeTable>, RegisterOrLiteral<i64>)> {
    if let Some(table) = resolver.schema().get_btree_table(ROLES_TABLE_NAME) {
        let root_page = RegisterOrLiteral::Literal(table.root_page);
        return Ok((table, root_page));
    }

    let root_page_reg = program.alloc_register();
    program.emit_insn(Insn::CreateBtree {
        db: MAIN_DB_ID,
        root: root_page_reg,
        flags: CreateBTreeFlags::new_table(),
    });

    let schema_table = resolver
        .schema()
        .get_btree_table(SQLITE_TABLEID)
        .expect("sqlite_schema always exists");
    let schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(schema_table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: schema_cursor_id,
        root_page: 1i64.into(),
        db: MAIN_DB_ID,
    });
    emit_schema_entry(
        program,
        resolver,
        schema_cursor_id,
        None,
        SchemaEntryType::Table,
        ROLES_TABLE_NAME,
        ROLES_TABLE_NAME,
        root_page_reg,
        Some(CREATE_ROLES_TABLE_SQL.to_string()),
    )?;
    program.emit_insn(Insn::ParseSchema {
        db: schema_cursor_id,
        where_clause: Some(format!(
            "tbl_name = '{ROLES_TABLE_NAME}' AND type != 'trigger'"
        )),
        trigger_target_database_id: None,
    });

    let table = Arc::new(BTreeTable::from_sql(CREATE_ROLES_TABLE_SQL, 0)?);
    Ok((table, RegisterOrLiteral::Register(root_page_reg)))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use turso_parser::ast;

    use crate::security::roles::RoleId;
    use crate::{
        Connection, Database, DatabaseOpts, MemoryIO, OpenFlags, SqliteDialect, StepResult, IO,
    };

    #[test]
    fn create_role_adds_the_role_to_the_catalog() {
        let conn = open_connection();

        create_role(&conn, "alice").unwrap();

        let roles = conn.role_catalog();
        let alice = roles.get_by_name("alice").unwrap();
        assert!(!alice.superuser);
        assert!(!alice.can_login);
    }

    #[test]
    fn create_role_fails_if_the_role_exists() {
        let conn = open_connection();
        create_role(&conn, "alice").unwrap();

        let error = create_role(&conn, "alice").unwrap_err();

        assert!(
            error.to_string().contains("role \"alice\" already exists"),
            "{error}"
        );
    }

    #[test]
    fn set_role_switches_the_current_role_and_reset_role_switches_back() {
        let conn = open_connection();
        create_role(&conn, "alice").unwrap();
        let alice = conn.role_catalog().get_by_name("alice").unwrap().id;

        set_role(&conn, "alice").unwrap();
        assert_eq!(conn.current_role(), alice);
        assert_eq!(conn.session_role(), RoleId::SUPERUSER);

        reset_role(&conn).unwrap();
        assert_eq!(conn.current_role(), RoleId::SUPERUSER);
    }

    #[test]
    fn set_role_to_a_missing_role_fails() {
        let conn = open_connection();

        let error = set_role(&conn, "nobody").unwrap_err();

        assert_eq!(error.to_string(), "role \"nobody\" does not exist");
        assert_eq!(conn.current_role(), RoleId::SUPERUSER);
    }

    #[test]
    fn set_role_inside_a_transaction_fails() {
        let conn = open_connection();
        create_role(&conn, "alice").unwrap();
        conn.execute("BEGIN").unwrap();

        let error = set_role(&conn, "alice").unwrap_err();

        assert_eq!(
            error.to_string(),
            "SET ROLE inside a transaction block is not supported"
        );
        assert_eq!(conn.current_role(), RoleId::SUPERUSER);
    }

    #[test]
    fn role_without_privileges_cannot_create_objects() {
        let conn = open_connection();
        create_role(&conn, "alice").unwrap();
        set_role(&conn, "alice").unwrap();

        let create_table = conn.execute("CREATE TABLE t (x)").unwrap_err();
        let create_role_error = create_role(&conn, "bob").unwrap_err();

        assert_eq!(
            create_table.to_string(),
            "permission denied for schema public"
        );
        assert_eq!(
            create_role_error.to_string(),
            "permission denied to create role"
        );
    }

    #[test]
    fn role_without_privileges_can_run_statements_without_objects() {
        let conn = open_connection();
        create_role(&conn, "alice").unwrap();
        set_role(&conn, "alice").unwrap();

        conn.execute("SELECT 1").unwrap();
        reset_role(&conn).unwrap();
        conn.execute("CREATE TABLE t (x)").unwrap();
    }

    #[test]
    fn role_without_privileges_cannot_read_or_change_tables() {
        let conn = open_connection();
        conn.execute("CREATE TABLE t (x)").unwrap();
        create_role(&conn, "alice").unwrap();
        set_role(&conn, "alice").unwrap();

        for sql in [
            "SELECT * FROM t",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET x = 2",
            "DELETE FROM t",
        ] {
            let error = conn.execute(sql).unwrap_err();
            assert_eq!(error.to_string(), "permission denied for table t", "{sql}");
        }
    }

    #[test]
    fn role_without_privileges_cannot_use_sequences() {
        let conn = open_connection();
        conn.execute("CREATE SEQUENCE s").unwrap();
        create_role(&conn, "alice").unwrap();
        set_role(&conn, "alice").unwrap();

        let error = conn.execute("SELECT nextval('s')").unwrap_err();

        assert_eq!(error.to_string(), "permission denied for sequence s");
    }

    #[test]
    fn open_pragma_cursor_does_not_skip_privilege_checks() {
        let conn = open_connection();
        conn.execute("CREATE TABLE t (x)").unwrap();
        create_role(&conn, "alice").unwrap();
        let mut pragma = conn
            .prepare("SELECT * FROM pragma_table_info('t')")
            .unwrap();
        assert!(matches!(pragma.step().unwrap(), StepResult::Row));
        set_role(&conn, "alice").unwrap();

        let error = conn.prepare("SELECT * FROM t").err().unwrap();

        assert_eq!(error.to_string(), "permission denied for table t");
    }

    #[test]
    fn role_without_privileges_cannot_read_views() {
        let conn = open_connection();
        conn.execute("CREATE TABLE t (x)").unwrap();
        conn.execute("CREATE VIEW over_table AS SELECT x FROM t")
            .unwrap();
        conn.execute("CREATE VIEW constant AS SELECT 'secret' AS value")
            .unwrap();
        create_role(&conn, "alice").unwrap();
        set_role(&conn, "alice").unwrap();

        for view in ["over_table", "constant"] {
            let error = conn.execute(format!("SELECT * FROM {view}")).unwrap_err();
            assert_eq!(
                error.to_string(),
                format!("permission denied for view {view}")
            );
        }
    }

    #[test]
    fn role_without_privileges_may_use_only_virtual_tables_that_allow_it() {
        let conn = open_connection();
        conn.execute("CREATE TABLE t (x)").unwrap();
        create_role(&conn, "alice").unwrap();
        set_role(&conn, "alice").unwrap();

        let pragma = conn
            .execute("SELECT * FROM pragma_table_info('t')")
            .unwrap_err();
        assert_eq!(
            pragma.to_string(),
            "permission denied for function pragma_table_info"
        );
        conn.execute("SELECT * FROM json_each('[1, 2]')").unwrap();
    }

    #[test]
    fn role_without_privileges_cannot_read_currval() {
        let conn = open_connection();
        conn.execute("CREATE SEQUENCE s").unwrap();
        conn.execute("SELECT nextval('s')").unwrap();
        create_role(&conn, "alice").unwrap();
        set_role(&conn, "alice").unwrap();

        let error = conn.execute("SELECT currval('s')").unwrap_err();

        assert_eq!(error.to_string(), "permission denied for sequence s");
    }

    #[test]
    fn role_without_privileges_can_read_the_schema_again() {
        let conn = open_connection();
        conn.execute("CREATE TABLE t (x)").unwrap();
        create_role(&conn, "alice").unwrap();
        set_role(&conn, "alice").unwrap();

        conn.reparse_schema().unwrap();

        assert!(conn.role_catalog().get_by_name("alice").is_some());
    }

    #[test]
    fn interrupted_set_role_keeps_the_current_role() {
        for interrupt_at_step in 1.. {
            let conn = open_connection();
            create_role(&conn, "alice").unwrap();
            let steps = Arc::new(AtomicU64::new(0));
            let steps_in_handler = steps.clone();
            conn.set_progress_handler(
                1,
                Some(Box::new(move || {
                    steps_in_handler.fetch_add(1, Ordering::SeqCst) + 1 == interrupt_at_step
                })),
            );
            let result = set_role(&conn, "alice");
            conn.set_progress_handler(0, None);
            if result.is_ok() {
                assert!(interrupt_at_step > 1, "the statement was never interrupted");
                return;
            }

            assert_eq!(
                conn.current_role(),
                RoleId::SUPERUSER,
                "interrupted at step {interrupt_at_step}, the role changed"
            );
        }
    }

    #[test]
    fn role_catalog_includes_roles_committed_by_another_connection() {
        let conn1 = open_connection();
        let conn2 = conn1.db.connect().unwrap();
        assert!(conn1.role_catalog().get_by_name("alice").is_none());

        create_role(&conn2, "alice").unwrap();

        assert!(conn1.role_catalog().get_by_name("alice").is_some());
    }

    #[test]
    fn role_without_privileges_cannot_read_the_sequence_watermark() {
        let conn = open_connection();
        conn.execute("CREATE SEQUENCE s").unwrap();
        create_role(&conn, "alice").unwrap();
        set_role(&conn, "alice").unwrap();

        let error = conn
            .execute("SELECT sequence_watermark_experimental('s')")
            .unwrap_err();

        assert_eq!(error.to_string(), "permission denied for sequence s");
    }

    #[test]
    fn interrupted_create_role_leaves_no_role_behind() {
        for interrupt_at_step in 1.. {
            let conn = open_connection();
            let steps = Arc::new(AtomicU64::new(0));
            let steps_in_handler = steps.clone();
            conn.set_progress_handler(
                1,
                Some(Box::new(move || {
                    steps_in_handler.fetch_add(1, Ordering::SeqCst) + 1 == interrupt_at_step
                })),
            );
            let result = create_role(&conn, "alice");
            conn.set_progress_handler(0, None);
            if result.is_ok() {
                assert!(interrupt_at_step > 1, "the statement was never interrupted");
                return;
            }

            assert!(
                conn.role_catalog().get_by_name("alice").is_none(),
                "interrupted at step {interrupt_at_step}, the role is still in the catalog"
            );
            create_role(&conn, "alice").unwrap();
        }
    }

    fn open_connection() -> Arc<Connection> {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            ":memory:",
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        db.connect().unwrap()
    }

    fn set_role(conn: &Arc<Connection>, name: &str) -> crate::Result<()> {
        let set_role = ast::Cmd::Stmt(ast::Stmt::SetRole {
            role_name: Some(name.to_string()),
        });
        conn.prepare_translated_cmd(set_role, &format!("SET ROLE {name}"))?
            .run_ignore_rows()
    }

    fn reset_role(conn: &Arc<Connection>) -> crate::Result<()> {
        let reset_role = ast::Cmd::Stmt(ast::Stmt::SetRole { role_name: None });
        conn.prepare_translated_cmd(reset_role, "RESET ROLE")?
            .run_ignore_rows()
    }

    fn create_role(conn: &Arc<Connection>, name: &str) -> crate::Result<()> {
        let create_role = ast::Cmd::Stmt(ast::Stmt::CreateRole {
            role_name: name.to_string(),
        });
        conn.prepare_translated_cmd(create_role, &format!("CREATE ROLE {name}"))?
            .run_ignore_rows()
    }
}
