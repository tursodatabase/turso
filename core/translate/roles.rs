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
//! Statements that the engine prepares for itself are checked the same way,
//! against the current role. The exception is a statement that reads the
//! engine's own metadata, such as sequence state or table statistics, to load
//! the schema. It is prepared with `PrepareOptions::engine_metadata_read()`,
//! keeps that setting when it is prepared again, and its rows never reach the
//! client.

use std::sync::Arc;

use crate::namespace::NamespaceId;
use turso_parser::ast;

use crate::schema::{BTreeTable, Schema, SEQ_BACKING_TABLE_PREFIX};
use crate::security::permission::{Action, Object, PermissionDenied};
use crate::security::roles::{CREATE_ROLES_TABLE_SQL, ROLES_TABLE_NAME};
use crate::storage::pager::CreateBTreeFlags;
use crate::translate::emitter::Resolver;
use crate::translate::schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID};
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{to_u32, Cookie, InsertFlags, Insn, RegisterOrLiteral};
use crate::{bail_parse_error, Connection, LimboError, Result, MAIN_DB_ID};

const MAIN_SCHEMA_NAME: &str = "main";

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
    for (insn, index) in &program.insns {
        if program.change_capture_writes.contains(index) {
            continue;
        }
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
        return Err(permission_denied(PermissionDenied::privilege(
            action, object,
        )));
    }
    Ok(())
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
    let ignores_row_policies = false;

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
        value: ignores_row_policies as i64,
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

    program.emit_insn(Insn::AddRole {
        db: MAIN_DB_ID,
        id_reg,
        name: role_name.to_string(),
        superuser,
        ignores_row_policies,
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
    if let Some(table) = resolver
        .schema()
        .get_btree_table(NamespaceId::ROOT, ROLES_TABLE_NAME)
    {
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
        .get_btree_table(NamespaceId::ROOT, SQLITE_TABLEID)
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

    use crate::connection::PrepareOptions;
    use crate::security::permission::{Action, Object, PermissionDenied};
    use crate::security::roles::RoleId;
    use crate::statement::StatementOrigin;
    use crate::{
        Connection, Database, DatabaseOpts, LimboError, MemoryIO, OpenFlags, SqliteDialect,
        StepResult, Value, IO,
    };

    #[test]
    fn create_role_adds_the_role_to_the_catalog() {
        let conn = open_connection();

        create_role(&conn, "alice").unwrap();

        let roles = conn.role_catalog();
        let alice = roles.get_by_name("alice").unwrap();
        assert!(!alice.superuser);
        assert!(!alice.ignores_row_policies);
    }

    #[test]
    fn create_role_keeps_the_last_inserted_rowid() {
        let conn = open_connection();
        conn.execute("CREATE TABLE t (x)").unwrap();
        conn.execute("INSERT INTO t (rowid, x) VALUES (42, 1)")
            .unwrap();

        create_role(&conn, "alice").unwrap();

        assert_eq!(conn.last_insert_rowid(), 42);
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
    fn set_role_inside_a_transaction_keeps_the_transaction() {
        let conn = open_connection();
        create_role(&conn, "alice").unwrap();
        conn.execute("CREATE TABLE t (x)").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();

        set_role(&conn, "alice").unwrap_err();

        assert!(!conn.get_auto_commit());
        conn.execute("COMMIT").unwrap();
        let mut stmt = conn.prepare("SELECT x FROM t").unwrap();
        assert_eq!(
            stmt.run_collect_rows().unwrap(),
            vec![vec![crate::Value::from_i64(1)]]
        );
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
            denial(create_table),
            PermissionDenied::privilege(Action::Create, Object::Schema("main".to_string()))
        );
        assert_eq!(
            denial(create_role_error),
            PermissionDenied::privilege(Action::Create, Object::Role("bob".to_string()))
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

        for (sql, action) in [
            ("SELECT * FROM t", Action::Read),
            ("INSERT INTO t VALUES (1)", Action::Write),
            ("UPDATE t SET x = 2", Action::Write),
            ("DELETE FROM t", Action::Write),
        ] {
            let error = conn.execute(sql).unwrap_err();
            assert_eq!(
                denial(error),
                PermissionDenied::privilege(action, Object::Table("t".to_string())),
                "{sql}"
            );
        }
    }

    #[test]
    fn role_without_privileges_cannot_use_sequences() {
        let conn = open_connection();
        conn.execute("CREATE SEQUENCE s").unwrap();
        create_role(&conn, "alice").unwrap();
        set_role(&conn, "alice").unwrap();

        let error = conn.execute("SELECT nextval('s')").unwrap_err();

        assert_eq!(
            denial(error),
            PermissionDenied::privilege(Action::Write, Object::Sequence("s".to_string()))
        );
    }

    #[test]
    fn role_without_privileges_can_commit_a_read_only_transaction_with_change_capture() {
        let conn = open_connection();
        conn.execute("PRAGMA capture_data_changes_conn('full')")
            .unwrap();
        create_role(&conn, "alice").unwrap();
        set_role(&conn, "alice").unwrap();

        conn.execute("BEGIN").unwrap();
        conn.execute("SELECT 1").unwrap();
        conn.execute("COMMIT").unwrap();

        assert!(conn.get_auto_commit());
    }

    #[test]
    fn role_without_privileges_cannot_write_the_change_capture_table() {
        let conn = open_connection();
        conn.execute("PRAGMA capture_data_changes_conn('full')")
            .unwrap();
        create_role(&conn, "alice").unwrap();
        set_role(&conn, "alice").unwrap();

        let error = conn.execute("DELETE FROM turso_cdc").unwrap_err();

        assert_eq!(
            denial(error),
            PermissionDenied::privilege(Action::Write, Object::Table("turso_cdc".to_string()))
        );
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

        assert_eq!(
            denial(error),
            PermissionDenied::privilege(Action::Read, Object::Table("t".to_string()))
        );
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
    fn role_without_privileges_can_read_a_schema_with_sequences_again() {
        let conn = open_connection();
        conn.execute("CREATE SEQUENCE s").unwrap();
        create_role(&conn, "alice").unwrap();
        set_role(&conn, "alice").unwrap();

        conn.reparse_schema().unwrap();

        assert!(conn
            .schema
            .read()
            .get_sequence(crate::NamespaceId::ROOT, "s")
            .is_some());
    }

    #[test]
    fn role_without_privileges_can_read_a_schema_with_statistics_again() {
        let conn = open_connection();
        conn.execute("CREATE TABLE t (x)").unwrap();
        conn.execute("CREATE INDEX t_x ON t (x)").unwrap();
        conn.execute("INSERT INTO t VALUES (1), (2)").unwrap();
        conn.execute("ANALYZE").unwrap();
        create_role(&conn, "alice").unwrap();
        set_role(&conn, "alice").unwrap();

        conn.reparse_schema().unwrap();
    }

    #[test]
    fn engine_metadata_read_is_not_checked_when_prepared_again() {
        let conn = open_connection();
        conn.execute("CREATE TABLE t (x)").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        create_role(&conn, "alice").unwrap();
        set_role(&conn, "alice").unwrap();
        let mut stmt = conn
            .prepare_with_origin_and_options(
                "SELECT x FROM t",
                StatementOrigin::Root,
                &PrepareOptions::engine_metadata_read(),
            )
            .unwrap();

        conn.bump_prepare_context_generation();

        assert_eq!(
            stmt.run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(1)]]
        );
    }

    #[test]
    fn role_without_privileges_can_read_the_catalog() {
        let conn = open_connection();
        conn.execute("CREATE TABLE t (x)").unwrap();
        create_role(&conn, "alice").unwrap();
        set_role(&conn, "alice").unwrap();

        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_schema WHERE name = 't'")
            .unwrap();
        let rows = stmt.run_collect_rows().unwrap();

        assert_eq!(rows, vec![vec![crate::Value::build_text("t")]]);
    }

    #[test]
    fn statement_prepared_by_the_engine_is_checked() {
        let conn = open_connection();
        conn.execute("CREATE TABLE t (x)").unwrap();
        create_role(&conn, "alice").unwrap();
        set_role(&conn, "alice").unwrap();

        let error = conn.prepare_internal("SELECT * FROM t").err().unwrap();

        assert_eq!(
            denial(error),
            PermissionDenied::privilege(Action::Read, Object::Table("t".to_string()))
        );
    }

    #[test]
    fn statement_prepared_while_the_schema_is_read_again_is_checked() {
        let conn = open_connection();
        create_role(&conn, "alice").unwrap();
        set_role(&conn, "alice").unwrap();
        let results = Arc::new(std::sync::Mutex::new(Vec::new()));
        let results_in_handler = results.clone();
        let conn_in_handler = Arc::downgrade(&conn);
        conn.set_progress_handler(
            1,
            Some(Box::new(move || {
                let conn = conn_in_handler.upgrade().unwrap();
                if conn.schema_reparse_in_progress() {
                    let result = conn.prepare("CREATE TABLE t (x)").map(drop);
                    results_in_handler.lock().unwrap().push(result);
                }
                false
            })),
        );

        conn.reparse_schema().unwrap();
        conn.set_progress_handler(0, None);

        let results = std::mem::take(&mut *results.lock().unwrap());
        assert!(!results.is_empty(), "no statement was prepared");
        for result in results {
            assert_eq!(
                denial(result.unwrap_err()),
                PermissionDenied::privilege(Action::Create, Object::Schema("main".to_string()))
            );
        }
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
        let conn2 = conn1
            .db
            .connect(crate::ConnectOpts::new(crate::SessionRole::Superuser))
            .unwrap();
        assert!(conn1.role_catalog().get_by_name("alice").is_none());

        create_role(&conn2, "alice").unwrap();

        assert!(conn1.role_catalog().get_by_name("alice").is_some());
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

    fn denial(error: LimboError) -> PermissionDenied {
        match error {
            LimboError::PermissionDenied(denial) => *denial,
            other => panic!("expected a permission error, got: {other}"),
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
        db.connect(crate::ConnectOpts::new(crate::SessionRole::Superuser))
            .unwrap()
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
