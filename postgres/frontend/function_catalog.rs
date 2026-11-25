//! How the functions that users define with `CREATE FUNCTION ... LANGUAGE
//! sql` are stored.
//!
//! A function is one row of the `__turso_internal_functions` table of the
//! main database: its oid, name, schema, number of parameters, owner and the
//! text of the CREATE FUNCTION statement, as `sqlite_schema` stores the text
//! of CREATE TABLE. The frontend's catalog writes the rows in the
//! statement's transaction and then tells the in-memory [`FunctionCatalog`]
//! of the schema state ([`crate::schema_state`]) what it wrote, as it does
//! for relations and roles. So a rollback undoes a CREATE FUNCTION, and
//! other connections see a function once its transaction commits.
//!
//! A function takes its oid from the same counter as relations, so no
//! function and relation share one.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use turso_core::namespace::NamespaceId;
use turso_core::schema::Schema;
use turso_core::security::roles::RoleId;
use turso_core::{
    to_u32, CatalogApply, CmdContext, CreateFunction, CursorType, DropFunction, InsertFlags, Insn,
    LimboError, Numeric, ProgramBuilder, RegisterOrLiteral, Value, MAIN_DB_ID, SQLITE_ERROR,
};
use turso_pg_parser::sqlstate::{SQLSTATE_DUPLICATE_FUNCTION, SQLSTATE_UNDEFINED_FUNCTION};

use crate::classes::{emit_allocate_object_oid, FIRST_NORMAL_OBJECT_ID};
use crate::descriptions::{emit_delete_comments, Dropped};
use crate::error::sqlstate_error;
use crate::schema_init::TableInit;
use crate::schema_state::{change_schema_state, emit_catalog_apply, schema_state};
use crate::sql_functions::{function_namespace, SqlFunction};

pub(crate) const FUNCTIONS_TABLE_NAME: &str = "__turso_internal_functions";

/// `sql` is the text of the CREATE FUNCTION statement. `pronargs` is the
/// number of parameters: a schema has one function of a name for each
/// number of parameters, which [`FunctionCatalog`] checks.
const CREATE_FUNCTIONS_TABLE_SQL: &str = "CREATE TABLE __turso_internal_functions (\
    oid INTEGER PRIMARY KEY, \
    proname TEXT NOT NULL, \
    pronamespace INTEGER NOT NULL, \
    pronargs INTEGER NOT NULL, \
    proowner INTEGER NOT NULL, \
    sql TEXT NOT NULL)";

/// The number of columns of the functions table.
const FUNCTION_COLUMN_COUNT: usize = 6;

pub(crate) const TABLE_INIT: TableInit = TableInit {
    table: FUNCTIONS_TABLE_NAME,
    statements: &[CREATE_FUNCTIONS_TABLE_SQL],
};

/// The query that reads the functions for the frontend's schema state, or
/// None when the database has no functions table, as in a read-only open
/// of a database that has none.
pub(crate) fn schema_state_query(schema: &Schema) -> Option<String> {
    schema
        .get_btree_table(NamespaceId::ROOT, FUNCTIONS_TABLE_NAME)
        .is_some()
        .then(|| {
            format!(
                "SELECT oid, proname, pronamespace, pronargs, proowner, sql \
                 FROM {FUNCTIONS_TABLE_NAME}"
            )
        })
}

/// The functions of a database.
#[derive(Debug, Clone, Default)]
pub(crate) struct FunctionCatalog {
    by_oid: BTreeMap<i64, Arc<SqlFunction>>,
    oids_by_name: HashMap<(NamespaceId, String, usize), i64>,
}

impl FunctionCatalog {
    /// The catalog of the rows read with [`schema_state_query`].
    pub(crate) fn new(rows: &[Vec<Value>]) -> turso_core::Result<Self> {
        let mut catalog = Self::default();
        for row in rows {
            catalog.add(function_from_row(row)?)?;
        }
        Ok(catalog)
    }

    /// The function `name` of schema `namespace` with `argument_count`
    /// parameters.
    pub(crate) fn get(
        &self,
        namespace: NamespaceId,
        name: &str,
        argument_count: usize,
    ) -> Option<&Arc<SqlFunction>> {
        self.oids_by_name
            .get(&(namespace, name.to_string(), argument_count))
            .and_then(|oid| self.by_oid.get(oid))
    }

    /// The functions in the order of their oids.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &Arc<SqlFunction>> {
        self.by_oid.values()
    }

    /// The functions of schema `namespace`.
    pub(crate) fn in_namespace(
        &self,
        namespace: NamespaceId,
    ) -> impl Iterator<Item = &Arc<SqlFunction>> {
        self.iter()
            .filter(move |function| function.namespace == namespace)
    }

    fn add(&mut self, function: SqlFunction) -> turso_core::Result<()> {
        let key = (
            function.namespace,
            function.name.clone(),
            function.argument_count,
        );
        if self.by_oid.contains_key(&function.oid) || self.oids_by_name.contains_key(&key) {
            return Err(LimboError::Corrupt(format!(
                "{FUNCTIONS_TABLE_NAME} has a duplicate function: {function:?}"
            )));
        }
        self.oids_by_name.insert(key, function.oid);
        self.by_oid.insert(function.oid, Arc::new(function));
        Ok(())
    }

    fn remove(&mut self, oid: i64) -> Option<Arc<SqlFunction>> {
        let function = self.by_oid.remove(&oid)?;
        self.oids_by_name.remove(&(
            function.namespace,
            function.name.clone(),
            function.argument_count,
        ));
        Some(function)
    }
}

/// The function of a row read with [`schema_state_query`].
fn function_from_row(row: &[Value]) -> turso_core::Result<SqlFunction> {
    let corrupt = || {
        LimboError::Corrupt(format!(
            "{FUNCTIONS_TABLE_NAME} has an invalid row: {row:?}"
        ))
    };
    let [Value::Numeric(Numeric::Integer(oid)), Value::Text(name), Value::Numeric(Numeric::Integer(namespace)), Value::Numeric(Numeric::Integer(argument_count)), Value::Numeric(Numeric::Integer(owner)), Value::Text(sql)] =
        row
    else {
        return Err(corrupt());
    };
    if *oid < FIRST_NORMAL_OBJECT_ID || *owner < 0 {
        return Err(corrupt());
    }
    Ok(SqlFunction::new(
        *oid,
        name.as_str().to_string(),
        NamespaceId::new(u32::try_from(*namespace).map_err(|_| corrupt())?),
        RoleId::from_stored(*owner),
        usize::try_from(*argument_count).map_err(|_| corrupt())?,
        sql.as_str().to_string(),
    ))
}

/// Emits the bytecode that stores the function of `CREATE [OR REPLACE]
/// FUNCTION`. The frontend has checked the statement, so its schema exists
/// and a function that it replaces may be replaced.
pub(crate) fn emit_create_function(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    create: &CreateFunction<'_>,
) -> turso_core::Result<()> {
    let namespace = function_namespace(ctx.connection, create.name)?;
    let name = create.name.name.as_str();
    let existing = schema_state(ctx.schema)
        .and_then(|state| state.functions.get(namespace, name, create.argument_count))
        .cloned();
    let cursor_id = open_functions_table(program, ctx.schema)?;
    match existing {
        Some(_) if !create.or_replace => Err(sqlstate_error(
            SQLSTATE_DUPLICATE_FUNCTION,
            format!("function \"{name}\" already exists with same argument types"),
            None,
        )),
        Some(function) => {
            let oid_reg = emit_seek_function(program, cursor_id, function.oid);
            let record_reg = emit_function_record(
                program,
                name,
                namespace,
                create.argument_count,
                function.owner,
                create.sql,
            );
            // MVCC has no update in place.
            if program.is_mvcc_enabled() {
                program.emit_insn(Insn::Delete {
                    cursor_id,
                    table_name: FUNCTIONS_TABLE_NAME.to_string(),
                    is_part_of_update: true,
                });
            }
            program.emit_insn(Insn::Insert {
                cursor: cursor_id,
                key_reg: oid_reg,
                record_reg,
                flag: InsertFlags::new()
                    .skip_last_rowid()
                    .skip_all_change_counts()
                    .require_seek(),
                table_name: FUNCTIONS_TABLE_NAME.to_string(),
            });
            emit_catalog_apply(
                program,
                FunctionReplaced {
                    oid: function.oid,
                    sql: create.sql.to_string(),
                },
                vec![],
            );
            Ok(())
        }
        None => {
            let owner = ctx.connection.current_role();
            let oid_reg = emit_allocate_object_oid(program, ctx.schema, cursor_id)?;
            let record_reg = emit_function_record(
                program,
                name,
                namespace,
                create.argument_count,
                owner,
                create.sql,
            );
            program.emit_insn(Insn::Insert {
                cursor: cursor_id,
                key_reg: oid_reg,
                record_reg,
                flag: InsertFlags::new().skip_last_rowid(),
                table_name: FUNCTIONS_TABLE_NAME.to_string(),
            });
            emit_catalog_apply(
                program,
                FunctionCreated {
                    name: name.to_string(),
                    namespace,
                    argument_count: create.argument_count,
                    owner,
                    sql: create.sql.to_string(),
                },
                vec![oid_reg],
            );
            Ok(())
        }
    }
}

/// Emits the bytecode that deletes the functions of `DROP FUNCTION`. The
/// frontend has looked them up, so each one that the statement names
/// exists, unless `IF EXISTS` skips it.
pub(crate) fn emit_drop_function(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    drop: &DropFunction<'_>,
) -> turso_core::Result<()> {
    let mut oids = Vec::new();
    for signature in drop.functions {
        let namespace = function_namespace(ctx.connection, &signature.name)?;
        let name = signature.name.name.as_str();
        let argument_count = signature.argument_types.as_ref().map_or(0, Vec::len);
        let function = schema_state(ctx.schema)
            .and_then(|state| state.functions.get(namespace, name, argument_count));
        match function {
            Some(function) => oids.push(function.oid),
            None if drop.if_exists => {}
            None => {
                return Err(sqlstate_error(
                    SQLSTATE_UNDEFINED_FUNCTION,
                    format!("function {name} does not exist"),
                    None,
                ))
            }
        }
    }
    emit_delete_functions(program, ctx.schema, &oids)
}

/// Emits the bytecode that deletes the functions of schema `namespace`,
/// which `DROP SCHEMA ... CASCADE` drops.
pub(crate) fn emit_drop_schema_functions(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    namespace: NamespaceId,
) -> turso_core::Result<()> {
    let oids: Vec<i64> = schema_state(ctx.schema)
        .map(|state| {
            state
                .functions
                .in_namespace(namespace)
                .map(|function| function.oid)
                .collect()
        })
        .unwrap_or_default();
    emit_delete_functions(program, ctx.schema, &oids)
}

/// Deletes the rows of the functions with `oids`.
fn emit_delete_functions(
    program: &mut ProgramBuilder,
    schema: &Schema,
    oids: &[i64],
) -> turso_core::Result<()> {
    if oids.is_empty() {
        return Ok(());
    }
    let cursor_id = open_functions_table(program, schema)?;
    for &oid in oids {
        emit_delete_comments(program, schema, oid, Dropped::Function);
        emit_seek_function(program, cursor_id, oid);
        program.emit_insn(Insn::Delete {
            cursor_id,
            table_name: FUNCTIONS_TABLE_NAME.to_string(),
            is_part_of_update: true,
        });
        emit_catalog_apply(program, FunctionDropped { oid }, vec![]);
    }
    Ok(())
}

/// Opens the functions table of the main database for writing.
fn open_functions_table(
    program: &mut ProgramBuilder,
    schema: &Schema,
) -> turso_core::Result<usize> {
    let table = schema
        .get_btree_table(NamespaceId::ROOT, FUNCTIONS_TABLE_NAME)
        .ok_or_else(|| {
            LimboError::ParseError(format!("the database has no {FUNCTIONS_TABLE_NAME} table"))
        })?;
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: RegisterOrLiteral::Literal(table.root_page),
        db: MAIN_DB_ID,
    });
    Ok(cursor_id)
}

/// Moves `cursor_id` to the row of the function with `oid`, and returns the
/// register that holds the oid. The in-memory catalog was loaded from the
/// table, so the row must be there.
fn emit_seek_function(program: &mut ProgramBuilder, cursor_id: usize, oid: i64) -> usize {
    let oid_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: oid,
        dest: oid_reg,
    });
    let found = program.allocate_label();
    let missing = program.allocate_label();
    program.emit_insn(Insn::NotExists {
        cursor: cursor_id,
        rowid_reg: oid_reg,
        target_pc: missing,
    });
    program.emit_insn(Insn::Goto { target_pc: found });
    program.preassign_label_to_next_insn(missing);
    program.emit_insn(Insn::Halt {
        constraint_info: None,
        err_code: SQLITE_ERROR,
        description: format!("{FUNCTIONS_TABLE_NAME} has no function with oid {oid}"),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(found);
    oid_reg
}

/// Makes the record of a row of the functions table, and returns its
/// register.
fn emit_function_record(
    program: &mut ProgramBuilder,
    name: &str,
    namespace: NamespaceId,
    argument_count: usize,
    owner: RoleId,
    sql: &str,
) -> usize {
    let first_column_reg = program.alloc_registers(FUNCTION_COLUMN_COUNT);
    // The oid is the rowid, which the record stores as NULL.
    program.emit_insn(Insn::Null {
        dest: first_column_reg,
        dest_end: None,
    });
    program.emit_insn(Insn::String8 {
        value: name.to_string(),
        dest: first_column_reg + 1,
    });
    program.emit_insn(Insn::Integer {
        value: i64::from(namespace.as_u32()),
        dest: first_column_reg + 2,
    });
    program.emit_insn(Insn::Integer {
        value: i64::try_from(argument_count).expect("a function has few parameters"),
        dest: first_column_reg + 3,
    });
    program.emit_insn(Insn::Integer {
        value: owner.get(),
        dest: first_column_reg + 4,
    });
    program.emit_insn(Insn::String8 {
        value: sql.to_string(),
        dest: first_column_reg + 5,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(first_column_reg),
        count: to_u32(FUNCTION_COLUMN_COUNT),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    record_reg
}

/// A function that CREATE FUNCTION inserted. The instruction's value is the
/// oid that the function got.
#[derive(Debug)]
struct FunctionCreated {
    name: String,
    namespace: NamespaceId,
    argument_count: usize,
    owner: RoleId,
    sql: String,
}

impl CatalogApply for FunctionCreated {
    fn apply(&self, schema: &mut Schema, values: &[Value]) -> turso_core::Result<()> {
        let [Value::Numeric(Numeric::Integer(oid))] = values else {
            return Err(LimboError::InternalError(format!(
                "CREATE FUNCTION read back {values:?} where the function's oid was written"
            )));
        };
        let function = SqlFunction::new(
            *oid,
            self.name.clone(),
            self.namespace,
            self.owner,
            self.argument_count,
            self.sql.clone(),
        );
        change_schema_state(schema, |state| {
            Arc::make_mut(&mut state.functions).add(function)
        })
    }
}

/// A function whose statement CREATE OR REPLACE FUNCTION replaced. It keeps
/// its oid and owner.
#[derive(Debug)]
struct FunctionReplaced {
    oid: i64,
    sql: String,
}

impl CatalogApply for FunctionReplaced {
    fn apply(&self, schema: &mut Schema, _values: &[Value]) -> turso_core::Result<()> {
        change_schema_state(schema, |state| {
            let functions = Arc::make_mut(&mut state.functions);
            let old = functions.remove(self.oid).ok_or_else(|| {
                LimboError::InternalError(format!("no function with oid {}", self.oid))
            })?;
            functions.add(SqlFunction::new(
                old.oid,
                old.name.clone(),
                old.namespace,
                old.owner,
                old.argument_count,
                self.sql.clone(),
            ))
        })
    }
}

/// A function that DROP FUNCTION or DROP SCHEMA deleted.
#[derive(Debug)]
struct FunctionDropped {
    oid: i64,
}

impl CatalogApply for FunctionDropped {
    fn apply(&self, schema: &mut Schema, _values: &[Value]) -> turso_core::Result<()> {
        change_schema_state(schema, |state| {
            Arc::make_mut(&mut state.functions).remove(self.oid);
            Ok(())
        })
    }
}
