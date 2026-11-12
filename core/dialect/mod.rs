//! SQL dialects.
//!
//! The [`Dialect`] trait is the boundary between the engine and the SQL
//! dialect a frontend speaks. The engine owns the mechanics — pages,
//! B-trees, the `sqlite_schema` table itself, bytecode — and consults the
//! dialect wherever the meaning of SQL text is dialect-specific: parsing
//! statements into the engine AST and interpreting persisted schema text.
//! The frontends live in their own crates: the SQLite dialect in
//! `turso_sqlite` and the PostgreSQL dialect in `turso_pg`. The [`engine`]
//! module has what they share: the parser entry points, the built-in
//! functions, the catalog tables that ship with every Turso build
//! (`pragma_*`, `json_each`/`json_tree`, `sqlite_dbpage`, `btree_dump`,
//! `sqlite_turso_types`), and the dialect of the statements the engine
//! runs for itself.

pub mod engine;

pub(crate) use engine::EngineDialect;

use crate::{
    function::ScalarFunc,
    namespace::NamespaceId,
    schema::{BTreeTable, RelationKind, Schema},
    security::{policy::RowPolicy, roles::RoleId},
    vdbe::{builder::ProgramBuilder, Register},
    Clock, CollationSeq, Connection, Func, LimboError, OpenOptions, Result, Value, IO,
};
use std::{any::Any, sync::Arc};
use turso_parser::ast::{
    self, Cmd, CreateFunction, CreateTableBody, DropFunction, Expr, Grantee, Privilege,
    QualifiedName, RoleSpec, Stmt,
};

/// Prefix the PostgreSQL frontend puts before the `CREATE TABLE` text it
/// stores in `sqlite_schema`.
pub const POSTGRES_TABLE_SQL_PREFIX: &str = "/* turso_frontend:postgres */ ";

/// A change a statement makes to the dialect's catalog of the main
/// database: to a relation, or to the roles. Translation tells the dialect
/// about it through
/// [`Dialect::translate_catalog_changes`], and the dialect's own bytecode
/// tells it back through [`Dialect::apply_catalog_change`] once the change
/// is written.
#[derive(Debug, Clone)]
pub enum CatalogChange {
    Create {
        namespace: NamespaceId,
        name: String,
        kind: RelationKind,
        /// For an index, the table it is on, in the same namespace. Its
        /// owner owns the index.
        table: Option<String>,
    },
    Drop {
        namespace: NamespaceId,
        name: String,
    },
    Rename {
        namespace: NamespaceId,
        old_name: String,
        new_name: String,
    },
    /// Whom the relation belongs to from now on.
    ChangeOwner {
        namespace: NamespaceId,
        name: String,
        owner: NewOwner,
    },
    /// `GRANT privileges ON relation TO grantees`.
    Grant {
        namespace: NamespaceId,
        name: String,
        privileges: Vec<Privilege>,
        grantees: Vec<Grantee>,
    },
    /// `ALTER TABLE ... ENABLE ROW LEVEL SECURITY`, or `DISABLE` when
    /// `enabled` is false.
    SetRowSecurity {
        namespace: NamespaceId,
        name: String,
        enabled: bool,
    },
    /// `CREATE POLICY` on table `name`. Its expressions refer to the
    /// table's columns by their unqualified names.
    CreatePolicy {
        namespace: NamespaceId,
        name: String,
        policy: Box<ast::CreatePolicy>,
    },
    /// `DROP POLICY policy_name ON name`.
    DropPolicy {
        namespace: NamespaceId,
        name: String,
        policy_name: String,
        if_exists: bool,
    },
    /// `CREATE ROLE name`: a role that is not a superuser and cannot log in.
    CreateRole { name: String },
    /// A change to an object that is not a relation and that only the
    /// dialect knows, such as a function that SQL defines. The engine never
    /// builds one: the dialect's own bytecode carries it from
    /// [`Dialect::translate_create_function`] or
    /// [`Dialect::translate_drop_function`] to
    /// [`Dialect::apply_catalog_change`].
    DialectObject(Arc<dyn Any + Send + Sync>),
}

/// Whom a relation belongs to after a [`CatalogChange::ChangeOwner`].
#[derive(Debug, Clone)]
pub enum NewOwner {
    /// A table of the same namespace, as `ALTER SEQUENCE ... OWNED BY
    /// table.column` names one for a sequence, or no table. What a table
    /// owns changes owner with it, and not on its own.
    Table(Option<String>),
    /// A role, as `ALTER ... OWNER TO` names one.
    Role(RoleSpec),
}

/// What a built-in scalar function may read besides its arguments.
pub struct ScalarFuncContext<'a> {
    pub connection: &'a Connection,
    pub io: &'a dyn IO,
    /// The statement's clock: every function of one step sees the same time.
    pub clock: &'a dyn Clock,
    /// The collation of the call site, for functions that compare arguments.
    pub collation: CollationSeq,
}

/// SQL dialect layered on top of the engine.
///
/// Every [`crate::Database`] carries a dialect, supplied explicitly by
/// every open path and fixed for the lifetime of the database;
/// SQLite-compatible callers pass `turso_sqlite::SqliteDialect`. Initial statement
/// preparation, re-preparation, and every schema load go through this
/// interface.
pub trait Dialect: Send + Sync + 'static {
    /// Stable identifier for this dialect (e.g. "sqlite", "postgres").
    ///
    /// A database file must always be opened with the same dialect it was
    /// created with; the process-wide database registry uses this name to
    /// reject an open whose dialect differs from the already-open instance.
    fn name(&self) -> &'static str;

    fn format_error(&self, error: LimboError) -> LimboError {
        error.into_sqlite_error()
    }

    /// Parse the first statement in `sql` into the engine AST.
    ///
    /// Returns the parsed command, if any, and the number of input bytes
    /// consumed. The engine uses the same method for initial preparation and
    /// re-preparation, so dialect-specific SQL remains valid after schema or
    /// connection compilation state changes. Implementations must accept the
    /// canonical SQLite text produced by the engine AST formatter because
    /// engine-generated and AST-only statements use that representation.
    fn parse(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
        engine::parse(sql)
    }

    /// Parse a `sqlite_schema` `type='table'` row's SQL into a table
    /// definition.
    ///
    /// Rows written by internal engine paths (sequence backing tables,
    /// `sqlite_sequence`) are plain SQLite text and carry no frontend
    /// marker, so every implementation must fall back to SQLite parsing
    /// for text it does not recognize as its own.
    fn parse_table_sql(&self, sql: &str, root_page: i64) -> Result<BTreeTable> {
        engine::reject_postgres_table_sql(sql)?;
        BTreeTable::from_sql(sql, root_page)
    }

    /// Decode a storage-backed table's persisted SQL into its `CREATE TABLE`
    /// AST.
    ///
    /// Unlike [`Dialect::parse`], this method receives SQL read from
    /// `sqlite_schema` and must recognize the representation produced by
    /// [`Dialect::format_table_sql`] and
    /// [`Dialect::format_rewritten_table_sql`]. Internal engine tables use
    /// plain SQLite text, so implementations must retain the same SQLite
    /// fallback required by [`Dialect::parse_table_sql`].
    fn parse_table_sql_ast(&self, sql: &str) -> Result<Stmt> {
        engine::reject_postgres_table_sql(sql)?;
        engine::parse_table_sql_ast(sql)
    }

    /// Recover SQL that can be prepared to recreate a persisted table.
    ///
    /// Dialects that wrap original frontend DDL in their stored representation
    /// must unwrap it here so replay preserves that DDL. The returned statement
    /// must create the table in the connection's main schema, even when the
    /// persisted statement originally qualified the source database. Unmarked
    /// internal engine tables must retain the SQLite fallback used by the
    /// schema parsing methods.
    fn table_sql_for_replay(&self, sql: &str) -> Result<String> {
        engine::reject_postgres_table_sql(sql)?;
        engine::table_sql_for_replay(sql)
    }

    /// Produce the SQL text to store in `sqlite_schema` for a
    /// `CREATE TABLE`.
    ///
    /// `input` is the original statement text as the user wrote it, in the
    /// frontend's dialect; `tbl_name` and `body` are the translated AST.
    /// The SQLite dialect formats canonical SQLite text from the AST; a
    /// frontend dialect typically stores `input` with a marker it can
    /// recognize in [`Dialect::parse_table_sql`].
    fn format_table_sql(
        &self,
        _input: &str,
        tbl_name: &QualifiedName,
        body: &CreateTableBody,
    ) -> Result<String> {
        engine::format_table_sql(tbl_name, body)
    }

    /// Produce stored SQL after the engine rewrites a `CREATE TABLE` AST.
    ///
    /// Schema rewrites cannot reuse the original frontend text because it no
    /// longer describes the rewritten table. Dialects that need syntax beyond
    /// a marker around canonical SQL can override this to render their native
    /// table definition from the rewritten AST.
    fn format_rewritten_table_sql(&self, stmt: &Stmt) -> Result<String> {
        let Stmt::CreateTable { tbl_name, body, .. } = stmt else {
            return Err(LimboError::InternalError(
                "format_rewritten_table_sql requires CREATE TABLE".to_string(),
            ));
        };
        self.format_table_sql(&stmt.to_string(), tbl_name, body)
    }

    /// Install the dialect's catalog tables into a freshly constructed
    /// schema.
    ///
    /// Called by [`Schema::with_options`] on every schema
    /// construction and rebuild, so catalog tables survive rebuilds
    /// structurally instead of being re-registered by hand. The SQLite
    /// dialect registers the standard built-in catalog here; other
    /// dialects typically compose with it via
    /// [`engine::register_builtin_catalog`] and then add their own tables
    /// (constructed with [`crate::VirtualTable::new_internal`], which
    /// requires no connection).
    fn register_catalog(&self, schema: &mut Schema, enable_custom_types: bool) -> Result<()> {
        engine::register_builtin_catalog(schema, enable_custom_types)
    }

    fn register_native_extensions(&self, options: OpenOptions) -> OpenOptions {
        options
    }

    /// Resolve a function name in user SQL to the engine's function IR.
    ///
    /// The dialect owns its scalar function surface: the SQLite dialect
    /// resolves the built-in set, another dialect resolves its own —
    /// mapping names onto engine primitives where it wants them (usually
    /// by composing with [`engine::resolve_builtin_function`]) and onto
    /// [`Func::Dialect`] for functions it executes
    /// itself via [`Dialect::exec_scalar_function`]. Consulted
    /// before extension functions; engine-generated helper statements
    /// always resolve with SQLite semantics instead.
    fn resolve_function(&self, name: &str, arg_count: usize) -> Result<Option<Func>> {
        engine::resolve_builtin_function(name, arg_count)
    }

    /// Run built-in scalar function `func` on the registers that hold its
    /// arguments.
    ///
    /// The default runs the shared implementation in
    /// [`engine::call_builtin_scalar`]; a dialect overrides it for the
    /// functions whose behavior differs and delegates the rest.
    fn call_scalar(
        &self,
        context: &ScalarFuncContext<'_>,
        func: &ScalarFunc,
        args: &[Register],
    ) -> Result<Value> {
        engine::call_builtin_scalar(context, func, args)
    }

    /// Execute a dialect scalar function at runtime.
    ///
    /// Receives the connection — unlike extension functions — because
    /// catalog functions (e.g. `pg_get_tabledef`) need to inspect the
    /// schema. Only reached through [`Func::Dialect`],
    /// so a dialect that never resolves to that variant can keep the
    /// default "no such function" error.
    fn exec_scalar_function(
        &self,
        _conn: &Connection,
        name: &str,
        _args: &[Value],
    ) -> Result<Value> {
        Err(LimboError::ParseError(format!("no such function: {name}")))
    }

    /// Whether this dialect needs the custom-type machinery (DECODE/ENCODE,
    /// affinity metadata) regardless of the experimental database flag.
    /// A dialect whose type system leans on custom types (e.g. PostgreSQL)
    /// returns true so its databases never open with the machinery off.
    fn requires_custom_types(&self) -> bool {
        false
    }

    /// Whether `t.c` reads only the nearest query that has a table named `t`.
    ///
    /// SQLite keeps searching outer queries when that table has no column
    /// `c`. PostgreSQL stops there and reports that the column does not
    /// exist.
    fn qualified_column_uses_nearest_table_only(&self) -> bool {
        false
    }

    /// Whether a sequence named without a schema in a column default or a
    /// view body is bound to the schema it is found in when the table or
    /// view is created.
    ///
    /// PostgreSQL stores the bound name, so the default or view keeps its
    /// sequence after the search path changes. SQLite looks the name up
    /// each time the default or view runs.
    fn binds_names_at_create(&self) -> bool {
        false
    }

    /// Whether a database of this dialect supports namespaces: the objects of
    /// a namespace are kept in the schema rows of the main database
    /// under names that encode the namespace's id, which the loader decodes
    /// and which no user object may be named.
    ///
    /// PostgreSQL stores its schemas this way. SQLite has no namespaces, so
    /// a name that looks like a mangled name is an ordinary name.
    fn supports_namespaces(&self) -> bool {
        false
    }

    /// Whether a relation name is unique within its schema across kinds, so
    /// that a table and a sequence cannot share one. PostgreSQL's are.
    /// SQLite lets a sequence take a table's name, and keeps its own rules
    /// for tables, views and indexes.
    fn relation_names_unique_across_kinds(&self) -> bool {
        false
    }

    /// SQL statements that read the dialect's own schema state, such as the
    /// PostgreSQL frontend's relation catalog table, or none when this
    /// database keeps none. The statements run with engine semantics after
    /// the schema rows load, and their rows go to
    /// [`Dialect::build_schema_state`].
    fn schema_state_queries(&self, schema: &Schema) -> Vec<String> {
        let _ = schema;
        Vec::new()
    }

    /// Builds [`Schema::dialect_schema`] from the rows of each statement of
    /// [`Dialect::schema_state_queries`], in the same order. `schema` is the
    /// schema the state is built for, already holding every other object.
    fn build_schema_state(
        &self,
        schema: &Schema,
        results: &[Vec<Vec<Value>>],
    ) -> Result<Arc<dyn Any + Send + Sync>> {
        let _ = (schema, results);
        Err(LimboError::InternalError(
            "the dialect queried schema state it cannot build".to_string(),
        ))
    }

    /// Translates `changes`, which the statement being compiled makes, into
    /// the bytecode that records them in the dialect's own schema state, in
    /// the same transaction as the statement's `sqlite_schema` rows. A table
    /// comes with its automatic indexes in one call. Does nothing for a
    /// dialect without schema state of its own, except that such a dialect
    /// has no roles, ownership, privileges or row-level security and refuses
    /// `CREATE ROLE`, `ALTER ... OWNER TO`, `ALTER SEQUENCE ... OWNED BY`,
    /// `GRANT`, `ALTER TABLE ... ENABLE ROW LEVEL SECURITY`, `CREATE POLICY`
    /// and `DROP POLICY`.
    fn translate_catalog_changes(
        &self,
        program: &mut ProgramBuilder,
        schema: &Schema,
        connection: &Arc<Connection>,
        changes: &[CatalogChange],
    ) -> Result<()> {
        let _ = (program, schema, connection);
        for change in changes {
            match change {
                CatalogChange::ChangeOwner {
                    owner: NewOwner::Role(_),
                    ..
                } => {
                    return Err(LimboError::ParseError(
                        "ALTER ... OWNER TO is not supported".to_string(),
                    ));
                }
                CatalogChange::ChangeOwner {
                    owner: NewOwner::Table(_),
                    ..
                } => {
                    return Err(LimboError::ParseError(
                        "ALTER SEQUENCE ... OWNED BY is not supported".to_string(),
                    ));
                }
                CatalogChange::Grant { .. } => {
                    return Err(LimboError::ParseError("GRANT is not supported".to_string()));
                }
                CatalogChange::SetRowSecurity { .. }
                | CatalogChange::CreatePolicy { .. }
                | CatalogChange::DropPolicy { .. } => {
                    return Err(LimboError::ParseError(
                        "row-level security is not supported".to_string(),
                    ));
                }
                CatalogChange::CreateRole { .. } => {
                    return Err(LimboError::ParseError(
                        "CREATE ROLE is not supported".to_string(),
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Applies `change`, which the running statement wrote to the dialect's
    /// schema state, to the in-memory copy in `schema`. `values` are the
    /// registers the dialect named when it emitted the instruction, such as
    /// the id its state gave a created relation.
    fn apply_catalog_change(
        &self,
        schema: &mut Schema,
        change: &CatalogChange,
        values: &[Value],
    ) -> Result<()> {
        let _ = (schema, change, values);
        Ok(())
    }

    /// Emits the writes that store the function of `CREATE FUNCTION`, such
    /// as rows of `sqlite_schema` (see
    /// [`crate::open_schema_table_for_dialect_rows`]) and of the dialect's
    /// own schema state. `input` is the statement as the user wrote it. The
    /// engine has no functions that SQL defines, so a dialect without them
    /// refuses the statement.
    fn translate_create_function(
        &self,
        program: &mut ProgramBuilder,
        schema: &Schema,
        connection: &Arc<Connection>,
        function: &CreateFunction,
        input: &str,
    ) -> Result<()> {
        let _ = (program, schema, connection, function, input);
        Err(LimboError::ParseError(
            "CREATE FUNCTION is not supported".to_string(),
        ))
    }

    /// Emits the writes that remove the function of `DROP FUNCTION`.
    ///
    /// The engine does not check who may drop a function, so the dialect
    /// must: only the dialect knows if the function exists and who owns it.
    /// PostgreSQL lets every role run `DROP FUNCTION IF EXISTS` for a
    /// function that does not exist.
    fn translate_drop_function(
        &self,
        program: &mut ProgramBuilder,
        schema: &Schema,
        connection: &Arc<Connection>,
        function: &DropFunction,
    ) -> Result<()> {
        let _ = (program, schema, connection, function);
        Err(LimboError::ParseError(
            "DROP FUNCTION is not supported".to_string(),
        ))
    }

    /// Whether `schema` has a function whose calls
    /// [`Dialect::expand_function_call`] can replace. The engine only looks
    /// for calls in a statement when it does.
    fn expands_function_calls(&self, schema: &Schema) -> bool {
        let _ = schema;
        false
    }

    /// The expression that `call`, an [`Expr::FunctionCall`], stands for,
    /// when the dialect defines the called function as an expression of its
    /// arguments, as PostgreSQL does for an SQL function that it inlines.
    /// None for every other function. The calls in the arguments of `call`
    /// are expanded already, and the result must not have a call that the
    /// dialect expands.
    ///
    /// The engine asks for the calls of SELECT, INSERT, UPDATE, DELETE and
    /// CREATE TABLE AS. It does not ask for a call that a statement stores
    /// in the schema, such as the query of a view, the body of a trigger or
    /// a column default: such a call fails as a call of a function that
    /// does not exist. A dialect that wants a clearer error must refuse
    /// these statements.
    ///
    /// The engine walks an expression with recursion. Thus it refuses a
    /// statement whose expression has more levels after the expansion than
    /// the parser permits ([`turso_parser::parser::MAX_EXPR_DEPTH`]).
    fn expand_function_call(
        &self,
        schema: &Schema,
        connection: &Connection,
        call: &Expr,
    ) -> Result<Option<Expr>> {
        let _ = (schema, connection, call);
        Ok(None)
    }

    /// Whether relations can be created, renamed or dropped in an attached
    /// database. The PostgreSQL frontend refuses it: its relation catalog
    /// covers only the main database, so a database file with relations
    /// made while it was attached could not be opened later.
    fn supports_relation_ddl_in_attached_databases(&self) -> bool {
        true
    }

    /// The id of the role named `name`, case-sensitively, when the dialect
    /// stores roles. A dialect without roles has none, so a connection can
    /// run only as the superuser.
    fn role_by_name(&self, schema: &Schema, name: &str) -> Option<RoleId> {
        let _ = (schema, name);
        None
    }

    /// Whether `role` passes every privilege check. [`RoleId::SUPERUSER`]
    /// does in every dialect.
    fn role_is_superuser(&self, schema: &Schema, role: RoleId) -> bool {
        let _ = schema;
        role == RoleId::SUPERUSER
    }

    /// Whether a session whose current role is `member` may switch to `role`
    /// with `SET ROLE`. A superuser may switch to any role, any role may
    /// switch to itself, and no session may switch to [`RoleId::PUBLIC`].
    fn can_set_role(&self, schema: &Schema, member: RoleId, role: RoleId) -> bool {
        role != RoleId::PUBLIC && (member == role || self.role_is_superuser(schema, member))
    }

    /// The role that owns relation `name` of `namespace`, when the dialect
    /// stores ownership. None means ownership is unknown, and only a
    /// superuser may drop or alter the relation.
    fn relation_owner(
        &self,
        schema: &Schema,
        namespace: NamespaceId,
        name: &str,
    ) -> Option<RoleId> {
        let _ = (schema, namespace, name);
        None
    }

    /// Whether `role` has `privilege` on relation `name` of `namespace`,
    /// from the privileges the dialect stores. A dialect without privileges
    /// gives every role every privilege.
    fn role_has_privilege(
        &self,
        schema: &Schema,
        namespace: NamespaceId,
        name: &str,
        role: RoleId,
        privilege: Privilege,
    ) -> bool {
        let _ = (schema, namespace, name, role, privilege);
        true
    }

    /// Whether table `name` of `namespace` has row-level security enabled.
    /// A dialect without row-level security never has it enabled.
    fn table_rls_enabled(&self, schema: &Schema, namespace: NamespaceId, name: &str) -> bool {
        let _ = (schema, namespace, name);
        false
    }

    /// The policies of table `name` of `namespace`, for every role and
    /// command. A dialect without row-level security has none.
    fn row_security_policies<'a>(
        &self,
        schema: &'a Schema,
        namespace: NamespaceId,
        name: &str,
    ) -> &'a [RowPolicy] {
        let _ = (schema, namespace, name);
        &[]
    }

    /// Whether `PRAGMA capture_data_changes_conn` may create its change
    /// tables in a database of this dialect.
    ///
    /// The PostgreSQL frontend refuses it: CDC tables are created at run
    /// time, outside the DDL that maintains the dialect's schema state, so
    /// the PostgreSQL frontend would have relations missing from its
    /// relation catalog.
    fn supports_capture_data_changes(&self) -> bool {
        true
    }

    /// The namespaces of a database that has never created or dropped one.
    /// They have ids from 1 in this order and no rows until the first
    /// CREATE SCHEMA or DROP SCHEMA creates the namespaces table, which then
    /// stores them with the change.
    ///
    /// PostgreSQL has the schema public. SQLite has no namespaces.
    fn default_namespaces(&self) -> &'static [&'static str] {
        &[]
    }

    /// Whether `INSERT ... ON CONFLICT DO UPDATE` fails when the conflicting
    /// row was already inserted or updated by the same statement.
    ///
    /// PostgreSQL raises an error so that the result does not depend on the
    /// order of the proposed rows. SQLite updates the row again.
    fn upsert_rejects_row_written_by_same_statement(&self) -> bool {
        false
    }

    /// Whether a subquery used as an expression, such as `x = (SELECT ...)`,
    /// raises an error when it returns more than one row.
    ///
    /// PostgreSQL raises SQLSTATE 21000. SQLite uses the first row.
    fn rejects_scalar_subquery_with_several_rows(&self) -> bool {
        false
    }

    /// Whether integer division by zero raises an error.
    ///
    /// PostgreSQL raises SQLSTATE 22012. SQLite returns NULL.
    fn errors_on_zero_divisor(&self) -> bool {
        false
    }

    /// Whether `mod` of two integers returns an integer.
    ///
    /// PostgreSQL returns an integer. SQLite always returns a REAL.
    fn mod_of_integers_is_integer(&self) -> bool {
        false
    }

    /// Whether `ALTER TABLE ADD COLUMN` with a non-constant default works on
    /// a table that has rows.
    ///
    /// PostgreSQL rewrites the table and computes the default for each
    /// existing row. SQLite raises an error unless the table is empty.
    fn add_column_computes_default_for_existing_rows(&self) -> bool {
        false
    }

    /// Whether `CREATE INDEX` accepts qualified column names such as `t.a`
    /// in its indexed terms.
    ///
    /// PostgreSQL accepts them. SQLite raises an error.
    fn allows_qualified_columns_in_index_terms(&self) -> bool {
        false
    }

    /// Whether a cast of an integer to the built-in `boolean` type gives
    /// true for every nonzero integer.
    ///
    /// PostgreSQL's integer-to-boolean cast does. Otherwise the cast uses the
    /// type's ENCODE, which accepts only 0 and 1.
    fn casts_nonzero_integer_to_true(&self) -> bool {
        false
    }

    /// Whether constraint errors report the values that broke the constraint.
    ///
    /// PostgreSQL shows them in the error DETAIL, so custom type and array
    /// values are decoded before the error is raised. SQLite's messages name
    /// only the columns, so no decode runs, and a DECODE that fails cannot
    /// replace the constraint error.
    fn constraint_errors_show_values(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::PlatformIO;
    use crate::native_ext::{VirtualTable, VirtualTableCursor, VirtualTableModule};
    use crate::security::roles::SessionRole;
    use crate::storage::database::DatabaseFile;
    use crate::sync::atomic::{AtomicUsize, Ordering};
    use crate::types::IOResultOr;
    use crate::{
        ConnectOpts, Database, DatabaseOpts, IOResult, MemoryIO, Numeric, OpenFlags,
        StatementStatusCounter, IO,
    };
    use std::sync::Arc;
    use turso_ext::{
        ConstraintInfo, ConstraintUsage, IndexInfo, OrderByInfo, ResultCode, VTabKind,
    };
    use turso_parser::parser::Parser;

    /// A dialect that counts schema-row parses and strips a `/* test */ `
    /// marker before delegating to SQLite parsing, mirroring how a frontend
    /// dialect recognizes its own stored text and falls back to SQLite for
    /// unmarked rows.
    #[derive(Default)]
    struct TestDialect {
        parse_calls: AtomicUsize,
        statement_parse_calls: AtomicUsize,
    }

    impl Dialect for TestDialect {
        fn name(&self) -> &'static str {
            "test"
        }

        fn parse(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
            self.statement_parse_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(sql) = sql.strip_prefix("test: ") {
                let (cmd, offset) = engine::parse(sql)?;
                Ok((cmd, "test: ".len() + offset))
            } else {
                engine::parse(sql)
            }
        }

        fn parse_table_sql(&self, sql: &str, root_page: i64) -> Result<BTreeTable> {
            self.parse_calls.fetch_add(1, Ordering::SeqCst);
            let sql = sql.strip_prefix("/* test */ ").unwrap_or(sql);
            BTreeTable::from_sql(sql, root_page)
        }

        fn parse_table_sql_ast(&self, sql: &str) -> Result<Stmt> {
            let sql = sql.strip_prefix("/* test */ ").unwrap_or(sql);
            engine::parse_table_sql_ast(sql)
        }

        fn table_sql_for_replay(&self, sql: &str) -> Result<String> {
            let sql = sql.strip_prefix("/* test */ ").unwrap_or(sql);
            engine::table_sql_for_replay(sql)
        }

        fn format_table_sql(
            &self,
            input: &str,
            _tbl_name: &QualifiedName,
            _body: &CreateTableBody,
        ) -> Result<String> {
            Ok(format!("/* test */ {input}"))
        }

        fn resolve_function(&self, name: &str, arg_count: usize) -> Result<Option<Func>> {
            if name.eq_ignore_ascii_case("nvl") {
                return engine::resolve_builtin_function("coalesce", arg_count);
            }
            if name.eq_ignore_ascii_case("test_add_one") && arg_count == 1 {
                return Ok(Some(Func::Dialect("test_add_one".to_string())));
            }
            engine::resolve_builtin_function(name, arg_count)
        }

        fn exec_scalar_function(
            &self,
            _conn: &Connection,
            name: &str,
            args: &[Value],
        ) -> Result<Value> {
            assert_eq!(name, "test_add_one");
            let Value::Numeric(Numeric::Integer(v)) = args[0] else {
                return Err(LimboError::InvalidArgument(
                    "test_add_one expects an integer".to_string(),
                ));
            };
            Ok(Value::Numeric(Numeric::Integer(v + 1)))
        }

        fn register_catalog(&self, schema: &mut Schema, enable_custom_types: bool) -> Result<()> {
            engine::register_builtin_catalog(schema, enable_custom_types)
        }

        fn register_native_extensions(&self, options: OpenOptions) -> OpenOptions {
            options.native_module(
                "test_catalog",
                VTabKind::TableValuedFunction,
                TestCatalogTable,
            )
        }
    }

    /// Stores table definitions in syntax that SQLite cannot parse and always
    /// adds its marker, so replay tests detect repeated storage formatting.
    struct StrictTestDialect;

    impl StrictTestDialect {
        const PREFIX: &'static str = "strict: ";
    }

    impl Dialect for StrictTestDialect {
        fn name(&self) -> &'static str {
            "strict-test"
        }

        fn parse(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
            engine::parse(sql)
        }

        fn parse_table_sql(&self, sql: &str, root_page: i64) -> Result<BTreeTable> {
            let sql = sql.strip_prefix(Self::PREFIX).unwrap_or(sql);
            BTreeTable::from_sql(sql, root_page)
        }

        fn parse_table_sql_ast(&self, sql: &str) -> Result<Stmt> {
            let sql = sql.strip_prefix(Self::PREFIX).unwrap_or(sql);
            engine::parse_table_sql_ast(sql)
        }

        fn table_sql_for_replay(&self, sql: &str) -> Result<String> {
            let sql = sql.strip_prefix(Self::PREFIX).unwrap_or(sql);
            engine::table_sql_for_replay(sql)
        }

        fn format_table_sql(
            &self,
            input: &str,
            _tbl_name: &QualifiedName,
            _body: &CreateTableBody,
        ) -> Result<String> {
            Ok(format!("{}{input}", Self::PREFIX))
        }

        fn register_catalog(&self, schema: &mut Schema, enable_custom_types: bool) -> Result<()> {
            engine::register_builtin_catalog(schema, enable_custom_types)
        }

        fn resolve_function(&self, name: &str, arg_count: usize) -> Result<Option<Func>> {
            engine::resolve_builtin_function(name, arg_count)
        }
    }

    /// A one-row catalog table installed by [`TestDialect`],
    /// standing in for a frontend catalog surface like `pg_class`.
    #[derive(Debug)]
    struct TestCatalogTable;

    impl VirtualTableModule for TestCatalogTable {
        type Table = Self;

        fn schema(&self, _args: &[Value]) -> Result<String> {
            Ok("CREATE TABLE test_catalog (value INTEGER)".to_string())
        }

        fn create(&self, _args: &[Value]) -> Result<Self::Table> {
            Ok(Self)
        }
    }

    impl VirtualTable for TestCatalogTable {
        type Cursor = TestCatalogCursor;

        fn open(&self, _conn: Arc<Connection>) -> Result<Self::Cursor> {
            Ok(TestCatalogCursor { row: 0 })
        }

        fn best_index(
            &self,
            constraints: &[ConstraintInfo],
            _order_by: &[OrderByInfo],
        ) -> Result<IndexInfo, ResultCode> {
            Ok(IndexInfo {
                idx_num: 0,
                idx_str: None,
                order_by_consumed: false,
                estimated_cost: 1.0,
                estimated_rows: 1,
                constraint_usages: constraints
                    .iter()
                    .map(|_| ConstraintUsage {
                        argv_index: None,
                        omit: false,
                    })
                    .collect(),
            })
        }
    }

    struct TestCatalogCursor {
        row: usize,
    }

    impl VirtualTableCursor for TestCatalogCursor {
        fn filter(
            &mut self,
            _args: &[Value],
            _idx_str: Option<&str>,
            _idx_num: i32,
        ) -> IOResultOr<bool> {
            self.row = 0;
            Ok(IOResult::Done(true))
        }

        fn next(&mut self) -> IOResultOr<bool> {
            self.row += 1;
            Ok(IOResult::Done(self.row < 1))
        }

        fn rowid(&self) -> i64 {
            self.row as i64
        }

        fn column(&mut self, column: usize) -> IOResultOr<Value> {
            Ok(IOResult::Done(match column {
                0 => Value::from_i64(42),
                _ => Value::Null,
            }))
        }
    }

    fn open_db(io: &Arc<dyn IO>, path: &str, dialect: Arc<dyn Dialect>) -> Result<Arc<Database>> {
        let file = io.open_file(path, OpenFlags::Create, true)?;
        let db_file = Arc::new(DatabaseFile::new(file));
        Database::open(io.clone(), path, OpenOptions::new(dialect).storage(db_file))
    }

    #[test]
    fn schema_load_routes_through_dialect() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        {
            let db = open_db(&io, "dialect-load.db", Arc::new(EngineDialect)).unwrap();
            let conn = db
                .connect(ConnectOpts::new(SessionRole::Superuser))
                .unwrap();
            conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
            conn.close().unwrap();
        }

        // Reopening the database parses the stored schema row for `t`
        // through the dialect.
        let dialect = Arc::new(TestDialect::default());
        let db = open_db(&io, "dialect-load.db", dialect.clone()).unwrap();
        assert!(dialect.parse_calls.load(Ordering::SeqCst) >= 1);

        // DDL reparses the schema via the ParseSchema opcode, again through
        // the dialect.
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let before = dialect.parse_calls.load(Ordering::SeqCst);
        conn.execute("CREATE TABLE u (y INTEGER)").unwrap();
        assert!(dialect.parse_calls.load(Ordering::SeqCst) > before);

        // Both tables are usable under the dialect.
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.execute("INSERT INTO u VALUES (2)").unwrap();
        conn.close().unwrap();
    }

    #[test]
    fn dialect_parser_is_used_for_reprepare() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let dialect = Arc::new(TestDialect::default());
        let db = open_db(&io, "dialect-reprepare.db", dialect.clone()).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();

        let mut stmt = conn.prepare("test: SELECT 42").unwrap();
        conn.set_full_column_names(true);
        let rows = stmt.run_collect_rows().unwrap();

        assert_eq!(rows, vec![vec![Value::from_i64(42)]]);
        assert_eq!(stmt.stmt_status(StatementStatusCounter::Reprepare), 1);
        assert_eq!(dialect.statement_parse_calls.load(Ordering::SeqCst), 2);
        conn.close().unwrap();
    }

    #[test]
    fn consume_stmt_reports_parse_error() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "consume-stmt-parse-error.db", Arc::new(EngineDialect)).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();

        assert!(conn.consume_stmt("SELECT * FROM").is_err());
        conn.close().unwrap();
    }

    #[test]
    fn dialect_catalog_available_on_every_schema_and_rebuild() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-catalog.db", Arc::new(TestDialect::default())).unwrap();

        let query_catalog = |conn: &Arc<Connection>| -> Vec<Vec<Value>> {
            conn.prepare("SELECT value FROM test_catalog")
                .unwrap()
                .run_collect_rows()
                .unwrap()
        };

        let conn1 = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let conn2 = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        assert_eq!(
            query_catalog(&conn1),
            vec![vec![Value::Numeric(Numeric::Integer(42))]]
        );
        assert_eq!(
            query_catalog(&conn2),
            vec![vec![Value::Numeric(Numeric::Integer(42))]]
        );

        // DDL on another connection forces conn1 to rebuild its schema from
        // sqlite_schema; the catalog table must survive because schema
        // construction re-registers it.
        conn2.execute("CREATE TABLE t (x INTEGER)").unwrap();
        assert_eq!(
            query_catalog(&conn1),
            vec![vec![Value::Numeric(Numeric::Integer(42))]]
        );

        conn1.close().unwrap();
        conn2.close().unwrap();
    }

    #[test]
    fn a_dialect_without_sql_functions_refuses_their_statements() {
        use turso_parser::ast::{CreateFunction, DropFunction, Name};

        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(
            &io,
            "dialect-functions.db",
            Arc::new(TestDialect::default()),
        )
        .unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let name = QualifiedName::single(Name::exact("one".to_string()));

        let create = Stmt::CreateFunction(CreateFunction {
            or_replace: false,
            name: name.clone(),
            parameter_types: vec![],
            return_type: "int4".to_string(),
            body: "SELECT 1".to_string(),
        });
        let error = conn.prepare_ast_stmt(create).err().unwrap();
        assert_eq!(error.to_string(), "CREATE FUNCTION is not supported");

        let drop = Stmt::DropFunction(DropFunction {
            if_exists: false,
            name,
            parameter_types: None,
        });
        let error = conn.prepare_ast_stmt(drop).err().unwrap();
        assert_eq!(error.to_string(), "DROP FUNCTION is not supported");
        conn.close().unwrap();
    }

    /// A dialect that defines `twice(x)` as `(x) + (x)`, as a frontend does
    /// for a function that SQL defines. `has_table_added()` is 1 when the
    /// schema has a table `added`, and 0 when it does not: a function whose
    /// expression changes with the schema. `wrapped(x)` is `((x))`: a
    /// function that makes its argument two levels deeper.
    struct ExpandingDialect;

    impl Dialect for ExpandingDialect {
        fn name(&self) -> &'static str {
            "expanding"
        }

        fn expands_function_calls(&self, _schema: &Schema) -> bool {
            true
        }

        fn expand_function_call(
            &self,
            schema: &Schema,
            _connection: &Connection,
            call: &Expr,
        ) -> Result<Option<Expr>> {
            let Expr::FunctionCall { name, args, .. } = call else {
                unreachable!("the engine expands function calls only");
            };
            if name.as_str() == "has_table_added" {
                let has_table = schema
                    .get_table(crate::namespace::NamespaceId::ROOT, "added")
                    .is_some();
                return Ok(Some(Expr::Literal(turso_parser::ast::Literal::Numeric(
                    if has_table { "1" } else { "0" }.to_string(),
                ))));
            }
            if name.as_str() == "wrapped" {
                let [argument] = args.as_slice() else {
                    return Err(LimboError::ParseError(
                        "wrapped takes one argument".to_string(),
                    ));
                };
                return Ok(Some(Expr::Parenthesized(vec![Box::new(
                    Expr::Parenthesized(vec![argument.clone()]),
                )])));
            }
            if name.as_str() == "selected" {
                let [argument] = args.as_slice() else {
                    return Err(LimboError::ParseError(
                        "selected takes one argument".to_string(),
                    ));
                };
                return Ok(Some(subquery_of(subquery_of(argument.as_ref().clone()))));
            }
            if name.as_str() == "first_of" {
                let [first, _] = args.as_slice() else {
                    return Err(LimboError::ParseError(
                        "first_of takes two arguments".to_string(),
                    ));
                };
                return Ok(Some(Expr::Parenthesized(vec![first.clone()])));
            }
            if name.as_str() != "twice" {
                return Ok(None);
            }
            let [argument] = args.as_slice() else {
                return Err(LimboError::ParseError(
                    "twice takes one argument".to_string(),
                ));
            };
            let mut argument_calls_twice = false;
            crate::translate::expr::walk_expr(argument, &mut |expr| {
                argument_calls_twice |= matches!(
                    expr,
                    Expr::FunctionCall { name, .. } if name.as_str() == "twice"
                );
                Ok(crate::translate::expr::WalkControl::Continue)
            })?;
            assert!(
                !argument_calls_twice,
                "the calls in the arguments are expanded before the call"
            );
            let operand = || Box::new(Expr::Parenthesized(vec![argument.clone()]));
            Ok(Some(Expr::Parenthesized(vec![Box::new(Expr::Binary(
                operand(),
                turso_parser::ast::Operator::Add,
                operand(),
            ))])))
        }
    }

    /// The expression `(SELECT expr)`.
    fn subquery_of(expr: Expr) -> Expr {
        let Cmd::Stmt(Stmt::Select(mut select)) =
            Parser::new(b"SELECT 0").next_cmd().unwrap().unwrap()
        else {
            unreachable!("the statement is a SELECT");
        };
        let turso_parser::ast::OneSelect::Select { columns, .. } = &mut select.body.select else {
            unreachable!("the statement has no VALUES");
        };
        columns[0] = turso_parser::ast::ResultColumn::Expr(Box::new(expr), None);
        Expr::Subquery(Box::new(select))
    }

    #[test]
    fn dialect_expands_function_calls_in_queries_and_data_changes() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-expand.db", Arc::new(ExpandingDialect)).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let integers = |sql: &str| -> Vec<Vec<i64>> {
            conn.prepare(sql)
                .unwrap()
                .run_collect_rows()
                .unwrap()
                .iter()
                .map(|row| row.iter().map(|value| value.as_int().unwrap()).collect())
                .collect()
        };

        assert_eq!(integers("SELECT twice(21)"), [[42]]);
        assert_eq!(integers("SELECT twice(twice(1)) + abs(twice(-1))"), [[6]]);

        // The expression of a call comes from the schema. A statement with
        // an expanded call opens a read transaction, which checks the schema
        // version, also when the statement reads no table.
        let plan = |sql: &str| {
            let rows = conn
                .prepare(format!("EXPLAIN {sql}"))
                .unwrap()
                .run_collect_rows()
                .unwrap();
            format!("{rows:?}")
        };
        assert!(plan("SELECT twice(21)").contains("Transaction"));
        assert!(!plan("SELECT abs(21)").contains("Transaction"));

        conn.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (twice(1), 1), (twice(2), 2), (twice(3), 3)")
            .unwrap();
        assert_eq!(
            integers("SELECT a FROM t WHERE a > twice(1) ORDER BY twice(a) DESC LIMIT twice(1)"),
            [[6], [4]]
        );
        assert_eq!(
            integers(
                "WITH doubled AS (SELECT twice(b) AS c FROM t) \
                 SELECT (SELECT max(twice(c)) FROM doubled), \
                        EXISTS (SELECT 1 FROM t WHERE a = twice(b)), \
                        twice(2) IN (SELECT a FROM t)"
            ),
            [[12, 1, 1]]
        );
        assert_eq!(
            integers(
                "SELECT t.b FROM t JOIN t AS other ON other.a = twice(t.a) \
                 GROUP BY t.b HAVING count(*) = twice(1) - 1"
            ),
            [[1]]
        );
        assert_eq!(
            integers(
                "SELECT sum(b) OVER (PARTITION BY twice(b) > 2 ORDER BY twice(a)) \
                 FROM t ORDER BY a"
            ),
            [[1], [2], [5]]
        );
        assert_eq!(
            integers("UPDATE t SET b = twice(b) WHERE a = twice(1) RETURNING twice(b)"),
            [[4]]
        );
        assert_eq!(
            integers(
                "INSERT INTO t VALUES (2, 0) \
                 ON CONFLICT (a) DO UPDATE SET b = twice(excluded.a) RETURNING b"
            ),
            [[4]]
        );
        assert_eq!(
            integers("DELETE FROM t WHERE a = twice(3) RETURNING twice(a)"),
            [[12]]
        );
        conn.execute("CREATE TABLE copied AS SELECT twice(a) AS doubled FROM t WHERE a = twice(1)")
            .unwrap();
        assert_eq!(integers("SELECT doubled FROM copied"), [[4]]);

        // The argument is one variable of the statement, used two times.
        let mut statement = conn.prepare("SELECT twice(?)").unwrap();
        assert_eq!(statement.parameters_count(), 1);
        statement
            .bind_at(std::num::NonZero::new(1).unwrap(), Value::from_i64(5))
            .unwrap();
        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(10)]]
        );
        conn.close().unwrap();
    }

    #[test]
    fn a_prepared_statement_uses_the_expression_of_a_call_as_it_is_at_each_run() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-expand-again.db", Arc::new(ExpandingDialect)).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let other_conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();

        let mut statement = conn.prepare("SELECT has_table_added()").unwrap();
        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(0)]]
        );

        other_conn
            .execute("CREATE TABLE added (a INTEGER)")
            .unwrap();
        statement.reset().unwrap();
        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(1)]]
        );
        conn.close().unwrap();
        other_conn.close().unwrap();
    }

    /// `wrapped(x)` nested `calls` times around the constant 1: an
    /// expression of `2 * calls + 1` levels after the expansion.
    fn nested_wrapped_calls(calls: usize) -> String {
        format!("{}1{}", "wrapped(".repeat(calls), ")".repeat(calls))
    }

    #[test]
    fn an_expression_that_is_too_deep_after_the_expansion_fails() {
        // The engine walks an expression with recursion. The depth limit of
        // the parser is for the stack of a main thread, which is larger than
        // the stack of a test thread.
        let on_a_large_stack = std::thread::Builder::new().stack_size(64 << 20);
        on_a_large_stack
            .spawn(|| {
                let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
                let db =
                    open_db(&io, "dialect-expand-deep.db", Arc::new(ExpandingDialect)).unwrap();
                let conn = db
                    .connect(ConnectOpts::new(SessionRole::Superuser))
                    .unwrap();
                let run = |sql: String| conn.prepare(sql)?.run_collect_rows();
                let too_large = "Expression tree is too large (maximum depth 100)";

                // 49 calls make 99 levels, and 50 calls make 101 levels.
                let rows = run(format!("SELECT {}", nested_wrapped_calls(49))).unwrap();
                assert_eq!(rows, vec![vec![Value::from_i64(1)]]);
                let error = run(format!("SELECT {}", nested_wrapped_calls(50))).unwrap_err();
                assert!(error.to_string().contains(too_large), "{error}");

                // The expansion of 40 calls has 81 levels. Each `-(` above
                // it adds two levels: 9 give 99 levels, and 10 give 101.
                let below_negations = |negations: usize| {
                    format!(
                        "SELECT {}{}{}",
                        "-(".repeat(negations),
                        nested_wrapped_calls(40),
                        ")".repeat(negations)
                    )
                };
                let rows = run(below_negations(9)).unwrap();
                assert_eq!(rows, vec![vec![Value::from_i64(-1)]]);
                let error = run(below_negations(10)).unwrap_err();
                assert!(error.to_string().contains(too_large), "{error}");

                // The dialect does not get an argument that is too deep,
                // also when the expansion of the call does not use it.
                let unused_argument =
                    |calls: usize| format!("SELECT first_of(1, {})", nested_wrapped_calls(calls));
                let rows = run(unused_argument(49)).unwrap();
                assert_eq!(rows, vec![vec![Value::from_i64(1)]]);
                let error = run(unused_argument(50)).unwrap_err();
                assert!(error.to_string().contains(too_large), "{error}");

                // The expressions of a subquery are one level below the
                // subquery, as the parser counts them. `selected(x)` is
                // `(SELECT (SELECT x))`: 49 calls make 99 levels, and 50
                // calls make 101 levels.
                let in_subqueries = |calls: usize| {
                    format!("SELECT {}1{}", "selected(".repeat(calls), ")".repeat(calls))
                };
                let rows = run(in_subqueries(49)).unwrap();
                assert_eq!(rows, vec![vec![Value::from_i64(1)]]);
                let error = run(in_subqueries(50)).unwrap_err();
                assert!(error.to_string().contains(too_large), "{error}");
                conn.close().unwrap();
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn a_statement_that_stores_a_call_keeps_the_call() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-stored-call.db", Arc::new(ExpandingDialect)).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute("CREATE TABLE t (a INTEGER)").unwrap();
        conn.execute("CREATE TABLE log (a INTEGER)").unwrap();

        conn.execute("CREATE VIEW doubled AS SELECT twice(a) FROM t")
            .unwrap();
        let error = conn.prepare("SELECT * FROM doubled").err().unwrap();
        assert!(error.to_string().contains("no such function: twice"));

        conn.execute(
            "CREATE TRIGGER log_doubled AFTER INSERT ON t \
             BEGIN INSERT INTO log VALUES (twice(new.a)); END",
        )
        .unwrap();
        let error = conn.execute("INSERT INTO t VALUES (1)").err().unwrap();
        assert!(error.to_string().contains("no such function: twice"));
        conn.close().unwrap();
    }

    #[test]
    fn a_dialect_without_function_expansion_keeps_the_call() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(
            &io,
            "dialect-no-expand.db",
            Arc::new(TestDialect::default()),
        )
        .unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let error = conn.prepare("SELECT twice(21)").err().unwrap();
        assert!(error.to_string().contains("no such function: twice"));
        conn.close().unwrap();
    }

    #[test]
    fn dialect_catalog_cannot_be_dropped() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(
            &io,
            "dialect-catalog-drop.db",
            Arc::new(TestDialect::default()),
        )
        .unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();

        let error = conn.execute("DROP TABLE test_catalog").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("table test_catalog may not be dropped"),
            "unexpected error: {error}"
        );

        let new_conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        for catalog_conn in [&conn, &new_conn] {
            let rows = catalog_conn
                .prepare("SELECT value FROM test_catalog")
                .unwrap()
                .run_collect_rows()
                .unwrap();
            assert_eq!(rows, vec![vec![Value::from_i64(42)]]);
        }
        conn.close().unwrap();
        new_conn.close().unwrap();
    }

    #[test]
    fn dialect_catalog_survives_mvcc_recovery() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let path = "dialect-catalog-mvcc-recovery.db";

        {
            let db = open_db(&io, path, Arc::new(TestDialect::default())).unwrap();
            let conn = db
                .connect(ConnectOpts::new(SessionRole::Superuser))
                .unwrap();
            conn.execute("PRAGMA journal_mode = mvcc").unwrap();
            conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
            conn.close().unwrap();
        }

        let db = open_db(&io, path, Arc::new(TestDialect::default())).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let rows = conn
            .prepare("SELECT value FROM test_catalog")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows, vec![vec![Value::from_i64(42)]]);
        conn.close().unwrap();
    }

    #[test]
    fn dialect_catalog_available_in_initialized_temp_schema() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(
            &io,
            "dialect-catalog-temp.db",
            Arc::new(TestDialect::default()),
        )
        .unwrap();

        for temp_store in ["MEMORY", "FILE"] {
            let conn = db
                .connect(ConnectOpts::new(SessionRole::Superuser))
                .unwrap();
            conn.execute(format!("PRAGMA temp_store = {temp_store}"))
                .unwrap();
            conn.execute("CREATE TEMP TABLE t (x INTEGER)").unwrap();
            let rows = conn
                .prepare("SELECT value FROM temp.test_catalog")
                .unwrap()
                .run_collect_rows()
                .unwrap();
            assert_eq!(rows, vec![vec![Value::from_i64(42)]]);
            conn.close().unwrap();
        }
    }

    #[test]
    fn create_table_stores_dialect_formatted_sql() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        {
            let dialect = Arc::new(TestDialect::default());
            let db = open_db(&io, "dialect-store.db", dialect).unwrap();
            let conn = db
                .connect(ConnectOpts::new(SessionRole::Superuser))
                .unwrap();

            // A frontend prepares its translated AST while supplying the
            // original statement text.
            let input = "CREATE TABLE t (x INTEGER)";
            let stmt = match Parser::new(input.as_bytes()).next_cmd().unwrap().unwrap() {
                Cmd::Stmt(stmt) => stmt,
                other => panic!("unexpected command: {other:?}"),
            };
            conn.prepare_translated_stmt(stmt, input)
                .unwrap()
                .run_ignore_rows()
                .unwrap();

            // The stored schema row carries the dialect marker and the
            // original text.
            let rows = conn
                .prepare("SELECT sql FROM sqlite_schema WHERE name = 't'")
                .unwrap()
                .run_collect_rows()
                .unwrap();
            assert_eq!(rows.len(), 1);
            let stored = rows[0][0].to_string();
            assert_eq!(stored.trim_matches('\''), format!("/* test */ {input}"));
            conn.close().unwrap();
        }

        // Round-trip: reopening parses the marked row back through the
        // dialect and the table stays usable.
        let dialect = Arc::new(TestDialect::default());
        let db = open_db(&io, "dialect-store.db", dialect.clone()).unwrap();
        assert!(dialect.parse_calls.load(Ordering::SeqCst) >= 1);
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        let rows = conn
            .prepare("SELECT x FROM t")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows.len(), 1);
        conn.close().unwrap();
    }

    /// A dialect that stores a function as a row of `sqlite_schema`, with
    /// the name of the function as the name of the row.
    struct FunctionStoringDialect;

    impl Dialect for FunctionStoringDialect {
        fn name(&self) -> &'static str {
            "function-storing"
        }

        fn parse(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
            engine::parse(sql)
        }

        fn parse_table_sql(&self, sql: &str, root_page: i64) -> Result<BTreeTable> {
            BTreeTable::from_sql(sql, root_page)
        }

        fn parse_table_sql_ast(&self, sql: &str) -> Result<Stmt> {
            engine::parse_table_sql_ast(sql)
        }

        fn table_sql_for_replay(&self, sql: &str) -> Result<String> {
            engine::table_sql_for_replay(sql)
        }

        fn format_table_sql(
            &self,
            _input: &str,
            tbl_name: &QualifiedName,
            body: &CreateTableBody,
        ) -> Result<String> {
            Ok(format!(
                "CREATE TABLE {} {}",
                tbl_name.name.as_ident(),
                body
            ))
        }

        fn register_catalog(&self, schema: &mut Schema, enable_custom_types: bool) -> Result<()> {
            engine::register_builtin_catalog(schema, enable_custom_types)
        }

        fn translate_create_function(
            &self,
            program: &mut ProgramBuilder,
            schema: &Schema,
            _connection: &Arc<Connection>,
            function: &CreateFunction,
            input: &str,
        ) -> Result<()> {
            let cursor_id = crate::open_schema_table_for_dialect_rows(program, schema)?;
            crate::emit_dialect_schema_row_insert(
                program,
                cursor_id,
                "test_function",
                function.name.name.as_str(),
                input,
            );
            Ok(())
        }
    }

    #[test]
    fn a_table_statement_does_not_change_a_dialect_row_with_the_name_of_the_table() {
        use turso_parser::ast::Name;

        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-rows.db", Arc::new(FunctionStoringDialect)).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let create = Stmt::CreateFunction(CreateFunction {
            or_replace: false,
            name: QualifiedName::single(Name::exact("one".to_string())),
            parameter_types: vec![],
            return_type: "int4".to_string(),
            body: "SELECT 1".to_string(),
        });
        conn.prepare_translated_stmt(create, "SELECT 1")
            .unwrap()
            .run_ignore_rows()
            .unwrap();
        let dialect_rows = || -> Vec<String> {
            conn.prepare("SELECT name FROM sqlite_schema WHERE type = 'test_function'")
                .unwrap()
                .run_collect_rows()
                .unwrap()
                .iter()
                .map(|row| row[0].to_string())
                .collect()
        };
        assert_eq!(dialect_rows(), ["one"]);

        conn.execute("CREATE TABLE one (a INTEGER)").unwrap();
        conn.execute("ALTER TABLE one RENAME TO two").unwrap();
        assert_eq!(dialect_rows(), ["one"]);

        conn.execute("CREATE TABLE one (a INTEGER)").unwrap();
        conn.execute("DROP TABLE one").unwrap();
        assert_eq!(dialect_rows(), ["one"]);

        // No table can have the `tbl_name` of the dialect rows.
        let error = conn
            .execute("CREATE TABLE __turso_internal_dialect_row (a INTEGER)")
            .unwrap_err();
        assert!(error.to_string().contains("reserved"), "{error}");
        conn.close().unwrap();
    }

    #[test]
    fn vacuum_refuses_a_database_with_a_dialect_row() {
        use turso_parser::ast::Name;

        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-vacuum.db", Arc::new(FunctionStoringDialect)).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let create = Stmt::CreateFunction(CreateFunction {
            or_replace: false,
            name: QualifiedName::single(Name::exact("one".to_string())),
            parameter_types: vec![],
            return_type: "int4".to_string(),
            body: "SELECT 1".to_string(),
        });
        conn.prepare_translated_stmt(create, "SELECT 1")
            .unwrap()
            .run_ignore_rows()
            .unwrap();

        let directory = tempfile::tempdir().unwrap();
        let copy = directory.path().join("copy.db");
        let error = conn
            .execute(format!("VACUUM INTO '{}'", copy.display()))
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "VACUUM is not supported for a database with a sqlite_schema row of type test_function"
        );
        conn.close().unwrap();
    }

    /// A dialect that resolves no functions at all, proving the dialect —
    /// not the engine — owns the function name surface of user SQL.
    struct NoFunctionsDialect;

    impl Dialect for NoFunctionsDialect {
        fn name(&self) -> &'static str {
            "nofuncs"
        }

        fn parse(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
            engine::parse(sql)
        }

        fn parse_table_sql(&self, sql: &str, root_page: i64) -> Result<BTreeTable> {
            BTreeTable::from_sql(sql, root_page)
        }

        fn parse_table_sql_ast(&self, sql: &str) -> Result<Stmt> {
            engine::parse_table_sql_ast(sql)
        }

        fn table_sql_for_replay(&self, sql: &str) -> Result<String> {
            engine::table_sql_for_replay(sql)
        }

        fn format_table_sql(
            &self,
            _input: &str,
            tbl_name: &QualifiedName,
            body: &CreateTableBody,
        ) -> Result<String> {
            Ok(format!(
                "CREATE TABLE {} {}",
                tbl_name.name.as_ident(),
                body
            ))
        }

        fn register_catalog(&self, schema: &mut Schema, enable_custom_types: bool) -> Result<()> {
            engine::register_builtin_catalog(schema, enable_custom_types)
        }

        fn resolve_function(&self, _name: &str, _arg_count: usize) -> Result<Option<Func>> {
            Ok(None)
        }
    }

    /// A dialect that resolves the built-in names but runs `upper` its own
    /// way, as a frontend does for a built-in whose behavior differs in its
    /// dialect.
    struct ShoutingUpperDialect;

    impl Dialect for ShoutingUpperDialect {
        fn name(&self) -> &'static str {
            "shouting"
        }

        fn parse(&self, sql: &str) -> Result<(Option<Cmd>, usize)> {
            engine::parse(sql)
        }

        fn parse_table_sql(&self, sql: &str, root_page: i64) -> Result<BTreeTable> {
            BTreeTable::from_sql(sql, root_page)
        }

        fn parse_table_sql_ast(&self, sql: &str) -> Result<Stmt> {
            engine::parse_table_sql_ast(sql)
        }

        fn table_sql_for_replay(&self, sql: &str) -> Result<String> {
            engine::table_sql_for_replay(sql)
        }

        fn format_table_sql(
            &self,
            _input: &str,
            tbl_name: &QualifiedName,
            body: &CreateTableBody,
        ) -> Result<String> {
            engine::format_table_sql(tbl_name, body)
        }

        fn register_catalog(&self, schema: &mut Schema, enable_custom_types: bool) -> Result<()> {
            engine::register_builtin_catalog(schema, enable_custom_types)
        }

        fn resolve_function(&self, name: &str, arg_count: usize) -> Result<Option<Func>> {
            engine::resolve_builtin_function(name, arg_count)
        }

        fn call_scalar(
            &self,
            context: &ScalarFuncContext<'_>,
            func: &ScalarFunc,
            args: &[Register],
        ) -> Result<Value> {
            let value = engine::call_builtin_scalar(context, func, args)?;
            if !matches!(func, ScalarFunc::Upper) {
                return Ok(value);
            }
            Ok(Value::build_text(format!("{value}!")))
        }
    }

    #[test]
    fn dialect_owns_builtin_scalar_function_behavior() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-shouting.db", Arc::new(ShoutingUpperDialect)).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();

        conn.execute("CREATE TABLE t (s TEXT)").unwrap();
        conn.execute("INSERT INTO t VALUES ('hi')").unwrap();
        let rows = conn
            .prepare("SELECT upper(s), lower('ABC'), abs(-7) FROM t")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(
            rows,
            vec![vec![
                Value::build_text("HI!"),
                Value::build_text("abc"),
                Value::from_i64(7),
            ]]
        );
        conn.close().unwrap();
    }

    #[test]
    fn dialect_scalar_function_resolves_and_executes() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-funcs.db", Arc::new(TestDialect::default())).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();

        // Dialect-provided scalar executes through exec_scalar_function.
        let rows = conn
            .prepare("SELECT test_add_one(41)")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows, vec![vec![Value::Numeric(Numeric::Integer(42))]]);

        // Built-ins still resolve because the dialect composes with the
        // shared table.
        let rows = conn
            .prepare("SELECT abs(-7)")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows, vec![vec![Value::Numeric(Numeric::Integer(7))]]);

        // Unknown names still error.
        let err = conn.prepare("SELECT no_such_function(1)").unwrap_err();
        assert!(err.to_string().contains("no such function"));
        conn.close().unwrap();
    }

    #[test]
    fn dialect_function_alias_preserves_outer_join() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(
            &io,
            "dialect-outer-join.db",
            Arc::new(TestDialect::default()),
        )
        .unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();

        conn.execute("CREATE TABLE lhs (id INTEGER)").unwrap();
        conn.execute("CREATE TABLE rhs (id INTEGER, value INTEGER)")
            .unwrap();
        conn.execute("INSERT INTO lhs VALUES (1), (2)").unwrap();
        conn.execute("INSERT INTO rhs VALUES (1, 0)").unwrap();

        let rows = conn
            .prepare(
                "SELECT lhs.id FROM lhs LEFT JOIN rhs ON rhs.id = lhs.id \
                 WHERE nvl(rhs.value, 1) = 1 ORDER BY lhs.id",
            )
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows, vec![vec![Value::from_i64(2)]]);
        conn.close().unwrap();
    }

    #[test]
    fn dialect_owns_the_function_surface() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-nofuncs.db", Arc::new(NoFunctionsDialect)).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();

        // Function-free SQL works, including DDL (whose internal helper
        // statements resolve with SQLite semantics regardless of dialect).
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (-7)").unwrap();

        // A SQLite built-in is not part of this dialect's surface.
        let err = conn.prepare("SELECT abs(x) FROM t").unwrap_err();
        assert!(
            err.to_string().contains("no such function"),
            "unexpected error: {err}"
        );
        conn.close().unwrap();
    }

    #[test]
    fn cdc_generated_functions_bypass_the_dialect() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-cdc.db", Arc::new(NoFunctionsDialect)).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();

        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        conn.execute("PRAGMA capture_data_changes_conn('full')")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (7)").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t VALUES (8)").unwrap();
        conn.execute("COMMIT").unwrap();

        let rows = conn
            .prepare(
                "SELECT change_type, table_name, id, change_txn_id \
                 FROM turso_cdc ORDER BY change_id",
            )
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0][0], Value::from_i64(1));
        assert_eq!(rows[0][1], Value::build_text("t"));
        assert_eq!(rows[0][2], Value::from_i64(1));
        assert_eq!(rows[1][0], Value::from_i64(2));
        assert_eq!(rows[1][1], Value::Null);
        assert_eq!(rows[1][2], Value::Null);
        assert_eq!(rows[0][3], rows[1][3]);
        assert_eq!(rows[2][0], Value::from_i64(1));
        assert_eq!(rows[2][1], Value::build_text("t"));
        assert_eq!(rows[2][2], Value::from_i64(2));
        assert_eq!(rows[3][0], Value::from_i64(2));
        assert_eq!(rows[3][1], Value::Null);
        assert_eq!(rows[3][2], Value::Null);
        assert_eq!(rows[2][3], rows[3][3]);
        conn.close().unwrap();
    }

    #[test]
    fn alter_table_rewrites_dialect_formatted_sql() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-alter-table.db", Arc::new(StrictTestDialect)).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();

        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        conn.execute("ALTER TABLE t RENAME TO u").unwrap();

        let rows = conn
            .prepare("SELECT sql FROM sqlite_schema WHERE name = 'u'")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0].to_string().trim_matches('\''),
            "strict: CREATE TABLE u (x INTEGER)"
        );
        conn.execute("INSERT INTO u VALUES (1)").unwrap();
        conn.close().unwrap();
    }

    #[test]
    fn alter_table_rename_column_decodes_dialect_formatted_sql() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_db(&io, "dialect-alter-column.db", Arc::new(StrictTestDialect)).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();

        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();
        conn.execute("ALTER TABLE t RENAME COLUMN x TO y").unwrap();

        let rows = conn
            .prepare("SELECT sql FROM sqlite_schema WHERE name = 't'")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0].to_string().trim_matches('\''),
            "strict: CREATE TABLE t (y INTEGER)"
        );
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        assert_eq!(
            conn.prepare("SELECT y FROM t")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![Value::from_i64(1)]]
        );
        conn.close().unwrap();
    }

    #[test]
    fn registry_rejects_dialect_mismatch() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let _db = open_db(&io, "dialect-mismatch.db", Arc::new(EngineDialect)).unwrap();

        let err =
            open_db(&io, "dialect-mismatch.db", Arc::new(TestDialect::default())).unwrap_err();
        assert!(
            err.to_string().contains("already open with dialect"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn registry_rejects_default_open_of_dialect_database() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let _db = open_db(
            &io,
            "dialect-mismatch-reverse.db",
            Arc::new(TestDialect::default()),
        )
        .unwrap();

        let err = open_db(&io, "dialect-mismatch-reverse.db", Arc::new(EngineDialect)).unwrap_err();
        assert!(
            err.to_string().contains("already open with dialect"),
            "unexpected error: {err}"
        );
    }

    #[cfg(feature = "fs")]
    #[test]
    fn shared_memory_registry_rejects_dialect_mismatch() {
        let name = "dialect-shared-memory-mismatch";
        let _db = Database::open_shared_memory(name, Arc::new(EngineDialect)).unwrap();

        let err = Database::open_shared_memory(name, Arc::new(TestDialect::default())).unwrap_err();
        assert!(
            err.to_string().contains("already open with dialect"),
            "unexpected error: {err}"
        );
    }

    #[cfg(all(feature = "fs", not(target_family = "wasm")))]
    #[test]
    fn vacuum_into_replays_schema_with_source_dialect() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.db");
        let output_path = dir.path().join("output.db");
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io.clone(),
            source_path.to_str().unwrap(),
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(StrictTestDialect),
        )
        .unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute("CREATE TABLE t(x INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (42)").unwrap();

        conn.execute(format!("VACUUM INTO '{}'", output_path.display()))
            .unwrap();

        let output_db = Database::open_file(
            io,
            output_path.to_str().unwrap(),
            Arc::new(StrictTestDialect),
        )
        .unwrap();
        let output_conn = output_db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let schema_rows = output_conn
            .prepare("SELECT sql FROM sqlite_schema WHERE name = 't'")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(schema_rows.len(), 1);
        assert_eq!(
            schema_rows[0][0].to_string().trim_matches('\''),
            "strict: CREATE TABLE t(x INTEGER)"
        );
        assert_eq!(
            output_conn
                .prepare("SELECT x FROM t")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![Value::from_i64(42)]]
        );
    }

    #[cfg(all(feature = "fs", not(target_family = "wasm")))]
    #[test]
    fn vacuum_attached_database_strips_source_schema_from_replay() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.db");
        let attached_path = dir.path().join("attached.db");
        let output_path = dir.path().join("output.db");
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io.clone(),
            source_path.to_str().unwrap(),
            OpenFlags::Create,
            DatabaseOpts::new().with_attach(true),
            None,
            Arc::new(StrictTestDialect),
        )
        .unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute(format!(
            "ATTACH DATABASE '{}' AS aux",
            attached_path.display()
        ))
        .unwrap();
        conn.execute("CREATE TABLE aux.t (x INTEGER)").unwrap();
        conn.execute("INSERT INTO aux.t VALUES (42)").unwrap();

        conn.execute(format!("VACUUM aux INTO '{}'", output_path.display()))
            .unwrap();

        let output_db = Database::open_file(
            io,
            output_path.to_str().unwrap(),
            Arc::new(StrictTestDialect),
        )
        .unwrap();
        let output_conn = output_db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        assert_eq!(
            output_conn
                .prepare("SELECT x FROM t")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![Value::from_i64(42)]]
        );
    }

    #[cfg(all(feature = "fs", not(target_family = "wasm")))]
    #[test]
    fn in_place_vacuum_replays_schema_with_source_dialect() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.db");
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            path.to_str().unwrap(),
            OpenFlags::Create,
            DatabaseOpts::new().with_vacuum(true),
            None,
            Arc::new(StrictTestDialect),
        )
        .unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.execute("CREATE TABLE t(x INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (42)").unwrap();

        conn.execute("VACUUM").unwrap();

        let schema_rows = conn
            .prepare("SELECT sql FROM sqlite_schema WHERE name = 't'")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        assert_eq!(schema_rows.len(), 1);
        assert_eq!(
            schema_rows[0][0].to_string().trim_matches('\''),
            "strict: CREATE TABLE t(x INTEGER)"
        );
        assert_eq!(
            conn.prepare("SELECT x FROM t")
                .unwrap()
                .run_collect_rows()
                .unwrap(),
            vec![vec![Value::from_i64(42)]]
        );
    }
}
