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

pub mod catalog;
pub mod engine;

pub use crate::translate::sqlite_schema_rows::SqliteCatalog;
pub use catalog::{
    AccessStatement, AfterWrites, AutomaticIndex, Catalog, CatalogApply, CatalogWrite, CreateTable,
    NewOwner, OwnerColumn, RelationChange, RelationName, TableText, WriteContext, WriteOrigin,
};
pub(crate) use engine::EngineDialect;

use crate::{
    function::ScalarFunc,
    namespace::NamespaceId,
    schema::{BTreeTable, Schema, SchemaBuilder},
    security::{policy::RowPolicy, roles::RoleId},
    vdbe::{builder::ProgramBuilder, Register},
    Clock, CollationSeq, Connection, Func, LimboError, OpenOptions, Result, Value, IO,
};
use std::{any::Any, sync::Arc};
use turso_parser::ast::{self, Cmd, CreateTableBody, IdentityKind, Privilege, QualifiedName, Stmt};

/// Prefix the PostgreSQL frontend puts before the `CREATE TABLE` text it
/// stores in `sqlite_schema`.
pub const POSTGRES_TABLE_SQL_PREFIX: &str = "/* turso_frontend:postgres */ ";

/// What a built-in scalar function may read besides its arguments.
pub struct ScalarFuncContext<'a> {
    pub connection: &'a Connection,
    pub io: &'a dyn IO,
    /// The statement's clock: every function of one step sees the same time.
    pub clock: &'a dyn Clock,
    /// The collation of the call site, for functions that compare arguments.
    pub collation: CollationSeq,
}

/// Start of the comment in stored table SQL that lists the positions and kinds
/// of its identity columns, as in `/* turso_identity 0:always 2:default */ `.
// FIXME: temporary. Remove the marker once the real PostgreSQL catalog is
// adopted (pedrocarlo is working on it) and stores the identity kind itself.
pub const IDENTITY_TABLE_SQL_MARKER: &str = "/* turso_identity";

impl dyn Dialect {
    /// The schema a database of this dialect has before its objects are
    /// read from the file. Called when a database is opened and on every
    /// schema rebuild.
    pub fn new_schema(&self, enable_custom_types: bool) -> Result<Schema> {
        Ok(self
            .schema_builder(enable_custom_types)?
            .supports_namespaces(self.supports_namespaces())
            .default_namespaces(self.default_namespaces())
            .build())
    }
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

    /// Starts the schema a database of this dialect has before its objects
    /// are read from the file. The default has the catalog tables and
    /// built-in types that every dialect has. A dialect with catalog tables
    /// of its own adds them to the default (constructed with
    /// [`crate::VirtualTable::new_internal`], which requires no connection).
    ///
    /// [`Dialect::new_schema`] finishes the schema.
    fn schema_builder(&self, enable_custom_types: bool) -> Result<SchemaBuilder> {
        crate::schema::builtin::schema_builder(enable_custom_types)
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

    /// Whether a sequence named without a schema in an expression stored in
    /// the schema is bound to the schema it is found in when the expression
    /// is created.
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

    /// Where the dialect stores the objects of a database. The default
    /// stores them as rows of `sqlite_schema`.
    fn catalog(&self) -> &dyn Catalog {
        &SqliteCatalog
    }

    /// Translates `changes`, which the statement being compiled makes to
    /// relations, into the bytecode that records them in the dialect's own
    /// schema state, in the same transaction as the statement's
    /// `sqlite_schema` rows. Does nothing for a dialect without schema state
    /// of its own. `CREATE TABLE` does not call this: it stores the table
    /// through [`Dialect::catalog`].
    fn translate_relation_changes(
        &self,
        program: &mut ProgramBuilder,
        schema: &Schema,
        connection: &Arc<Connection>,
        changes: &[RelationChange<'_>],
    ) -> Result<()> {
        let _ = (program, schema, connection, changes);
        Ok(())
    }

    /// Translates `statement` into the bytecode that changes the roles,
    /// owners, privileges or policies the dialect stores. A dialect that
    /// stores none refuses `CREATE ROLE`, `GRANT role TO role`,
    /// `ALTER ... OWNER TO`, `ALTER SEQUENCE ... OWNED BY`, `GRANT`,
    /// `REVOKE`, `ALTER TABLE ... ENABLE ROW LEVEL SECURITY`,
    /// `CREATE POLICY` and `DROP POLICY`.
    fn translate_access_statement(
        &self,
        program: &mut ProgramBuilder,
        schema: &Schema,
        connection: &Arc<Connection>,
        statement: &AccessStatement<'_>,
    ) -> Result<()> {
        let _ = (program, schema, connection);
        Err(statement.not_supported())
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

    /// Whether row-level security does not limit `role`. As in PostgreSQL,
    /// it does not limit a superuser.
    fn role_bypass_rls(&self, schema: &Schema, role: RoleId) -> bool {
        self.role_is_superuser(schema, role)
    }

    /// Whether a session whose current role is `member` may switch to `role`
    /// with `SET ROLE`. A superuser may switch to any role, any role may
    /// switch to itself, and no session may switch to [`RoleId::PUBLIC`].
    fn can_set_role(&self, schema: &Schema, member: RoleId, role: RoleId) -> bool {
        role != RoleId::PUBLIC && (member == role || self.role_is_superuser(schema, member))
    }

    /// Whether `member` has the privileges of `role`: it is `role`, a
    /// superuser, or, in a dialect with role membership, a role that
    /// inherits the privileges of `role`. An owner check asks this of the
    /// owner, so a role with the owner's privileges acts as the owner.
    fn role_has_privileges_of(&self, schema: &Schema, member: RoleId, role: RoleId) -> bool {
        member == role || self.role_is_superuser(schema, member)
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

    /// The sequences of `namespace` that `column` of table `table` owns, or
    /// that any column of the table owns when `column` is None.
    ///
    /// Which column owns a sequence has to be stored in a table of its own,
    /// so only a dialect that keeps one has owned sequences. SQLite has none.
    fn owned_sequences(
        &self,
        schema: &Schema,
        namespace: NamespaceId,
        table: &str,
        column: Option<&str>,
    ) -> Vec<String> {
        let _ = (schema, namespace, table, column);
        Vec::new()
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

    /// Whether INSERT generates a rowid for an `INTEGER PRIMARY KEY` that is
    /// NULL or not given.
    ///
    /// SQLite does. PostgreSQL gives a key that is not given the column
    /// DEFAULT, and rejects a NULL key.
    fn generates_integer_primary_keys(&self) -> bool {
        true
    }
}

/// Returns true if `sql` parses as a `CREATE VIRTUAL TABLE` statement.
///
/// Like SQLite (whose `sqlite3InitCallback` feeds schema SQL to the real
/// parser, so a row is a virtual table purely as a byproduct of the
/// `create_vtab` grammar rule), classification is done by parsing rather than
/// by substring or token matching, which would misclassify regular tables
/// whose SQL merely contains the text (e.g. in a DEFAULT literal).
pub fn sql_is_create_virtual_table(sql: &str) -> bool {
    matches!(
        engine::parse_statement(sql),
        Ok(Some(ast::Cmd::Stmt(ast::Stmt::CreateVirtualTable(_))))
    )
}

/// The comment that marks the columns at the given positions as identity
/// columns of the given kinds in stored table SQL, or nothing when there are none.
pub fn identity_table_sql_marker(
    columns: impl IntoIterator<Item = (usize, IdentityKind)>,
) -> String {
    let columns: Vec<String> = columns
        .into_iter()
        .map(|(position, kind)| {
            let kind = match kind {
                IdentityKind::Always => "always",
                IdentityKind::ByDefault => "default",
            };
            format!("{position}:{kind}")
        })
        .collect();
    if columns.is_empty() {
        return String::new();
    }
    format!("{IDENTITY_TABLE_SQL_MARKER} {} */ ", columns.join(" "))
}

/// Splits the identity comment off the start of `sql`, returning the column
/// positions and kinds it lists and the rest of the text.
pub fn split_identity_table_sql_marker(
    sql: &str,
) -> crate::Result<(Vec<(usize, IdentityKind)>, &str)> {
    let Some(marked) = sql.strip_prefix(IDENTITY_TABLE_SQL_MARKER) else {
        return Ok((Vec::new(), sql));
    };
    let corrupt =
        || crate::LimboError::Corrupt(format!("malformed identity marker in table SQL: {sql}"));
    let (list, rest) = marked.split_once(" */ ").ok_or_else(corrupt)?;
    let columns = list
        .split_whitespace()
        .map(|entry| {
            let (position, kind) = entry.split_once(':').ok_or_else(corrupt)?;
            let kind = match kind {
                "always" => IdentityKind::Always,
                "default" => IdentityKind::ByDefault,
                _ => return Err(corrupt()),
            };
            Ok((position.parse::<usize>().map_err(|_| corrupt())?, kind))
        })
        .collect::<crate::Result<Vec<_>>>()?;
    if columns.is_empty() {
        return Err(corrupt());
    }
    Ok((columns, rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_marker_keeps_each_columns_position_and_kind() {
        let marker =
            identity_table_sql_marker([(0, IdentityKind::Always), (2, IdentityKind::ByDefault)]);
        assert_eq!(marker, "/* turso_identity 0:always 2:default */ ");

        let sql = format!("{marker}CREATE TABLE t (a, b, c)");
        let (columns, rest) = split_identity_table_sql_marker(&sql).unwrap();
        assert_eq!(
            columns,
            [(0, IdentityKind::Always), (2, IdentityKind::ByDefault)]
        );
        assert_eq!(rest, "CREATE TABLE t (a, b, c)");
    }

    #[test]
    fn identity_marker_is_absent_without_identity_columns() {
        assert_eq!(identity_table_sql_marker([]), "");
        let (columns, rest) = split_identity_table_sql_marker("CREATE TABLE t (a)").unwrap();
        assert!(columns.is_empty());
        assert_eq!(rest, "CREATE TABLE t (a)");
    }

    #[test]
    fn malformed_identity_marker_is_corruption() {
        for sql in [
            "/* turso_identity */ CREATE TABLE t (a)",
            "/* turso_identity 0 */ CREATE TABLE t (a)",
            "/* turso_identity 0:sometimes */ CREATE TABLE t (a)",
            "/* turso_identity x:always */ CREATE TABLE t (a)",
            "/* turso_identity 0:always CREATE TABLE t (a)",
        ] {
            assert!(split_identity_table_sql_marker(sql).is_err(), "{sql}");
        }
    }
}
