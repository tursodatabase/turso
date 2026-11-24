//! SQL dialects.
//!
//! The [`Dialect`] trait is the boundary between the engine and the SQL
//! dialect a frontend speaks. The engine owns the mechanics — pages,
//! B-trees, the `sqlite_schema` table itself, bytecode — and consults the
//! dialect wherever the meaning of SQL text is dialect-specific: parsing
//! statements into the engine AST and interpreting persisted schema text.
//! The frontends live in their own crates: the SQLite dialect in
//! `turso_sqlite` and the PostgreSQL dialect in `turso_pg`. Both compose
//! with what ships with every Turso build: the built-in functions
//! ([`resolve_builtin_function`], [`call_builtin_scalar`]) and the catalog
//! tables (`pragma_*`, `json_each`/`json_tree`, `sqlite_dbpage`,
//! `btree_dump`, `sqlite_turso_types`) that
//! [`crate::schema::builtin::schema_builder`] installs.
//!
//! The SQL text that core keeps, such as the definitions in
//! `sqlite_schema`, is SQLite syntax in every database. Core parses it with
//! [`Dialect::parse_internal`] of the database's dialect.

#[cfg(test)]
pub(crate) mod cannot_parse;
pub mod catalog;
pub(crate) mod seeded;

pub use crate::functions::builtin::{
    call_builtin_scalar, resolve_builtin_function, SQLITE_VERSION, SQLITE_VERSION_NUMBER,
};
pub use crate::translate::schema::SqliteCatalog;
pub use catalog::{
    AccessStatement, AddColumn, AlterColumn, AutomaticIndex, Catalog, CatalogApply, CatalogCmd,
    CmdContext, CmdOrigin, CreateIndex, CreateMaterializedView, CreateSequence, CreateTable,
    CreateTrigger, CreateView, CreateVirtualTable, Dependent, DependentObject, DropColumn,
    DropIndex, DropSequence, DropTable, DropTrigger, DropView, NewOwner, OwnerColumn, RelationName,
    RenameColumn, RenameTable, RewrittenTrigger, RewrittenView, Seed, SeededTable, TableText,
    TempTriggerOfRenamedTable,
};

use crate::ast::{self, Cmd, CreateTableBody, IdentityKind, Privilege, QualifiedName, Stmt};
use crate::{
    function::ScalarFunc,
    namespace::NamespaceId,
    schema::{BTreeTable, Schema, SchemaBuilder},
    security::{policy::RowPolicy, roles::RoleId},
    vdbe::Register,
    Clock, CollationSeq, Connection, Func, LimboError, OpenOptions, Result, Value,
    WallClockInstant, IO,
};
use std::{cell::Cell, ops::Range, sync::Arc};

/// Prefix the PostgreSQL frontend puts before the `CREATE TABLE` text it
/// stores in `sqlite_schema`.
pub const POSTGRES_TABLE_SQL_PREFIX: &str = "/* turso_frontend:postgres */ ";

/// What a scalar function may read besides its arguments.
pub struct ScalarFuncContext<'a> {
    pub connection: &'a Arc<Connection>,
    pub io: &'a dyn IO,
    /// The statement's clock: every function of one step sees the same time.
    pub clock: &'a dyn Clock,
    /// Set by the first call of `statement_times` in the statement.
    pub statement_times: &'a Cell<Option<StatementTimes>>,
    /// The collation of the call site, for functions that compare arguments.
    pub collation: CollationSeq,
}

impl ScalarFuncContext<'_> {
    /// The same times for every call until the statement ends, triggers
    /// included.
    pub fn statement_times(&self) -> StatementTimes {
        if let Some(times) = self.statement_times.get() {
            return times;
        }
        let times = StatementTimes::new(self.io.current_time_wall_clock(), self.connection);
        self.statement_times.set(Some(times));
        times
    }

    /// The wall clock now. The statement start is read first, so no time
    /// read during a statement is earlier than its `statement_times`.
    pub fn current_time(&self) -> WallClockInstant {
        self.statement_times();
        self.io.current_time_wall_clock()
    }
}

/// When a statement and its transaction started, as PostgreSQL's
/// `statement_timestamp()` and `now()` report them. A client command with
/// several statements, or an extended-protocol batch that ends at Sync, is
/// one transaction in autocommit mode, so its statements share these times.
#[derive(Debug, Clone, Copy)]
pub struct StatementTimes {
    pub statement: WallClockInstant,
    pub transaction: WallClockInstant,
}

impl StatementTimes {
    /// Times for a statement that starts at `now`. In autocommit mode the
    /// statement is its own transaction, unless a command set the times.
    pub(crate) fn new(now: WallClockInstant, connection: &Connection) -> Self {
        let command = connection.command_times();
        let statement = command.map_or(now, |times| times.statement);
        let transaction = if connection.get_auto_commit() {
            command.map_or(now, |times| times.transaction)
        } else {
            connection.explicit_transaction_start()
        };
        Self {
            statement,
            transaction,
        }
    }
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
    fn parse(&self, sql: &str) -> Result<(Option<Cmd>, usize)>;

    /// Parse the first statement in SQL that core keeps: the stored
    /// definitions of tables, indexes, views, triggers and virtual tables,
    /// the statements that `ALTER TABLE` generates, the built-in
    /// `CREATE TYPE` text, the patterns of index methods, and the schema
    /// writes of the engine. The text is SQLite syntax in every dialect.
    ///
    /// Returns the parsed command, if any, and the number of input bytes
    /// consumed.
    fn parse_internal(&self, sql: &str) -> Result<(Option<Cmd>, usize)>;

    /// Parse the column definition that core keeps for `ALTER TABLE`, such
    /// as `b INTEGER DEFAULT 0`. The text is SQLite syntax in every dialect.
    fn parse_internal_column_definition(&self, sql: &str) -> Result<ast::ColumnDefinition>;

    /// The byte ranges of the parameter markers in a statement that
    /// [`Dialect::parse`] accepted, in the order they appear. Markers inside
    /// string literals, quoted identifiers and comments are not parameters.
    fn parameter_marker_ranges(&self, sql: &str) -> Result<Vec<Range<usize>>>;

    /// The dialect for the statements that core and the frontend run for
    /// themselves. Those statements are SQLite syntax and expect SQLite
    /// rules in every database.
    fn get_internal_dialect(&self) -> Arc<dyn Dialect>;

    /// Parse a `sqlite_schema` `type='table'` row's SQL into a table
    /// definition.
    ///
    /// Rows written by internal engine paths (sequence backing tables,
    /// `sqlite_sequence`) are plain SQLite text and carry no frontend
    /// marker, so every implementation must fall back to SQLite parsing
    /// for text it does not recognize as its own.
    fn parse_table_sql(&self, sql: &str, root_page: i64) -> Result<BTreeTable> {
        reject_postgres_table_sql(sql)?;
        BTreeTable::from_stored_sql(self, sql, root_page, false)
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
        reject_postgres_table_sql(sql)?;
        parse_table_sql_ast(self, sql)
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
        reject_postgres_table_sql(sql)?;
        table_sql_for_replay(self, sql)
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
        format_table_sql(tbl_name, body)
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
    /// by composing with [`resolve_builtin_function`]) and onto
    /// [`Func::Dialect`] for functions it executes
    /// itself via [`Dialect::exec_scalar_function`]. Consulted
    /// before extension functions; engine-generated helper statements
    /// always resolve with SQLite semantics instead.
    fn resolve_function(&self, name: &str, arg_count: usize) -> Result<Option<Func>> {
        resolve_builtin_function(name, arg_count)
    }

    /// Run built-in scalar function `func` on the registers that hold its
    /// arguments.
    ///
    /// The default runs the shared implementation in
    /// [`call_builtin_scalar`]; a dialect overrides it for the
    /// functions whose behavior differs and delegates the rest.
    fn call_scalar(
        &self,
        context: &ScalarFuncContext<'_>,
        func: &ScalarFunc,
        args: &[Register],
    ) -> Result<Value> {
        call_builtin_scalar(context, func, args)
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

    /// Whether the arguments of a table function can only name columns of the
    /// tables listed before it in FROM.
    ///
    /// SQLite looks in every table of the query, so `json_each(value)` reads
    /// `json_each.value` and `json_each(t.x), t` is accepted. PostgreSQL only
    /// lets a function in FROM read the tables to its left.
    fn table_function_arguments_read_only_earlier_tables(&self) -> bool {
        false
    }

    /// Whether a sequence named without a schema in an expression stored in
    /// the schema is bound to the schema it is found in when the expression
    /// is created.
    fn binds_names_at_create(&self) -> bool {
        false
    }

    /// The name that a message gives the relation `name` of `namespace`.
    fn relation_name_in_messages(
        &self,
        connection: &Connection,
        schema: &Schema,
        namespace: NamespaceId,
        name: &str,
    ) -> String {
        let _ = (connection, schema, namespace);
        name.to_string()
    }

    /// The objects that depend on the table or view `name` of `namespace`,
    /// directly or through other objects, which DROP without CASCADE refuses
    /// to leave without it. Each object comes before the objects that it
    /// depends on, so they can be dropped in this order.
    ///
    /// SQLite records no dependencies: it drops the relation, and a view
    /// that reads it fails when it is read.
    fn relation_dependents(
        &self,
        schema: &Schema,
        namespace: NamespaceId,
        name: &str,
    ) -> Vec<Dependent> {
        let _ = (schema, namespace, name);
        Vec::new()
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

    /// Where the dialect stores the objects of a database. The default
    /// stores them as rows of `sqlite_schema`.
    fn catalog(&self) -> &dyn Catalog {
        &SqliteCatalog
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

    /// How an ORDER BY or DISTINCT ON term is matched to the output columns.
    fn sort_term_matching(&self) -> SortTermMatching {
        SortTermMatching::Sqlite
    }

    /// Whether integer division by zero raises an error.
    ///
    /// PostgreSQL raises SQLSTATE 22012. SQLite returns NULL.
    fn errors_on_zero_divisor(&self) -> bool {
        false
    }

    /// FIXME(pg-catalog): This hand-codes prorettype of the pg_proc entries mod(smallint, smallint), mod(integer, integer) and mod(bigint, bigint), which return their argument type. A lookup in a pg_proc table replaces this.
    /// Whether `mod` of two integers returns an integer.
    ///
    /// PostgreSQL returns an integer. SQLite always returns a REAL.
    fn mod_of_integers_is_integer(&self) -> bool {
        false
    }

    /// Whether `sum()` and `avg()` count their infinite inputs apart from
    /// the total of the finite ones.
    ///
    /// PostgreSQL does, so a moving window frame gives a finite result again
    /// once its infinite values leave it. SQLite adds them to the total.
    /// Removing a row from a window frame adds its negation, so once an
    /// infinite value leaves, the total is `Inf - Inf`, which is NaN, and
    /// the result stays NULL.
    fn sum_counts_infinite_inputs(&self) -> bool {
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

    /// FIXME(pg-catalog): This hand-codes the absence of pg_cast entries from date, timestamp, json and inet to numbers, booleans and bytea. A lookup in a pg_cast table replaces this.
    /// Whether a CAST from a custom type that reads text and has an ENCODE
    /// (such as date, timestamp, json or inet) is refused when its target does
    /// not read text.
    ///
    /// PostgreSQL has no cast from these types to numbers, booleans or bytea:
    /// `date '2020-01-01'::int` fails. SQLite reads the text as a number, so
    /// it gives 2020, which is a wrong answer. A number stored into or cast
    /// to one of these types is read as its text, so `20200101::date` gives
    /// 2020-01-01 where PostgreSQL refuses it: tursopg binds a parameter sent
    /// without a type as a number when its text is one, and refusing numbers
    /// would refuse such parameters too.
    fn text_input_types_refuse_other_kinds(&self) -> bool {
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

    /// Checks the type of a result column of CREATE TABLE AS or SELECT INTO
    /// before the table is created. `type_name` is the custom type of the
    /// column, or `None` when its type is not known at that point.
    ///
    /// The new table types its columns with SQLite declared types, so a column
    /// of a custom type would become a plain column that compares and accepts
    /// values differently. SQLite has no custom types and accepts every
    /// column. PostgreSQL refuses the statement, which PostgreSQL itself
    /// executes with the type kept.
    fn check_ctas_column_type(&self, column: &str, type_name: Option<&str>) -> Result<()> {
        let _ = (column, type_name);
        Ok(())
    }

    /// Appends a blob element of an array to the array's text, as in
    /// `{"X'6162'"}`.
    ///
    /// PostgreSQL writes a bytea element as quoted hex: `{"\\x6162"}`.
    fn write_array_blob_element(&self, out: &mut String, blob: &[u8]) {
        out.push_str("\"X'");
        crate::vdbe::value::push_hex_upper(out, blob);
        out.push_str("'\"");
    }

    /// The name of the function that runs `a @> b` when neither operand is
    /// an array, or None when the array function runs it too.
    ///
    /// PostgreSQL's `@>` also compares jsonb values: the frontend gives a
    /// function that it registers. SQLite has no `@>`, and Turso's `@>`
    /// compares arrays.
    ///
    /// FIXME(pg-catalog): This hand-codes the pg_operator entry `jsonb @> jsonb`, whose function is jsonb_contains, beside the entry `anyarray @> anyarray`, whose function is arraycontains. A lookup in pg_operator by the types of the operands replaces this.
    fn json_contains_function(&self) -> Option<&'static str> {
        None
    }
}

/// How an ORDER BY or DISTINCT ON term is matched to the output columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortTermMatching {
    /// A name anywhere in the term can be an output column name, and the first output
    /// column with that name is used.
    Sqlite,
    /// Only a term that is a bare name can be an output column name, and it is an error
    /// when output columns with different expressions share that name. Every ORDER BY
    /// term of a SELECT DISTINCT must be an output column.
    Sql92,
}

/// Refuses table SQL stored by the PostgreSQL frontend. The SQL comment that
/// marks it is valid SQLite syntax, so without this check the SQLite parser
/// would accept the PostgreSQL DDL with SQLite column semantics.
pub fn reject_postgres_table_sql(sql: &str) -> Result<()> {
    if sql.starts_with(POSTGRES_TABLE_SQL_PREFIX) {
        return Err(LimboError::Corrupt(format!(
            "malformed database schema: table was created by the postgres frontend: {sql}"
        )));
    }
    Ok(())
}

/// Formats canonical SQLite `CREATE TABLE` text from its AST.
pub fn format_table_sql(tbl_name: &QualifiedName, body: &CreateTableBody) -> Result<String> {
    match body {
        CreateTableBody::ColumnsAndConstraints { .. } => Ok(format!(
            "CREATE TABLE {} {}",
            tbl_name.name.as_ident(),
            body
        )),
        CreateTableBody::AsSelect(_) => {
            crate::bail_parse_error!("CREATE TABLE AS SELECT is not supported")
        }
    }
}

/// Recover persisted SQLite table SQL for schema replay.
///
/// SQLite text without a database qualifier can be replayed unchanged. A
/// qualified name must be removed because the vacuum target only exposes its
/// main schema.
pub fn table_sql_for_replay(dialect: &(impl Dialect + ?Sized), sql: &str) -> Result<String> {
    let stmt = parse_table_sql_ast(dialect, sql)?;
    let Stmt::CreateTable {
        mut tbl_name,
        temporary,
        if_not_exists,
        body,
    } = stmt
    else {
        unreachable!("parse_table_sql_ast returned a non-CREATE TABLE statement");
    };

    if tbl_name.namespace.take().is_none() {
        return Ok(sql.to_string());
    }

    Ok(Stmt::CreateTable {
        tbl_name,
        temporary,
        if_not_exists,
        body,
    }
    .to_string())
}

/// Parse persisted SQLite table SQL into a `CREATE TABLE` statement.
pub fn parse_table_sql_ast(dialect: &(impl Dialect + ?Sized), sql: &str) -> Result<Stmt> {
    match parse_statement(dialect, sql)? {
        Some(Cmd::Stmt(stmt @ Stmt::CreateTable { .. })) => Ok(stmt),
        other => Err(LimboError::Corrupt(format!(
            "persisted table SQL is not CREATE TABLE: {other:?}"
        ))),
    }
}

/// Returns true if `sql` parses as a `CREATE VIRTUAL TABLE` statement.
///
/// Like SQLite (whose `sqlite3InitCallback` feeds schema SQL to the real
/// parser, so a row is a virtual table purely as a byproduct of the
/// `create_vtab` grammar rule), classification is done by parsing rather than
/// by substring or token matching, which would misclassify regular tables
/// whose SQL merely contains the text (e.g. in a DEFAULT literal).
pub fn sql_is_create_virtual_table(dialect: &dyn Dialect, sql: &str) -> bool {
    matches!(
        parse_statement(dialect, sql),
        Ok(Some(ast::Cmd::Stmt(ast::Stmt::CreateVirtualTable(_))))
    )
}

/// Parse the first statement in SQL that core keeps, which is SQLite syntax.
pub fn parse_statement(dialect: &(impl Dialect + ?Sized), sql: &str) -> Result<Option<Cmd>> {
    Ok(dialect.parse_internal(sql)?.0)
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
