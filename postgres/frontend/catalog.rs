use crate::functions::validate_pg_input;
use crate::pg_type::{built_in_type, type_modifier_input, BuiltInType, BUILT_IN_TYPES};
use crate::roles::{connection_roles, schema_roles, Role, RoleCatalog};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::borrow::Cow;
use std::fmt::{self, Debug, Display, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use turso_core::ast::{self, RefAct, SortOrder};
use turso_core::schema::RelationKind;
use turso_core::{
    dialect::{
        call_builtin_scalar, identity_table_sql_marker, split_identity_table_sql_marker,
        ScalarFuncContext, SortTermMatching, POSTGRES_TABLE_SQL_PREFIX,
    },
    namespace::mangle_name,
    native_ext::{VirtualTable, VirtualTableCursor, VirtualTableModule},
    schema::{
        generated_expr_with_names, BTreeTable, GeneratedType, Schema, Sequence, Table, UniqueSet,
        AUTOINCREMENT_SEQ_PREFIX,
    },
    security::policy::RowPolicy,
    security::roles::RoleId,
    text_array_literal, walk_expr_mut, Connection, Dialect, Func, IOResult, LimboError,
    NamespaceId, NamespaceRef, OpenOptions, Register, Result, ScalarFunc, Statement, Value,
    WalkControl, MAIN_DB_ID,
};
use turso_core::{Dependent, DependentObject};
use turso_ext::{ConstraintInfo, IndexInfo, OrderByInfo, ResultCode, VTabKind};
use turso_pg_catalog::{CatalogTable, PgDescription};
use turso_pg_parser::quote_identifier;
use turso_pg_parser::sqlstate::{
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INTERNAL_ERROR, SQLSTATE_UNDEFINED_TABLE,
};

use crate::error::{sql_parse_error, sqlstate_error};

use turso_pg_parser::translator::BOOTSTRAP_SUPERUSER_NAME;
pub use turso_pg_parser::translator::{is_catalog_table_name, is_catalog_view_name};
use turso_pg_parser::translator::{PG_CLASS_OID, PG_NAMESPACE_OID};

/// PostgreSQL uses OID zero when no object is referenced.
const PG_INVALID_OID: i64 = 0;
pub(crate) const PG_DATABASE_OWNER_OID: i64 = 6171;
/// The owner of the public schema, which PostgreSQL makes for every database.
/// It is listed in pg_roles but is not a role of the engine.
pub(crate) const PG_DATABASE_OWNER_NAME: &str = "pg_database_owner";
/// pg_depend uses column zero for a whole object; table columns are numbered from one.
const WHOLE_OBJECT_COLUMN_NUMBER: i64 = 0;
/// Catalog BOOLEAN values are stored as integers; one means true.
const CATALOG_BOOLEAN_TRUE: i64 = 1;

/// The name prefix of a table's automatic indexes.
const PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX: &str = "sqlite_autoindex_";

/// The first synthetic constraint oid. Constraints are not relations and
/// have no stored oid, so theirs are positional; they start far above the
/// catalog's relation oids, so they never meet one.
const CONSTRAINT_OID_START: i64 = 1 << 30;

#[derive(Debug)]
pub struct PostgresDialect;

impl Dialect for PostgresDialect {
    fn name(&self) -> &'static str {
        "postgres"
    }

    fn format_error(&self, error: LimboError) -> LimboError {
        error
    }

    fn compares_timestamp_with_timestamptz(&self) -> bool {
        true
    }

    fn parse(&self, sql: &str) -> Result<(Option<turso_core::ast::Cmd>, usize)> {
        // Engine-generated helper statements and pragmas are canonical SQLite
        // text that pg_query rejects, so anything the PostgreSQL parser cannot
        // handle falls back to SQLite parsing.
        let Ok(parse_result) = turso_pg_parser::parse(sql) else {
            return self.parse_internal(sql);
        };
        let stmts = &parse_result.protobuf.stmts;
        if stmts.is_empty() {
            return Ok((None, sql.len()));
        }
        // The translator consumes the first statement only; report how many
        // input bytes it covers so multi-statement iteration can resume after
        // it. pg_query records where the next statement starts, which also
        // accounts for the semicolon and any trailing whitespace in between.
        let consumed = match stmts.get(1) {
            Some(next) => next.stmt_location as usize,
            None => sql.len(),
        };
        let translator = turso_pg_parser::translator::PostgreSQLTranslator::new();
        match translator.translate_with_prereqs(&parse_result) {
            Ok(translated) => Ok((Some(translated.cmd), consumed)),
            Err(_) => self.parse_internal(sql),
        }
    }

    fn parse_internal(&self, sql: &str) -> Result<(Option<turso_core::ast::Cmd>, usize)> {
        let mut parser = turso_parser::parser::Parser::new(sql.as_bytes());
        let cmd = parser.next_cmd()?;
        Ok((cmd, parser.offset()))
    }

    fn parse_internal_column_definition(
        &self,
        sql: &str,
    ) -> Result<turso_core::ast::ColumnDefinition> {
        Ok(turso_parser::parser::Parser::new(sql.as_bytes()).parse_column_definition(true)?)
    }

    fn parameter_marker_ranges(&self, sql: &str) -> Result<Vec<std::ops::Range<usize>>> {
        turso_sqlite::SqliteDialect.parameter_marker_ranges(sql)
    }

    fn validate_attach(&self) -> Result<()> {
        Err(sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            "ATTACH and DETACH are not supported by the PostgreSQL frontend",
            None,
        ))
    }

    fn get_internal_dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(turso_sqlite::SqliteDialect)
    }

    fn parse_table_sql(&self, sql: &str, root_page: i64) -> Result<BTreeTable> {
        // Schema rows written by internal SQLite paths (e.g. sqlite_sequence)
        // carry no frontend marker and are plain SQLite SQL.
        let Some(raw_sql) = decode_stored_pg_schema_sql(sql) else {
            let turso_core::ast::Stmt::CreateTable { tbl_name, body, .. } =
                canonical_table_sql_ast(self, sql)?
            else {
                unreachable!("canonical_table_sql_ast returned a non-CREATE TABLE statement");
            };
            let table = BTreeTable::from_create_table_ast(&tbl_name, &body, root_page, true)?;
            if !table.has_rowid {
                return Err(sqlstate_error(
                    SQLSTATE_FEATURE_NOT_SUPPORTED,
                    format!(
                        "PostgreSQL does not support WITHOUT ROWID table '{}'",
                        table.name
                    ),
                    None,
                ));
            }
            return Ok(table);
        };

        let parse_result = turso_pg_parser::parse_stored_table(raw_sql).map_err(sql_parse_error)?;
        let translator = turso_pg_parser::translator::PostgreSQLTranslator::new();
        let stmt = translator
            .translate(&parse_result)
            .map_err(sql_parse_error)?;
        match stmt {
            turso_core::ast::Stmt::CreateTable { tbl_name, body, .. } => {
                let table = BTreeTable::from_create_table_ast(&tbl_name, &body, root_page, true)?;
                Ok(table)
            }
            _ => Err(sqlstate_error(
                SQLSTATE_INTERNAL_ERROR,
                "expected CREATE TABLE statement",
                None,
            )),
        }
    }

    fn parse_table_sql_ast(&self, sql: &str) -> Result<turso_core::ast::Stmt> {
        // Schema rows written by internal SQLite paths (e.g. sqlite_sequence)
        // carry no frontend marker and are plain SQLite SQL.
        let Some(raw_sql) = decode_stored_pg_schema_sql(sql) else {
            return canonical_table_sql_ast(self, sql);
        };

        let parse_result = turso_pg_parser::parse_stored_table(raw_sql).map_err(sql_parse_error)?;
        let translator = turso_pg_parser::translator::PostgreSQLTranslator::new();
        let stmt = translator
            .translate(&parse_result)
            .map_err(sql_parse_error)?;
        match stmt {
            stmt @ turso_core::ast::Stmt::CreateTable { .. } => Ok(stmt),
            _ => Err(sqlstate_error(
                SQLSTATE_INTERNAL_ERROR,
                "expected CREATE TABLE statement",
                None,
            )),
        }
    }

    fn table_sql_for_replay(&self, sql: &str) -> Result<String> {
        let Some(raw_sql) = decode_stored_pg_schema_sql(sql) else {
            return turso_core::dialect::table_sql_for_replay(self, sql);
        };

        let stmt = self.parse_table_sql_ast(sql)?;
        let turso_core::ast::Stmt::CreateTable {
            mut tbl_name,
            temporary,
            if_not_exists,
            body,
        } = stmt
        else {
            unreachable!("parse_table_sql_ast returned a non-CREATE TABLE statement");
        };

        // Unqualified statements replay as the original PostgreSQL DDL. A
        // schema-qualified statement targets an attached database the replay
        // destination does not have, so re-render the translated AST without
        // the qualifier; the canonical text round-trips through the SQLite
        // fallback in `Dialect::parse`.
        if tbl_name.namespace.take().is_none() {
            return Ok(raw_sql.to_string());
        }

        Ok(turso_core::ast::Stmt::CreateTable {
            tbl_name,
            temporary,
            if_not_exists,
            body,
        }
        .to_string())
    }

    fn format_table_sql(
        &self,
        input: &str,
        tbl_name: &turso_core::ast::QualifiedName,
        _body: &turso_core::ast::CreateTableBody,
    ) -> Result<String> {
        let sql = turso_pg_parser::name_create_table(input, tbl_name.name.as_str())
            .unwrap_or_else(|_| input.to_string());
        Ok(encode_pg_schema_sql(&sql))
    }

    fn format_rewritten_table_sql(&self, stmt: &turso_core::ast::Stmt) -> Result<String> {
        let turso_core::ast::Stmt::CreateTable { tbl_name, body, .. } = stmt else {
            return Err(LimboError::InternalError(
                "format_rewritten_table_sql requires CREATE TABLE".to_string(),
            ));
        };
        let sql = turso_core::dialect::format_table_sql(tbl_name, body)?;
        let identity_marker = identity_table_sql_marker(
            table_columns(body)
                .iter()
                .enumerate()
                .filter_map(|(position, column)| Some((position, column.identity_kind()?))),
        );
        Ok(format!("{identity_marker}{sql}"))
    }

    fn schema_builder(
        &self,
        enable_custom_types: bool,
    ) -> Result<turso_core::schema::SchemaBuilder> {
        let builder = turso_core::schema::builtin::schema_builder(enable_custom_types)?;
        if !enable_custom_types {
            return Ok(builder);
        }
        crate::pg_type::register_builtin_types(builder)
    }

    fn register_native_extensions(&self, options: OpenOptions) -> OpenOptions {
        let options = crate::functions::register_functions(options);
        let options = crate::auth_functions::register_functions(options);
        register_catalog_modules(options)
    }

    fn resolve_function(&self, name: &str, arg_count: usize) -> Result<Option<Func>> {
        // random() is a native function (see functions.rs) because it returns
        // a double, not SQLite's integer. Skipping the builtin lets that one
        // be found.
        if name.eq_ignore_ascii_case("random") {
            return Ok(None);
        }
        turso_core::dialect::resolve_builtin_function(name, arg_count)
    }

    fn call_scalar(
        &self,
        context: &ScalarFuncContext<'_>,
        func: &ScalarFunc,
        args: &[Register],
    ) -> Result<Value> {
        // PostgreSQL joins every argument that is not NULL, so an empty one
        // keeps its place and its separator: 'concat_ws(",", "", "a")' is
        // ",a". The shared implementation follows SQLite instead, where the
        // separator only goes after content, so concat_ws alone needs the
        // PostgreSQL rule here.
        if matches!(func, ScalarFunc::ConcatWs) {
            return Ok(concat_ws_joining_empty_arguments(args));
        }
        call_builtin_scalar(context, func, args)
    }

    fn choose_operator(
        &self,
        op: &str,
        operands: &dyn Fn() -> Vec<turso_core::dialect::OperandType>,
    ) -> Result<turso_core::dialect::OperatorChoice> {
        crate::operators::choose_operator(op, &operands())
    }

    fn choose_list_operator(
        &self,
        op: &str,
        operands: &dyn Fn() -> Vec<turso_core::dialect::OperandType>,
    ) -> Result<Option<turso_core::dialect::OperatorChoice>> {
        crate::operators::choose_list_operator(op, &operands())
    }

    // FIXME(registered-types): PostgreSQL reads the literal as the type. Here the statement is not analyzed before this expression is compiled, so nothing read it, and it is refused. A typed expression tree that every context analyzes once reads it.
    fn check_string_literal_comparison(&self, type_name: &str) -> Result<()> {
        Err(crate::error::sqlstate_error_with_hint(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!(
                "a string literal compared with a value of type {type_name} is not supported here"
            ),
            format!("Write the literal as '...'::{type_name}."),
        ))
    }

    fn undefined_operator_error(&self, op: &str, operand_types: &[String]) -> LimboError {
        let names: Vec<&str> = operand_types.iter().map(String::as_str).collect();
        crate::functions::undefined_operator_of(op, &names)
    }

    fn apply_cast(
        &self,
        operand: &turso_core::ast::Expr,
        source: &dyn Fn() -> turso_core::dialect::CastSource,
        target: &turso_core::dialect::TypeRef,
        context: turso_core::dialect::CastContext,
    ) -> Result<turso_core::dialect::CastDecision> {
        crate::cast_planning::apply_cast(operand, source, target, context)
    }

    fn check_column_default(
        &self,
        column_name: &str,
        source: &dyn Fn() -> turso_core::dialect::CastSource,
        column_type: &turso_core::dialect::TypeRef,
    ) -> Result<()> {
        crate::cast_planning::check_column_default(column_name, source, column_type)
    }

    fn requires_custom_types(&self) -> bool {
        true
    }

    fn qualified_column_uses_nearest_table_only(&self) -> bool {
        true
    }

    fn table_function_arguments_read_only_earlier_tables(&self) -> bool {
        true
    }

    fn binds_names_at_create(&self) -> bool {
        true
    }

    fn relation_name_in_messages(
        &self,
        connection: &Connection,
        schema: &Schema,
        namespace: NamespaceId,
        name: &str,
    ) -> String {
        relation_name(connection, schema, namespace, name)
    }

    fn relation_dependents(
        &self,
        schema: &Schema,
        namespace: NamespaceId,
        name: &str,
    ) -> Vec<Dependent> {
        relation_dependents(schema, namespace, name)
    }

    fn supports_namespaces(&self) -> bool {
        true
    }

    fn relation_names_unique_across_kinds(&self) -> bool {
        true
    }

    fn supports_capture_data_changes(&self) -> bool {
        false
    }

    fn catalog(&self) -> &dyn turso_core::Catalog {
        &crate::classes::PostgresCatalog
    }

    /// The built-in superuser is found by its name, `postgres`, too.
    fn role_by_name(&self, schema: &Schema, name: &str) -> Option<RoleId> {
        engine_role_by_name(schema_roles(schema), name)
    }

    fn role_is_superuser(&self, schema: &Schema, role: RoleId) -> bool {
        schema_roles(schema).is_superuser(role)
    }

    fn role_bypass_rls(&self, schema: &Schema, role: RoleId) -> bool {
        schema_roles(schema).bypass_rls(role)
    }

    fn can_set_role(&self, schema: &Schema, member: RoleId, role: RoleId) -> bool {
        schema_roles(schema).can_set_role(member, role)
    }

    fn role_has_privileges_of(&self, schema: &Schema, member: RoleId, role: RoleId) -> bool {
        schema_roles(schema).has_privileges_of(member, role)
    }

    fn relation_owner(
        &self,
        schema: &Schema,
        namespace: NamespaceId,
        name: &str,
    ) -> Option<RoleId> {
        crate::classes::relation_owner(schema, namespace, name)
    }

    fn owned_sequences(
        &self,
        schema: &Schema,
        namespace: NamespaceId,
        table: &str,
        column: Option<&str>,
    ) -> Vec<String> {
        crate::classes::owned_sequences(schema, namespace, table, column)
    }

    fn role_has_privilege(
        &self,
        schema: &Schema,
        namespace: NamespaceId,
        name: &str,
        role: RoleId,
        privilege: ast::Privilege,
    ) -> bool {
        crate::classes::role_has_privilege(schema, namespace, name, role, privilege)
    }

    fn table_rls_enabled(&self, schema: &Schema, namespace: NamespaceId, name: &str) -> bool {
        crate::classes::table_rls_enabled(schema, namespace, name)
    }

    fn row_security_policies<'a>(
        &self,
        schema: &'a Schema,
        namespace: NamespaceId,
        name: &str,
    ) -> &'a [turso_core::security::policy::RowPolicy] {
        crate::classes::row_security_policies(schema, namespace, name)
    }

    fn default_namespaces(&self) -> &'static [&'static str] {
        &["public"]
    }

    fn upsert_rejects_row_written_by_same_statement(&self) -> bool {
        true
    }

    fn rejects_scalar_subquery_with_several_rows(&self) -> bool {
        true
    }

    fn errors_on_zero_divisor(&self) -> bool {
        true
    }

    fn write_array_blob_element(&self, out: &mut String, blob: &[u8]) {
        out.push_str(&crate::text::encode_bytea(blob, true));
    }

    fn sort_term_matching(&self) -> SortTermMatching {
        SortTermMatching::Sql92
    }

    fn constraint_errors_show_values(&self) -> bool {
        true
    }

    fn generates_integer_primary_keys(&self) -> bool {
        false
    }

    /// FIXME(registered-types): Core types the columns of the new table with the storage types of the engine. A column of a type that converts values is refused here. A copy of a smallint, integer, bigint or varchar(n) column is accepted, because its values stay what they are, but it is a plain column: it loses the range or length check, and a copied key is not described as its type. A CREATE TABLE AS that gives each column the registered type of its expression replaces this.
    fn check_ctas_column_type(&self, column: &str, type_name: Option<&str>) -> Result<()> {
        let sqlstate = turso_pg_parser::sqlstate::SQLSTATE_FEATURE_NOT_SUPPORTED;
        Err(match type_name {
            Some(type_name) => crate::error::sqlstate_error_with_hint(
                sqlstate,
                format!(
                    "CREATE TABLE AS with a column of type {type_name} is not supported (column \"{column}\")"
                ),
                "Create the table with CREATE TABLE, then fill it with INSERT ... SELECT.",
            ),
            None => crate::error::sqlstate_error(
                sqlstate,
                format!(
                    "CREATE TABLE AS with a column whose type cannot be determined is not supported (column \"{column}\")"
                ),
                None,
            ),
        })
    }

    fn mod_of_integers_is_integer(&self) -> bool {
        true
    }

    fn sum_counts_infinite_inputs(&self) -> bool {
        true
    }

    fn add_column_computes_default_for_existing_rows(&self) -> bool {
        true
    }

    fn converts_values_before_not_null(&self) -> bool {
        true
    }

    fn allows_qualified_columns_in_index_terms(&self) -> bool {
        true
    }

    fn text_input_types_refuse_other_kinds(&self) -> bool {
        true
    }

    fn json_contains_function(&self) -> Option<&'static str> {
        Some("jsonb_contains")
    }
}

/// `concat_ws(separator, ...)` with PostgreSQL's rule: every argument that is
/// not NULL takes part in the join, an empty one included, so an empty leading
/// argument still gets a separator before the argument after it.
fn concat_ws_joining_empty_arguments(args: &[Register]) -> Value {
    let Some((separator, arguments)) = args.split_first() else {
        return Value::Null;
    };
    let Some(separator) = separator.get_value().cast_text_ref() else {
        return Value::Null;
    };
    let parts: Vec<Cow<'_, str>> = arguments
        .iter()
        .filter_map(|argument| argument.get_value().cast_text_ref())
        .collect();
    Value::build_text(parts.join(&separator))
}

pub fn encode_pg_schema_sql(sql: &str) -> String {
    format!("{POSTGRES_TABLE_SQL_PREFIX}{sql}")
}

pub fn decode_stored_pg_schema_sql(sql: &str) -> Option<&str> {
    sql.strip_prefix(POSTGRES_TABLE_SQL_PREFIX)
}

/// The `CREATE TABLE` of stored table SQL that is SQLite text, with its
/// identity columns marked as the SQL says.
fn canonical_table_sql_ast(dialect: &PostgresDialect, sql: &str) -> Result<ast::Stmt> {
    let (identity_columns, sql) = split_identity_table_sql_marker(sql)?;
    let mut stmt = turso_core::dialect::parse_table_sql_ast(dialect, sql)?;
    let ast::Stmt::CreateTable { body, .. } = &mut stmt else {
        unreachable!("parse_table_sql_ast returned a non-CREATE TABLE statement");
    };
    let ast::CreateTableBody::ColumnsAndConstraints { columns, .. } = body else {
        return Err(LimboError::Corrupt(format!(
            "stored table SQL has no column definitions: {sql}"
        )));
    };
    for (position, kind) in identity_columns {
        let column = columns.get_mut(position).ok_or_else(|| {
            LimboError::Corrupt(format!(
                "identity column {position} is not in table SQL: {sql}"
            ))
        })?;
        column.constraints.push(ast::NamedColumnConstraint {
            name: None,
            constraint: ast::ColumnConstraint::Identity(kind),
        });
    }
    Ok(stmt)
}

fn table_columns(body: &ast::CreateTableBody) -> &[ast::ColumnDefinition] {
    match body {
        ast::CreateTableBody::ColumnsAndConstraints { columns, .. } => columns,
        ast::CreateTableBody::AsSelect(_) => &[],
    }
}

/// Returns the user tables of every namespace as (namespace, name, table)
/// in deterministic order: the root namespace first, then the stored
/// namespaces by id, each sorted by name. Every catalog that assigns OIDs
/// to tables must use this function so the OIDs agree.
fn user_tables_sorted(schema: &Schema) -> Vec<(NamespaceId, &str, &Arc<Table>)> {
    let mut tables = Vec::new();
    for namespace in namespaces_sorted(schema) {
        let mut namespace_tables: Vec<_> = schema
            .tables(namespace)
            .map(|table| (namespace, table.get_name(), table))
            .filter(|(_, name, table)| {
                turso_core::schema::is_user_relation_name(RelationKind::Table, name)
                    && matches!(table.as_ref(), Table::BTree(_))
            })
            .collect();
        namespace_tables.sort_by_key(|(_, name, _)| *name);
        tables.extend(namespace_tables);
    }
    tables
}

/// The schemas of `schema`: the namespaces by id. The root
/// namespace holds the engine's own tables and is not a schema.
fn namespaces_sorted(schema: &Schema) -> Vec<NamespaceId> {
    let mut namespaces: Vec<_> = schema
        .namespace_ids()
        .filter(|namespace| !namespace.is_root())
        .collect();
    namespaces.sort();
    namespaces
}

/// The PostgreSQL schema name of `namespace`.
fn schema_name_of(schema: &Schema, namespace: NamespaceId) -> &str {
    schema.namespace_name(namespace).unwrap_or("")
}

/// OID of the public schema, which initdb gives it.
pub(crate) const PUBLIC_NAMESPACE_OID: i64 = 2200;

/// What is added to the id of a namespace to get the OID of its
/// schema.
const FIRST_USER_SCHEMA_OID: i64 = 16384;

pub(crate) fn namespace_oid(schema: &Schema, namespace: NamespaceId) -> i64 {
    if namespace.is_root() {
        return 11;
    }
    if schema.namespace_name(namespace) == Some("public") {
        return PUBLIC_NAMESPACE_OID;
    }
    FIRST_USER_SCHEMA_OID + i64::from(namespace.as_u32())
}

/// Whether `name` is a table that the engine or this frontend owns, rather
/// than the user.
pub(crate) fn is_system_table_name(name: &str) -> bool {
    ["sqlite_", "pg_", "pragma_", "json_", "__turso_internal_"]
        .iter()
        .any(|prefix| name.starts_with(prefix))
}

/// Map a SQLite type string to a PostgreSQL type OID.
/// Strips parenthesized parameters (e.g. `varchar(100)` -> `VARCHAR`) before matching.
fn sqlite_type_to_pg_oid(ty_str: &str) -> i64 {
    pg_type_oid(ty_str).unwrap_or(25) // default to text
}

/// FIXME(pg-catalog): This maps type names to pg_type oids by hand, and a type it does not list reads as text elsewhere. It maps CHAR to varchar (1043), where PostgreSQL's char is bpchar (1042). A lookup in a pg_type table replaces this.
/// The oid of the PostgreSQL type of a column declared as `ty_str`, or None
/// for a type name tursopg does not know.
pub(crate) fn pg_type_oid(ty_str: &str) -> Option<i64> {
    let base = match ty_str.find('(') {
        Some(pos) => &ty_str[..pos],
        None => ty_str,
    };
    let oid = match base.to_uppercase().as_str() {
        "INTEGER" | "INT" | "INT4" => 23,
        "SMALLINT" | "INT2" => 21,
        "BIGINT" | "INT8" => 20,
        "TINYINT" | "MEDIUMINT" => 23,
        "TEXT" => 25,
        "VARCHAR" | "CHAR" | "CLOB" | "NCHAR" | "NVARCHAR" | "CHARACTER VARYING" => 1043,
        "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT" | "FLOAT8" => 701,
        "FLOAT4" => 700,
        "BLOB" | "BYTEA" => 17,
        "NUMERIC" | "DECIMAL" => 1700,
        "BOOLEAN" | "BOOL" => 16,
        "UUID" => 2950,
        "JSON" => 114,
        "JSONB" => 3802,
        "DATE" => 1082,
        "TIME" => 1083,
        "TIMESTAMP" => 1114,
        "TIMESTAMPTZ" => 1184,
        "INET" => 869,
        "CIDR" => 650,
        "MACADDR" => 829,
        "OID" => 26,
        "REGCLASS" => 2205,
        "REGTYPE" => 2206,
        _ => return None,
    };
    Some(oid)
}

/// The oid of relation `name` of `namespace`, from its stored class row.
/// Every relation has one: the frontend refuses a database whose relations
/// predate the catalog, so None means the schema and the catalog are out of
/// step, and the caller serves 0 rather than inventing an oid.
fn stored_relation_oid(schema: &Schema, namespace: NamespaceId, name: &str) -> Option<i64> {
    Some(
        crate::classes::class_catalog(schema)?
            .get_by_name(namespace, name)?
            .oid,
    )
}

/// The name of the role that owns relation `name`, for the owner columns of
/// the information views, such as `pg_tables.tableowner`.
fn relation_owner_name(
    conn: &Connection,
    schema: &Schema,
    namespace: NamespaceId,
    name: &str,
) -> String {
    let owner =
        crate::classes::relation_owner(schema, namespace, name).unwrap_or(RoleId::SUPERUSER);
    postgres_roles(&connection_roles(conn))
        .find(|role| role.id == owner)
        .map(|role| role.name)
        .unwrap_or_else(|| BOOTSTRAP_SUPERUSER_NAME.to_string())
}

/// The user tables with their oids, sorted like [`user_tables_sorted`].
fn user_tables_with_oids(schema: &Schema) -> Vec<(NamespaceId, &str, &Arc<Table>, i64)> {
    user_tables_sorted(schema)
        .into_iter()
        .map(|(namespace, name, table)| {
            let oid = stored_relation_oid(schema, namespace, name).unwrap_or(0);
            (namespace, name, table, oid)
        })
        .collect()
}

/// The oid of the relation that a regclass input names, as pg_class lists it:
/// a table, view, index, sequence or catalog. A catalog table that pg_class
/// does not list has its PostgreSQL oid. Fails with 42P01 when there is no such
/// relation, as it does for a catalog view that this frontend does not have.
pub(crate) fn regclass_oid(
    conn: &Arc<Connection>,
    schema: Option<&str>,
    name: &str,
) -> Result<i64> {
    if schema.is_none_or(|schema| schema == "pg_catalog") {
        let listed = conn.with_schema(MAIN_DB_ID, |schema| {
            crate::classes::class_catalog(schema)?
                .get_by_relname(NamespaceId::ROOT, name)
                .map(|class| class.oid)
        });
        if let Some(oid) = listed {
            return Ok(oid);
        }
        if let Some(oid) = catalog_oid_by_name(name) {
            return Ok(oid);
        }
    }
    for namespace in crate::session::relation_lookup_namespaces(conn, schema) {
        if namespace.target.id.is_root() {
            continue;
        }
        let found = conn.with_schema(namespace.database_id(), |schema| {
            crate::classes::class_catalog(schema)?
                .get_by_relname(namespace.target.id, name)
                .map(|class| class.oid)
        });
        if let Some(oid) = found {
            return Ok(oid);
        }
    }
    let written = match schema {
        Some(schema) => format!("{schema}.{name}"),
        None => name.to_string(),
    };
    Err(sqlstate_error(
        SQLSTATE_UNDEFINED_TABLE,
        format!("relation \"{written}\" does not exist"),
        None,
    ))
}

/// The text of the regclass value `oid`: the name of the relation, with its
/// schema when the relation is not on the search path, or the number when no
/// relation has that oid. 0 is `-`.
pub(crate) fn regclass_text(conn: &Connection, oid: i64) -> String {
    if oid == 0 {
        return "-".to_string();
    }
    let schema = conn.current_schema();
    let Some(catalog) = crate::classes::class_catalog(&schema) else {
        return oid.to_string();
    };
    let Some(relation) = catalog.get_by_oid(oid) else {
        return match catalog_name_by_oid(oid) {
            Some(name) => name.to_string(),
            None => oid.to_string(),
        };
    };
    if relation.relnamespace.is_root() {
        return quote_identifier(&relation.relname);
    }
    let engine_name = relation.engine_name.as_deref().unwrap_or(&relation.relname);
    relation_name(conn, &schema, relation.relnamespace, engine_name)
}

/// The oid that the text of a regclass names: digits are an oid, and anything
/// else is the name of a relation, which must exist.
pub(crate) fn regclass_oid_of_text(conn: &Arc<Connection>, text: &str) -> Result<i64> {
    match crate::pg_type::parse_regclass(text)? {
        crate::pg_type::RegClassInput::Oid(oid) => Ok(oid),
        crate::pg_type::RegClassInput::Name { schema, name } => {
            regclass_oid(conn, schema.as_deref(), &name)
        }
    }
}

/// The oid of the PostgreSQL catalog `name`, from the catalogs of
/// turso_pg_catalog, for a catalog that pg_class does not list.
fn catalog_oid_by_name(name: &str) -> Option<i64> {
    turso_pg_catalog::BUILTIN_CATALOGS
        .iter()
        .find(|catalog| catalog.name == name)
        .map(|catalog| i64::from(catalog.oid))
}

/// The name of the PostgreSQL catalog with the oid `oid`.
fn catalog_name_by_oid(oid: i64) -> Option<&'static str> {
    turso_pg_catalog::BUILTIN_CATALOGS
        .iter()
        .find(|catalog| i64::from(catalog.oid) == oid)
        .map(|catalog| catalog.name)
}

/// Build a mapping from namespace and table name to OID for all user tables.
fn table_oid_map(schema: &Schema) -> HashMap<(NamespaceId, String), i64> {
    let mut map = HashMap::default();
    for (namespace, name, _, oid) in user_tables_with_oids(schema) {
        map.insert((namespace, name.to_string()), oid);
    }
    map
}

/// Convert a RefAct to its PostgreSQL single-character representation.
fn ref_act_to_char(act: &RefAct) -> &'static str {
    match act {
        RefAct::NoAction => "a",
        RefAct::Restrict => "r",
        RefAct::Cascade => "c",
        RefAct::SetNull => "n",
        RefAct::SetDefault => "d",
    }
}

// Each implemented virtual catalog is registered and given its pg_class
// identity here. Empty compatibility modules use the separate path below.
// The OIDs and kinds were checked against PostgreSQL 18 after initdb.
macro_rules! builtin_catalogs {
    ($(($oid:expr, $name:literal, $table:ident, $kind:ident, $shared:expr)),* $(,)?) => {
        $(impl $table {
            const CATALOG_OID: i64 = $oid;
        })*

        pub(crate) const BUILTIN_CATALOGS: &[(i64, &str, RelationKind, bool)] = &[
            $(($table::CATALOG_OID, $name, RelationKind::$kind, $shared),)*
            // catalog_table registers the virtual table for pg_description.
            (PgDescription::OID as i64, PgDescription::NAME, RelationKind::Table, false),
        ];

        fn register_builtin_catalogs(mut options: OpenOptions) -> OpenOptions {
            $(options = options.native_module(
                $name, VTabKind::TableValuedFunction, SnapshotCatalog::<$table>(PhantomData)
            );)*
            options
        }
    };
}

builtin_catalogs! {
    (2601, "pg_am", PgAmTable, Table, false),
    (2604, "pg_attrdef", PgAttrdefTable, Table, false),
    (1249, "pg_attribute", PgAttributeTable, Table, false),
    (1261, "pg_auth_members", PgAuthMembersTable, Table, true),
    (2605, "pg_cast", PgCastTable, Table, false),
    (PG_CLASS_OID, "pg_class", PgClassTable, Table, false),
    (2606, "pg_constraint", PgConstraintTable, Table, false),
    (1262, "pg_database", PgDatabaseTable, Table, true),
    (2608, "pg_depend", PgDependTable, Table, false),
    (2610, "pg_index", PgIndexTable, Table, false),
    (12043, "pg_indexes", PgIndexesTable, View, false),
    (2612, "pg_language", PgLanguageTable, Table, false),
    (PG_NAMESPACE_OID, "pg_namespace", PgNamespaceTable, Table, false),
    (2617, "pg_operator", PgOperatorTable, Table, false),
    (12018, "pg_policies", PgPoliciesTable, View, false),
    (3256, "pg_policy", PgPolicyTable, Table, false),
    (1255, "pg_proc", PgProcTable, Table, false),
    (PG_REWRITE_OID, "pg_rewrite", PgRewriteTable, Table, false),
    (12000, "pg_roles", PgRolesTable, View, false),
    (2224, "pg_sequence", PgSequenceTable, Table, false),
    (12048, "pg_sequences", PgSequencesTable, View, false),
    (12104, "pg_settings", PgSettingsTable, View, false),
    (12033, "pg_tables", PgTablesTable, View, false),
    (1247, "pg_type", PgTypeTable, Table, false),
}

/// The exposed columns, excluding hidden implementation and system columns.
pub(crate) fn builtin_catalog_columns(
    schema: &Schema,
    name: &str,
) -> Vec<turso_core::schema::Column> {
    let table = schema.get_table(NamespaceId::ROOT, name);
    let fallback;
    let columns = if let Some(table) = table.as_ref() {
        table.columns()
    } else {
        // Stored catalogs do not exist when bootstrap statements are built.
        let sql = match name {
            PgDescription::NAME => PgDescription::CREATE_SQL,
            "pg_class" => PgClassTable::SCHEMA,
            _ => panic!("a registered catalog has a schema: {name}"),
        };
        fallback = PostgresDialect
            .parse_table_sql(sql, 0)
            .expect("catalog DDL is valid");
        fallback.columns()
    };
    columns
        .iter()
        .filter(|column| !column.hidden())
        .cloned()
        .collect()
}

/// Combines stored user relations with catalog definitions from this binary.
#[derive(Debug)]
struct PgClassTable;

impl SnapshotRows for PgClassTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_class (\
    oid INTEGER PRIMARY KEY, \
    relname TEXT NOT NULL, \
    relnamespace INTEGER NOT NULL, \
    reltype INTEGER NOT NULL, \
    reloftype INTEGER NOT NULL, \
    relowner INTEGER NOT NULL, \
    relam INTEGER NOT NULL, \
    relfilenode INTEGER NOT NULL, \
    reltablespace INTEGER NOT NULL, \
    relpages INTEGER NOT NULL, \
    reltuples REAL NOT NULL, \
    relallvisible INTEGER NOT NULL, \
    relallfrozen INTEGER NOT NULL, \
    reltoastrelid INTEGER NOT NULL, \
    relhasindex BOOLEAN NOT NULL, \
    relisshared BOOLEAN NOT NULL, \
    relpersistence TEXT NOT NULL, \
    relkind TEXT NOT NULL, \
    relnatts INTEGER NOT NULL, \
    relchecks INTEGER NOT NULL, \
    relhasrules BOOLEAN NOT NULL, \
    relhastriggers BOOLEAN NOT NULL, \
    relhassubclass BOOLEAN NOT NULL, \
    relrowsecurity BOOLEAN NOT NULL, \
    relforcerowsecurity BOOLEAN NOT NULL, \
    relispopulated BOOLEAN NOT NULL, \
    relreplident TEXT NOT NULL, \
    relispartition BOOLEAN NOT NULL, \
    relrewrite INTEGER NOT NULL, \
    relfrozenxid INTEGER NOT NULL, \
    relminmxid INTEGER NOT NULL, \
    relacl TEXT, \
    reloptions TEXT, \
    relpartbound TEXT, \
    tableoid INTEGER HIDDEN)";
    /// Fixed PostgreSQL OID of pg_class, which describes tables and indexes.
    /// Must match PostgreSQL for tableoid and pg_depend references used by pg_dump.
    const TABLE_OID: Option<i64> = Some(Self::CATALOG_OID);
    const ESTIMATED_COST: f64 = 1000.0;
    const ESTIMATED_ROWS: u32 = 100;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let schema = conn.current_schema();
        crate::classes::class_catalog(&schema)
            .map_or_else(Vec::new, |catalog| catalog.rows(&schema))
    }
}

/// Virtual table implementation for pg_catalog.pg_namespace
/// Maps schema information to PostgreSQL's pg_namespace
#[derive(Debug)]
struct PgNamespaceTable;

impl SnapshotRows for PgNamespaceTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_namespace (
            oid INTEGER,
            nspname TEXT,
            nspowner INTEGER,
            nspacl TEXT,
            tableoid INTEGER HIDDEN
        )";
    /// Fixed PostgreSQL OID of pg_namespace, which describes schemas.
    /// Must match PostgreSQL for tableoid and pg_depend references used by pg_dump.
    const TABLE_OID: Option<i64> = Some(Self::CATALOG_OID);
    const ESTIMATED_COST: f64 = 10.0;
    const ESTIMATED_ROWS: u32 = 5;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let mut rows: Vec<_> = BUILTIN_SCHEMAS
            .iter()
            .map(|(oid, name)| {
                vec![
                    Value::from_i64(*oid),
                    Value::Text((*name).into()),
                    Value::from_i64(10), // nspowner (bootstrap superuser)
                    Value::Null,         // nspacl
                ]
            })
            .collect();

        let schema = conn.current_schema();
        let mut stored: Vec<_> = schema
            .namespaces()
            .map(|(id, name)| (name.to_string(), id))
            .collect();
        stored.sort();
        for (name, id) in stored {
            let (owner, acl) = if name == "public" {
                (
                    PG_DATABASE_OWNER_OID,
                    Value::build_text(
                        "{pg_database_owner=UC/pg_database_owner,=U/pg_database_owner}",
                    ),
                )
            } else {
                (10, Value::Null)
            };
            rows.push(vec![
                Value::from_i64(namespace_oid(&schema, id)),
                Value::build_text(name),
                Value::from_i64(owner),
                acl,
            ]);
        }
        rows
    }
}

/// The schemas that PostgreSQL always has, with their fixed OIDs.
const BUILTIN_SCHEMAS: [(i64, &str); 2] = [(11, "pg_catalog"), (11394, "information_schema")];

/// Whether pg_namespace lists schema `name`: one of the schemas that
/// PostgreSQL always has, or a namespace stored in the main database,
/// which is what CREATE SCHEMA creates and what public is.
pub(crate) fn namespace_exists(conn: &Connection, name: &str) -> bool {
    BUILTIN_SCHEMAS.iter().any(|(_, schema)| *schema == name)
        || conn
            .qualifier(name)
            .is_some_and(|namespace| !namespace.is_database())
}

/// Virtual table implementation for information_schema.schemata
#[derive(Debug)]
struct SchemataTable;

impl SnapshotRows for SchemataTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_information_schema_schemata (
            catalog_name TEXT,
            schema_name TEXT,
            schema_owner TEXT,
            default_character_set_catalog TEXT,
            default_character_set_schema TEXT,
            default_character_set_name TEXT,
            sql_path TEXT
        )";
    const TABLE_OID: Option<i64> = None;
    const ESTIMATED_COST: f64 = 10.0;
    const ESTIMATED_ROWS: u32 = 5;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let db_name = db_name_from_path(conn.db_file_path());
        PgNamespaceTable::load_rows(conn)
            .into_iter()
            .map(|namespace| {
                vec![
                    Value::build_text(db_name.clone()),
                    namespace[1].clone(),
                    Value::build_text("turso"),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]
            })
            .collect()
    }
}

/// Virtual table implementation for pg_catalog.pg_attribute
/// Maps column information to PostgreSQL's pg_attribute
#[derive(Debug)]
struct PgAttributeTable;

impl SnapshotRows for PgAttributeTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_attribute (
            attrelid INTEGER,
            attname TEXT,
            atttypid INTEGER,
            attstattarget INTEGER,
            attlen INTEGER,
            attnum INTEGER,
            attndims INTEGER,
            attcacheoff INTEGER,
            atttypmod INTEGER,
            attbyval BOOLEAN,
            attstorage TEXT,
            attalign TEXT,
            attcompression TEXT,
            attnotnull BOOLEAN,
            atthasdef BOOLEAN,
            atthasmissing BOOLEAN,
            attidentity TEXT,
            attgenerated TEXT,
            attisdropped BOOLEAN,
            attislocal BOOLEAN,
            attinhcount INTEGER,
            attcollation INTEGER,
            attacl TEXT,
            attoptions TEXT,
            attfdwoptions TEXT,
            attmissingval TEXT,
            tableoid INTEGER HIDDEN
        )";
    const TABLE_OID: Option<i64> = Some(Self::CATALOG_OID);
    const ESTIMATED_COST: f64 = 1000.0;
    const ESTIMATED_ROWS: u32 = 1000;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let schema = conn.current_schema();
        let mut rows = Vec::new();

        for (_, _, table, table_oid) in user_tables_with_oids(&schema) {
            let columns = table.columns();
            for (i, col) in columns.iter().enumerate() {
                let col_name = col.name.clone().unwrap_or_default();
                let type_oid = attribute_type_oid(&schema, col, table.is_strict());
                let storage = if col.is_array() {
                    "x"
                } else {
                    type_storage(&schema, &col.ty_str)
                };
                let attnum = (i + 1) as i64; // 1-based
                let notnull = i64::from(col.notnull());
                let has_def = i64::from(
                    (col.default.is_some() && col.identity.is_none()) || col.is_stored_generated(),
                );
                let generated = if col.is_stored_generated() { "s" } else { "" };
                let identity = match col.identity {
                    Some(ast::IdentityKind::Always) => "a",
                    Some(ast::IdentityKind::ByDefault) => "d",
                    None => "",
                };

                rows.push(vec![
                    Value::from_i64(table_oid),   // attrelid
                    Value::Text(col_name.into()), // attname
                    Value::from_i64(type_oid),    // atttypid
                    Value::from_i64(-1),          // attstattarget
                    Value::from_i64(-1),          // attlen
                    Value::from_i64(attnum),      // attnum
                    Value::from_i64(0),           // attndims
                    Value::from_i64(-1),          // attcacheoff
                    Value::from_i64(-1),          // atttypmod
                    Value::from_i64(1),           // attbyval
                    Value::build_text(storage),   // attstorage
                    Value::Text("i".into()),      // attalign (int)
                    Value::Text("".into()),
                    Value::from_i64(notnull),      // attnotnull
                    Value::from_i64(has_def),      // atthasdef
                    Value::from_i64(0),            // atthasmissing
                    Value::Text(identity.into()),  // attidentity
                    Value::Text(generated.into()), // attgenerated
                    Value::from_i64(0),            // attisdropped
                    Value::from_i64(1),            // attislocal
                    Value::from_i64(0),            // attinhcount
                    Value::from_i64(0),            // attcollation
                    Value::Null,                   // attacl
                    Value::Null,                   // attoptions
                    Value::Null,                   // attfdwoptions
                    Value::Null,                   // attmissingval
                ]);
            }
        }

        for rule in view_rules(&schema) {
            for (position, column) in rule.view.columns.iter().enumerate() {
                rows.push(simple_attribute_row(
                    &schema,
                    rule.view_oid,
                    position,
                    column,
                ));
            }
        }
        for index in catalog_indexes(conn, &schema) {
            let classes = crate::classes::class_catalog(&schema).expect("indexes have classes");
            let class = classes
                .get_by_oid(index.table_oid)
                .expect("an index has a table");
            let table = schema
                .get_btree_table(class.relnamespace, &class.relname)
                .expect("an index has a table");
            for (position, indexed) in index.columns.iter().enumerate() {
                let mut column = if let Some(expression) = &indexed.expression {
                    let sql = format!(
                        "SELECT {expression} FROM {}",
                        quote_identifier(&class.relname)
                    );
                    let (Some(ast::Cmd::Stmt(ast::Stmt::Select(select))), _) = PostgresDialect
                        .parse_internal(&sql)
                        .expect("index expressions are valid engine SQL")
                    else {
                        unreachable!("the statement is SELECT");
                    };
                    turso_core::schema::view::extract_view_columns(
                        class.relnamespace,
                        &select,
                        &schema,
                    )
                    .expect("index expressions reference their table")
                    .columns
                    .remove(0)
                    .column
                } else {
                    table.columns()[indexed.position].clone()
                };
                column.name = Some(indexed.name.clone());
                // FIXME(pg-catalog): PostgreSQL index attributes have no NOT NULL
                // constraint. A stored pg_attribute should own this flag.
                column.set_notnull(false);
                column.set_rowid_alias(false);
                rows.push(simple_attribute_row(&schema, index.oid, position, &column));
            }
        }
        let sequence = PostgresDialect.parse_table_sql("CREATE TABLE sequence_columns (last_value BIGINT, log_cnt BIGINT, is_called BOOLEAN)", 0).expect("sequence definition is valid");
        for (oid, _, _) in sequence_catalog_entries(&schema) {
            for (position, column) in sequence.columns().iter().enumerate() {
                rows.push(simple_attribute_row(&schema, oid, position, column));
            }
        }
        for &(oid, name, _, _) in BUILTIN_CATALOGS {
            for (position, column) in builtin_catalog_columns(&schema, name).iter().enumerate() {
                rows.push(simple_attribute_row(&schema, oid, position, column));
            }
        }
        rows
    }
}

/// Attributes of catalog and view columns, which have no defaults or identity.
fn simple_attribute_row(
    schema: &Schema,
    oid: i64,
    position: usize,
    column: &turso_core::schema::Column,
) -> Vec<Value> {
    vec![
        Value::from_i64(oid),
        Value::build_text(column.name.clone().unwrap_or_default()),
        Value::from_i64(attribute_type_oid(schema, column, true)),
        Value::from_i64(-1),
        Value::from_i64(-1),
        Value::from_i64(position as i64 + 1),
        Value::from_i64(0),
        Value::from_i64(-1),
        Value::from_i64(-1),
        Value::from_i64(1),
        Value::build_text("p"),
        Value::build_text("i"),
        Value::build_text(""),
        Value::from_i64(i64::from(column.notnull())),
        Value::from_i64(0),
        Value::from_i64(0),
        Value::build_text(""),
        Value::build_text(""),
        Value::from_i64(0),
        Value::from_i64(1),
        Value::from_i64(0),
        Value::from_i64(0),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
    ]
}

/// Resolves declared column types for table, view and index attributes.
fn attribute_type_oid(
    schema: &Schema,
    column: &turso_core::schema::Column,
    is_strict: bool,
) -> i64 {
    match schema.get_type_def(&column.ty_str, is_strict) {
        Some(td) if !td.is_builtin => user_type_oid(&td.name),
        _ => sqlite_type_to_pg_oid(&column.ty_str),
    }
}

const SUPERUSER_OID: i64 = 10;
const USER_ROLE_OID_START: i64 = 16384;

/// Returns the PostgreSQL OID of a role. The built-in superuser has the OID
/// of the PostgreSQL bootstrap superuser, and `public` has OID 0, as in
/// PostgreSQL's access control lists.
pub(crate) fn role_oid(id: RoleId) -> i64 {
    match id {
        RoleId::SUPERUSER => SUPERUSER_OID,
        RoleId::PUBLIC => 0,
        _ => USER_ROLE_OID_START + id.get(),
    }
}

/// The engine role ID corresponding to a PostgreSQL role OID.
pub(crate) fn role_id_from_oid(oid: i64) -> Option<RoleId> {
    match oid {
        SUPERUSER_OID => Some(RoleId::SUPERUSER),
        0 => Some(RoleId::PUBLIC),
        oid if oid > USER_ROLE_OID_START => Some(RoleId::from_stored(oid - USER_ROLE_OID_START)),
        _ => None,
    }
}

/// The name of the role with PostgreSQL oid `oid`, including
/// `pg_database_owner`, which `pg_roles` lists without its being a role of
/// the engine.
pub(crate) fn role_name_by_oid(roles: &RoleCatalog, oid: i64) -> Option<String> {
    if oid == PG_DATABASE_OWNER_OID {
        return Some(PG_DATABASE_OWNER_NAME.to_string());
    }
    postgres_roles(roles)
        .find(|role| role_oid(role.id) == oid)
        .map(|role| role.name)
}

/// The PostgreSQL oid of the role named `name`, the inverse of
/// [`role_name_by_oid`].
pub(crate) fn role_oid_by_name(roles: &RoleCatalog, name: &str) -> Option<i64> {
    pg_role_by_name(roles, name).map(|role| match role {
        PgRole::Engine(id) => role_oid(id),
        PgRole::DatabaseOwner => PG_DATABASE_OWNER_OID,
    })
}

/// A role that `pg_roles` lists: one of the engine's roles, or
/// `pg_database_owner`, which is not a role of the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PgRole {
    Engine(RoleId),
    DatabaseOwner,
}

/// The engine's role named `name`, the built-in superuser `postgres`
/// included. `pg_database_owner` is not a role of the engine.
pub(crate) fn engine_role_by_name(roles: &RoleCatalog, name: &str) -> Option<RoleId> {
    match pg_role_by_name(roles, name)? {
        PgRole::Engine(id) => Some(id),
        PgRole::DatabaseOwner => None,
    }
}

/// The role named `name` among those `pg_roles` lists.
pub(crate) fn pg_role_by_name(roles: &RoleCatalog, name: &str) -> Option<PgRole> {
    if name == PG_DATABASE_OWNER_NAME {
        return Some(PgRole::DatabaseOwner);
    }
    postgres_roles(roles)
        .find(|role| role.name == name)
        .map(|role| PgRole::Engine(role.id))
}

/// The roles that PostgreSQL has: its bootstrap superuser, which is the
/// built-in [RoleId::SUPERUSER], and the roles in the core role catalog.
/// `public` is not a role in PostgreSQL, only a name for [RoleId::PUBLIC] in
/// access control lists.
pub(crate) fn postgres_roles(roles: &RoleCatalog) -> impl Iterator<Item = Role> + '_ {
    let superuser = Role {
        id: RoleId::SUPERUSER,
        name: BOOTSTRAP_SUPERUSER_NAME.to_string(),
        superuser: true,
        inherit: true,
        bypassrls: true,
    };
    std::iter::once(superuser).chain(roles.iter().cloned())
}

/// Virtual table implementation for pg_catalog.pg_roles.
#[derive(Debug)]
struct PgRolesTable;

impl SnapshotRows for PgRolesTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_roles (
            oid INTEGER,
            rolname TEXT,
            rolsuper INTEGER,
            rolinherit INTEGER,
            rolcreaterole INTEGER,
            rolcreatedb INTEGER,
            rolcanlogin INTEGER,
            rolreplication INTEGER,
            rolconnlimit INTEGER,
            rolpassword TEXT,
            rolvaliduntil TEXT,
            rolbypassrls INTEGER,
            rolconfig TEXT
        )";
    const TABLE_OID: Option<i64> = None;
    const ESTIMATED_COST: f64 = 10.0;
    const ESTIMATED_ROWS: u32 = 2;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let mut rows: Vec<_> = postgres_roles(&connection_roles(conn))
            .map(|role| role_row(&role))
            .collect();
        rows.push(vec![
            Value::from_i64(PG_DATABASE_OWNER_OID),
            Value::build_text(PG_DATABASE_OWNER_NAME),
            Value::from_i64(0),
            Value::from_i64(1),
            Value::from_i64(0),
            Value::from_i64(0),
            Value::from_i64(0),
            Value::from_i64(0),
            Value::from_i64(-1),
            Value::Null,
            Value::Null,
            Value::from_i64(0),
            Value::Null,
        ]);
        rows
    }
}

/// `CREATE ROLE` creates roles that cannot log in, so only the built-in
/// superuser can.
fn role_row(role: &Role) -> Vec<Value> {
    let superuser = Value::from_i64(role.superuser as i64);
    let can_login = role.id == RoleId::SUPERUSER;
    vec![
        Value::from_i64(role_oid(role.id)),     // oid
        Value::build_text(role.name.clone()),   // rolname
        superuser.clone(),                      // rolsuper
        Value::from_i64(role.inherit as i64),   // rolinherit
        superuser.clone(),                      // rolcreaterole
        superuser.clone(),                      // rolcreatedb
        Value::from_i64(can_login as i64),      // rolcanlogin
        superuser,                              // rolreplication
        Value::from_i64(-1),                    // rolconnlimit (-1 = no limit)
        Value::Null,                            // rolpassword (never exposed)
        Value::Null,                            // rolvaliduntil
        Value::from_i64(role.bypassrls as i64), // rolbypassrls
        Value::Null,                            // rolconfig
    ]
}

/// Virtual table implementation for pg_catalog.pg_auth_members: the
/// memberships that `GRANT role TO role` made.
#[derive(Debug)]
struct PgAuthMembersTable;

impl SnapshotRows for PgAuthMembersTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_auth_members (
            oid INTEGER,
            roleid INTEGER,
            member INTEGER,
            grantor INTEGER,
            admin_option BOOLEAN,
            inherit_option BOOLEAN,
            set_option BOOLEAN
        )";
    const TABLE_OID: Option<i64> = None;
    const ESTIMATED_COST: f64 = 10.0;
    const ESTIMATED_ROWS: u32 = 2;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        connection_roles(conn)
            .memberships()
            .iter()
            .map(|membership| {
                vec![
                    // Membership oids start where PostgreSQL's own objects
                    // end, as role oids do.
                    Value::from_i64(USER_ROLE_OID_START + membership.id),
                    Value::from_i64(role_oid(membership.role)),
                    Value::from_i64(role_oid(membership.member)),
                    Value::from_i64(role_oid(membership.grantor)),
                    Value::from_i64(membership.admin_option as i64),
                    Value::from_i64(membership.inherit_option as i64),
                    Value::from_i64(membership.set_option as i64),
                ]
            })
            .collect()
    }
}

/// Virtual table implementation for pg_catalog.pg_proc
#[derive(Debug)]
struct PgProcTable;

impl SnapshotRows for PgProcTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_proc (
            oid INTEGER,
            proname TEXT,
            pronamespace INTEGER,
            proowner INTEGER,
            prolang INTEGER,
            procost REAL,
            prorows REAL,
            provariadic INTEGER,
            prokind TEXT,
            prosecdef BOOLEAN,
            proleakproof BOOLEAN,
            proisstrict BOOLEAN,
            proretset BOOLEAN,
            provolatile TEXT,
            proparallel TEXT,
            pronargs INTEGER,
            pronargdefaults INTEGER,
            prorettype INTEGER,
            proargtypes TEXT,
            proallargtypes TEXT,
            proargmodes TEXT,
            proargnames TEXT,
            proargdefaults TEXT,
            protrftypes TEXT,
            prosrc TEXT,
            probin TEXT,
            prosqlbody TEXT,
            proconfig TEXT,
            proacl TEXT,
            tableoid INTEGER HIDDEN
        )";
    const TABLE_OID: Option<i64> = Some(Self::CATALOG_OID);
    const ESTIMATED_COST: f64 = 100.0;
    const ESTIMATED_ROWS: u32 = 100;

    fn load_rows(_conn: &Connection) -> Vec<Vec<Value>> {
        const INTERNAL_LANGUAGE: i64 = 12;
        crate::procs::PROCS
            .iter()
            .map(|proc| {
                let arg_types = proc
                    .arg_types
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(" ");
                vec![
                    Value::from_i64(proc.oid),
                    Value::build_text(proc.name),
                    Value::from_i64(11),
                    Value::from_i64(SUPERUSER_OID),
                    Value::from_i64(INTERNAL_LANGUAGE),
                    Value::from_f64(proc.cost),
                    Value::from_f64(proc.rows),
                    Value::from_i64(proc.variadic_type),
                    Value::build_text(proc.kind),
                    Value::from_i64(0),
                    Value::from_i64(i64::from(proc.leakproof)),
                    Value::from_i64(i64::from(proc.strict)),
                    Value::from_i64(i64::from(proc.returns_set)),
                    Value::build_text(proc.volatility),
                    Value::build_text(proc.parallel),
                    Value::from_i64(proc.arg_types.len() as i64),
                    Value::from_i64(0),
                    Value::from_i64(proc.result),
                    Value::build_text(arg_types),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]
            })
            .collect()
    }
}

/// Virtual table implementation for pg_catalog.pg_database
/// Returns one row per database, deriving the name from the database file path.
#[derive(Debug)]
struct PgDatabaseTable;

impl SnapshotRows for PgDatabaseTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_database (
            oid INTEGER,
            datname TEXT,
            datdba INTEGER,
            encoding INTEGER,
            datlocprovider TEXT,
            datistemplate BOOLEAN,
            datallowconn BOOLEAN,
            datconnlimit INTEGER,
            datfrozenxid INTEGER,
            datminmxid INTEGER,
            dattablespace INTEGER,
            datcollate TEXT,
            datctype TEXT,
            daticulocale TEXT,
            daticurules TEXT,
            datacl TEXT,
            tableoid INTEGER HIDDEN
        )";
    const TABLE_OID: Option<i64> = Some(Self::CATALOG_OID);
    const ESTIMATED_COST: f64 = 10.0;
    const ESTIMATED_ROWS: u32 = 1;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let db_name = db_name_from_path(conn.db_file_path());
        vec![vec![
            Value::from_i64(16384),           // oid
            Value::build_text(db_name),       // datname
            Value::from_i64(10),              // datdba (bootstrap superuser OID)
            Value::from_i64(6),               // encoding (UTF8)
            Value::build_text("c"),           // datlocprovider (libc)
            Value::from_i64(0),               // datistemplate
            Value::from_i64(1),               // datallowconn
            Value::from_i64(-1),              // datconnlimit (unlimited)
            Value::from_i64(0),               // datfrozenxid
            Value::from_i64(0),               // datminmxid
            Value::from_i64(1663),            // dattablespace (pg_default)
            Value::build_text("en_US.UTF-8"), // datcollate
            Value::build_text("en_US.UTF-8"), // datctype
            Value::Null,                      // daticulocale
            Value::Null,                      // daticurules
            Value::Null,                      // datacl
        ]]
    }
}

/// Shared by the pg_database virtual table and current_database() so both
/// report the same database name.
pub(crate) fn db_name_from_path(path: &str) -> String {
    std::path::Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(path)
        .to_string()
}

/// Virtual table implementation for pg_catalog.pg_am
/// Stub: returns two access methods (heap and btree).
#[derive(Debug)]
struct PgAmTable;

impl SnapshotRows for PgAmTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_am (
            oid INTEGER,
            amname TEXT,
            amhandler TEXT,
            amtype TEXT,
            tableoid INTEGER HIDDEN
        )";
    /// Fixed PostgreSQL OID of pg_am, which lists table and index access methods.
    /// Must match PostgreSQL for tableoid and pg_depend references used by pg_dump.
    const TABLE_OID: Option<i64> = Some(Self::CATALOG_OID);
    const ESTIMATED_COST: f64 = 10.0;
    const ESTIMATED_ROWS: u32 = 2;

    fn load_rows(_conn: &Connection) -> Vec<Vec<Value>> {
        vec![
            vec![
                Value::from_i64(2),                        // oid
                Value::build_text("heap"),                 // amname
                Value::build_text("heap_tableam_handler"), // amhandler
                Value::build_text("t"),                    // amtype (table)
            ],
            vec![
                Value::from_i64(403),           // oid
                Value::build_text("btree"),     // amname
                Value::build_text("bthandler"), // amhandler
                Value::build_text("i"),         // amtype (index)
            ],
        ]
    }
}

/// Generic empty PG catalog table — always returns no rows.
/// Used for catalog tables psql queries but we don't yet need real data for.
#[derive(Debug, Clone)]
struct EmptyPgCatalogTable {
    create_sql: String,
}

impl VirtualTableModule for EmptyPgCatalogTable {
    type Table = Self;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        Ok(self.create_sql.clone())
    }

    fn create(&self, _args: &[Value]) -> Result<Self::Table> {
        Ok(self.clone())
    }

    fn innocuous(&self) -> bool {
        true
    }
}

impl VirtualTable for EmptyPgCatalogTable {
    type Cursor = EmptyPgCatalogCursor;

    fn open(&self, _conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(EmptyPgCatalogCursor)
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        let constraint_usages = constraints
            .iter()
            .map(|_| turso_ext::ConstraintUsage {
                argv_index: None,
                omit: false,
            })
            .collect();
        Ok(IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 10.0,
            estimated_rows: 0,
            constraint_usages,
        })
    }
}

struct EmptyPgCatalogCursor;

impl VirtualTableCursor for EmptyPgCatalogCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        Ok(IOResult::Done(false))
    }
    fn rowid(&self) -> i64 {
        0
    }
    fn column(&mut self, _column: usize) -> turso_core::types::IOResultOr<Value> {
        Ok(IOResult::Done(Value::Null))
    }
    fn filter(
        &mut self,
        _args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> turso_core::types::IOResultOr<bool> {
        Ok(IOResult::Done(false))
    }
}

/// Virtual table implementation for pg_tables
/// Maps user tables to PostgreSQL's pg_tables view
#[derive(Debug)]
struct PgTablesTable;

impl SnapshotRows for PgTablesTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_tables (
            schemaname TEXT,
            tablename TEXT,
            tableowner TEXT,
            tablespace TEXT,
            hasindexes INTEGER,
            hasrules INTEGER,
            hastriggers INTEGER,
            rowsecurity INTEGER
        )";
    const TABLE_OID: Option<i64> = None;
    const ESTIMATED_COST: f64 = 1000.0;
    const ESTIMATED_ROWS: u32 = 100;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let schema = conn.current_schema();
        let mut rows = Vec::new();
        let tables = user_tables_sorted(&schema);

        for (namespace, name, table) in tables {
            let btree = table.btree().expect("user tables are B-trees");
            rows.push(vec![
                Value::build_text(schema_name_of(&schema, namespace).to_string()), // schemaname
                Value::Text(btree.declared_name.clone().into()),                   // tablename
                Value::build_text(relation_owner_name(conn, &schema, namespace, name)), // tableowner
                Value::Null,        // tablespace
                Value::from_i64(0), // hasindexes
                Value::from_i64(0), // hasrules
                Value::from_i64(0), // hastriggers
                Value::from_i64(0), // rowsecurity
            ]);
        }

        rows
    }
}

#[derive(Debug)]
struct PgPolicyTable;

impl SnapshotRows for PgPolicyTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_policy (
            oid INTEGER,
            polname TEXT,
            polrelid INTEGER,
            polcmd TEXT,
            polpermissive BOOLEAN,
            polroles INTEGER[],
            polqual TEXT,
            polwithcheck TEXT,
            tableoid INTEGER HIDDEN
        )";
    /// PostgreSQL's oid of pg_policy.
    const TABLE_OID: Option<i64> = Some(Self::CATALOG_OID);
    const ESTIMATED_COST: f64 = 100.0;
    const ESTIMATED_ROWS: u32 = 50;

    /// `polqual` and `polwithcheck` hold the conditions as text, which
    /// `pg_get_expr` returns as it is.
    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let schema = conn.current_schema();
        catalog_policies(conn, &schema)
            .into_iter()
            .map(|policy| {
                let roles = policy
                    .roles
                    .iter()
                    .map(|role| role_oid(*role).to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                vec![
                    Value::from_i64(policy.oid),
                    Value::build_text(policy.policy.name.clone()),
                    Value::from_i64(policy.table_oid),
                    Value::build_text(
                        crate::policies::command_letter(policy.policy.command).to_string(),
                    ),
                    Value::from_i64(i64::from(policy.policy.permissive)),
                    Value::build_text(format!("{{{roles}}}")),
                    policy.qual.map_or(Value::Null, Value::build_text),
                    policy.with_check.map_or(Value::Null, Value::build_text),
                ]
            })
            .collect()
    }
}

#[derive(Debug)]
struct PgPoliciesTable;

impl SnapshotRows for PgPoliciesTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_policies (
            schemaname TEXT,
            tablename TEXT,
            policyname TEXT,
            permissive TEXT,
            roles TEXT[],
            cmd TEXT,
            qual TEXT,
            with_check TEXT
        )";
    const TABLE_OID: Option<i64> = None;
    const ESTIMATED_COST: f64 = 100.0;
    const ESTIMATED_ROWS: u32 = 50;

    /// As PostgreSQL's view: `roles` is `{public}` for PUBLIC and the role
    /// names in order otherwise.
    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let schema = conn.current_schema();
        let role_catalog = schema_roles(&schema);
        catalog_policies(conn, &schema)
            .into_iter()
            .map(|policy| {
                let mut roles: Vec<String> = if policy.roles == [RoleId::PUBLIC] {
                    vec!["public".to_string()]
                } else {
                    policy
                        .roles
                        .iter()
                        .filter_map(|role| role_name_by_oid(role_catalog, role_oid(*role)))
                        .collect()
                };
                roles.sort();
                let permissive = if policy.policy.permissive {
                    "PERMISSIVE"
                } else {
                    "RESTRICTIVE"
                };
                let command = match policy.policy.command {
                    ast::PolicyCommand::All => "ALL",
                    ast::PolicyCommand::Select => "SELECT",
                    ast::PolicyCommand::Insert => "INSERT",
                    ast::PolicyCommand::Update => "UPDATE",
                    ast::PolicyCommand::Delete => "DELETE",
                };
                vec![
                    Value::build_text(schema_name_of(&schema, policy.namespace).to_string()),
                    Value::build_text(policy.table_name.to_string()),
                    Value::build_text(policy.policy.name.clone()),
                    Value::build_text(permissive),
                    Value::build_text(text_array_literal(roles.iter().map(String::as_str))),
                    Value::build_text(command),
                    policy.qual.map_or(Value::Null, Value::build_text),
                    policy.with_check.map_or(Value::Null, Value::build_text),
                ]
            })
            .collect()
    }
}

/// A row-level security policy as the catalogs show it.
struct CatalogPolicy<'a> {
    oid: i64,
    table_oid: i64,
    namespace: NamespaceId,
    table_name: &'a str,
    policy: &'a RowPolicy,
    /// The roles the policy applies to: only [`RoleId::PUBLIC`] when it is
    /// one of them, as PostgreSQL stores them.
    roles: Vec<RoleId>,
    /// The `USING` condition as PostgreSQL prints it.
    qual: Option<String>,
    /// The `WITH CHECK` condition as PostgreSQL prints it.
    with_check: Option<String>,
}

/// The policies of every table, ordered by table oid and then as created.
fn catalog_policies<'a>(conn: &Connection, schema: &'a Schema) -> Vec<CatalogPolicy<'a>> {
    let Some(catalog) = crate::classes::class_catalog(schema) else {
        return Vec::new();
    };
    let mut policies = Vec::new();
    for class in catalog.classes() {
        if class.relpolicies.is_empty() {
            continue;
        }
        let Some(table) = schema.get_btree_table(class.relnamespace, &class.relname) else {
            continue;
        };
        // A condition the printer cannot print as pg_get_expr does is shown
        // as CREATE POLICY wrote it, which PostgreSQL also reads.
        let condition_text = |condition: &Option<ast::Expr>, written: &Option<String>| {
            let condition = condition.as_ref()?;
            crate::deparse::condition_text(conn, condition, &table).or_else(|| written.clone())
        };
        for (details, policy) in class.relpolicies.iter() {
            let roles = if policy.roles.contains(&RoleId::PUBLIC) {
                vec![RoleId::PUBLIC]
            } else {
                policy.roles.clone()
            };
            policies.push(CatalogPolicy {
                oid: details.oid,
                table_oid: class.oid,
                namespace: class.relnamespace,
                table_name: &class.relname,
                policy,
                roles,
                qual: condition_text(&policy.using_expr, &details.using_sql),
                with_check: condition_text(&policy.check_expr, &details.check_sql),
            });
        }
    }
    policies
}

#[derive(Debug)]
struct PgIndexesTable;

impl SnapshotRows for PgIndexesTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_indexes (
            schemaname TEXT,
            tablename TEXT,
            indexname TEXT,
            tablespace TEXT,
            indexdef TEXT
        )";
    const TABLE_OID: Option<i64> = None;
    const ESTIMATED_COST: f64 = 100.0;
    const ESTIMATED_ROWS: u32 = 50;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let schema = conn.current_schema();
        let mut rows = Vec::new();

        for index in catalog_indexes(conn, &schema) {
            rows.push(vec![
                Value::build_text(schema_name_of(&schema, index.namespace).to_string()),
                Value::build_text(index.table_name.clone()),
                Value::build_text(index.name.clone()),
                Value::Null,
                Value::build_text(index_definition(&schema, &index)),
            ]);
        }

        rows
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_type
// ──────────────────────────────────────────────────────────────────────

/// A pg_type row of a built-in type or of the array of one.
struct PgTypeInfo {
    oid: i64,
    name: String,
    typcategory: &'static str,
    typispreferred: bool,
    typlen: i64,
    typarray: i64,
    typelem: i64,
    typbyval: bool,
    typalign: &'static str,
    typstorage: &'static str,
    typdelim: &'static str,
}

impl PgTypeInfo {
    fn of(ty: &BuiltInType) -> Self {
        Self {
            oid: ty.oid,
            name: ty.name.to_string(),
            typcategory: ty.category,
            typispreferred: ty.is_preferred,
            typlen: ty.len,
            typarray: ty.array_oid,
            typelem: 0,
            typbyval: ty.by_value,
            typalign: ty.align,
            typstorage: ty.storage,
            typdelim: ty.delimiter,
        }
    }

    /// The row of the array of `element`, as PostgreSQL makes it: a
    /// varlena type that is aligned as a double when its element is.
    fn of_array(element: &BuiltInType) -> Self {
        Self {
            oid: element.array_oid,
            name: format!("_{}", element.name),
            typcategory: "A",
            typispreferred: false,
            typlen: -1,
            typarray: 0,
            typelem: element.oid,
            typbyval: false,
            typalign: if element.align == "d" { "d" } else { "i" },
            typstorage: "x",
            typdelim: element.delimiter,
        }
    }
}

const PG_TYPE_SQL: &str = "CREATE TABLE pg_type (oid INTEGER, typname TEXT, typnamespace INTEGER, typowner INTEGER, typlen INTEGER, typbyval BOOLEAN, typtype TEXT, typcategory TEXT, typispreferred BOOLEAN, typisdefined BOOLEAN, typdelim TEXT, typrelid INTEGER, typsubscript TEXT, typelem INTEGER, typarray INTEGER, typinput TEXT, typoutput TEXT, typreceive TEXT, typsend TEXT, typmodin TEXT, typmodout TEXT, typanalyze TEXT, typalign TEXT, typstorage TEXT, typnotnull BOOLEAN, typbasetype INTEGER, typtypmod INTEGER, typndims INTEGER, typcollation INTEGER, typdefaultbin TEXT, typdefault TEXT, typacl TEXT, tableoid INTEGER HIDDEN)";

/// The `typname` of the built-in type with this oid.
pub(crate) fn pg_type_name(oid: i64) -> Option<&'static str> {
    built_in_type(oid).map(|info| info.name)
}

/// The name of the type made by CREATE TYPE or CREATE DOMAIN that has the oid
/// `oid`, quoted where it needs it. The oid of such a type is the one that its
/// pg_type row has.
pub(crate) fn custom_type_name(conn: &Connection, oid: i64) -> Option<String> {
    let schema = conn.current_schema();
    schema
        .type_registry
        .values()
        .find(|ty| !ty.is_builtin && user_type_oid(&ty.name) == oid)
        .map(|ty| quote_identifier(&ty.name))
}

/// The `typcategory` of the built-in type with this oid.
pub(crate) fn pg_type_category(oid: i64) -> Option<&'static str> {
    built_in_type(oid).map(|info| info.category)
}

#[derive(Debug)]
struct PgTypeTable;

impl SnapshotRows for PgTypeTable {
    const SCHEMA: &'static str = PG_TYPE_SQL;
    const TABLE_OID: Option<i64> = Some(Self::CATALOG_OID);
    const ESTIMATED_COST: f64 = 100.0;
    const ESTIMATED_ROWS: u32 = 50;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();

        for ty in &BUILT_IN_TYPES {
            rows.push(make_type_row(&PgTypeInfo::of(ty)));
            if ty.array_oid != 0 {
                rows.push(make_type_row(&PgTypeInfo::of_array(ty)));
            }
        }

        let schema = conn.current_schema();
        for (name, td) in &schema.type_registry {
            if td.is_builtin {
                continue;
            }
            let (typtype, typcategory, typbasetype) = if td.is_domain {
                let base_oid = match schema.get_type_def_unchecked(td.base()) {
                    Some(base) if !base.is_builtin => user_type_oid(&base.name),
                    _ => sqlite_type_to_pg_oid(td.base()),
                };
                let base_category = built_in_type(base_oid).map_or("U", |ty| ty.category);
                ("d", base_category, base_oid)
            } else {
                ("e", "E", 0)
            };
            let oid = user_type_oid(&td.name);
            let storage = type_storage(&schema, &td.name);
            rows.push(vec![
                Value::from_i64(oid),             // oid
                Value::Text(name.clone().into()), // typname
                Value::from_i64(2200),            // typnamespace (public)
                Value::from_i64(10),              // typowner
                Value::from_i64(4),               // typlen
                Value::from_i64(1),               // typbyval
                Value::build_text(typtype),       // typtype
                Value::build_text(typcategory),   // typcategory
                Value::from_i64(0),               // typispreferred
                Value::from_i64(1),               // typisdefined
                Value::build_text(","),           // typdelim
                Value::from_i64(0),               // typrelid
                Value::Null,                      // typsubscript
                Value::from_i64(0),               // typelem
                Value::from_i64(0),               // typarray
                Value::Null,                      // typinput
                Value::Null,                      // typoutput
                Value::Null,                      // typreceive
                Value::Null,                      // typsend
                Value::build_text("-"),           // typmodin
                Value::Null,                      // typmodout
                Value::Null,                      // typanalyze
                Value::build_text("i"),           // typalign
                Value::build_text(storage),       // typstorage
                Value::from_i64(0),               // typnotnull
                Value::from_i64(typbasetype),     // typbasetype
                Value::from_i64(-1),              // typtypmod
                Value::from_i64(0),               // typndims
                Value::from_i64(0),               // typcollation
                Value::Null,                      // typdefaultbin
                Value::Null,                      // typdefault
                Value::Null,                      // typacl
            ]);
        }
        rows
    }
}

pub(crate) fn user_type_oid(name: &str) -> i64 {
    50000
        + (name
            .as_bytes()
            .iter()
            .fold(0u64, |acc, &b| acc.wrapping_mul(31).wrapping_add(b as u64))
            % 10000) as i64
}

fn type_storage<'a>(schema: &'a Schema, mut name: &'a str) -> &'static str {
    while let Some(ty) = schema.get_type_def_unchecked(name) {
        if ty.is_builtin {
            break;
        }
        if !ty.is_domain {
            return "p";
        }
        name = ty.base();
    }
    let oid = sqlite_type_to_pg_oid(name);
    built_in_type(oid)
        .expect("column type OIDs refer to PostgreSQL base types")
        .storage
}

fn make_type_row(t: &PgTypeInfo) -> Vec<Value> {
    vec![
        Value::from_i64(t.oid),                                       // oid
        Value::build_text(t.name.clone()),                            // typname
        Value::from_i64(11),                                          // typnamespace (pg_catalog)
        Value::from_i64(10),                                          // typowner
        Value::from_i64(t.typlen),                                    // typlen
        Value::from_i64(i64::from(t.typbyval)),                       // typbyval
        Value::build_text("b"),                                       // typtype
        Value::build_text(t.typcategory),                             // typcategory
        Value::from_i64(i64::from(t.typispreferred)),                 // typispreferred
        Value::from_i64(1),                                           // typisdefined
        Value::build_text(t.typdelim),                                // typdelim
        Value::from_i64(0),                                           // typrelid
        Value::Null,                                                  // typsubscript
        Value::from_i64(t.typelem),                                   // typelem
        Value::from_i64(t.typarray),                                  // typarray
        Value::Null,                                                  // typinput
        Value::Null,                                                  // typoutput
        Value::Null,                                                  // typreceive
        Value::Null,                                                  // typsend
        Value::build_text(type_modifier_input(t.oid).unwrap_or("-")), // typmodin
        Value::Null,                                                  // typmodout
        Value::Null,                                                  // typanalyze
        Value::build_text(t.typalign),                                // typalign
        Value::build_text(t.typstorage),                              // typstorage
        Value::from_i64(0),                                           // typnotnull
        Value::from_i64(0),                                           // typbasetype
        Value::from_i64(-1),                                          // typtypmod
        Value::from_i64(0),                                           // typndims
        Value::from_i64(0),                                           // typcollation
        Value::Null,                                                  // typdefaultbin
        Value::Null,                                                  // typdefault
        Value::Null,                                                  // typacl
    ]
}

// ──────────────────────────────────────────────────────────────────────
// pg_index
// ──────────────────────────────────────────────────────────────────────

const PG_INDEX_SQL: &str = "CREATE TABLE pg_index (indexrelid INTEGER, indrelid INTEGER, indnatts INTEGER, indnkeyatts INTEGER, indisunique BOOLEAN, indnullsnotdistinct BOOLEAN, indisprimary BOOLEAN, indisexclusion BOOLEAN, indimmediate BOOLEAN, indisclustered BOOLEAN, indisvalid BOOLEAN, indcheckxmin BOOLEAN, indisready BOOLEAN, indislive BOOLEAN, indisreplident BOOLEAN, indkey TEXT, indcollation TEXT, indclass TEXT, indoption TEXT, indexprs TEXT, indpred TEXT, tableoid INTEGER HIDDEN)";

#[derive(Debug)]
struct PgIndexTable;

impl SnapshotRows for PgIndexTable {
    const SCHEMA: &'static str = PG_INDEX_SQL;
    const TABLE_OID: Option<i64> = Some(Self::CATALOG_OID);
    const ESTIMATED_COST: f64 = 100.0;
    const ESTIMATED_ROWS: u32 = 50;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let schema = conn.current_schema();
        let mut rows = Vec::new();

        for index in catalog_indexes(conn, &schema) {
            let indnatts = index.columns.len() as i64;
            let indkey: String = index
                .columns
                .iter()
                .map(|column| {
                    if column.expression.is_some() {
                        "0".to_string()
                    } else {
                        (column.position + 1).to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join(" ");

            let indpred = index
                .where_clause
                .as_ref()
                .map(|e| Value::build_text(e.clone()))
                .unwrap_or(Value::Null);

            let indexprs = if index
                .columns
                .iter()
                .any(|column| column.expression.is_some())
            {
                let exprs: Vec<String> = index
                    .columns
                    .iter()
                    .filter_map(|column| column.expression.clone())
                    .collect();
                Value::build_text(exprs.join(", "))
            } else {
                Value::Null
            };

            rows.push(vec![
                Value::from_i64(index.oid),               // indexrelid
                Value::from_i64(index.table_oid),         // indrelid
                Value::from_i64(indnatts),                // indnatts
                Value::from_i64(indnatts),                // indnkeyatts
                Value::from_i64(i64::from(index.unique)), // indisunique
                Value::from_i64(0),
                Value::from_i64(i64::from(index.primary)), // indisprimary
                Value::from_i64(0),                        // indisexclusion
                Value::from_i64(1),                        // indimmediate
                Value::from_i64(0),                        // indisclustered
                Value::from_i64(1),                        // indisvalid
                Value::from_i64(0),                        // indcheckxmin
                Value::from_i64(1),                        // indisready
                Value::from_i64(1),                        // indislive
                Value::from_i64(0),                        // indisreplident
                Value::build_text(indkey),                 // indkey
                Value::Null,                               // indcollation
                Value::Null,                               // indclass
                Value::Null,                               // indoption
                indexprs,                                  // indexprs
                indpred,                                   // indpred
            ]);
        }
        rows
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_constraint
// ──────────────────────────────────────────────────────────────────────

#[derive(strum::EnumString, strum::IntoStaticStr)]
enum ConstraintKind {
    #[strum(serialize = "p")]
    PrimaryKey,
    #[strum(serialize = "u")]
    Unique,
    #[strum(serialize = "f")]
    ForeignKey,
    #[strum(serialize = "c")]
    Check,
}

const PG_CONSTRAINT_SQL: &str = "CREATE TABLE pg_constraint (oid INTEGER, conname TEXT, connamespace INTEGER, contype TEXT, condeferrable BOOLEAN, condeferred BOOLEAN, convalidated BOOLEAN, conrelid INTEGER, contypid INTEGER, conindid INTEGER, conparentid INTEGER, confrelid INTEGER, confupdtype TEXT, confdeltype TEXT, confmatchtype TEXT, conislocal BOOLEAN, coninhcount INTEGER, connoinherit BOOLEAN, conkey TEXT, confkey TEXT, conpfeqop TEXT, conppeqop TEXT, conffeqop TEXT, conexclop TEXT, conbin TEXT, tableoid INTEGER HIDDEN)";

#[derive(Debug)]
struct PgConstraintTable;

impl SnapshotRows for PgConstraintTable {
    const SCHEMA: &'static str = PG_CONSTRAINT_SQL;
    /// Fixed PostgreSQL OID of pg_constraint, which describes primary keys,
    /// unique constraints, foreign keys and checks.
    /// Must match PostgreSQL for tableoid and pg_depend references used by pg_dump.
    const TABLE_OID: Option<i64> = Some(Self::CATALOG_OID);
    const ESTIMATED_COST: f64 = 100.0;
    const ESTIMATED_ROWS: u32 = 50;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let schema = conn.current_schema();
        let mut rows = Vec::new();

        let tables = user_tables_sorted(&schema);
        let tbl_oid_map = table_oid_map(&schema);
        let indexes = catalog_indexes(conn, &schema);
        let mut constraint_oid = CONSTRAINT_OID_START;

        for (namespace, table_name, table) in &tables {
            let btree = match table.as_ref() {
                Table::BTree(bt) => bt,
                _ => continue,
            };
            let table_oid = tbl_oid_map
                .get(&(*namespace, (*table_name).to_string()))
                .copied()
                .unwrap_or(0);

            let table_name = &btree.declared_name;
            if btree.get_rowid_alias_column().is_some() {
                let index = indexes
                    .iter()
                    .find(|index| index.table_oid == table_oid && index.primary)
                    .expect("rowid primary keys have a catalog index");
                let conname = index.name.clone();
                let conkey: String = btree
                    .primary_key_columns
                    .iter()
                    .map(|(name, _)| {
                        btree
                            .get_column(name)
                            .map(|(pos, _)| (pos + 1).to_string())
                            .unwrap_or_else(|| "0".to_string())
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                let contype: &'static str = ConstraintKind::PrimaryKey.into();
                rows.push(vec![
                    Value::from_i64(constraint_oid),
                    Value::build_text(conname),
                    Value::from_i64(2200),
                    Value::build_text(contype),
                    Value::from_i64(0),
                    Value::from_i64(0),
                    Value::from_i64(1),
                    Value::from_i64(table_oid),
                    Value::from_i64(0),
                    Value::from_i64(index.oid),
                    Value::from_i64(0),
                    Value::from_i64(0),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::from_i64(1),
                    Value::from_i64(0),
                    Value::from_i64(0),
                    Value::build_text(conkey),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]);
                constraint_oid += 1;
            }

            // PK / UNIQUE constraints from unique_sets
            for us in &btree.unique_sets {
                let primary = unique_set_is_primary_key(btree, us);
                let contype = if primary {
                    ConstraintKind::PrimaryKey
                } else {
                    ConstraintKind::Unique
                };
                let col_names: Vec<&str> = us.columns.iter().map(|c| c.name.as_str()).collect();

                // Build conkey: space-separated 1-based attnums
                let conkey: String = col_names
                    .iter()
                    .map(|name| {
                        btree
                            .get_column(name)
                            .map(|(pos, _)| (pos + 1).to_string())
                            .unwrap_or_else(|| "0".to_string())
                    })
                    .collect::<Vec<_>>()
                    .join(" ");

                let positions: Vec<usize> = col_names
                    .iter()
                    .map(|name| btree.get_column(name).expect("constraint columns exist").0)
                    .collect();
                let index = indexes
                    .iter()
                    .find(|index| {
                        index.table_oid == table_oid
                            && index.unique
                            && index.for_constraint
                            && index.where_clause.is_none()
                            && index
                                .columns
                                .iter()
                                .all(|column| column.expression.is_none())
                            && index.primary == primary
                            && index
                                .columns
                                .iter()
                                .map(|column| column.position)
                                .eq(positions.iter().copied())
                    })
                    .expect("unique constraints have a catalog index");
                let conindid = index.oid;
                let conname = index.name.clone();

                let contype: &'static str = contype.into();
                rows.push(vec![
                    Value::from_i64(constraint_oid), // oid
                    Value::build_text(conname),      // conname
                    Value::from_i64(2200),           // connamespace (public)
                    Value::build_text(contype),      // contype
                    Value::from_i64(0),              // condeferrable
                    Value::from_i64(0),              // condeferred
                    Value::from_i64(1),              // convalidated
                    Value::from_i64(table_oid),      // conrelid
                    Value::from_i64(0),              // contypid
                    Value::from_i64(conindid),       // conindid
                    Value::from_i64(0),              // conparentid
                    Value::from_i64(0),              // confrelid
                    Value::Null,                     // confupdtype
                    Value::Null,                     // confdeltype
                    Value::Null,                     // confmatchtype
                    Value::from_i64(1),              // conislocal
                    Value::from_i64(0),              // coninhcount
                    Value::from_i64(0),              // connoinherit
                    Value::build_text(conkey),       // conkey
                    Value::Null,                     // confkey
                    Value::Null,                     // conpfeqop
                    Value::Null,                     // conppeqop
                    Value::Null,                     // conffeqop
                    Value::Null,                     // conexclop
                    Value::Null,                     // conbin
                ]);
                constraint_oid += 1;
            }

            // FK constraints from foreign_keys
            for fk in &btree.foreign_keys {
                let child_cols = fk.child_columns.join("_");
                let conname = fk
                    .constraint_name
                    .clone()
                    .unwrap_or_else(|| format!("{table_name}_{child_cols}_fkey"));

                let conkey: String = fk
                    .child_columns
                    .iter()
                    .map(|name| {
                        btree
                            .get_column(name)
                            .map(|(pos, _)| (pos + 1).to_string())
                            .unwrap_or_else(|| "0".to_string())
                    })
                    .collect::<Vec<_>>()
                    .join(" ");

                let confrelid = tbl_oid_map
                    .get(&(fk.parent_namespace, fk.parent_table.clone()))
                    .copied()
                    .unwrap_or(0);

                let confkey: String = fk
                    .parent_columns
                    .iter()
                    .map(|name| {
                        schema
                            .get_btree_table(fk.parent_namespace, &fk.parent_table)
                            .and_then(|parent_bt| {
                                parent_bt
                                    .get_column(name)
                                    .map(|(pos, _)| (pos + 1).to_string())
                            })
                            .unwrap_or_else(|| "0".to_string())
                    })
                    .collect::<Vec<_>>()
                    .join(" ");

                let contype: &'static str = ConstraintKind::ForeignKey.into();
                rows.push(vec![
                    Value::from_i64(constraint_oid),                   // oid
                    Value::build_text(conname),                        // conname
                    Value::from_i64(2200),                             // connamespace
                    Value::build_text(contype),                        // contype
                    Value::from_i64(i64::from(fk.deferred)),           // condeferrable
                    Value::from_i64(i64::from(fk.deferred)),           // condeferred
                    Value::from_i64(1),                                // convalidated
                    Value::from_i64(table_oid),                        // conrelid
                    Value::from_i64(0),                                // contypid
                    Value::from_i64(0),                                // conindid
                    Value::from_i64(0),                                // conparentid
                    Value::from_i64(confrelid),                        // confrelid
                    Value::build_text(ref_act_to_char(&fk.on_update)), // confupdtype
                    Value::build_text(ref_act_to_char(&fk.on_delete)), // confdeltype
                    Value::build_text("s"),                            // confmatchtype (simple)
                    Value::from_i64(1),                                // conislocal
                    Value::from_i64(0),                                // coninhcount
                    Value::from_i64(0),                                // connoinherit
                    Value::build_text(conkey),                         // conkey
                    Value::build_text(confkey),                        // confkey
                    Value::Null,                                       // conpfeqop
                    Value::Null,                                       // conppeqop
                    Value::Null,                                       // conffeqop
                    Value::Null,                                       // conexclop
                    Value::Null,                                       // conbin
                ]);
                constraint_oid += 1;
            }

            // CHECK constraints from check_constraints
            for chk in &btree.check_constraints {
                let conname = chk
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("{table_name}_check"));

                let conkey = chk
                    .column
                    .as_ref()
                    .and_then(|col_name| {
                        btree
                            .get_column(col_name)
                            .map(|(pos, _)| (pos + 1).to_string())
                    })
                    .unwrap_or_default();

                let contype: &'static str = ConstraintKind::Check.into();
                rows.push(vec![
                    Value::from_i64(constraint_oid), // oid
                    Value::build_text(conname),      // conname
                    Value::from_i64(2200),           // connamespace
                    Value::build_text(contype),      // contype
                    Value::from_i64(0),              // condeferrable
                    Value::from_i64(0),              // condeferred
                    Value::from_i64(1),              // convalidated
                    Value::from_i64(table_oid),      // conrelid
                    Value::from_i64(0),              // contypid
                    Value::from_i64(0),              // conindid
                    Value::from_i64(0),              // conparentid
                    Value::from_i64(0),              // confrelid
                    Value::Null,                     // confupdtype
                    Value::Null,                     // confdeltype
                    Value::Null,                     // confmatchtype
                    Value::from_i64(1),              // conislocal
                    Value::from_i64(0),              // coninhcount
                    Value::from_i64(0),              // connoinherit
                    if conkey.is_empty() {
                        Value::Null
                    } else {
                        Value::build_text(conkey)
                    }, // conkey
                    Value::Null,                     // confkey
                    Value::Null,                     // conpfeqop
                    Value::Null,                     // conppeqop
                    Value::Null,                     // conffeqop
                    Value::Null,                     // conexclop
                    Value::build_text(check_condition_text(conn, &chk.expr, btree)), // conbin
                ]);
                constraint_oid += 1;
            }
        }
        rows
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_attrdef
// ──────────────────────────────────────────────────────────────────────

const PG_ATTRDEF_SQL: &str = "CREATE TABLE pg_attrdef (oid INTEGER, adrelid INTEGER, adnum INTEGER, adbin TEXT, tableoid INTEGER HIDDEN)";

#[derive(Debug)]
struct PgAttrdefTable;

impl SnapshotRows for PgAttrdefTable {
    const SCHEMA: &'static str = PG_ATTRDEF_SQL;
    /// Fixed PostgreSQL OID of pg_attrdef, which describes column default expressions.
    /// Must match PostgreSQL for tableoid and pg_depend references used by pg_dump.
    const TABLE_OID: Option<i64> = Some(Self::CATALOG_OID);
    const ESTIMATED_COST: f64 = 100.0;
    const ESTIMATED_ROWS: u32 = 50;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let schema = conn.current_schema();
        let mut rows = Vec::new();

        let tables = user_tables_sorted(&schema);
        let tbl_oid_map = table_oid_map(&schema);

        // OID counter for pg_attrdef rows — start after constraint OIDs
        // Use a high base to avoid collisions
        let mut attrdef_oid: i64 = 50000;

        for (namespace, table_name, table) in &tables {
            let btree = match table.as_ref() {
                Table::BTree(bt) => bt,
                _ => continue,
            };
            let table_oid = tbl_oid_map
                .get(&(*namespace, (*table_name).to_string()))
                .copied()
                .unwrap_or(0);

            for (col_idx, col) in btree.columns().iter().enumerate() {
                if col.identity.is_some() {
                    continue;
                }
                let default = match (&col.default, col.generated_type()) {
                    (Some(default_expr), _) => default_expr.as_ref().clone(),
                    (None, GeneratedType::Stored { expr, .. }) => {
                        generated_expr_with_names(expr, btree.columns())
                            .expect("stored generated column expression resolves its columns")
                    }
                    (None, _) => continue,
                };
                // A default with a form the printer does not know is shown
                // as the engine's SQL.
                let adbin = crate::deparse::default_text(conn, &default, btree, &col.ty_str)
                    .unwrap_or_else(|| render_expr(conn, &default));
                rows.push(vec![
                    Value::from_i64(attrdef_oid),        // oid
                    Value::from_i64(table_oid),          // adrelid
                    Value::from_i64(col_idx as i64 + 1), // adnum (1-based)
                    Value::build_text(adbin),            // adbin
                ]);
                attrdef_oid += 1;
            }
        }
        rows
    }
}

#[derive(Debug)]
struct PgSequenceTable;

impl SnapshotRows for PgSequenceTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_sequence (
        seqrelid INTEGER, seqtypid INTEGER, seqstart BIGINT, seqincrement BIGINT,
        seqmax BIGINT, seqmin BIGINT, seqcache BIGINT, seqcycle BOOLEAN,
        tableoid INTEGER HIDDEN
    )";
    const TABLE_OID: Option<i64> = Some(Self::CATALOG_OID);
    const ESTIMATED_COST: f64 = 100.0;
    const ESTIMATED_ROWS: u32 = 10;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        sequence_catalog_entries(&conn.current_schema())
            .into_iter()
            .map(|(oid, _, seq)| {
                vec![
                    Value::from_i64(oid),
                    Value::from_i64(20),
                    Value::from_i64(seq.start_value),
                    Value::from_i64(seq.increment_by),
                    Value::from_i64(seq.max_value),
                    Value::from_i64(seq.min_value),
                    Value::from_i64(1),
                    Value::from_i64(i64::from(seq.cycle)),
                ]
            })
            .collect()
    }
}

/// The sequences the catalogs list, each with its stored oid and its
/// namespace, so a join on them agrees across catalogs.
fn sequence_catalog_entries(schema: &Schema) -> Vec<(i64, NamespaceId, &Sequence)> {
    let mut sequences = Vec::new();
    for namespace in namespaces_sorted(schema) {
        let mut named: Vec<_> = schema
            .sequences_by_name(namespace)
            .iter()
            .filter(|(name, _)| !name.starts_with(AUTOINCREMENT_SEQ_PREFIX))
            .collect();
        named.sort_by_key(|(name, _)| *name);
        sequences.extend(
            named
                .into_iter()
                .map(|(name, sequence)| (namespace, name, sequence.as_ref())),
        );
    }
    sequences
        .into_iter()
        .map(|(namespace, name, sequence)| {
            let oid = stored_relation_oid(schema, namespace, name).unwrap_or(0);
            (oid, namespace, sequence)
        })
        .collect()
}

/// Virtual table implementation for pg_catalog.pg_sequences
/// Reads sequence metadata from Schema.sequences at scan time.
#[derive(Debug)]
struct PgSequencesTable;

impl SnapshotRows for PgSequencesTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_sequences (
            schemaname TEXT,
            sequencename TEXT,
            sequenceowner TEXT,
            data_type TEXT,
            start_value INTEGER,
            min_value INTEGER,
            max_value INTEGER,
            increment_by INTEGER,
            cycle INTEGER,
            cache_size INTEGER,
            last_value INTEGER
        )";
    const TABLE_OID: Option<i64> = None;
    const ESTIMATED_COST: f64 = 100.0;
    const ESTIMATED_ROWS: u32 = 10;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();
        let schema = conn.current_schema();
        let namespaces = namespaces_sorted(&schema);
        for namespace in namespaces {
            let sequences = schema.sequences_by_name(namespace);
            let mut names: Vec<_> = sequences.keys().cloned().collect();
            names.sort();
            for name in names {
                let Some(seq) = sequences.get(&name) else {
                    continue;
                };
                let seq_name = seq.name.clone();
                // currval is per-connection; expose this connection's last
                // value via the connection currval map, falling back to start.
                let last_val = conn
                    .get_sequence_currval(MAIN_DB_ID, &mangle_name(namespace, &seq_name))
                    .unwrap_or(seq.start_value);
                let owner_name = relation_owner_name(conn, &schema, namespace, &seq_name);
                let may_read = conn.may_read_sequence(
                    NamespaceRef {
                        database_id: MAIN_DB_ID,
                        id: namespace,
                    },
                    &seq_name,
                );
                let last_value = if may_read {
                    Value::from_i64(last_val)
                } else {
                    Value::Null
                };
                rows.push(vec![
                    Value::build_text(schema_name_of(&schema, namespace).to_string()), // schemaname
                    Value::build_text(seq_name),           // sequencename
                    Value::build_text(owner_name),         // sequenceowner
                    Value::build_text("bigint"),           // data_type
                    Value::from_i64(seq.start_value),      // start_value
                    Value::from_i64(seq.min_value),        // min_value
                    Value::from_i64(seq.max_value),        // max_value
                    Value::from_i64(seq.increment_by),     // increment_by
                    Value::from_i64(i64::from(seq.cycle)), // cycle
                    Value::from_i64(1), // cache_size (PG default; Turso doesn't cache)
                    last_value,
                ]);
            }
        }
        rows
    }
}

pub(crate) fn register_catalog_modules(mut options: OpenOptions) -> OpenOptions {
    options = register_builtin_catalogs(options);
    options = options.native_module(
        "unnest",
        VTabKind::TableValuedFunction,
        crate::unnest::Unnest,
    );
    options = options.native_module(
        "pg_json_each",
        VTabKind::TableValuedFunction,
        crate::options::TwoColumnFunction {
            schema: "CREATE TABLE pg_json_each (key TEXT, value TEXT, input TEXT HIDDEN)",
            append_rows: crate::functions::exec_json_each,
        },
    );
    options = options.native_module(
        "pg_options_to_table",
        VTabKind::TableValuedFunction,
        crate::options::TwoColumnFunction {
            schema: "CREATE TABLE pg_options_to_table (option_name TEXT, option_value TEXT, options_array TEXT HIDDEN)",
            append_rows: crate::options::append_options,
        },
    );

    options = options.native_module(
        "pg_information_schema_schemata",
        VTabKind::TableValuedFunction,
        SnapshotCatalog::<SchemataTable>(PhantomData),
    );
    options = options.native_module(
        "pg_get_tabledef",
        VTabKind::TableValuedFunction,
        CatalogModule {
            schema: PgGetTableDefTable::schema,
            create: PgGetTableDefTable::new,
        },
    );

    options = options.native_module(
        "pg_input_error_info",
        VTabKind::TableValuedFunction,
        CatalogModule {
            schema: PgInputErrorInfoTable::schema,
            create: PgInputErrorInfoTable::new,
        },
    );

    options = options.native_module(
        "pg_extension",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable {
            create_sql: "CREATE TABLE pg_extension (
                oid INTEGER, extname TEXT, extowner INTEGER, extnamespace INTEGER,
                extrelocatable BOOLEAN, extversion TEXT, extconfig INTEGER[], extcondition TEXT[],
                tableoid INTEGER HIDDEN
            )"
            .to_string(),
        },
    );

    options = options.native_module(
        "pg_trigger",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_trigger (oid INTEGER, tgrelid INTEGER, tgname TEXT, tgfoid INTEGER, tgtype INTEGER, tgenabled TEXT, tgisinternal INTEGER, tgconstrrelid INTEGER, tgconstrindid INTEGER, tgconstraint INTEGER, tgdeferrable INTEGER, tginitdeferred INTEGER, tgnargs INTEGER, tgattr TEXT, tgargs TEXT, tgqual TEXT, tgoldtable TEXT, tgnewtable TEXT, tgparentid INTEGER, tableoid INTEGER HIDDEN)".to_string() },
    );
    options = options.native_module(
        "pg_statistic_ext",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_statistic_ext (oid INTEGER, stxrelid INTEGER, stxname TEXT, stxnamespace INTEGER, stxowner INTEGER, stxstattarget INTEGER, stxkeys TEXT, stxkind TEXT, stxexprs TEXT, tableoid INTEGER HIDDEN)".to_string() },
    );
    options = options.native_module(
        "pg_inherits",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_inherits (inhrelid INTEGER, inhparent INTEGER, inhseqno INTEGER, inhdetachpending INTEGER)".to_string() },
    );

    options = options.native_module(
        "pg_foreign_table",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable {
            create_sql:
                "CREATE TABLE pg_foreign_table (ftrelid INTEGER, ftserver INTEGER, ftoptions TEXT)"
                    .to_string(),
        },
    );
    options = options.native_module(
        "pg_partitioned_table",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_partitioned_table (partrelid INTEGER, partstrat TEXT, partnatts INTEGER, partdefid INTEGER, partattrs TEXT, partclass TEXT, partcollation TEXT, partexprs TEXT)".to_string() },
    );
    options = options.native_module(
        "pg_collation",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable {
            create_sql: "CREATE TABLE pg_collation (
                oid INTEGER, collname TEXT, collnamespace INTEGER, collowner INTEGER,
                collprovider TEXT, collisdeterministic INTEGER, collencoding INTEGER,
                collcollate TEXT, collctype TEXT, colliculocale TEXT, collicurules TEXT,
                collversion TEXT, tableoid INTEGER HIDDEN
            )"
            .to_string(),
        },
    );
    options = crate::catalog_table::register_catalog_tables(options);
    options = options.native_module(
        "pg_seclabels",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable {
            create_sql: "CREATE TABLE pg_seclabels (
                objoid INTEGER, classoid INTEGER, objsubid INTEGER, objtype TEXT,
                objnamespace INTEGER, objname TEXT, provider TEXT, label TEXT
            )"
            .to_string(),
        },
    );
    options = options.native_module(
        "pg_publication",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_publication (oid INTEGER, pubname TEXT, pubowner INTEGER, puballtables INTEGER, pubinsert INTEGER, pubupdate INTEGER, pubdelete INTEGER, pubtruncate INTEGER, pubviaroot INTEGER, tableoid INTEGER HIDDEN)".to_string() },
    );
    options = options.native_module(
        "pg_publication_namespace",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_publication_namespace (oid INTEGER, pnpubid INTEGER, pnnspid INTEGER, tableoid INTEGER HIDDEN)".to_string() },
    );
    options = options.native_module(
        "pg_publication_rel",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable { create_sql: "CREATE TABLE pg_publication_rel (oid INTEGER, prpubid INTEGER, prrelid INTEGER, prqual TEXT, prattrs TEXT, tableoid INTEGER HIDDEN)".to_string() },
    );

    options = options.native_module(
        "pg_tablespace",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable {
            create_sql: "CREATE TABLE pg_tablespace (
                oid INTEGER, spcname TEXT, spcowner INTEGER, spcacl TEXT[], spcoptions TEXT[],
                tableoid INTEGER HIDDEN
            )"
            .to_string(),
        },
    );
    options = options.native_module(
        "pg_init_privs",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable {
            create_sql: "CREATE TABLE pg_init_privs (
                objoid INTEGER, classoid INTEGER, objsubid INTEGER, privtype TEXT,
                initprivs TEXT[], tableoid INTEGER HIDDEN
            )"
            .to_string(),
        },
    );

    options = options.native_module(
        "pg_transform",
        VTabKind::TableValuedFunction,
        EmptyPgCatalogTable {
            create_sql: "CREATE TABLE pg_transform (
                oid INTEGER, trftype INTEGER, trflang INTEGER, trffromsql INTEGER,
                trftosql INTEGER, tableoid INTEGER HIDDEN
            )"
            .to_string(),
        },
    );

    for (name, create_sql) in [
        (
            "pg_opclass",
            "CREATE TABLE pg_opclass (
            oid INTEGER, opcmethod INTEGER, opcname TEXT, opcnamespace INTEGER,
            opcowner INTEGER, opcfamily INTEGER, opcintype INTEGER, opcdefault BOOLEAN,
            opckeytype INTEGER, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_opfamily",
            "CREATE TABLE pg_opfamily (
            oid INTEGER, opfmethod INTEGER, opfname TEXT, opfnamespace INTEGER,
            opfowner INTEGER, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_ts_parser",
            "CREATE TABLE pg_ts_parser (
            oid INTEGER, prsname TEXT, prsnamespace INTEGER, prsstart INTEGER,
            prstoken INTEGER, prsend INTEGER, prsheadline INTEGER, prslextype INTEGER,
            tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_ts_template",
            "CREATE TABLE pg_ts_template (
            oid INTEGER, tmplname TEXT, tmplnamespace INTEGER, tmplinit INTEGER,
            tmpllexize INTEGER, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_ts_dict",
            "CREATE TABLE pg_ts_dict (
            oid INTEGER, dictname TEXT, dictnamespace INTEGER, dictowner INTEGER,
            dicttemplate INTEGER, dictinitoption TEXT, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_ts_config",
            "CREATE TABLE pg_ts_config (
            oid INTEGER, cfgname TEXT, cfgnamespace INTEGER, cfgowner INTEGER,
            cfgparser INTEGER, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_foreign_data_wrapper",
            "CREATE TABLE pg_foreign_data_wrapper (
            oid INTEGER, fdwname TEXT, fdwowner INTEGER, fdwhandler INTEGER,
            fdwvalidator INTEGER, fdwacl TEXT[], fdwoptions TEXT[], tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_foreign_server",
            "CREATE TABLE pg_foreign_server (
            oid INTEGER, srvname TEXT, srvowner INTEGER, srvfdw INTEGER, srvtype TEXT,
            srvversion TEXT, srvacl TEXT[], srvoptions TEXT[], tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_default_acl",
            "CREATE TABLE pg_default_acl (
            oid INTEGER, defaclrole INTEGER, defaclnamespace INTEGER, defaclobjtype TEXT,
            defaclacl TEXT[], tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_conversion",
            "CREATE TABLE pg_conversion (
            oid INTEGER, conname TEXT, connamespace INTEGER, conowner INTEGER,
            conforencoding INTEGER, contoencoding INTEGER, conproc INTEGER,
            condefault BOOLEAN, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_range",
            "CREATE TABLE pg_range (
            rngtypid INTEGER, rngsubtype INTEGER, rngmultitypid INTEGER,
            rngcollation INTEGER, rngsubopc INTEGER, rngcanonical INTEGER,
            rngsubdiff INTEGER, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_event_trigger",
            "CREATE TABLE pg_event_trigger (
            oid INTEGER, evtname TEXT, evtevent TEXT, evtowner INTEGER, evtfoid INTEGER,
            evtenabled TEXT, evttags TEXT[], tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_subscription",
            "CREATE TABLE pg_subscription (
            oid INTEGER, subdbid INTEGER, subskiplsn TEXT, subname TEXT, subowner INTEGER,
            subenabled BOOLEAN, subbinary BOOLEAN, substream TEXT, subtwophasestate TEXT,
            subdisableonerr BOOLEAN, subpasswordrequired BOOLEAN, subrunasowner BOOLEAN,
            subconninfo TEXT, subslotname TEXT, subsynccommit TEXT, subpublications TEXT[],
            suborigin TEXT, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_largeobject_metadata",
            "CREATE TABLE pg_largeobject_metadata (
            oid INTEGER, lomowner INTEGER, lomacl TEXT[], tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_amop",
            "CREATE TABLE pg_amop (
            oid INTEGER, amopfamily INTEGER, amoplefttype INTEGER, amoprighttype INTEGER,
            amopstrategy INTEGER, amoppurpose TEXT, amopopr INTEGER, amopmethod INTEGER,
            amopsortfamily INTEGER, tableoid INTEGER HIDDEN
        )",
        ),
        (
            "pg_amproc",
            "CREATE TABLE pg_amproc (
            oid INTEGER, amprocfamily INTEGER, amproclefttype INTEGER, amprocrighttype INTEGER,
            amprocnum INTEGER, amproc INTEGER, tableoid INTEGER HIDDEN
        )",
        ),
    ] {
        options = options.native_module(
            name,
            VTabKind::TableValuedFunction,
            EmptyPgCatalogTable {
                create_sql: create_sql.to_owned(),
            },
        );
    }
    options
}

#[derive(Debug)]
struct PgLanguageTable;

impl SnapshotRows for PgLanguageTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_language (
        oid INTEGER, lanname TEXT, lanowner INTEGER, lanispl BOOLEAN,
        lanpltrusted BOOLEAN, lanplcallfoid INTEGER, laninline INTEGER,
        lanvalidator INTEGER, lanacl TEXT[], tableoid INTEGER HIDDEN
    )";
    const TABLE_OID: Option<i64> = Some(Self::CATALOG_OID);
    const ESTIMATED_COST: f64 = 1.0;
    const ESTIMATED_ROWS: u32 = 2;

    fn load_rows(_conn: &Connection) -> Vec<Vec<Value>> {
        [(12, "internal"), (13, "c")]
            .into_iter()
            .map(|(oid, name)| {
                vec![
                    Value::from_i64(oid),
                    Value::build_text(name),
                    Value::from_i64(10),
                    Value::from_i64(0),
                    Value::from_i64(0),
                    Value::from_i64(0),
                    Value::from_i64(0),
                    Value::from_i64(0),
                    Value::Null,
                ]
            })
            .collect()
    }
}

#[derive(Debug)]
struct PgOperatorTable;

impl SnapshotRows for PgOperatorTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_operator (
        oid INTEGER, oprname TEXT, oprnamespace INTEGER, oprowner INTEGER,
        oprkind TEXT, oprcanmerge BOOLEAN, oprcanhash BOOLEAN, oprleft INTEGER,
        oprright INTEGER, oprresult INTEGER, oprcom INTEGER, oprnegate INTEGER,
        oprcode INTEGER, oprrest INTEGER, oprjoin INTEGER, tableoid INTEGER HIDDEN
    )";
    const TABLE_OID: Option<i64> = Some(Self::CATALOG_OID);
    const ESTIMATED_COST: f64 = 10.0;
    const ESTIMATED_ROWS: u32 = 200;

    fn load_rows(_conn: &Connection) -> Vec<Vec<Value>> {
        // FIXME(pg-catalog): pg_operator has 216 of PostgreSQL 18's 799 rows. The rest need types the frontend does not have yet (interval, time, bytea, json, jsonb, inet, arrays) and operators it does not parse (`^`, `@`, `~~`, `#`).
        // FIXME(pg-catalog): oprcode, oprrest and oprjoin are 0, because pg_proc does not list PostgreSQL's functions under PostgreSQL's oids. The oids of the pg_proc rows replace this.
        const NO_FUNCTION: i64 = 0;
        crate::operators::OPERATORS
            .iter()
            .map(|operator| {
                let kind = if operator.left == 0 { "l" } else { "b" };
                vec![
                    Value::from_i64(operator.oid),
                    Value::build_text(operator.name),
                    Value::from_i64(11),
                    Value::from_i64(SUPERUSER_OID),
                    Value::build_text(kind),
                    Value::from_i64(i64::from(operator.can_merge)),
                    Value::from_i64(i64::from(operator.can_hash)),
                    Value::from_i64(operator.left),
                    Value::from_i64(operator.right),
                    Value::from_i64(operator.result),
                    Value::from_i64(operator.commutator),
                    Value::from_i64(operator.negator),
                    Value::from_i64(NO_FUNCTION),
                    Value::from_i64(NO_FUNCTION),
                    Value::from_i64(NO_FUNCTION),
                ]
            })
            .collect()
    }
}

#[derive(Debug)]
struct PgCastTable;

impl SnapshotRows for PgCastTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_cast (
        oid INTEGER, castsource INTEGER, casttarget INTEGER, castfunc INTEGER,
        castcontext TEXT, castmethod TEXT, tableoid INTEGER HIDDEN
    )";
    const TABLE_OID: Option<i64> = Some(Self::CATALOG_OID);
    const ESTIMATED_COST: f64 = 1.0;
    const ESTIMATED_ROWS: u32 = 50;

    fn load_rows(_conn: &Connection) -> Vec<Vec<Value>> {
        crate::casts::cast_rows()
            .enumerate()
            .map(|(index, row)| {
                vec![
                    Value::from_i64(crate::casts::FIRST_CAST_OID + index as i64),
                    Value::from_i64(row.source.oid()),
                    Value::from_i64(row.target.oid()),
                    Value::from_i64(row.function),
                    Value::build_text(row.context),
                    Value::build_text(row.method),
                ]
            })
            .collect()
    }
}

#[derive(Debug)]
struct PgSettingsTable;

impl SnapshotRows for PgSettingsTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_settings (
        name TEXT,
        setting TEXT,
        unit TEXT,
        category TEXT,
        short_desc TEXT,
        extra_desc TEXT,
        context TEXT,
        vartype TEXT,
        source TEXT,
        min_val TEXT,
        max_val TEXT,
        enumvals TEXT[],
        boot_val TEXT,
        reset_val TEXT,
        sourcefile TEXT,
        sourceline INTEGER,
        pending_restart BOOLEAN
    )";
    const TABLE_OID: Option<i64> = None;
    const ESTIMATED_COST: f64 = 1.0;
    const ESTIMATED_ROWS: u32 = 1;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        use crate::session::{search_path_setting, DEFAULT_SEARCH_PATH};

        let setting = search_path_setting(conn);
        let source = if setting.is_some() {
            "session"
        } else {
            "default"
        };
        vec![vec![
            Value::build_text("search_path"),
            Value::build_text(setting.unwrap_or_else(|| DEFAULT_SEARCH_PATH.to_owned())),
            Value::Null,
            Value::build_text("Client Connection Defaults / Statement Behavior"),
            Value::build_text(
                "Sets the schema search order for names that are not schema-qualified.",
            ),
            Value::Null,
            Value::build_text("user"),
            Value::build_text("string"),
            Value::build_text(source),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::build_text(DEFAULT_SEARCH_PATH),
            Value::build_text(DEFAULT_SEARCH_PATH),
            Value::Null,
            Value::Null,
            Value::from_i64(0),
        ]]
    }
}

#[derive(Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
enum DependencyKind {
    #[strum(serialize = "n")]
    Normal,
    #[strum(serialize = "a")]
    Auto,
    #[strum(serialize = "i")]
    Internal,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct ObjectAddress {
    catalog_oid: i64,
    object_oid: i64,
    column_number: i64,
}

struct RelationRow {
    oid: i64,
    namespace_oid: i64,
    access_method_oid: i64,
    kind: RelationKind,
}

struct ConstraintRow<'a> {
    oid: i64,
    kind: ConstraintKind,
    table_oid: i64,
    index_oid: i64,
    referenced_table_oid: i64,
    column_numbers: Option<&'a str>,
    referenced_column_numbers: Option<&'a str>,
}

struct IndexRow<'a> {
    oid: i64,
    table_oid: i64,
    is_unique: bool,
    column_numbers: Option<&'a str>,
}

struct ColumnDefaultRow {
    oid: i64,
    table_oid: i64,
    column_number: i64,
}

#[derive(Debug)]
struct PgDependTable;

impl SnapshotRows for PgDependTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_depend (
        classid INTEGER, objid INTEGER, objsubid INTEGER,
        refclassid INTEGER, refobjid INTEGER, refobjsubid INTEGER, deptype TEXT,
        tableoid INTEGER HIDDEN
    )";
    /// Fixed PostgreSQL OID of pg_depend, which records dependencies between objects.
    /// Must match PostgreSQL for tableoid and pg_depend references used by pg_dump.
    const TABLE_OID: Option<i64> = Some(Self::CATALOG_OID);
    const ESTIMATED_COST: f64 = 1000.0;
    const ESTIMATED_ROWS: u32 = 100;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();
        let constraint_rows = PgConstraintTable::load_rows(conn);
        let constraints = constraint_rows
            .iter()
            .map(Vec::as_slice)
            .map(ConstraintRow::from_row);
        let index_rows = PgIndexTable::load_rows(conn);
        let indexes = index_rows.iter().map(Vec::as_slice).map(IndexRow::from_row);
        let relation_rows = PgClassTable::load_rows(conn);
        let relations = relation_rows
            .iter()
            .map(Vec::as_slice)
            .map(RelationRow::from_row);

        // Report each table's normal dependencies on its schema and access method.
        for relation in relations.clone() {
            if !matches!(relation.kind, RelationKind::Table) {
                continue;
            }
            rows.push(dependency_row(
                ObjectAddress::whole::<PgClassTable>(relation.oid),
                ObjectAddress::whole::<PgNamespaceTable>(relation.namespace_oid),
                DependencyKind::Normal,
            ));
            if relation.access_method_oid != PG_INVALID_OID {
                rows.push(dependency_row(
                    ObjectAddress::whole::<PgClassTable>(relation.oid),
                    ObjectAddress::whole::<PgAmTable>(relation.access_method_oid),
                    DependencyKind::Normal,
                ));
            }
        }

        // Match indexes to their owning PK/UNIQUE constraints for internal dependencies.
        // Other indexes have automatic dependencies on their indexed columns.
        for index in indexes.clone() {
            let columns = catalog_column_numbers(index.column_numbers);
            let constraint = constraints.clone().find(|constraint| {
                matches!(
                    constraint.kind,
                    ConstraintKind::PrimaryKey | ConstraintKind::Unique
                ) && constraint.index_oid == index.oid
            });
            if let Some(constraint) = constraint {
                rows.push(dependency_row(
                    ObjectAddress::whole::<PgClassTable>(index.oid),
                    ObjectAddress::whole::<PgConstraintTable>(constraint.oid),
                    DependencyKind::Internal,
                ));
            } else {
                // Emit one automatic dependency row per indexed column.
                for column_number in columns {
                    rows.push(dependency_row(
                        ObjectAddress::whole::<PgClassTable>(index.oid),
                        ObjectAddress::column(index.table_oid, column_number),
                        DependencyKind::Auto,
                    ));
                }
            }
        }

        // Report constraints' dependencies on local columns and foreign-key references.
        for constraint in constraints {
            // Use automatic dependencies for local columns, or the whole table if none are listed.
            for column_number in catalog_column_numbers(constraint.column_numbers) {
                rows.push(dependency_row(
                    ObjectAddress::whole::<PgConstraintTable>(constraint.oid),
                    ObjectAddress::column(constraint.table_oid, column_number),
                    DependencyKind::Auto,
                ));
            }
            if constraint.referenced_table_oid != PG_INVALID_OID {
                let referenced_columns =
                    catalog_column_numbers(constraint.referenced_column_numbers);
                // Foreign keys have normal dependencies on their referenced columns.
                for &column_number in &referenced_columns {
                    rows.push(dependency_row(
                        ObjectAddress::whole::<PgConstraintTable>(constraint.oid),
                        ObjectAddress::column(constraint.referenced_table_oid, column_number),
                        DependencyKind::Normal,
                    ));
                }
                // Also report a normal dependency on a matching unique index for the referenced key.
                if let Some(index) = indexes.clone().find(|index| {
                    index.table_oid == constraint.referenced_table_oid
                        && index.is_unique
                        && catalog_column_numbers(index.column_numbers) == referenced_columns
                }) {
                    rows.push(dependency_row(
                        ObjectAddress::whole::<PgConstraintTable>(constraint.oid),
                        ObjectAddress::whole::<PgClassTable>(index.oid),
                        DependencyKind::Normal,
                    ));
                }
            }
        }

        let default_rows = PgAttrdefTable::load_rows(conn);
        let defaults = default_rows
            .iter()
            .map(Vec::as_slice)
            .map(ColumnDefaultRow::from_row);
        // Default expressions automatically depend on the column that owns them.
        for default in defaults {
            rows.push(dependency_row(
                ObjectAddress::whole::<PgAttrdefTable>(default.oid),
                ObjectAddress::column(default.table_oid, default.column_number),
                DependencyKind::Auto,
            ));
        }

        // A sequence owned by a column depends on it: internally for identity columns, automatically otherwise.
        let schema = conn.current_schema();
        rows.extend(
            view_rule_dependencies(&schema)
                .into_iter()
                .map(|dependency| {
                    dependency_row(dependency.dependent, dependency.referenced, dependency.kind)
                }),
        );
        for (namespace, table_name, table, table_oid) in user_tables_with_oids(&schema) {
            for (index, column) in table.columns().iter().enumerate() {
                let Some(column_name) = column.name.as_deref() else {
                    continue;
                };
                for sequence_name in crate::classes::owned_sequences(
                    &schema,
                    namespace,
                    table_name,
                    Some(column_name),
                ) {
                    let Some(sequence_oid) =
                        stored_relation_oid(&schema, namespace, &sequence_name)
                    else {
                        continue;
                    };
                    rows.push(dependency_row(
                        ObjectAddress::whole::<PgClassTable>(sequence_oid),
                        ObjectAddress::column(table_oid, index as i64 + 1),
                        if column.identity.is_some() {
                            DependencyKind::Internal
                        } else {
                            DependencyKind::Auto
                        },
                    ));
                }
            }
        }
        rows
    }
}

/// One row of pg_depend: `dependent` depends on `referenced`.
struct Dependency {
    dependent: ObjectAddress,
    referenced: ObjectAddress,
    kind: DependencyKind,
}

/// The pg_depend rows of the views' `_RETURN` rules: each rule is part of
/// its view, and depends on every table and view that the view reads.
fn view_rule_dependencies(schema: &Schema) -> Vec<Dependency> {
    let mut dependencies = Vec::new();
    for rule in view_rules(schema) {
        let rule_address = ObjectAddress::rule(rule.oid);
        dependencies.push(Dependency {
            dependent: rule_address,
            referenced: ObjectAddress::whole::<PgClassTable>(rule.view_oid),
            kind: DependencyKind::Internal,
        });
        let mut read_oids: Vec<i64> = schema
            .referenced_tables_of(rule.namespace, &rule.view.select_stmt)
            .iter()
            .filter_map(|(namespace, name)| stored_relation_oid(schema, *namespace, name))
            .collect();
        read_oids.sort_unstable();
        read_oids.dedup();
        for read_oid in read_oids {
            dependencies.push(Dependency {
                dependent: rule_address,
                referenced: ObjectAddress::whole::<PgClassTable>(read_oid),
                kind: DependencyKind::Normal,
            });
        }
    }
    dependencies
}

/// The views that depend on the table or view `name` of `namespace` in
/// pg_depend, directly or through other views, each before the relations it
/// reads.
pub(crate) fn relation_dependents(
    schema: &Schema,
    namespace: NamespaceId,
    name: &str,
) -> Vec<Dependent> {
    fn visit(
        relation_oid: i64,
        dependencies: &[Dependency],
        classes: &crate::classes::ClassCatalog,
        seen: &mut HashSet<i64>,
        dependents: &mut Vec<Dependent>,
    ) {
        let Some(relation) = classes.get_by_oid(relation_oid) else {
            return;
        };
        let rules_that_read = dependencies.iter().filter(|dependency| {
            dependency.kind == DependencyKind::Normal
                && dependency.referenced == ObjectAddress::whole::<PgClassTable>(relation_oid)
                && dependency.dependent.catalog_oid == PG_REWRITE_OID
        });
        let mut view_oids: Vec<i64> = rules_that_read
            .filter_map(|read| {
                dependencies.iter().find(|dependency| {
                    dependency.kind == DependencyKind::Internal
                        && dependency.dependent == read.dependent
                })
            })
            .map(|rule_of_view| rule_of_view.referenced.object_oid)
            .collect();
        view_oids.sort_unstable();
        for view_oid in view_oids {
            let Some(view) = classes.get_by_oid(view_oid) else {
                continue;
            };
            if !seen.insert(view_oid) {
                continue;
            }
            visit(view_oid, dependencies, classes, seen, dependents);
            dependents.push(Dependent {
                object: DependentObject::View {
                    namespace: view.relnamespace,
                    name: view.relname.clone(),
                },
                depends_on: (relation.relnamespace, relation.relname.clone()),
            });
        }
    }

    let Some(classes) = crate::classes::class_catalog(schema) else {
        return Vec::new();
    };
    let Some(relation) = classes.get_by_name(namespace, name) else {
        return Vec::new();
    };
    let mut seen = HashSet::default();
    seen.insert(relation.oid);
    let mut dependents = Vec::new();
    visit(
        relation.oid,
        &view_rule_dependencies(schema),
        classes,
        &mut seen,
        &mut dependents,
    );
    dependents
}

/// Fixed PostgreSQL OID of pg_rewrite, which has the rewrite rules of views.
const PG_REWRITE_OID: i64 = 2618;

#[derive(Debug)]
struct PgRewriteTable;

impl SnapshotRows for PgRewriteTable {
    const SCHEMA: &'static str = "CREATE TABLE pg_rewrite (oid INTEGER, rulename TEXT, ev_class INTEGER, ev_type TEXT, ev_enabled TEXT, is_instead BOOLEAN, ev_qual TEXT, ev_action TEXT, tableoid INTEGER HIDDEN)";
    const TABLE_OID: Option<i64> = Some(Self::CATALOG_OID);
    const ESTIMATED_COST: f64 = 100.0;
    const ESTIMATED_ROWS: u32 = 10;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>> {
        // FIXME(pg-rewrite): PostgreSQL's _RETURN is an enabled INSTEAD
        // SELECT rule with no qualification (<>). The action is SQL here;
        // a PostgreSQL query-tree representation would replace it.
        view_rules(&conn.current_schema())
            .into_iter()
            .map(|rule| {
                vec![
                    Value::from_i64(rule.oid),
                    Value::build_text("_RETURN"),
                    Value::from_i64(rule.view_oid),
                    Value::build_text("1"),
                    Value::build_text("O"),
                    Value::from_i64(1),
                    Value::build_text("<>"),
                    Value::build_text(rule.view.select_stmt.to_string()),
                ]
            })
            .collect()
    }
}

/// The `_RETURN` rule of a view, also described by pg_depend.
struct ViewRule<'a> {
    oid: i64,
    view_oid: i64,
    namespace: NamespaceId,
    view: &'a turso_core::schema::View,
}

/// The rules of the views that have a class row, ordered by oid.
fn view_rules(schema: &Schema) -> Vec<ViewRule<'_>> {
    let mut rules: Vec<ViewRule> = schema
        .namespace_ids()
        .flat_map(|namespace| {
            schema.views(namespace).filter_map(move |(name, view)| {
                let view_oid = stored_relation_oid(schema, namespace, name)?;
                Some(ViewRule {
                    oid: view_oid + VIEW_RULE_OID_OFFSET,
                    view_oid,
                    namespace,
                    view,
                })
            })
        })
        .collect();
    rules.sort_by_key(|rule| rule.oid);
    rules
}

/// What is added to the oid of a view to make the oid of its `_RETURN` rule.
/// The oids of relations stay below `CONSTRAINT_OID_START`.
const VIEW_RULE_OID_OFFSET: i64 = 3 << 30;

fn catalog_column_numbers(text: Option<&str>) -> Vec<i64> {
    match text {
        None => vec![WHOLE_OBJECT_COLUMN_NUMBER],
        Some(text) => {
            let mut columns: Vec<i64> = text
                .split_ascii_whitespace()
                .map(|column| column.parse().expect("catalog column numbers are integers"))
                .collect();
            if columns.is_empty() {
                columns.push(WHOLE_OBJECT_COLUMN_NUMBER);
            }
            columns.sort_unstable();
            columns.dedup();
            columns
        }
    }
}

fn dependency_row(
    dependent: ObjectAddress,
    referenced: ObjectAddress,
    kind: DependencyKind,
) -> Vec<Value> {
    let kind: &'static str = kind.into();
    vec![
        Value::from_i64(dependent.catalog_oid),
        Value::from_i64(dependent.object_oid),
        Value::from_i64(dependent.column_number),
        Value::from_i64(referenced.catalog_oid),
        Value::from_i64(referenced.object_oid),
        Value::from_i64(referenced.column_number),
        Value::build_text(kind),
    ]
}

impl ObjectAddress {
    const fn whole<T: SnapshotRows>(object_oid: i64) -> Self {
        Self {
            catalog_oid: T::TABLE_OID.expect("dependency catalogs have OIDs"),
            object_oid,
            column_number: WHOLE_OBJECT_COLUMN_NUMBER,
        }
    }

    const fn rule(rule_oid: i64) -> Self {
        Self {
            catalog_oid: PG_REWRITE_OID,
            object_oid: rule_oid,
            column_number: WHOLE_OBJECT_COLUMN_NUMBER,
        }
    }

    const fn column(table_oid: i64, column_number: i64) -> Self {
        Self {
            column_number,
            ..Self::whole::<PgClassTable>(table_oid)
        }
    }
}

impl RelationRow {
    // Zero-based positions in PgClassTable's SQL column order.
    const OID_COLUMN: usize = 0;
    const NAMESPACE_OID_COLUMN: usize = 2;
    const ACCESS_METHOD_OID_COLUMN: usize = 6;
    const KIND_COLUMN: usize = 17;

    fn from_row(row: &[Value]) -> Self {
        Self {
            oid: row[Self::OID_COLUMN]
                .as_int()
                .expect("catalog OIDs are integers"),
            namespace_oid: row[Self::NAMESPACE_OID_COLUMN]
                .as_int()
                .expect("catalog OIDs are integers"),
            access_method_oid: row[Self::ACCESS_METHOD_OID_COLUMN]
                .as_int()
                .expect("catalog OIDs are integers"),
            kind: crate::classes::relation_kind_of_letter(
                row[Self::KIND_COLUMN]
                    .to_text()
                    .expect("catalog relation kinds are text"),
            )
            .expect("catalog relation kinds are valid"),
        }
    }
}

impl<'a> ConstraintRow<'a> {
    // Zero-based positions in PgConstraintTable's SQL column order.
    const OID_COLUMN: usize = 0;
    const KIND_COLUMN: usize = 3;
    const TABLE_OID_COLUMN: usize = 7;
    const INDEX_OID_COLUMN: usize = 9;
    const REFERENCED_TABLE_OID_COLUMN: usize = 11;
    const COLUMN_NUMBERS_COLUMN: usize = 18;
    const REFERENCED_COLUMN_NUMBERS_COLUMN: usize = 19;

    fn from_row(row: &'a [Value]) -> Self {
        Self {
            oid: row[Self::OID_COLUMN]
                .as_int()
                .expect("catalog OIDs are integers"),
            kind: row[Self::KIND_COLUMN]
                .to_text()
                .expect("catalog constraint kinds are text")
                .parse()
                .expect("catalog constraint kinds are valid"),
            table_oid: row[Self::TABLE_OID_COLUMN]
                .as_int()
                .expect("catalog OIDs are integers"),
            index_oid: row[Self::INDEX_OID_COLUMN]
                .as_int()
                .expect("catalog OIDs are integers"),
            referenced_table_oid: row[Self::REFERENCED_TABLE_OID_COLUMN]
                .as_int()
                .expect("catalog OIDs are integers"),
            column_numbers: catalog_column_text(&row[Self::COLUMN_NUMBERS_COLUMN]),
            referenced_column_numbers: catalog_column_text(
                &row[Self::REFERENCED_COLUMN_NUMBERS_COLUMN],
            ),
        }
    }
}

impl<'a> IndexRow<'a> {
    // Zero-based positions in PgIndexTable's SQL column order.
    const OID_COLUMN: usize = 0;
    const TABLE_OID_COLUMN: usize = 1;
    const IS_UNIQUE_COLUMN: usize = 4;
    const COLUMN_NUMBERS_COLUMN: usize = 15;

    fn from_row(row: &'a [Value]) -> Self {
        Self {
            oid: row[Self::OID_COLUMN]
                .as_int()
                .expect("catalog OIDs are integers"),
            table_oid: row[Self::TABLE_OID_COLUMN]
                .as_int()
                .expect("catalog OIDs are integers"),
            is_unique: row[Self::IS_UNIQUE_COLUMN]
                .as_int()
                .expect("catalog index uniqueness is an integer")
                == CATALOG_BOOLEAN_TRUE,
            column_numbers: catalog_column_text(&row[Self::COLUMN_NUMBERS_COLUMN]),
        }
    }
}

impl ColumnDefaultRow {
    // Zero-based positions in PgAttrdefTable's SQL column order.
    const OID_COLUMN: usize = 0;
    const TABLE_OID_COLUMN: usize = 1;
    const COLUMN_NUMBER_COLUMN: usize = 2;

    fn from_row(row: &[Value]) -> Self {
        Self {
            oid: row[Self::OID_COLUMN]
                .as_int()
                .expect("catalog OIDs are integers"),
            table_oid: row[Self::TABLE_OID_COLUMN]
                .as_int()
                .expect("catalog OIDs are integers"),
            column_number: row[Self::COLUMN_NUMBER_COLUMN]
                .as_int()
                .expect("catalog column numbers are integers"),
        }
    }
}

fn catalog_column_text(value: &Value) -> Option<&str> {
    match value {
        Value::Null => None,
        Value::Text(text) => Some(text.as_str()),
        _ => unreachable!("catalog column lists are text or null"),
    }
}

trait SnapshotRows: Debug + Send + Sync + 'static {
    const SCHEMA: &'static str;
    const TABLE_OID: Option<i64>;
    const ESTIMATED_COST: f64;
    const ESTIMATED_ROWS: u32;

    fn load_rows(conn: &Connection) -> Vec<Vec<Value>>;
}

#[derive(Debug)]
struct SnapshotCatalog<T>(PhantomData<T>);

impl<T: SnapshotRows> VirtualTableModule for SnapshotCatalog<T> {
    type Table = Self;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        Ok(T::SCHEMA.to_string())
    }

    fn create(&self, _args: &[Value]) -> Result<Self::Table> {
        Ok(Self(PhantomData))
    }

    fn innocuous(&self) -> bool {
        true
    }
}

impl<T: SnapshotRows> VirtualTable for SnapshotCatalog<T> {
    type Cursor = SnapshotCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(SnapshotCursor {
            conn,
            load_rows: T::load_rows,
            table_oid: T::TABLE_OID,
            rows: Vec::new(),
            current_row: 0,
        })
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        let constraint_usages = constraints
            .iter()
            .map(|_| turso_ext::ConstraintUsage {
                argv_index: None,
                omit: false,
            })
            .collect();

        Ok(IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: T::ESTIMATED_COST,
            estimated_rows: T::ESTIMATED_ROWS,
            constraint_usages,
        })
    }
}

struct SnapshotCursor {
    conn: Arc<Connection>,
    load_rows: fn(&Connection) -> Vec<Vec<Value>>,
    table_oid: Option<i64>,
    rows: Vec<Vec<Value>>,
    current_row: usize,
}

impl VirtualTableCursor for SnapshotCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.current_row += 1;
        Ok(IOResult::Done(self.current_row < self.rows.len()))
    }

    fn rowid(&self) -> i64 {
        self.current_row as i64
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        if self.current_row < self.rows.len() {
            if column < self.rows[self.current_row].len() {
                return Ok(IOResult::Done(self.rows[self.current_row][column].clone()));
            }
            if column == self.rows[self.current_row].len() {
                if let Some(table_oid) = self.table_oid {
                    return Ok(IOResult::Done(Value::from_i64(table_oid)));
                }
            }
        }
        Ok(IOResult::Done(Value::Null))
    }

    fn filter(
        &mut self,
        _args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> turso_core::types::IOResultOr<bool> {
        self.current_row = 0;
        self.rows = (self.load_rows)(&self.conn);
        Ok(IOResult::Done(!self.rows.is_empty()))
    }
}

#[derive(Debug)]
struct CatalogModule<T> {
    schema: fn() -> String,
    create: fn() -> T,
}

impl<T> VirtualTableModule for CatalogModule<T>
where
    T: VirtualTable + 'static,
{
    type Table = T;

    fn schema(&self, _args: &[Value]) -> Result<String> {
        Ok((self.schema)())
    }

    fn create(&self, _args: &[Value]) -> Result<Self::Table> {
        Ok((self.create)())
    }

    fn innocuous(&self) -> bool {
        true
    }
}

/// Table-valued function: `pg_input_error_info(input TEXT, type TEXT)`
///
/// Returns one row with columns (message, detail, hint, sql_error_code).
/// If the input is valid for the given type, all columns are NULL.
/// If invalid, message and sql_error_code describe the error.
#[derive(Debug)]
struct PgInputErrorInfoTable;

impl PgInputErrorInfoTable {
    fn new() -> Self {
        Self
    }

    fn schema() -> String {
        "CREATE TABLE pg_input_error_info (
            message TEXT,
            detail TEXT,
            hint TEXT,
            sql_error_code TEXT,
            input TEXT HIDDEN,
            type_name TEXT HIDDEN
        )"
        .to_string()
    }
}

struct PgInputErrorInfoCursor {
    conn: Arc<Connection>,
    row: Option<[Value; 4]>,
    returned: bool,
}

impl PgInputErrorInfoCursor {
    fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            row: None,
            returned: false,
        }
    }
}

impl VirtualTableCursor for PgInputErrorInfoCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.returned = true;
        Ok(IOResult::Done(false))
    }

    fn rowid(&self) -> i64 {
        0
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        match &self.row {
            Some(row) if column < 4 => Ok(IOResult::Done(row[column].clone())),
            _ => Ok(IOResult::Done(Value::Null)),
        }
    }

    fn filter(
        &mut self,
        args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> turso_core::types::IOResultOr<bool> {
        self.returned = false;

        if args.len() < 2 {
            // Not enough arguments — return one row of NULLs
            self.row = Some([Value::Null, Value::Null, Value::Null, Value::Null]);
            return Ok(IOResult::Done(true));
        }

        let input = match &args[0] {
            Value::Text(t) => t.as_str().to_string(),
            Value::Null => {
                self.row = Some([Value::Null, Value::Null, Value::Null, Value::Null]);
                return Ok(IOResult::Done(true));
            }
            v => v.to_string(),
        };

        let type_name = match &args[1] {
            Value::Text(t) => t.as_str().to_string(),
            _ => {
                self.row = Some([Value::Null, Value::Null, Value::Null, Value::Null]);
                return Ok(IOResult::Done(true));
            }
        };

        self.row = Some(match validate_pg_input(&self.conn, &input, &type_name)? {
            Some((message, code)) => [
                Value::build_text(message),
                Value::Null,
                Value::Null,
                Value::build_text(code),
            ],
            None => [Value::Null, Value::Null, Value::Null, Value::Null],
        });

        Ok(IOResult::Done(true))
    }
}

impl VirtualTable for PgInputErrorInfoTable {
    type Cursor = PgInputErrorInfoCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgInputErrorInfoCursor::new(conn))
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        use turso_ext::{ConstraintOp, ConstraintUsage};

        let mut usages = vec![
            ConstraintUsage {
                argv_index: None,
                omit: false,
            };
            constraints.len()
        ];

        // Hidden columns: input (col 4) and type_name (col 5)
        let mut input_idx = None;
        let mut type_idx = None;
        for (i, c) in constraints.iter().enumerate() {
            if c.op != ConstraintOp::Eq || !c.usable {
                continue;
            }
            match c.column_index as usize {
                4 => input_idx = Some(i),
                5 => type_idx = Some(i),
                _ => {}
            }
        }

        if let Some(i) = input_idx {
            usages[i] = ConstraintUsage {
                argv_index: Some(1),
                omit: true,
            };
        }
        if let Some(i) = type_idx {
            usages[i] = ConstraintUsage {
                argv_index: Some(2),
                omit: true,
            };
        }

        Ok(IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1.0,
            estimated_rows: 1,
            constraint_usages: usages,
        })
    }
}

/// Virtual table for getting PostgreSQL-compatible CREATE TABLE statements
#[derive(Debug)]
struct PgGetTableDefTable;

impl PgGetTableDefTable {
    fn new() -> Self {
        Self
    }

    fn schema() -> String {
        "CREATE TABLE pg_get_tabledef (
            schema_name TEXT,
            table_name TEXT,
            ddl TEXT
        )"
        .to_string()
    }
}

struct PgGetTableDefCursor {
    conn: Arc<Connection>,
    statement: Option<Statement>,
    sql_map: HashMap<String, String>,
    rows: Vec<Vec<Value>>,
    current_row: usize,
    row_count: usize,
}

impl PgGetTableDefCursor {
    fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            statement: None,
            sql_map: HashMap::default(),
            rows: Vec::new(),
            current_row: 0,
            row_count: 0,
        }
    }

    fn load_table_defs(&mut self) {
        let schema = self.conn.current_schema();
        self.rows.clear();

        for (namespace, table_name, table) in user_tables_sorted(&schema) {
            let Table::BTree(btree_table) = table.as_ref() else {
                continue;
            };

            let postgres_ddl = match self.sql_map.get(&mangle_name(namespace, table_name)) {
                Some(schema_sql) => decode_stored_pg_schema_sql(schema_sql)
                    .map(|sql| {
                        ddl_with_user_names(&schema, namespace, &btree_table.declared_name, sql)
                    })
                    .unwrap_or_else(|| self.convert_to_postgres_ddl(schema_sql)),
                None => {
                    self.convert_to_postgres_ddl(&btree_table.to_sql_as(&btree_table.declared_name))
                }
            };

            self.rows.push(vec![
                Value::Text(schema_name_of(&schema, namespace).to_string().into()),
                Value::Text(btree_table.declared_name.clone().into()),
                Value::Text(postgres_ddl.into()),
            ]);
        }
    }

    fn convert_to_postgres_ddl(&self, sqlite_ddl: &str) -> String {
        let sqlite_ddl =
            split_identity_table_sql_marker(sqlite_ddl).map_or(sqlite_ddl, |(_, sql)| sql);
        let mut postgres_ddl = sqlite_ddl.to_string();

        // Basic SQLite to PostgreSQL type conversions
        // Handle INTEGER PRIMARY KEY specially for SERIAL
        postgres_ddl = postgres_ddl.replace(" INTEGER PRIMARY KEY", " SERIAL PRIMARY KEY");
        postgres_ddl = postgres_ddl.replace(" AUTOINCREMENT", "");

        // Type conversions - use lowercase for PostgreSQL standard
        // Use regex-like replacements to handle case-insensitive matches
        let type_replacements = [
            (" INTEGER", " integer"),
            (" intEgEr", " integer"),
            (" REAL", " double precision"),
            (" real", " double precision"),
            (" TEXT", " text"),
            (" text", " text"),
            (" BLOB", " bytea"),
            (" blob", " bytea"),
            (" DATETIME", " timestamp"),
            (" datetime", " timestamp"),
        ];

        for (from, to) in &type_replacements {
            postgres_ddl = postgres_ddl.replace(from, to);
        }

        // Remove SQLite-specific features
        postgres_ddl = postgres_ddl.replace(" WITHOUT ROWID", "");

        postgres_ddl
    }
}

/// `sql`, the stored text of table `table_name` of `namespace`, with the
/// names a user writes: the stored text names the table and the tables its
/// foreign keys refer to by their mangled names. A foreign key target in
/// another schema is named with its schema.
fn ddl_with_user_names(
    schema: &Schema,
    namespace: NamespaceId,
    table_name: &str,
    sql: &str,
) -> String {
    let Ok(parse_result) = turso_pg_parser::parse(sql) else {
        return sql.to_string();
    };
    let mut ddl = sql.to_string();
    // From the last name to the first, so the offsets of the others hold.
    for table in turso_pg_parser::foreign_key_tables(&parse_result)
        .iter()
        .rev()
    {
        let (target, bare) = schema.demangle_name(&table.name);
        if target.is_root() {
            continue;
        }
        let Ok(renamed) = turso_pg_parser::name_at(&ddl, table.location, bare) else {
            return sql.to_string();
        };
        ddl = if target == namespace {
            renamed
        } else {
            turso_pg_parser::qualify_create_table_name(
                &renamed,
                table.location,
                schema_name_of(schema, target),
            )
        };
    }
    turso_pg_parser::name_create_table(&ddl, table_name).unwrap_or(ddl)
}

impl VirtualTableCursor for PgGetTableDefCursor {
    fn next(&mut self) -> turso_core::types::IOResultOr<bool> {
        self.current_row += 1;
        Ok(IOResult::Done(self.current_row < self.row_count))
    }

    fn rowid(&self) -> i64 {
        self.current_row as i64
    }

    fn column(&mut self, column: usize) -> turso_core::types::IOResultOr<Value> {
        if self.current_row < self.rows.len() && column < 3 {
            Ok(IOResult::Done(self.rows[self.current_row][column].clone()))
        } else {
            Ok(IOResult::Done(Value::Null))
        }
    }

    fn filter(
        &mut self,
        _args: &[Value],
        _idx_str: Option<&str>,
        _idx_num: i32,
    ) -> turso_core::types::IOResultOr<bool> {
        if self.statement.is_none() {
            self.statement = Some(
                self.conn
                    .prepare_internal("SELECT name, sql FROM sqlite_schema WHERE type = 'table'")?,
            );
            self.sql_map.clear();
            self.rows.clear();
            self.current_row = 0;
            self.row_count = 0;
        }
        let sql_map = &mut self.sql_map;
        if let IOResult::IO(completions) = self
            .statement
            .as_mut()
            .unwrap()
            .run_with_row_callback_nonblock(|row| {
                if let (Value::Text(name), Value::Text(sql)) = (row.get_value(0), row.get_value(1))
                {
                    sql_map.insert(name.as_str().to_string(), sql.as_str().to_string());
                }
                Ok(())
            })?
        {
            return Ok(IOResult::IO(completions));
        }
        self.statement = None;
        self.load_table_defs();
        self.sql_map.clear();
        self.row_count = self.rows.len();
        Ok(IOResult::Done(!self.rows.is_empty()))
    }
}

impl VirtualTable for PgGetTableDefTable {
    type Cursor = PgGetTableDefCursor;

    fn open(&self, conn: Arc<Connection>) -> Result<Self::Cursor> {
        Ok(PgGetTableDefCursor::new(conn))
    }

    fn best_index(
        &self,
        _constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        Ok(IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 100.0,
            estimated_rows: 20,
            constraint_usages: vec![],
        })
    }
}

// ──────────────────────────────────────────────────────────────────────
// pg_get_constraintdef / pg_get_indexdef helper functions
// ──────────────────────────────────────────────────────────────────────

/// Format a referential action character code to SQL clause text.
fn ref_act_to_sql(code: &str) -> &'static str {
    match code {
        "c" => "CASCADE",
        "n" => "SET NULL",
        "d" => "SET DEFAULT",
        "r" => "RESTRICT",
        _ => "NO ACTION",
    }
}

/// Look up a constraint by OID and return its definition string.
/// Uses the same OID assignment as [PgConstraintTable].
pub(crate) fn pg_get_constraintdef(conn: &Connection, target_oid: i64) -> Option<String> {
    let schema = conn.current_schema();
    let tables = user_tables_sorted(&schema);
    let mut constraint_oid = CONSTRAINT_OID_START;

    for (_, _, table) in &tables {
        let btree = match table.as_ref() {
            Table::BTree(bt) => bt,
            _ => continue,
        };

        if btree.get_rowid_alias_column().is_some() {
            if constraint_oid == target_oid {
                let cols: Vec<String> = btree
                    .primary_key_columns
                    .iter()
                    .map(|(name, _)| {
                        let (_, column) =
                            btree.get_column(name).expect("primary key columns exist");
                        quote_identifier(
                            column
                                .name
                                .as_deref()
                                .expect("primary key columns have names"),
                        )
                    })
                    .collect();
                return Some(format!("PRIMARY KEY ({})", cols.join(", ")));
            }
            constraint_oid += 1;
        }

        // PK / UNIQUE from unique_sets
        for us in &btree.unique_sets {
            if constraint_oid == target_oid {
                let col_names: Vec<String> = us
                    .columns
                    .iter()
                    .map(|column| {
                        let (_, column) = btree
                            .get_column(&column.name)
                            .expect("constraint columns exist");
                        quote_identifier(
                            column
                                .name
                                .as_deref()
                                .expect("constraint columns have names"),
                        )
                    })
                    .collect();
                let kw = if unique_set_is_primary_key(btree, us) {
                    "PRIMARY KEY"
                } else {
                    "UNIQUE"
                };
                return Some(format!("{kw} ({})", col_names.join(", ")));
            }
            constraint_oid += 1;
        }

        // FK constraints
        for fk in &btree.foreign_keys {
            if constraint_oid == target_oid {
                let parent = schema.get_btree_table(fk.parent_namespace, &fk.parent_table);
                let child_cols = constraint_column_list(
                    Some(btree),
                    fk.child_columns.iter().map(String::as_str),
                );
                let parent_cols = if fk.parent_columns.is_empty() {
                    parent.as_ref().map_or_else(String::new, |parent| {
                        constraint_column_list(
                            Some(parent),
                            parent
                                .primary_key_columns
                                .iter()
                                .map(|(name, _)| name.as_str()),
                        )
                    })
                } else {
                    constraint_column_list(
                        parent.as_deref(),
                        fk.parent_columns.iter().map(String::as_str),
                    )
                };
                let parent_name =
                    relation_name(conn, &schema, fk.parent_namespace, &fk.parent_table);
                let mut def =
                    format!("FOREIGN KEY ({child_cols}) REFERENCES {parent_name}({parent_cols})");
                let on_update = ref_act_to_char(&fk.on_update);
                let on_delete = ref_act_to_char(&fk.on_delete);
                if on_update != "a" {
                    def.push_str(&format!(" ON UPDATE {}", ref_act_to_sql(on_update)));
                }
                if on_delete != "a" {
                    def.push_str(&format!(" ON DELETE {}", ref_act_to_sql(on_delete)));
                }
                return Some(def);
            }
            constraint_oid += 1;
        }

        // CHECK constraints
        for chk in &btree.check_constraints {
            if constraint_oid == target_oid {
                return Some(format!(
                    "CHECK ({})",
                    check_condition_text(conn, &chk.expr, btree)
                ));
            }
            constraint_oid += 1;
        }
    }

    None
}

/// The name of relation `name` of `namespace` as PostgreSQL writes it in
/// definitions and messages: bare when the search path finds this relation
/// by that name, and qualified with its schema otherwise, each part quoted
/// where needed. A table is named as it was created, as pg_class shows it.
pub(crate) fn relation_name(
    conn: &Connection,
    schema: &Schema,
    namespace: NamespaceId,
    name: &str,
) -> String {
    let class = crate::classes::class_catalog(schema)
        .and_then(|catalog| catalog.get_by_name(namespace, name));
    let declared_name = match class {
        Some(class) => class.relname.clone(),
        None => schema
            .get_btree_table(namespace, name)
            .map_or_else(|| name.to_string(), |table| table.declared_name.clone()),
    };
    // The name alone finds this relation when the exact lookup of the name
    // does, as the cast to regclass does it.
    if class
        .is_some_and(|class| visible_relation_oid(conn, schema, &declared_name) == Some(class.oid))
    {
        return quote_identifier(&declared_name);
    }
    format!(
        "{}.{}",
        quote_identifier(schema_name_of(schema, namespace)),
        quote_identifier(&declared_name)
    )
}

/// The oid of the relation that the name `name`, written without a schema,
/// finds: a catalog, since pg_catalog is searched first, or else the first
/// relation of that name in the search path. Names are compared exactly.
fn visible_relation_oid(conn: &Connection, schema: &Schema, name: &str) -> Option<i64> {
    let catalog = crate::classes::class_catalog(schema)?;
    if let Some(class) = catalog.get_by_relname(NamespaceId::ROOT, name) {
        return Some(class.oid);
    }
    if let Some(oid) = catalog_oid_by_name(name) {
        return Some(oid);
    }
    conn.search_order()
        .into_iter()
        .filter(|qualifier| qualifier.target.database_id == MAIN_DB_ID)
        .map(|qualifier| qualifier.target.id)
        .filter(|namespace| !namespace.is_root())
        .find_map(|namespace| {
            catalog
                .classes()
                .find(|class| class.relnamespace == namespace && is_named(class, name))
                .map(|class| class.oid)
        })
}

/// Whether the relation of `class` has the name `name`.
fn is_named(class: &crate::classes::RelationClass, name: &str) -> bool {
    // FIXME(pg-identifiers): The engine folds the name of a sequence to
    // lowercase, so pg_class shows `ss` for `CREATE SEQUENCE "Ss"`, and the
    // name written is not known. A sequence is found by its name in any case.
    // PostgreSQL compares every name exactly.
    class.relname == name
        || (class.relkind == RelationKind::Sequence && class.relname == name.to_ascii_lowercase())
}

fn constraint_column_list<'a>(
    table: Option<&BTreeTable>,
    names: impl IntoIterator<Item = &'a str>,
) -> String {
    names
        .into_iter()
        .map(|name| {
            let declared_name = table
                .and_then(|table| table.get_column(name))
                .and_then(|(_, column)| column.name.as_deref())
                .unwrap_or(name);
            quote_identifier(declared_name)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The text of a CHECK condition of `table` as PostgreSQL's `pg_get_expr`
/// prints it. A condition with a form the printer does not know is shown as
/// the engine's SQL.
fn check_condition_text(conn: &Connection, condition: &ast::Expr, table: &BTreeTable) -> String {
    crate::deparse::condition_text(conn, condition, table)
        .unwrap_or_else(|| render_expr(conn, condition))
}

fn render_expr(conn: &Connection, expr: &ast::Expr) -> String {
    let mut expr = crate::deparse::name_reg_constants(conn, expr);
    walk_expr_mut(&mut expr, &mut |expr| {
        // The translator compiles PostgreSQL's binary collations to BINARY.
        if let ast::Expr::Collate(_, collation) = expr {
            if collation.as_str().eq_ignore_ascii_case("binary") {
                *collation = ast::Name::from_string("\"C\"");
            }
        }
        if let ast::Expr::FunctionCall { name, args, .. } = expr {
            if args.is_empty() {
                if let Some(keyword) = turso_pg_parser::translator::role_keyword(name.as_str()) {
                    *expr = ast::Expr::Id(ast::Name::from_string(keyword));
                }
            } else if args.len() > 1 {
                let pg_name = match name.as_str().to_ascii_uppercase().as_str() {
                    "MAX" => Some("greatest"),
                    "MIN" => Some("least"),
                    _ => None,
                };
                if let Some(pg_name) = pg_name {
                    *name = ast::Name::from_string(pg_name);
                }
            }
        }
        Ok(WalkControl::Continue)
    })
    .expect("rewriting role functions and collations does not fail");
    expr.to_string()
}

/// Look up an index by OID and return its definition (CREATE INDEX ...).
/// Uses the same OID assignment as [PgIndexTable] / [PgClassTable].
pub(crate) fn pg_get_indexdef(conn: &Connection, target_oid: i64) -> Option<String> {
    let schema = conn.current_schema();
    catalog_indexes(conn, &schema)
        .iter()
        .find(|index| index.oid == target_oid)
        .map(|index| index_definition(&schema, index))
}

fn index_definition(schema: &Schema, index: &CatalogIndex) -> String {
    IndexDefinition {
        name: &index.name,
        schema_name: schema_name_of(schema, index.namespace),
        table_name: &index.table_name,
        unique: index.unique,
        columns: &index.columns,
        where_clause: index.where_clause.as_deref(),
    }
    .to_string()
}

struct IndexDefinition<'a> {
    name: &'a str,
    schema_name: &'a str,
    table_name: &'a str,
    unique: bool,
    columns: &'a [CatalogIndexColumn],
    where_clause: Option<&'a str>,
}

impl Display for IndexDefinition<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE ")?;
        if self.unique {
            f.write_str("UNIQUE ")?;
        }
        write!(
            f,
            "INDEX {} ON {}.{} USING btree (",
            quote_identifier(self.name),
            quote_identifier(self.schema_name),
            quote_identifier(self.table_name),
        )?;
        for (position, column) in self.columns.iter().enumerate() {
            if position != 0 {
                f.write_str(", ")?;
            }
            match column.expression.as_deref() {
                Some(expression) => f.write_str(expression)?,
                None => f.write_str(&quote_identifier(&column.name))?,
            }
            match column.order {
                SortOrder::Asc => {}
                SortOrder::Desc => f.write_str(" DESC")?,
            }
        }
        f.write_str(")")?;
        if let Some(where_clause) = self.where_clause {
            write!(f, " WHERE {where_clause}")?;
        }
        Ok(())
    }
}

struct CatalogIndex {
    oid: i64,
    table_oid: i64,
    namespace: NamespaceId,
    table_name: String,
    name: String,
    columns: Vec<CatalogIndexColumn>,
    unique: bool,
    primary: bool,
    /// Whether the index belongs to a PRIMARY KEY or UNIQUE constraint.
    for_constraint: bool,
    where_clause: Option<String>,
}

/// The indexes of the user tables, from their class rows, in the order of
/// the tables. An index is named as its class row names it.
fn catalog_indexes(conn: &Connection, schema: &Schema) -> Vec<CatalogIndex> {
    let Some(classes) = crate::classes::class_catalog(schema) else {
        return Vec::new();
    };
    let mut indexes = Vec::new();
    for (namespace, table_name, table, table_oid) in user_tables_with_oids(schema) {
        let Table::BTree(btree) = table.as_ref() else {
            continue;
        };
        for class in classes.indexes_of(table_oid) {
            let Some(engine_name) = &class.engine_name else {
                let (position, column) = btree
                    .get_rowid_alias_column()
                    .expect("an index without a schema object belongs to a rowid table");
                indexes.push(CatalogIndex {
                    oid: class.oid,
                    table_oid,
                    namespace,
                    table_name: btree.declared_name.clone(),
                    name: class.relname.clone(),
                    columns: vec![CatalogIndexColumn {
                        name: column.name.clone().expect("primary key columns have names"),
                        position,
                        expression: None,
                        order: SortOrder::Asc,
                    }],
                    unique: true,
                    primary: true,
                    for_constraint: true,
                    where_clause: None,
                });
                continue;
            };
            let index = schema
                .get_indices(namespace, table_name)
                .find(|index| index.name.eq_ignore_ascii_case(engine_name))
                .expect("every index class row has an index in the schema");
            let primary = btree.get_rowid_alias_column().is_none()
                && index
                    .name
                    .starts_with(PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX)
                && index
                    .columns
                    .iter()
                    .map(|column| column.pos_in_table)
                    .eq(btree.primary_key_columns.iter().map(|(name, _)| {
                        btree.get_column(name).expect("primary key columns exist").0
                    }));
            indexes.push(CatalogIndex {
                oid: class.oid,
                table_oid,
                namespace,
                table_name: btree.declared_name.clone(),
                name: class.relname.clone(),
                columns: index
                    .columns
                    .iter()
                    .map(|column| CatalogIndexColumn {
                        name: if column.expr.is_some() {
                            column.name.clone()
                        } else {
                            btree.columns()[column.pos_in_table]
                                .name
                                .clone()
                                .expect("index columns have names")
                        },
                        position: column.pos_in_table,
                        expression: column
                            .expr
                            .as_ref()
                            .map(|expr| crate::deparse::name_reg_constants(conn, expr).to_string()),
                        order: column.order,
                    })
                    .collect(),
                unique: index.unique,
                primary,
                for_constraint: index
                    .name
                    .starts_with(PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX),
                where_clause: index
                    .where_clause
                    .as_ref()
                    .map(|expr| crate::deparse::name_reg_constants(conn, expr).to_string()),
            });
        }
    }
    indexes
}

struct CatalogIndexColumn {
    name: String,
    position: usize,
    expression: Option<String>,
    order: SortOrder,
}

pub(crate) fn unique_set_is_primary_key(table: &BTreeTable, set: &UniqueSet) -> bool {
    table.get_rowid_alias_column().is_none()
        && set.columns.len() == table.primary_key_columns.len()
        && set
            .columns
            .iter()
            .zip(&table.primary_key_columns)
            .all(|(column, (name, _))| column.name.eq_ignore_ascii_case(name))
}

// TODO: Fix tests to use correct API
#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use super::*;
    use crate::{Database, Numeric, PlatformIO, StepResult};
    use tempfile::tempdir;
    use turso_core::{ConnectOpts, SessionRole};

    /// Connects like the frontend does: with the `public` schema in place
    /// and on the search path.
    fn connect_with_public(db: &Arc<Database>) -> Arc<Connection> {
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        conn.set_search_path(Some(vec!["public".to_string()]));
        conn
    }

    #[test]
    fn builtin_catalog_column_changes_preserve_stored_relations() {
        let db = crate::session::open_database_with_io(
            Arc::new(turso_core::MemoryIO::new()),
            "builtin-catalog-column-changes.db",
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = connect_with_public(&db);
        conn.execute("CREATE TABLE kept (id int PRIMARY KEY)")
            .unwrap();
        let mut schema =
            turso_core::alloc::TryClone::try_clone(conn.current_schema().as_ref()).unwrap();
        let rows = conn
            .prepare_engine_schema_statement(
                &crate::classes::schema_state_query(&schema)
                    .unwrap()
                    .unwrap(),
            )
            .unwrap()
            .run_collect_rows()
            .unwrap();
        let oid = crate::classes::class_catalog(&schema)
            .unwrap()
            .classes()
            .find(|class| class.relname == "kept")
            .unwrap()
            .oid;
        // Simulate a new binary exposing fewer columns without changing the file.
        let table = schema.table_mut(NamespaceId::ROOT, "pg_am").unwrap();
        let Table::Virtual(table) = Arc::make_mut(table) else {
            panic!("pg_am is virtual")
        };
        Arc::make_mut(table).columns[0].set_hidden(true);
        let catalog = crate::classes::ClassCatalog::build(&schema, &rows).unwrap();
        assert_eq!(catalog.get_by_oid(oid).unwrap().relname, "kept");
        let am = catalog
            .rows(&schema)
            .into_iter()
            .find(|row| row[0].as_int() == Some(2601))
            .unwrap();
        assert_eq!(
            am[18].as_int(),
            Some(builtin_catalog_columns(&schema, "pg_am").len() as i64)
        );
    }

    #[test]
    fn dialect_parse_preserves_explain_command() {
        use turso_core::ast::{Cmd, EqpFormat};

        for sql in ["EXPLAIN SELECT 7", "EXPLAIN SELECT 7::text"] {
            let (cmd, consumed) = PostgresDialect.parse(sql).unwrap();
            assert_eq!(consumed, sql.len());
            assert!(
                matches!(
                    cmd,
                    Some(Cmd::ExplainQueryPlan {
                        format: EqpFormat::Text,
                        ..
                    })
                ),
                "{cmd:?}"
            );
        }
    }

    #[test]
    fn postgres_rejects_without_rowid_schema() {
        let sql = "CREATE TABLE t(a TEXT PRIMARY KEY, b) WITHOUT ROWID";
        assert!(
            !BTreeTable::from_stored_sql(&PostgresDialect, sql, 2, false)
                .unwrap()
                .has_rowid
        );
        let error = PostgresDialect.parse_table_sql(sql, 2).unwrap_err();
        assert_eq!(
            error.to_string(),
            "PostgreSQL does not support WITHOUT ROWID table 't'"
        );
        assert!(PostgresDialect
            .parse_table_sql("CREATE TABLE t(a TEXT PRIMARY KEY, b)", 2)
            .is_ok());
    }

    #[test]
    fn catalogs_registered_at_open_survive_schema_refresh() {
        let catalog_names = [
            "pg_namespace",
            "pg_attribute",
            "pg_roles",
            "pg_auth_members",
            "pg_am",
            "pg_proc",
            "pg_database",
            "pg_tables",
            "pg_indexes",
            "pg_get_tabledef",
            "pg_index",
            "pg_constraint",
            "pg_type",
            "pg_attrdef",
            "pg_input_error_info",
            "pg_sequence",
            "pg_sequences",
            "pg_settings",
            "pg_extension",
            "pg_policy",
            "pg_policies",
            "pg_trigger",
            "pg_statistic_ext",
            "pg_inherits",
            "pg_rewrite",
            "pg_foreign_table",
            "pg_partitioned_table",
            "pg_collation",
            "pg_publication",
            "pg_publication_namespace",
            "pg_publication_rel",
        ];
        for mvcc in [false, true] {
            let db = crate::session::open_database_with_io(
                Arc::new(turso_core::MemoryIO::new()),
                "catalog.db",
                crate::OpenFlags::default(),
                crate::DatabaseOpts::new(),
            )
            .unwrap();
            let conn = connect_with_public(&db);
            if mvcc {
                conn.pragma_update("journal_mode", "'mvcc'").unwrap();
            }
            for name in catalog_names {
                assert!(
                    matches!(
                        conn.current_schema()
                            .get_table(NamespaceId::ROOT, name)
                            .as_deref(),
                        Some(Table::Virtual(_))
                    ),
                    "{name}"
                );
            }
            assert_eq!(
                conn.prepare("SELECT setting FROM pg_settings WHERE name = 'search_path'")
                    .unwrap()
                    .run_collect_rows()
                    .unwrap(),
                vec![vec![Value::build_text("\"$user\", public")]]
            );
            let mut namespaces = conn
                .prepare("SELECT nspname FROM pg_namespace() ORDER BY nspname")
                .unwrap();
            assert_eq!(
                namespaces.run_collect_rows().unwrap(),
                vec![
                    vec![Value::build_text("information_schema")],
                    vec![Value::build_text("pg_catalog")],
                    vec![Value::build_text("public")],
                ]
            );
            let mut tables = conn
                .prepare("SELECT relname FROM pg_class WHERE relname = 'second_connection'")
                .unwrap();
            assert!(tables.run_collect_rows().unwrap().is_empty());
            let other = connect_with_public(&db);
            other
                .execute("CREATE TABLE second_connection (v INT)")
                .unwrap();
            tables.reset().unwrap();
            assert_eq!(
                tables.run_collect_rows().unwrap(),
                vec![vec![Value::build_text("second_connection")]]
            );
            conn.force_reparse_schema().unwrap();
            for name in catalog_names {
                assert!(
                    matches!(
                        conn.current_schema()
                            .get_table(NamespaceId::ROOT, name)
                            .as_deref(),
                        Some(Table::Virtual(_))
                    ),
                    "{name}"
                );
            }
            let mut error_info = conn
                .prepare("SELECT sql_error_code FROM pg_input_error_info('abc', 'integer')")
                .unwrap();
            assert_eq!(
                error_info.run_collect_rows().unwrap(),
                vec![vec![Value::build_text("22P02")]]
            );
            conn.execute("CREATE TABLE namespace_counts (n INT)")
                .unwrap();
            conn.execute("CREATE TRIGGER catalog_check AFTER INSERT ON second_connection BEGIN INSERT INTO namespace_counts SELECT COUNT(*) FROM main.pg_namespace LEFT JOIN main.pg_policy ON 1; END").unwrap();
            conn.execute("INSERT INTO second_connection VALUES (7)")
                .unwrap();
            let mut counts = conn.prepare("SELECT n FROM namespace_counts").unwrap();
            assert_eq!(
                counts.run_collect_rows().unwrap(),
                vec![vec![Value::from_i64(3)]]
            );
        }
    }

    #[test]
    fn catalog_view_columns_survive_schema_refresh() {
        let expected = ["oid", "nspname", "nspowner", "nspacl"]
            .map(Value::build_text)
            .map(|value| vec![value])
            .to_vec();
        let hidden_expected = ["nspname", "tableoid"]
            .map(Value::build_text)
            .map(|value| vec![value])
            .to_vec();
        let views = [
            ("catalog_names", "SELECT * FROM pg_namespace", false),
            (
                "catalog_qualified_names",
                "SELECT pg_namespace.* FROM pg_namespace",
                false,
            ),
            (
                "catalog_group_names",
                "SELECT * FROM (pg_namespace) AS namespaces",
                false,
            ),
            (
                "catalog_qualified_group_names",
                "SELECT namespaces.* FROM (pg_namespace) AS namespaces",
                false,
            ),
            (
                "catalog_hidden",
                "SELECT nspname, tableoid FROM pg_namespace",
                true,
            ),
            (
                "catalog_group_hidden",
                "SELECT namespaces.nspname, namespaces.tableoid FROM (pg_namespace) AS namespaces",
                true,
            ),
        ];
        let check_columns = |conn: &Arc<Connection>| {
            for (name, _, selects_hidden) in views {
                let columns = conn
                    .prepare(format!(
                        "SELECT name FROM pragma_table_info('{name}') ORDER BY cid"
                    ))
                    .unwrap()
                    .run_collect_rows()
                    .unwrap();
                assert_eq!(
                    &columns,
                    if selects_hidden {
                        &hidden_expected
                    } else {
                        &expected
                    },
                    "{name}"
                );
            }
        };
        let dir = tempdir().unwrap();
        for mvcc in [false, true] {
            let path = dir.path().join(format!("catalog-view-{mvcc}.db"));
            let io = Arc::new(PlatformIO::new().unwrap());
            let opts = crate::DatabaseOpts::new()
                .with_views(true)
                .with_experimental_mvcc_passive_checkpoint(true);
            {
                let db = crate::session::open_database_with_io(
                    io.clone(),
                    path.to_str().unwrap(),
                    crate::OpenFlags::default(),
                    opts,
                )
                .unwrap();
                let conn = connect_with_public(&db);
                if mvcc {
                    conn.pragma_update("journal_mode", "'mvcc'").unwrap();
                }
                let stmt = conn.prepare("SELECT * FROM pg_namespace").unwrap();
                assert_eq!(stmt.num_columns(), 4);
                for (name, select, _) in views {
                    conn.execute(format!("CREATE VIEW {name} AS {select}"))
                        .unwrap();
                }
                conn.execute("CREATE VIEW catalog_join AS WITH namespaces AS (SELECT * FROM pg_namespace) SELECT namespaces.nspname FROM namespaces JOIN pg_namespace USING (oid)").unwrap();
                check_columns(&conn);
                conn.force_reparse_schema().unwrap();
                check_columns(&conn);
            }
            let db = crate::session::open_database_with_io(
                io,
                path.to_str().unwrap(),
                crate::OpenFlags::default(),
                opts,
            )
            .unwrap();
            let conn = connect_with_public(&db);
            check_columns(&conn);
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
            conn.force_reparse_schema().unwrap();
            check_columns(&conn);
            for name in ["catalog_hidden", "catalog_group_hidden"] {
                let mut stmt = conn
                    .prepare(format!("SELECT * FROM {name} ORDER BY nspname"))
                    .unwrap();
                assert_eq!(stmt.num_columns(), 2);
                assert_eq!(
                    stmt.run_collect_rows().unwrap(),
                    vec![
                        vec![
                            Value::build_text("information_schema"),
                            Value::from_i64(2615)
                        ],
                        vec![Value::build_text("pg_catalog"), Value::from_i64(2615)],
                        vec![Value::build_text("public"), Value::from_i64(2615)],
                    ]
                );
            }
            assert_eq!(
                conn.prepare("SELECT nspname FROM catalog_join ORDER BY nspname")
                    .unwrap()
                    .run_collect_rows()
                    .unwrap(),
                vec![
                    vec![Value::build_text("information_schema")],
                    vec![Value::build_text("pg_catalog")],
                    vec![Value::build_text("public")],
                ]
            );
        }
    }

    #[test]
    fn postgres_rejects_attach_before_opening_the_target() {
        for enabled in [false, true] {
            let dir = tempdir().unwrap();
            let path = dir.path().join("must-not-exist.db");
            let db = crate::session::open_database_with_io(
                Arc::new(turso_core::MemoryIO::new()),
                ":memory:",
                crate::OpenFlags::default(),
                crate::DatabaseOpts::new().with_attach(enabled),
            )
            .unwrap();
            let conn = connect_with_public(&db);
            for sql in [
                format!("ATTACH '{}' AS aux", path.display()),
                "ATTACH ':memory:' AS aux".to_string(),
            ] {
                let error = conn
                    .execute(&sql)
                    .expect_err("PostgreSQL must reject ATTACH");
                assert_eq!(
                    crate::error::sqlstate(&error),
                    SQLSTATE_FEATURE_NOT_SUPPORTED,
                    "{error}"
                );
                assert!(!path.exists());
            }
            let mut state = turso_core::attach::AttachDatabaseState::Start;
            let error = match conn.attach_database(path.to_str().unwrap(), "aux", &mut state) {
                Err(error) => error,
                Ok(_) => panic!("the connection API must reject ATTACH before opening the file"),
            };
            assert_eq!(
                crate::error::sqlstate(&error),
                SQLSTATE_FEATURE_NOT_SUPPORTED
            );
            assert!(!path.exists());
        }
    }

    #[test]
    fn catalogs_available_in_temporary_databases() {
        let dir = tempdir().unwrap();
        let db = crate::session::open_database_with_io(
            Arc::new(PlatformIO::new().unwrap()),
            dir.path().join("main.db").to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        for temp_store in ["MEMORY", "FILE"] {
            let temp_conn = connect_with_public(&db);
            temp_conn
                .execute(format!("PRAGMA temp_store = {temp_store}"))
                .unwrap();
            assert_eq!(
                temp_conn
                    .prepare("SELECT COUNT(*) FROM temp.pg_namespace")
                    .unwrap()
                    .run_collect_rows()
                    .unwrap(),
                vec![vec![Value::from_i64(3)]]
            );
        }
    }

    #[test]

    fn test_pg_namespace_query() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = crate::session::open_database_with_io(
            io,
            db_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = crate::Connection::connect(&db).unwrap();

        // Query pg_namespace
        let mut stmt = conn.prepare("SELECT * FROM pg_namespace").unwrap();

        let mut found_pg_catalog = false;
        let mut found_public = false;
        let mut found_information_schema = false;

        loop {
            match stmt.step().unwrap() {
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    if let Value::Text(nspname) = row.get_value(1) {
                        match nspname.value.as_ref() {
                            "pg_catalog" => found_pg_catalog = true,
                            "public" => found_public = true,
                            "information_schema" => found_information_schema = true,
                            _ => {}
                        }
                    }
                }
                StepResult::Done => break,
                _ => {}
            }
        }

        assert!(found_pg_catalog, "pg_catalog namespace not found");
        assert!(found_public, "public namespace not found");
        assert!(
            found_information_schema,
            "information_schema namespace not found"
        );
    }

    #[test]

    fn test_pg_class_lists_user_tables() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = crate::session::open_database_with_io(
            io,
            db_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = connect_with_public(&db);

        // Create test tables in SQLite mode (default)
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("CREATE TABLE products (id INTEGER, title TEXT, price REAL)")
            .unwrap();
        conn.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER, product_id INTEGER)")
            .unwrap();

        drop(conn);
        let conn = crate::Connection::connect(&db).unwrap();

        // Query pg_class for regular tables
        let mut stmt = conn
            .prepare("SELECT relname FROM pg_class WHERE relkind = 'r' AND relnamespace = 2200")
            .unwrap();

        let mut tables = Vec::new();
        loop {
            match stmt.step().unwrap() {
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    if let Value::Text(relname) = row.get_value(0) {
                        tables.push(relname.to_string());
                    }
                }
                StepResult::Done => break,
                _ => {}
            }
        }

        // Should find our three tables
        assert!(
            tables.contains(&"users".to_string()),
            "users table not found"
        );
        assert!(
            tables.contains(&"products".to_string()),
            "products table not found"
        );
        assert!(
            tables.contains(&"orders".to_string()),
            "orders table not found"
        );
        assert_eq!(tables.len(), 3, "Expected exactly 3 tables");
    }

    #[test]

    fn test_pg_class_table_details() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = crate::session::open_database_with_io(
            io,
            db_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = connect_with_public(&db);

        // Create a test table with known columns
        conn.execute("CREATE TABLE test_table (id INTEGER, name TEXT, value REAL)")
            .unwrap();

        drop(conn);
        let conn = crate::Connection::connect(&db).unwrap();

        // Query pg_class for table details
        let mut stmt = conn
            .prepare(
                "SELECT oid, relname, relkind, relnatts
             FROM pg_class
             WHERE relname = 'test_table'",
            )
            .unwrap();

        if let StepResult::Row = stmt.step().unwrap() {
            let row = stmt.row().unwrap();
            let oid = if let Value::Numeric(Numeric::Integer(v)) = row.get_value(0) {
                *v
            } else {
                panic!("Expected OID")
            };
            let relname = if let Value::Text(v) = row.get_value(1) {
                v
            } else {
                panic!("Expected relname")
            };
            let relkind = if let Value::Text(v) = row.get_value(2) {
                v
            } else {
                panic!("Expected relkind")
            };
            let relnatts = if let Value::Numeric(Numeric::Integer(v)) = row.get_value(3) {
                *v
            } else {
                panic!("Expected relnatts")
            };

            assert!(oid >= 16384, "OID should be >= 16384 for user tables");
            assert_eq!(relname.value, "test_table", "Table name should match");
            assert_eq!(
                relkind.value, "r",
                "relkind should be 'r' for regular table"
            );
            assert_eq!(relnatts, 3, "Table should have 3 columns");
        } else {
            panic!("test_table not found in pg_class");
        }
    }

    #[test]

    fn test_sqlite_tables_hidden_in_postgres_mode() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = crate::session::open_database_with_io(
            io,
            db_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = connect_with_public(&db);

        // Create a test table
        conn.execute("CREATE TABLE test_table (id INTEGER)")
            .unwrap();

        drop(conn);
        let conn = crate::Connection::connect(&db).unwrap();

        // Try to query sqlite_master - should fail
        let result = conn.prepare("SELECT * FROM sqlite_master");
        assert!(
            result.is_err(),
            "sqlite_master should not be accessible in PostgreSQL mode"
        );

        // Try to query sqlite_schema - should also fail
        let result = conn.prepare("SELECT * FROM sqlite_schema");
        assert!(
            result.is_err(),
            "sqlite_schema should not be accessible in PostgreSQL mode"
        );
    }

    /// User-created tables are visible through both the SQLite-side catalog
    /// (`sqlite_master`) on a SQLite-mode connection and the PG-side catalog
    /// (`pg_class`) on a PostgreSQL-mode connection. Each direction uses its
    /// own connection — dialect is fixed per connection and we don't
    /// hot-swap at runtime.
    #[test]
    fn user_table_is_listed_in_dialect_specific_catalog() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = crate::session::open_database_with_io(
            io,
            db_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();

        // SQLite-mode connection: sqlite_master sees the user table.
        let sqlite_conn = connect_with_public(&db);
        sqlite_conn
            .execute("CREATE TABLE users (id INTEGER, name TEXT)")
            .unwrap();
        let mut stmt = sqlite_conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
            .unwrap();
        let mut found = false;
        loop {
            match stmt.step().unwrap() {
                StepResult::Row => {
                    if let Value::Text(name) = stmt.row().unwrap().get_value(0) {
                        if name.value == "__turso_ns1_users" {
                            found = true;
                        }
                    }
                }
                StepResult::Done => break,
                _ => {}
            }
        }
        assert!(
            found,
            "users table not found in sqlite_master under its mangled name"
        );

        // PostgreSQL-mode connection: pg_class sees the user table.
        let pg_conn = crate::Connection::connect(&db).unwrap();
        let mut stmt = pg_conn
            .prepare("SELECT relname FROM pg_class WHERE relkind = 'r'")
            .unwrap();
        let mut found = false;
        loop {
            match stmt.step().unwrap() {
                StepResult::Row => {
                    if let Value::Text(name) = stmt.row().unwrap().get_value(0) {
                        if name.value == "users" {
                            found = true;
                        }
                    }
                }
                StepResult::Done => break,
                _ => {}
            }
        }
        assert!(found, "users table not found in pg_class");
    }

    #[test]

    fn test_pg_class_with_where_constraints() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = crate::session::open_database_with_io(
            io,
            db_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = connect_with_public(&db);

        // Create multiple tables
        conn.execute("CREATE TABLE table1 (id INTEGER)").unwrap();
        conn.execute("CREATE TABLE table2 (id INTEGER, name TEXT)")
            .unwrap();
        conn.execute("CREATE TABLE table3 (id INTEGER, name TEXT, value REAL)")
            .unwrap();

        drop(conn);
        let conn = crate::Connection::connect(&db).unwrap();

        // Test various WHERE clause combinations

        // Test 1: Filter by relkind = 'r'
        let mut stmt = conn
            .prepare("SELECT COUNT(*) FROM pg_class WHERE relkind = 'r' AND relnamespace = 2200")
            .unwrap();
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Numeric(Numeric::Integer(count)) = row.get_value(0) {
                    assert_eq!(*count, 3, "Should have 3 regular tables");
                }
            }
            _ => panic!("Expected row from COUNT query"),
        }

        // Test 2: Filter by relnamespace = 2200 (public schema)
        let mut stmt = conn
            .prepare("SELECT COUNT(*) FROM pg_class WHERE relnamespace = 2200")
            .unwrap();
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let Value::Numeric(Numeric::Integer(count)) = row.get_value(0) {
                    assert_eq!(*count, 3, "Should have 3 tables in public schema");
                }
            }
            _ => panic!("Expected row from COUNT query"),
        }

        // Test 3: Combined filters
        let mut stmt = conn.prepare("SELECT relname FROM pg_class WHERE relkind = 'r' AND relnamespace = 2200 ORDER BY relname").unwrap();
        let mut tables = Vec::new();
        loop {
            match stmt.step().unwrap() {
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    if let Value::Text(name) = row.get_value(0) {
                        tables.push(name.to_string());
                    }
                }
                StepResult::Done => break,
                _ => {}
            }
        }
        assert_eq!(tables, vec!["table1", "table2", "table3"]);
    }

    #[test]
    fn test_pg_tables_lists_user_tables() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = crate::session::open_database_with_io(
            io,
            db_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = connect_with_public(&db);

        // Create test tables
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER)")
            .unwrap();

        drop(conn);
        let conn = crate::Connection::connect(&db).unwrap();

        // Query pg_tables
        let mut stmt = conn
            .prepare("SELECT schemaname, tablename FROM pg_tables WHERE schemaname = 'public'")
            .unwrap();

        let mut tables = Vec::new();
        loop {
            match stmt.step().unwrap() {
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    if let (Value::Text(schema), Value::Text(name)) =
                        (row.get_value(0), row.get_value(1))
                    {
                        assert_eq!(schema.as_str(), "public");
                        tables.push(name.to_string());
                    }
                }
                StepResult::Done => break,
                _ => {}
            }
        }

        tables.sort();
        assert_eq!(tables, vec!["orders", "users"]);
    }

    #[test]
    fn test_pg_tables_excludes_internal_tables() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = crate::session::open_database_with_io(
            io,
            db_path.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = connect_with_public(&db);

        conn.execute("CREATE TABLE mydata (id INTEGER PRIMARY KEY)")
            .unwrap();

        drop(conn);
        let conn = crate::Connection::connect(&db).unwrap();

        let mut stmt = conn.prepare("SELECT tablename FROM pg_tables").unwrap();

        loop {
            match stmt.step().unwrap() {
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    if let Value::Text(name) = row.get_value(0) {
                        assert!(
                            !name.as_str().starts_with("sqlite_"),
                            "internal table {} should not appear in pg_tables",
                            name.as_str()
                        );
                    }
                }
                StepResult::Done => break,
                _ => {}
            }
        }
    }
}
