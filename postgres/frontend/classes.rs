//! The relation catalog: one row per user relation, in the
//! `__turso_internal_classes` table of the main database.
//!
//! `sqlite_schema` stays the source of truth for storage: type, name, root
//! page and SQL text. The catalog holds what `sqlite_schema` does not: a
//! stable `oid` that survives creating and dropping other relations, the
//! owning role, the privileges `GRANT` gave, and row-level security with
//! its policies. The columns use
//! PostgreSQL's `pg_class` names and value conventions, because when
//! `sqlite_schema` itself moves behind the engine's dialect hooks, this
//! table becomes `pg_class`, by renaming and adding columns.
//!
//! Core knows nothing of this table. [`crate::catalog::PostgresDialect`]
//! maintains it through the engine's catalog hooks: every statement that
//! adds, renames or removes a relation calls back here to write the matching
//! row in the same transaction as its `sqlite_schema` rows, so the two can
//! never disagree after a crash, and the engine loads the rows back with
//! the rest of the frontend's schema state, see [`crate::schema_state`],
//! whenever it loads the schema.
//!
//! The PostgreSQL opening functions create these tables before
//! returning the database. Every relation receives its catalog row when created.
//!
//! Oids start at 16384, PostgreSQL's `FirstNormalObjectId`, and are never
//! reused inside one database, even after the relation is dropped: the
//! database's oid counter gives them out, see [`crate::oid`]. Row-level security
//! policies take their oids from the same counter.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use turso_core::ast;
use turso_core::namespace::NamespaceId;
use turso_core::schema::{is_user_relation_name, RelationKind, Schema};
use turso_core::security::permission::{Action, Object, PermissionDenied};
use turso_core::security::roles::RoleId;
use turso_core::{
    to_u32, AccessStatement, Catalog, CatalogApply, CatalogCmd, CmdContext, CmdOrigin, CmpInsFlags,
    CollationSeq, Connection, CursorType, DropColumn, InsertFlags, Insn, LimboError, LoadStep,
    NewOwner, Numeric, OwnerColumn, ProgramBuilder, RegisterOrLiteral, RelationName, SqliteCatalog,
    Value,
};

use turso_core::MAIN_DB_ID;
use turso_pg_parser::sqlstate::{
    SQLSTATE_DUPLICATE_OBJECT, SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INVALID_GRANT_OPERATION,
    SQLSTATE_UNDEFINED_OBJECT,
};

use crate::acl::{Acl, Privileges, PUBLIC_OID};
use crate::catalog::{engine_role_by_name, role_oid, role_oid_by_name};
use crate::descriptions::{emit_delete_comments, Dropped};
use crate::error::sqlstate_error;
use crate::oid::{OidAllocator, COUNTER_INIT_STATEMENTS, FIRST_NORMAL_OBJECT_ID};
use crate::policies::{self, TablePolicies};
use crate::roles::schema_roles;
use crate::schema_init::{is_stored_catalog_table, TableInit};
use crate::schema_state::{change_schema_state, emit_catalog_apply, schema_state};
use crate::unique_index::emit_unique_index_insert;
use turso_core::security::policy::RowPolicy;

pub(crate) const CLASSES_TABLE_NAME: &str = "__turso_internal_classes";

pub(crate) const CLASSES_NAME_INDEX_NAME: &str = "__turso_internal_classes_name";

/// The columns the rest of this module reads and writes. `relownedby` is
/// the oid of the table whose serial column owns the relation, 0 when no
/// relation owns it, and `relownedbycolumn` is the name of that column:
/// what PostgreSQL records in `pg_depend` as an automatic dependency, kept
/// here until there is a `pg_depend`, NULL when no relation owns it.
/// `relacl` is the access control list in the text form of PostgreSQL's `aclitem[]`, NULL
/// until the first `GRANT`, when the owner has every privilege and nobody
/// else has any. `relrowsecurity` is 1 when the table has row-level
/// security, 0 otherwise. `relpolicies` holds the table's row-level security
/// policies in the form [`policies`] describes, NULL when it has none.
const CREATE_CLASSES_TABLE_SQL: &str = "CREATE TABLE __turso_internal_classes (\
    oid INTEGER PRIMARY KEY, \
    relname TEXT NOT NULL, \
    relnamespace INTEGER NOT NULL, \
    relkind TEXT NOT NULL, \
    relowner INTEGER NOT NULL, \
    relownedby INTEGER NOT NULL, \
    relacl TEXT, \
    relrowsecurity INTEGER NOT NULL DEFAULT 0, \
    relpolicies TEXT, \
    relownedbycolumn TEXT)";

const CLASS_COLUMN_COUNT: usize = 10;

const CREATE_CLASSES_NAME_INDEX_SQL: &str = "CREATE UNIQUE INDEX __turso_internal_classes_name \
     ON __turso_internal_classes (relnamespace, relname)";

/// Fails for a database that has relations and no class table, as an
/// earlier version of tursopg made it.
pub(crate) fn refuse_relations_without_class_table(schema: &Schema) -> turso_core::Result<()> {
    if schema
        .get_btree_table(NamespaceId::ROOT, CLASSES_TABLE_NAME)
        .is_none()
        && !schema.user_relations().is_empty()
    {
        return Err(LimboError::ParseError(
            "the database has relations but no relation catalog, as an earlier version of tursopg \
             made it; this version does not open it"
                .to_string(),
        ));
    }
    Ok(())
}

pub(crate) const TABLE_INIT: TableInit = TableInit {
    table: CLASSES_TABLE_NAME,
    statements: &[
        CREATE_CLASSES_TABLE_SQL,
        CREATE_CLASSES_NAME_INDEX_SQL,
        COUNTER_INIT_STATEMENTS[0],
        COUNTER_INIT_STATEMENTS[1],
    ],
};

/// PostgreSQL's `pg_class.relkind` letter for `kind`.
pub(crate) fn relkind_letter(kind: RelationKind) -> &'static str {
    match kind {
        RelationKind::Table => "r",
        RelationKind::View => "v",
        RelationKind::MaterializedView => "m",
        RelationKind::Sequence => "S",
        RelationKind::Index => "i",
    }
}

/// The relation kind that the `pg_class.relkind` letter `letter` stands for.
pub(crate) fn relation_kind_of_letter(letter: &str) -> Option<RelationKind> {
    const KINDS: [RelationKind; 5] = [
        RelationKind::Table,
        RelationKind::View,
        RelationKind::MaterializedView,
        RelationKind::Sequence,
        RelationKind::Index,
    ];
    KINDS
        .into_iter()
        .find(|kind| relkind_letter(*kind) == letter)
}

/// One row of the catalog: the identity and ownership of a relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelationClass {
    pub(crate) oid: i64,
    /// The relation's bare name, with the namespace in `relnamespace`.
    pub(crate) relname: String,
    pub(crate) relnamespace: NamespaceId,
    pub(crate) relkind: RelationKind,
    pub(crate) relowner: RoleId,
    /// The oid of the table whose serial column owns this sequence; 0 when
    /// no relation owns it.
    pub(crate) relownedby: i64,
    /// The name of the column that owns this sequence, None when no relation
    /// owns it.
    pub(crate) relownedbycolumn: Option<String>,
    /// The privileges `GRANT` gave, None before the first `GRANT`.
    pub(crate) relacl: Option<Acl>,
    /// Whether the table has row-level security.
    pub(crate) relrowsecurity: bool,
    /// The table's row-level security policies.
    pub(crate) relpolicies: TablePolicies,
}

impl RelationClass {
    fn is_owned_by_column(&self, column: &str) -> bool {
        self.relownedbycolumn
            .as_deref()
            .is_some_and(|owner| owner.eq_ignore_ascii_case(column))
    }
}

/// The class rows of a database, loaded as the engine's opaque dialect
/// schema state.
#[derive(Debug, Clone, Default)]
pub(crate) struct ClassCatalog {
    by_oid: BTreeMap<i64, RelationClass>,
    oids_by_name: HashMap<(NamespaceId, String), i64>,
}

impl ClassCatalog {
    /// Returns the catalog of the rows read from the classes table for
    /// `schema`, after checking that every user relation of `schema` has a
    /// row.
    pub(crate) fn build(schema: &Schema, rows: &[Vec<Value>]) -> turso_core::Result<Self> {
        let catalog = Self::new(rows)?;
        for (namespace, name, _) in schema.user_relations() {
            if is_stored_catalog_table(namespace, &name) {
                continue;
            }
            if catalog.get_by_name(namespace, &name).is_none() {
                return Err(LimboError::Corrupt(format!(
                    "relation \"{name}\" of namespace {} has no row in {CLASSES_TABLE_NAME}",
                    namespace.as_u32()
                )));
            }
        }
        Ok(catalog)
    }

    /// Returns a catalog of the rows read from the classes table. Each row is
    /// `(oid, relname, relnamespace, relkind, relowner, relownedby, relacl,
    /// relrowsecurity, relpolicies, relownedbycolumn)`.
    fn new(rows: &[Vec<Value>]) -> turso_core::Result<Self> {
        let mut catalog = Self::default();
        for row in rows {
            let class = class_from_row(row)?;
            if catalog.by_oid.contains_key(&class.oid)
                || catalog
                    .get_by_name(class.relnamespace, &class.relname)
                    .is_some()
            {
                return Err(LimboError::Corrupt(format!(
                    "{CLASSES_TABLE_NAME} has a duplicate relation: {class:?}"
                )));
            }
            catalog.add(class);
        }
        Ok(catalog)
    }

    /// The class rows, ordered by oid.
    pub(crate) fn classes(&self) -> impl Iterator<Item = &RelationClass> {
        self.by_oid.values()
    }

    pub(crate) fn get_by_oid(&self, oid: i64) -> Option<&RelationClass> {
        self.by_oid.get(&oid)
    }

    pub(crate) fn get_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&RelationClass> {
        self.oids_by_name
            .get(&(namespace, name.to_string()))
            .and_then(|oid| self.by_oid.get(oid))
    }

    /// The relations the table with oid `table_oid` owns: the sequences of
    /// its serial columns.
    fn owned_by(&self, table_oid: i64) -> impl Iterator<Item = &RelationClass> {
        self.by_oid
            .values()
            .filter(move |class| class.relownedby == table_oid)
    }

    fn get_mut_by_name(
        &mut self,
        namespace: NamespaceId,
        name: &str,
    ) -> Option<&mut RelationClass> {
        let oid = self.oids_by_name.get(&(namespace, name.to_string()))?;
        Some(self.by_oid.get_mut(oid).expect("both maps hold the row"))
    }

    fn add(&mut self, class: RelationClass) {
        assert!(
            !self.by_oid.contains_key(&class.oid),
            "oid {} is already in use",
            class.oid
        );
        let previous = self
            .oids_by_name
            .insert((class.relnamespace, class.relname.clone()), class.oid);
        assert!(
            previous.is_none(),
            "relation {} already has a class row",
            class.relname
        );
        self.by_oid.insert(class.oid, class);
    }

    fn remove(&mut self, namespace: NamespaceId, name: &str) -> Option<RelationClass> {
        let oid = self.oids_by_name.remove(&(namespace, name.to_string()))?;
        self.by_oid.remove(&oid)
    }
}

fn class_from_row(row: &[Value]) -> turso_core::Result<RelationClass> {
    let [oid, relname, relnamespace, relkind, relowner, relownedby, relacl, relrowsecurity, relpolicies, relownedbycolumn] =
        row
    else {
        return Err(corrupt_row(row));
    };
    match (
        oid,
        relname,
        relnamespace,
        relkind,
        relowner,
        relownedby,
        relrowsecurity,
    ) {
        (
            Value::Numeric(Numeric::Integer(oid)),
            Value::Text(relname),
            Value::Numeric(Numeric::Integer(relnamespace)),
            Value::Text(relkind),
            Value::Numeric(Numeric::Integer(relowner)),
            Value::Numeric(Numeric::Integer(relownedby)),
            Value::Numeric(Numeric::Integer(relrowsecurity @ (0 | 1))),
        ) if *oid >= FIRST_NORMAL_OBJECT_ID
            && u32::try_from(*relnamespace).is_ok()
            && *relowner >= 0
            && *relownedby >= 0 =>
        {
            let Some(relkind) = relation_kind_of_letter(relkind.as_str()) else {
                return Err(corrupt_row(row));
            };
            let relacl = match relacl {
                Value::Null => None,
                Value::Text(text) => {
                    Some(Acl::parse(text.as_str()).ok_or_else(|| corrupt_row(row))?)
                }
                _ => return Err(corrupt_row(row)),
            };
            let relpolicies = match relpolicies {
                Value::Null => TablePolicies::default(),
                Value::Text(text) => {
                    policies::parse(text.as_str()).ok_or_else(|| corrupt_row(row))?
                }
                _ => return Err(corrupt_row(row)),
            };
            let relownedbycolumn = match relownedbycolumn {
                Value::Null => None,
                Value::Text(text) if *relownedby != 0 => Some(text.as_str().to_string()),
                _ => return Err(corrupt_row(row)),
            };
            Ok(RelationClass {
                oid: *oid,
                relname: relname.as_str().to_string(),
                relnamespace: NamespaceId::new(*relnamespace as u32),
                relkind,
                relowner: RoleId::from_stored(*relowner),
                relownedby: *relownedby,
                relownedbycolumn,
                relacl,
                relrowsecurity: *relrowsecurity == 1,
                relpolicies,
            })
        }
        _ => Err(corrupt_row(row)),
    }
}

fn corrupt_row(row: &[Value]) -> LimboError {
    LimboError::Corrupt(format!("{CLASSES_TABLE_NAME} has an invalid row: {row:?}"))
}

/// The catalog that the engine loaded with the frontend's schema state, or
/// None when it is not loaded, as in a read-only open of an empty database.
pub(crate) fn class_catalog(schema: &Schema) -> Option<&ClassCatalog> {
    schema_state(schema).map(|state| state.classes.as_ref())
}

/// The query that reads the relation catalog for the frontend's schema
/// state, or None when the database has no classes table.
pub(crate) fn schema_state_query(schema: &Schema) -> Option<String> {
    schema.get_btree_table(NamespaceId::ROOT, CLASSES_TABLE_NAME)?;
    Some(format!(
        "SELECT oid, relname, relnamespace, relkind, relowner, relownedby, relacl, \
         relrowsecurity, relpolicies, relownedbycolumn FROM {CLASSES_TABLE_NAME}"
    ))
}

/// [`turso_core::Dialect::relation_owner`] for the PostgreSQL frontend.
pub(crate) fn relation_owner(
    schema: &Schema,
    namespace: NamespaceId,
    name: &str,
) -> Option<RoleId> {
    Some(
        class_catalog(schema)?
            .get_by_name(namespace, name)?
            .relowner,
    )
}

/// [`turso_core::Dialect::owned_sequences`] for the PostgreSQL frontend.
pub(crate) fn owned_sequences(
    schema: &Schema,
    namespace: NamespaceId,
    table_name: &str,
    column: Option<&str>,
) -> Vec<String> {
    let Some(catalog) = class_catalog(schema) else {
        return Vec::new();
    };
    let Some(table) = catalog.get_by_name(namespace, table_name) else {
        return Vec::new();
    };
    catalog
        .owned_by(table.oid)
        .filter(|owned| owned.relkind == RelationKind::Sequence)
        .filter(|owned| column.is_none_or(|column| owned.is_owned_by_column(column)))
        .map(|owned| owned.relname.clone())
        .collect()
}

/// [`turso_core::Dialect::role_has_privilege`] for the PostgreSQL frontend:
/// a role has the privileges the relation's list gives it, by oid or through
/// PUBLIC, together with those it gives the roles whose privileges the role
/// inherits. A relation without a list has the default one, which gives the
/// owner every privilege and no one else any. As in PostgreSQL, an owner
/// that revoked a privilege from itself no longer has it. A table without a
/// class row is one of the engine's own, such as `__turso_internal_authid` or
/// `sqlite_sequence`, and only a superuser uses it, because nothing says who
/// else may.
pub(crate) fn role_has_privilege(
    schema: &Schema,
    namespace: NamespaceId,
    name: &str,
    role: RoleId,
    privilege: ast::Privilege,
) -> bool {
    if is_stored_catalog_table(namespace, name) {
        return privilege == ast::Privilege::Select;
    }
    let Some(class) =
        class_catalog(schema).and_then(|catalog| catalog.get_by_name(namespace, name))
    else {
        return false;
    };
    let roles = schema_roles(schema);
    let Some(acl) = &class.relacl else {
        return roles.has_privileges_of(role, class.relowner);
    };
    let wanted = Privileges::named_on(class.relkind, &[privilege]);
    !wanted.is_empty() && acl.grants(&roles.oids_of_roles_used_by(role), wanted)
}

/// The access control list of relation `name` of `namespace`, or None when
/// no `GRANT` has touched it.
pub(crate) fn relation_acl(schema: &Schema, namespace: NamespaceId, name: &str) -> Option<Acl> {
    class_catalog(schema)?
        .get_by_name(namespace, name)?
        .relacl
        .clone()
}

/// [`turso_core::Dialect::table_rls_enabled`] for the PostgreSQL frontend:
/// whether table `name` of `namespace` has row-level security, as
/// `pg_class.relrowsecurity` shows it. A relation without a class row is
/// the engine's, such as a catalog table, and has none; nor does any table
/// while the catalog is not loaded, when no relation has an owner either.
pub(crate) fn table_rls_enabled(schema: &Schema, namespace: NamespaceId, name: &str) -> bool {
    class_catalog(schema)
        .and_then(|catalog| catalog.get_by_name(namespace, name))
        .is_some_and(|class| class.relrowsecurity)
}

/// [`turso_core::Dialect::row_security_policies`] for the PostgreSQL
/// frontend.
pub(crate) fn row_security_policies<'a>(
    schema: &'a Schema,
    namespace: NamespaceId,
    name: &str,
) -> &'a [RowPolicy] {
    class_catalog(schema)
        .and_then(|catalog| catalog.get_by_name(namespace, name))
        .map_or(&[], |class| class.relpolicies.as_slice())
}

/// The catalog of a PostgreSQL database: the engine's `sqlite_schema` rows,
/// and a class row for each relation of the main database.
#[derive(Debug)]
pub(crate) struct PostgresCatalog;

impl Catalog for PostgresCatalog {
    fn emit(
        &self,
        program: &mut ProgramBuilder,
        ctx: &CmdContext<'_>,
        cmd: &CatalogCmd<'_>,
    ) -> turso_core::Result<()> {
        SqliteCatalog.emit(program, ctx, cmd)?;
        match cmd {
            CatalogCmd::CreateTable(table) => {
                if let Some(rows) = ClassRowWriter::open(program, ctx, true)? {
                    rows.new_relation(
                        program,
                        table.namespace,
                        table.name,
                        RelationKind::Table,
                        None,
                    )?;
                    for index in table.automatic_indexes {
                        rows.new_relation(
                            program,
                            table.namespace,
                            &index.name,
                            RelationKind::Index,
                            Some(table.name),
                        )?;
                    }
                }
            }
            CatalogCmd::CreateIndex(index) => {
                if let Some(rows) = ClassRowWriter::open(program, ctx, true)? {
                    rows.new_relation(
                        program,
                        index.namespace,
                        index.name,
                        RelationKind::Index,
                        Some(index.table),
                    )?;
                }
            }
            CatalogCmd::DropIndex(index) => {
                if let Some(rows) = ClassRowWriter::open(program, ctx, false)? {
                    rows.dropped_relation(program, index.namespace, index.name);
                }
            }
            CatalogCmd::CreateSequence(sequence) => {
                if let Some(rows) = ClassRowWriter::open(program, ctx, true)? {
                    rows.new_relation(
                        program,
                        sequence.namespace,
                        sequence.name,
                        RelationKind::Sequence,
                        None,
                    )?;
                }
            }
            CatalogCmd::DropSequence(sequence) => {
                if let Some(rows) = ClassRowWriter::open(program, ctx, false)? {
                    rows.dropped_relation(program, sequence.namespace, sequence.name);
                }
            }
            CatalogCmd::DropTable(table) => {
                // A virtual table has no class row, in any database.
                if table.is_virtual {
                    return Ok(());
                }
                // The class rows that go away with the table: its own and
                // those of its indexes.
                if let Some(rows) = ClassRowWriter::open(program, ctx, false)? {
                    rows.dropped_relation(program, table.namespace, table.name);
                    for index in table.indexes {
                        rows.dropped_relation(program, table.namespace, index);
                    }
                }
            }
            CatalogCmd::CreateView(view) => {
                if let Some(rows) = ClassRowWriter::open(program, ctx, true)? {
                    rows.new_relation(
                        program,
                        view.namespace,
                        view.name,
                        RelationKind::View,
                        None,
                    )?;
                }
            }
            CatalogCmd::CreateMaterializedView(view) => {
                if let Some(rows) = ClassRowWriter::open(program, ctx, true)? {
                    rows.new_relation(
                        program,
                        view.namespace,
                        view.name,
                        RelationKind::MaterializedView,
                        None,
                    )?;
                }
            }
            CatalogCmd::DropView(view) => {
                if let Some(rows) = ClassRowWriter::open(program, ctx, false)? {
                    rows.dropped_relation(program, view.namespace, view.name);
                }
            }
            CatalogCmd::RenameTable(table) => {
                // A virtual table has no class row, in any database.
                if table.is_virtual {
                    return Ok(());
                }
                // The automatic indexes get their new names with the table.
                if let Some(rows) = ClassRowWriter::open(program, ctx, false)? {
                    rows.renamed_relation(program, table.namespace, table.old_name, table.new_name);
                    for (old_name, new_name) in table.renamed_automatic_indexes() {
                        rows.renamed_relation(program, table.namespace, old_name, &new_name);
                    }
                }
            }
            CatalogCmd::RenameColumn(column) => {
                rename_owner_column(
                    program,
                    ctx,
                    column.namespace,
                    column.table,
                    column.old_name,
                    column.new_name.as_str(),
                )?;
            }
            CatalogCmd::AlterColumn(column) => {
                rename_owner_column(
                    program,
                    ctx,
                    column.namespace,
                    column.table,
                    column.old_name,
                    column.definition.col_name.as_str(),
                )?;
            }
            // A new column owns no sequence, so no class row changes.
            CatalogCmd::AddColumn(_) => {}
            // The statement drops the sequences that the column owns
            // itself, each with its class row.
            CatalogCmd::DropColumn(column) => {
                if ctx.origin != CmdOrigin::Engine && ctx.database_id == MAIN_DB_ID {
                    emit_dropped_column_comments(program, ctx, column);
                }
            }
        }
        Ok(())
    }

    fn emit_access(
        &self,
        program: &mut ProgramBuilder,
        ctx: &CmdContext<'_>,
        statement: &AccessStatement<'_>,
    ) -> turso_core::Result<()> {
        match statement {
            AccessStatement::CreateRole { name, options } => {
                crate::roles::translate_create_role(program, ctx.schema, name, options)
            }
            AccessStatement::DropRole { if_exists, roles } => crate::roles::translate_drop_role(
                program,
                ctx.schema,
                ctx.connection,
                *if_exists,
                roles,
            ),
            AccessStatement::GrantRole {
                revoke,
                roles,
                grantees,
            } => crate::auth_members::translate_grant_role(
                program,
                ctx.schema,
                ctx.connection,
                *revoke,
                roles,
                grantees,
            ),
            // The owner column of a sequence is in the sequence's class row.
            AccessStatement::ChangeOwner {
                owner: NewOwner::Column(_),
                ..
            } if !writes_class_rows(ctx)? => Ok(()),
            _ => translate_access_statement(program, ctx.schema, ctx.connection, statement),
        }
    }

    fn load_steps(&self) -> Vec<Box<dyn LoadStep>> {
        crate::schema_state::load_steps()
    }
}

/// Deletes the comment of the column that `column.table` no longer has, and
/// moves the comments of the later columns to the numbers those columns now
/// have.
fn emit_dropped_column_comments(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    column: &DropColumn<'_>,
) {
    // An engine table has no row and no comments.
    let Some(class) = class_catalog(ctx.schema)
        .and_then(|catalog| catalog.get_by_name(column.namespace, column.name))
    else {
        return;
    };
    let Some(table_before) = ctx.schema.get_btree_table(column.namespace, column.name) else {
        return;
    };
    let remaining = column.table.columns();
    let position = table_before
        .columns()
        .iter()
        .zip(remaining)
        .position(|(before, after)| before.name != after.name)
        .unwrap_or(remaining.len());
    let column_number = position as i64 + 1;
    emit_delete_comments(
        program,
        ctx.schema,
        class.oid,
        Dropped::Column(column_number),
    );
}

/// Stores the new name of column `old_name` of `table` in the class rows
/// of the sequences that the column owns. A column that keeps its name owns
/// the same sequences as before.
fn rename_owner_column(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    namespace: NamespaceId,
    table: &str,
    old_name: &str,
    new_name: &str,
) -> turso_core::Result<()> {
    if old_name == new_name {
        return Ok(());
    }
    if let Some(rows) = ClassRowWriter::open(program, ctx, false)? {
        rows.renamed_owner_column(program, namespace, table, old_name, new_name);
    }
    Ok(())
}

/// Writes the class rows that one statement changes, with cursors that it
/// opens once.
struct ClassRowWriter<'a> {
    schema: &'a Schema,
    connection: &'a Arc<Connection>,
    cursors: ClassTableCursors,
}

impl<'a> ClassRowWriter<'a> {
    /// Opens the class table for the statement of `ctx`. None when the
    /// relations of the statement have no class rows, see
    /// [`writes_class_rows`], or the database has no class table.
    /// `allocates_oids` says that the statement adds a class row.
    fn open(
        program: &mut ProgramBuilder,
        ctx: &CmdContext<'a>,
        allocates_oids: bool,
    ) -> turso_core::Result<Option<Self>> {
        if !writes_class_rows(ctx)? {
            return Ok(None);
        }
        let cursors = open_class_table_for_writing(program, ctx.schema, allocates_oids)?;
        Ok(cursors.map(|cursors| Self {
            schema: ctx.schema,
            connection: ctx.connection,
            cursors,
        }))
    }

    /// Inserts the class row of a relation that the statement creates.
    /// `table` is the table of an index, in the same namespace: its owner
    /// owns the index. Anything else is owned by the current role, which is
    /// what PostgreSQL's `GetUserId()` gives after `SET ROLE`.
    fn new_relation(
        &self,
        program: &mut ProgramBuilder,
        namespace: NamespaceId,
        name: &str,
        kind: RelationKind,
        table: Option<&str>,
    ) -> turso_core::Result<()> {
        // Relation names are unique per schema across kinds.
        if is_user_relation_name(kind, name)
            && class_catalog(self.schema)
                .is_some_and(|catalog| catalog.get_by_name(namespace, name).is_some())
        {
            return Err(LimboError::ParseError(format!(
                "relation \"{name}\" already exists"
            )));
        }
        let owner = table
            .and_then(|table| relation_owner(self.schema, namespace, table))
            .unwrap_or_else(|| self.connection.current_role());
        emit_class_row_insert(program, &self.cursors, namespace, name, kind, owner);
        Ok(())
    }

    /// Deletes the class row of a relation that the statement drops.
    fn dropped_relation(&self, program: &mut ProgramBuilder, namespace: NamespaceId, name: &str) {
        // An engine relation has no row and nothing to do.
        let Some(class) =
            class_catalog(self.schema).and_then(|catalog| catalog.get_by_name(namespace, name))
        else {
            return;
        };
        emit_delete_comments(program, self.schema, class.oid, Dropped::Relation);
        emit_class_row_delete(program, &self.cursors, namespace, name)
    }

    /// Stores the new name of a relation. Its row keeps its oid and owner.
    fn renamed_relation(
        &self,
        program: &mut ProgramBuilder,
        namespace: NamespaceId,
        old_name: &str,
        new_name: &str,
    ) {
        let has_row = class_catalog(self.schema)
            .is_some_and(|catalog| catalog.get_by_name(namespace, old_name).is_some());
        if has_row {
            emit_class_row_rename(program, &self.cursors, namespace, old_name, new_name);
        }
    }

    /// Stores the new name of column `old_name` of `table` in the class
    /// rows of the sequences that the column owns.
    fn renamed_owner_column(
        &self,
        program: &mut ProgramBuilder,
        namespace: NamespaceId,
        table: &str,
        old_name: &str,
        new_name: &str,
    ) {
        let owned: Vec<(String, i64)> = class_catalog(self.schema)
            .and_then(|catalog| {
                let table = catalog.get_by_name(namespace, table)?;
                Some(
                    catalog
                        .owned_by(table.oid)
                        .filter(|owned| owned.is_owned_by_column(old_name))
                        .map(|owned| (owned.relname.clone(), table.oid))
                        .collect(),
                )
            })
            .unwrap_or_default();
        if owned.is_empty() {
            return;
        }
        for (sequence, table_oid) in owned {
            emit_class_row_set_owned_by(
                program,
                &self.cursors,
                namespace,
                &sequence,
                table_oid,
                Some(new_name),
            );
        }
        OwnerColumnRenamed {
            namespace,
            table: table.to_string(),
            old_name: old_name.to_string(),
            new_name: new_name.to_string(),
        }
        .emit(program);
    }
}

/// Whether the relations of the statement get class rows. Only the
/// relations a user creates in the main database do. A statement of the
/// engine copies or replays a database whose class rows are stored as data
/// already, and the temp database has no class table. Fails for a user's
/// statement against an attached database: the class table covers only the
/// main database, so a database file with relations made while it was
/// attached could not be opened later.
fn writes_class_rows(ctx: &CmdContext<'_>) -> turso_core::Result<bool> {
    if ctx.origin == CmdOrigin::Engine {
        return Ok(false);
    }
    if turso_core::is_attached_db(ctx.database_id) {
        return Err(LimboError::ParseError(
            "creating, renaming or dropping relations in an attached database is not supported"
                .to_string(),
        ));
    }
    Ok(ctx.database_id == MAIN_DB_ID)
}

/// Emits the bytecode of the [`AccessStatement`]s that change a class row.
/// `ALTER ... OWNER TO` sets the owner, with PostgreSQL's errors for a
/// missing role (42704) and a catalog relation (42501). A sequence a serial column owns changes owner with its table
/// only, as in PostgreSQL: on its own it fails with `cannot change owner of
/// sequence` (0A000). `GRANT` adds to the access control list when the owner
/// or a superuser runs it; anyone else grants nothing, as in PostgreSQL,
/// which only warns. `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` sets
/// `relrowsecurity` of the table's row, which a catalog table does not have
/// (42501).
pub(crate) fn translate_access_statement(
    program: &mut ProgramBuilder,
    schema: &Schema,
    connection: &Arc<Connection>,
    statement: &AccessStatement<'_>,
) -> turso_core::Result<()> {
    let allocates_oids = matches!(statement, AccessStatement::CreatePolicy { .. });
    let Some(cursors) = open_class_table_for_writing(program, schema, allocates_oids)? else {
        return match statement {
            AccessStatement::ChangeOwner {
                owner: NewOwner::Column(_),
                ..
            } => Ok(()),
            AccessStatement::Grant { relations: [], .. } => Ok(()),
            _ => Err(LimboError::ParseError(
                "the database has no relation catalog".to_string(),
            )),
        };
    };
    match statement {
        AccessStatement::ChangeOwner {
            namespace,
            name,
            owner: NewOwner::Column(owner),
        } => {
            let table_oid = match owner {
                Some(owner) => class_catalog(schema)
                    .and_then(|catalog| catalog.get_by_name(*namespace, &owner.table))
                    .map(|class| class.oid)
                    .ok_or_else(|| {
                        LimboError::ParseError(format!(
                            "relation \"{}\" does not exist",
                            owner.table
                        ))
                    })?,
                None => 0,
            };
            emit_class_row_set_owned_by(
                program,
                &cursors,
                *namespace,
                name,
                table_oid,
                owner.as_ref().map(|owner| owner.column.as_str()),
            );
            OwnerColumnSet {
                namespace: *namespace,
                name: name.to_string(),
                owner: owner.clone(),
            }
            .emit(program);
        }
        AccessStatement::ChangeOwner {
            namespace,
            name,
            owner: NewOwner::Role(new_owner),
        } => {
            let owner = resolve_new_owner(schema, connection, *namespace, name, new_owner)?;
            let Some(class) =
                class_catalog(schema).and_then(|catalog| catalog.get_by_name(*namespace, name))
            else {
                return Ok(());
            };
            // As in PostgreSQL: nothing changes when the owner stays, an
            // index's owner is its table's, and a serial column's
            // sequence changes owner with its table only.
            if class.relowner == owner || class.relkind == RelationKind::Index {
                return Ok(());
            }
            if class.relownedby != 0 {
                return Err(LimboError::ParseError(format!(
                    "cannot change owner of sequence \"{name}\""
                )));
            }
            // A table's indexes and what it owns, such as the sequences
            // of its serial columns, go with it.
            let mut with_it = vec![(*namespace, name.to_string())];
            with_it.extend(
                class_catalog(schema)
                    .into_iter()
                    .flat_map(|catalog| catalog.owned_by(class.oid))
                    .map(|owned| (owned.relnamespace, owned.relname.clone())),
            );
            with_it.extend(
                schema
                    .get_indices(*namespace, name)
                    .filter(|index| {
                        is_user_relation_name(RelationKind::Index, &index.name)
                            && !index.is_backing_btree_index()
                    })
                    .map(|index| (*namespace, index.name.clone())),
            );
            for (other_namespace, other_name) in with_it {
                // The new owner takes the old one's place in the list.
                let acl = class_catalog(schema)
                    .and_then(|catalog| catalog.get_by_name(other_namespace, &other_name))
                    .and_then(|other| other.relacl.clone())
                    .map(|mut acl| {
                        acl.change_owner(role_oid(class.relowner), role_oid(owner));
                        acl
                    });
                emit_class_row_set_owner(
                    program,
                    &cursors,
                    other_namespace,
                    &other_name,
                    owner,
                    acl.as_ref(),
                );
            }
        }
        AccessStatement::Grant {
            revoke,
            relations,
            privileges,
            grantees: statement_grantees,
        } => {
            let statement = if *revoke { "REVOKE" } else { "GRANT" };
            let grantees = statement_grantees
                .iter()
                .map(|grantee| resolve_grantee(schema, connection, grantee))
                .collect::<turso_core::Result<Vec<_>>>()?;
            for RelationName { namespace, name } in *relations {
                // A relation without a class row has no list to store the
                // privileges in. Only a superuser uses the engine's own
                // tables, so a GRANT or REVOKE on one would claim a change it
                // does not make, and is refused. Every role reads the catalog
                // views, such as pg_class, so a GRANT on one changes nothing,
                // but a REVOKE would leave every role reading it, and is
                // refused too.
                let Some(class) =
                    class_catalog(schema).and_then(|catalog| catalog.get_by_name(*namespace, name))
                else {
                    if schema.get_btree_table(*namespace, name).is_some() {
                        return Err(sqlstate_error(
                            SQLSTATE_FEATURE_NOT_SUPPORTED,
                            format!("{statement} on internal table {name} is not supported"),
                            None,
                        ));
                    }
                    if *revoke {
                        return Err(sqlstate_error(
                            SQLSTATE_FEATURE_NOT_SUPPORTED,
                            format!(
                                "REVOKE on system catalog {} is not supported",
                                catalog_view_name(name)
                            ),
                            None,
                        ));
                    }
                    continue;
                };
                // `ON TABLE` may name a sequence, so USAGE is refused here,
                // for a relation that is not one, as in PostgreSQL.
                if class.relkind != RelationKind::Sequence
                    && privileges.contains(&ast::Privilege::Usage)
                {
                    return Err(sqlstate_error(
                        SQLSTATE_INVALID_GRANT_OPERATION,
                        "invalid privilege type USAGE for table",
                        None,
                    ));
                }
                let owner = role_oid(class.relowner);
                let mut acl = class
                    .relacl
                    .clone()
                    .unwrap_or_else(|| Acl::default_for(class.relkind, owner));
                // Only the owner may grant or revoke, and a superuser or a
                // role with the owner's privileges does so as the owner. As
                // in PostgreSQL, anyone else is refused when the list gives
                // it nothing on the relation, and changes nothing otherwise,
                // for want of a grant option.
                let roles = schema_roles(schema);
                let current = connection.current_role();
                if !roles.has_privileges_of(current, class.relowner) {
                    if acl.gives_anything_to(&roles.oids_of_roles_used_by(current)) {
                        continue;
                    }
                    return Err(LimboError::PermissionDenied(Box::new(
                        PermissionDenied::privilege(Action::Read, relation_object(class)),
                    )));
                }
                // As in PostgreSQL, a statement that names no privilege of
                // the relation, such as INSERT on a sequence, leaves its list
                // as it is, even when it has none yet. Otherwise a relation
                // without a list gets the owner's default one first, so a
                // REVOKE that takes nothing away still stores it.
                let named = Privileges::named_on(class.relkind, privileges);
                if named.is_empty() {
                    continue;
                }
                for grantee in &grantees {
                    if *revoke {
                        acl.revoke(*grantee, named, owner);
                    } else {
                        acl.grant(*grantee, named, owner);
                    }
                }
                if class.relacl.as_ref() == Some(&acl) {
                    continue;
                }
                emit_class_row_set_acl(program, &cursors, *namespace, name, &acl);
            }
        }
        AccessStatement::SetRowSecurity {
            namespace,
            name,
            enabled,
        } => {
            let Some(class) =
                class_catalog(schema).and_then(|catalog| catalog.get_by_name(*namespace, name))
            else {
                return Err(LimboError::ParseError(format!(
                    "permission denied: \"{name}\" is a system catalog"
                )));
            };
            if class.relrowsecurity == *enabled {
                return Ok(());
            }
            emit_class_row_set_row_security(program, &cursors, *namespace, name, *enabled);
        }
        AccessStatement::CreatePolicy {
            namespace,
            name,
            policy,
        } => {
            let class = table_class(schema, *namespace, name)?;
            let policy_name = policy.policy_name.as_str();
            if class.relpolicies.contains(policy_name) {
                return Err(sqlstate_error(
                    SQLSTATE_DUPLICATE_OBJECT,
                    format!("policy \"{policy_name}\" for table \"{name}\" already exists"),
                    None,
                ));
            }
            let roles = policy
                .roles
                .iter()
                .map(|role| resolve_policy_role(schema, connection, role))
                .collect::<turso_core::Result<Vec<_>>>()?;
            let (before_oid, after_oid) = class.relpolicies.stored_text_around_new_oid(
                RowPolicy {
                    name: policy_name.to_string(),
                    permissive: policy.permissive,
                    command: policy.command,
                    roles,
                    using_expr: policy.using_expr.as_deref().cloned(),
                    check_expr: policy.check_expr.as_deref().cloned(),
                },
                policy.using_sql.clone(),
                policy.check_sql.clone(),
            );
            // The catalog reads the conditions back from the stored form,
            // so a condition that does not read back is refused here
            // rather than leaving a database that cannot be opened.
            let some_oid = FIRST_NORMAL_OBJECT_ID;
            if policies::parse(&format!("{before_oid}{some_oid}{after_oid}")).is_none() {
                return Err(LimboError::ParseError(format!(
                    "the conditions of policy \"{policy_name}\" cannot be stored"
                )));
            }
            emit_class_row_set_policies(
                program,
                &cursors,
                *namespace,
                name,
                StoredPolicies::WithNewOid {
                    before_oid,
                    after_oid,
                },
            );
        }
        AccessStatement::DropPolicy {
            namespace,
            name,
            policy_name,
            if_exists,
        } => {
            let class = table_class(schema, *namespace, name)?;
            if !class.relpolicies.contains(policy_name) {
                if *if_exists {
                    return Ok(());
                }
                return Err(sqlstate_error(
                    SQLSTATE_UNDEFINED_OBJECT,
                    format!("policy \"{policy_name}\" for table \"{name}\" does not exist"),
                    None,
                ));
            }
            let relpolicies = class.relpolicies.without(policy_name);
            let stored = if relpolicies.is_empty() {
                StoredPolicies::None
            } else {
                StoredPolicies::Text(relpolicies.stored_text())
            };
            emit_class_row_set_policies(program, &cursors, *namespace, name, stored);
        }
        AccessStatement::CreateRole { .. }
        | AccessStatement::DropRole { .. }
        | AccessStatement::GrantRole { .. } => {
            unreachable!("statements about roles do not change a class row")
        }
    }
    Ok(())
}

/// The name a catalog view has in PostgreSQL: the engine names
/// `information_schema.schemata` `pg_information_schema_schemata`.
fn catalog_view_name(name: &str) -> String {
    match name.strip_prefix("pg_information_schema_") {
        Some(view) => format!("information_schema.{view}"),
        None => name.to_string(),
    }
}

/// The class row of table `name` of `namespace`, which row-level security
/// statements change. A relation without one is the engine's.
fn table_class<'a>(
    schema: &'a Schema,
    namespace: NamespaceId,
    name: &str,
) -> turso_core::Result<&'a RelationClass> {
    class_catalog(schema)
        .and_then(|catalog| catalog.get_by_name(namespace, name))
        .ok_or_else(|| {
            LimboError::ParseError(format!("permission denied: \"{name}\" is a system catalog"))
        })
}

/// The role a policy's TO clause names.
fn resolve_policy_role(
    schema: &Schema,
    connection: &Arc<Connection>,
    role: &ast::Grantee,
) -> turso_core::Result<RoleId> {
    match role {
        ast::Grantee::Public => Ok(RoleId::PUBLIC),
        ast::Grantee::Role(role) => resolve_role(schema, connection, role),
    }
}

/// The relation of `class` as a permission error names it.
fn relation_object(class: &RelationClass) -> Object {
    match class.relkind {
        RelationKind::View | RelationKind::MaterializedView => Object::View(class.relname.clone()),
        RelationKind::Sequence => Object::Sequence(class.relname.clone()),
        RelationKind::Table | RelationKind::Index => Object::Table(class.relname.clone()),
    }
}

/// The role oid a `GRANT` stores for `grantee`, [`PUBLIC_OID`] for
/// everyone. A named role must exist.
fn resolve_grantee(
    schema: &Schema,
    connection: &Arc<Connection>,
    grantee: &ast::Grantee,
) -> turso_core::Result<i64> {
    Ok(match grantee {
        ast::Grantee::Public => PUBLIC_OID,
        ast::Grantee::Role(ast::RoleSpec::CurrentRole) => role_oid(connection.current_role()),
        ast::Grantee::Role(ast::RoleSpec::SessionUser) => role_oid(connection.session_role()),
        ast::Grantee::Role(ast::RoleSpec::Name(name)) => {
            role_oid_by_name(schema_roles(schema), name)
                .ok_or_else(|| LimboError::ParseError(format!("role \"{name}\" does not exist")))?
        }
    })
}

/// The role `ALTER ... OWNER TO new_owner` gives relation `name` of
/// `namespace` to. A relation without a class row is the engine's.
fn resolve_new_owner(
    schema: &Schema,
    connection: &Arc<Connection>,
    namespace: NamespaceId,
    name: &str,
    new_owner: &ast::RoleSpec,
) -> turso_core::Result<RoleId> {
    let has_row =
        class_catalog(schema).is_some_and(|catalog| catalog.get_by_name(namespace, name).is_some());
    if !has_row {
        return Err(LimboError::ParseError(format!(
            "permission denied: \"{name}\" is a system catalog"
        )));
    }
    resolve_role(schema, connection, new_owner)
}

/// The role that `role` names. A named role must exist.
fn resolve_role(
    schema: &Schema,
    connection: &Arc<Connection>,
    role: &ast::RoleSpec,
) -> turso_core::Result<RoleId> {
    Ok(match role {
        ast::RoleSpec::CurrentRole => connection.current_role(),
        ast::RoleSpec::SessionUser => connection.session_role(),
        ast::RoleSpec::Name(name) => engine_role_by_name(schema_roles(schema), name)
            .ok_or_else(|| LimboError::ParseError(format!("role \"{name}\" does not exist")))?,
    })
}

/// The cursors of one statement's class row writes: the classes table and
/// its name index, and the oid counter.
struct ClassTableCursors {
    table_cursor_id: usize,
    index_cursor_id: usize,
    index_name: String,
    /// Only opened when the statement adds a relation or a policy, which
    /// takes an oid.
    oids: Option<OidAllocator>,
}

/// Opens the classes table of the main database for writing, or returns None
/// when the database has no catalog, which only happens outside the
/// frontend's own open path, such as a core-level open for tests.
/// `allocates_oids` also opens the oid counter.
fn open_class_table_for_writing(
    program: &mut ProgramBuilder,
    schema: &Schema,
    allocates_oids: bool,
) -> turso_core::Result<Option<ClassTableCursors>> {
    let Some(table) = schema.get_btree_table(NamespaceId::ROOT, CLASSES_TABLE_NAME) else {
        return Ok(None);
    };
    let name_index = schema
        .get_indices(NamespaceId::ROOT, CLASSES_TABLE_NAME)
        .find(|index| index.name == CLASSES_NAME_INDEX_NAME)
        .cloned()
        .ok_or_else(|| LimboError::Corrupt(format!("{CLASSES_TABLE_NAME} has no name index")))?;

    let table_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: table_cursor_id,
        root_page: RegisterOrLiteral::Literal(table.root_page),
        db: MAIN_DB_ID,
    });
    let index_cursor_id = program.alloc_cursor_index(None, &name_index)?;
    program.emit_insn(Insn::OpenWrite {
        cursor_id: index_cursor_id,
        root_page: RegisterOrLiteral::Literal(name_index.root_page),
        db: MAIN_DB_ID,
    });
    let oids = if allocates_oids {
        Some(OidAllocator::open(program, schema)?)
    } else {
        None
    };
    Ok(Some(ClassTableCursors {
        table_cursor_id,
        index_cursor_id,
        index_name: name_index.name.clone(),
        oids,
    }))
}

/// Inserts the class row of relation `name` of `namespace` under the next
/// oid, owned by `owner`. Engine relations get no row.
fn emit_class_row_insert(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    kind: RelationKind,
    owner: RoleId,
) {
    if !is_user_relation_name(kind, name) {
        return;
    }
    let oid_reg = oid_allocator(cursors).emit_allocate_rowid(
        program,
        cursors.table_cursor_id,
        CLASSES_TABLE_NAME,
    );

    emit_class_name_index_insert(program, cursors, namespace, name, oid_reg);

    // The oid is the rowid, so the record keeps NULL for it, as SQLite does
    // for an INTEGER PRIMARY KEY column.
    let first_column_reg = program.alloc_registers(CLASS_COLUMN_COUNT);
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
    program.emit_insn(Insn::String8 {
        value: relkind_letter(kind).to_string(),
        dest: first_column_reg + 3,
    });
    program.emit_insn(Insn::Integer {
        value: owner.get(),
        dest: first_column_reg + 4,
    });
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: first_column_reg + 5,
    });
    // No GRANT has touched a new relation, and it has no row-level security.
    program.emit_insn(Insn::Null {
        dest: first_column_reg + 6,
        dest_end: None,
    });
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: first_column_reg + 7,
    });
    program.emit_insn(Insn::Null {
        dest: first_column_reg + 8,
        dest_end: Some(first_column_reg + 9),
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(first_column_reg),
        count: to_u32(CLASS_COLUMN_COUNT),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: cursors.table_cursor_id,
        key_reg: oid_reg,
        record_reg,
        flag: InsertFlags::new()
            .skip_last_rowid()
            .skip_all_change_counts(),
        table_name: CLASSES_TABLE_NAME.to_string(),
    });

    ClassCreated {
        namespace,
        name: name.to_string(),
        kind,
    }
    .emit(program, oid_reg, first_column_reg + 4);
}

/// The oid counter of a statement that adds a relation or a policy.
fn oid_allocator(cursors: &ClassTableCursors) -> &OidAllocator {
    cursors
        .oids
        .as_ref()
        .expect("the caller opened the cursors with allocates_oids")
}

/// Makes column `column` of the table with oid `table_oid` the owner of
/// relation `name` of `namespace`: its `relownedby` and `relownedbycolumn`.
/// 0 and None make the relation nobody's. The caller tells the in-memory
/// catalog.
fn emit_class_row_set_owned_by(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    table_oid: i64,
    column: Option<&str>,
) {
    emit_find_class_row(program, cursors, namespace, name, |program, row| {
        let record_reg = emit_class_record(
            program,
            ClassRecordColumn::Literal(name.to_string()),
            ClassRecordColumn::LiteralInt(i64::from(namespace.as_u32())),
            ClassRecordColumn::Register(row.relkind_reg),
            ClassRecordColumn::Register(row.relowner_reg),
            ClassRecordColumn::LiteralInt(table_oid),
            ClassRecordColumn::Register(row.relacl_reg),
            ClassRecordColumn::Register(row.relrowsecurity_reg),
            ClassRecordColumn::Register(row.relpolicies_reg),
            match column {
                Some(column) => ClassRecordColumn::Literal(column.to_string()),
                None => ClassRecordColumn::Null,
            },
        );
        emit_class_row_overwrite(program, cursors, row.rowid_reg, record_reg);
    });
}

/// Deletes the class row of relation `name` of `namespace`.
fn emit_class_row_delete(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
) {
    emit_find_class_row(program, cursors, namespace, name, |program, row| {
        emit_class_name_index_delete(program, cursors, namespace, name, row.rowid_reg);
        // Marked as part of an update so that changes() does not count it.
        program.emit_insn(Insn::Delete {
            cursor_id: cursors.table_cursor_id,
            table_name: CLASSES_TABLE_NAME.to_string(),
            is_part_of_update: true,
        });
    });
    ClassDropped {
        namespace,
        name: name.to_string(),
    }
    .emit(program);
}

/// Renames the class row of relation `old_name` of `namespace` to
/// `new_name`, keeping its oid and owner.
fn emit_class_row_rename(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    old_name: &str,
    new_name: &str,
) {
    emit_find_class_row(program, cursors, namespace, old_name, |program, row| {
        emit_class_name_index_delete(program, cursors, namespace, old_name, row.rowid_reg);
        emit_class_name_index_insert(program, cursors, namespace, new_name, row.rowid_reg);
        let record_reg = emit_class_record(
            program,
            ClassRecordColumn::Literal(new_name.to_string()),
            ClassRecordColumn::LiteralInt(i64::from(namespace.as_u32())),
            ClassRecordColumn::Register(row.relkind_reg),
            ClassRecordColumn::Register(row.relowner_reg),
            ClassRecordColumn::Register(row.relownedby_reg),
            ClassRecordColumn::Register(row.relacl_reg),
            ClassRecordColumn::Register(row.relrowsecurity_reg),
            ClassRecordColumn::Register(row.relpolicies_reg),
            ClassRecordColumn::Register(row.relownedbycolumn_reg),
        );
        emit_class_row_overwrite(program, cursors, row.rowid_reg, record_reg);
    });
    ClassRenamed {
        namespace,
        old_name: old_name.to_string(),
        new_name: new_name.to_string(),
    }
    .emit(program);
}

/// Sets the owner of the class row of relation `name` of `namespace` to
/// `owner`, and its access control list to `acl`.
fn emit_class_row_set_owner(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    owner: RoleId,
    acl: Option<&Acl>,
) {
    let owner_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: owner.get(),
        dest: owner_reg,
    });
    let acl_reg = emit_acl(program, acl);
    emit_find_class_row(program, cursors, namespace, name, |program, row| {
        let record_reg = emit_class_record(
            program,
            ClassRecordColumn::Literal(name.to_string()),
            ClassRecordColumn::LiteralInt(i64::from(namespace.as_u32())),
            ClassRecordColumn::Register(row.relkind_reg),
            ClassRecordColumn::Register(owner_reg),
            ClassRecordColumn::Register(row.relownedby_reg),
            ClassRecordColumn::Register(acl_reg),
            ClassRecordColumn::Register(row.relrowsecurity_reg),
            ClassRecordColumn::Register(row.relpolicies_reg),
            ClassRecordColumn::Register(row.relownedbycolumn_reg),
        );
        emit_class_row_overwrite(program, cursors, row.rowid_reg, record_reg);
    });
    OwnerSet {
        namespace,
        name: name.to_string(),
    }
    .emit(program, owner_reg, acl_reg);
}

/// Sets the access control list of the class row of relation `name` of
/// `namespace` to `acl`.
fn emit_class_row_set_acl(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    acl: &Acl,
) {
    let acl_reg = emit_acl(program, Some(acl));
    emit_find_class_row(program, cursors, namespace, name, |program, row| {
        let record_reg = emit_class_record(
            program,
            ClassRecordColumn::Literal(name.to_string()),
            ClassRecordColumn::LiteralInt(i64::from(namespace.as_u32())),
            ClassRecordColumn::Register(row.relkind_reg),
            ClassRecordColumn::Register(row.relowner_reg),
            ClassRecordColumn::Register(row.relownedby_reg),
            ClassRecordColumn::Register(acl_reg),
            ClassRecordColumn::Register(row.relrowsecurity_reg),
            ClassRecordColumn::Register(row.relpolicies_reg),
            ClassRecordColumn::Register(row.relownedbycolumn_reg),
        );
        emit_class_row_overwrite(program, cursors, row.rowid_reg, record_reg);
    });
    AclSet {
        namespace,
        name: name.to_string(),
    }
    .emit(program, acl_reg);
}

/// Sets `relrowsecurity` of the class row of table `name` of `namespace` to
/// `enabled`.
fn emit_class_row_set_row_security(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    enabled: bool,
) {
    emit_find_class_row(program, cursors, namespace, name, |program, row| {
        let record_reg = emit_class_record(
            program,
            ClassRecordColumn::Literal(name.to_string()),
            ClassRecordColumn::LiteralInt(i64::from(namespace.as_u32())),
            ClassRecordColumn::Register(row.relkind_reg),
            ClassRecordColumn::Register(row.relowner_reg),
            ClassRecordColumn::Register(row.relownedby_reg),
            ClassRecordColumn::Register(row.relacl_reg),
            ClassRecordColumn::LiteralInt(i64::from(enabled)),
            ClassRecordColumn::Register(row.relpolicies_reg),
            ClassRecordColumn::Register(row.relownedbycolumn_reg),
        );
        emit_class_row_overwrite(program, cursors, row.rowid_reg, record_reg);
    });
    RowSecuritySet {
        namespace,
        name: name.to_string(),
        enabled,
    }
    .emit(program);
}

/// What a statement stores in `relpolicies`.
enum StoredPolicies {
    /// NULL, for no policies.
    None,
    Text(String),
    /// The stored form of policies, one of which is new and gets the next
    /// oid, which goes between the two parts.
    WithNewOid {
        before_oid: String,
        after_oid: String,
    },
}

/// Sets `relpolicies` of the class row of table `name` of `namespace` to
/// `stored`.
fn emit_class_row_set_policies(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    stored: StoredPolicies,
) {
    let policies_reg = program.alloc_register();
    match stored {
        StoredPolicies::None => {
            program.emit_insn(Insn::Null {
                dest: policies_reg,
                dest_end: None,
            });
        }
        StoredPolicies::Text(text) => {
            program.emit_insn(Insn::String8 {
                value: text,
                dest: policies_reg,
            });
        }
        StoredPolicies::WithNewOid {
            before_oid,
            after_oid,
        } => {
            let oid_reg = oid_allocator(cursors).emit_allocate(program);
            program.emit_insn(Insn::String8 {
                value: before_oid,
                dest: policies_reg,
            });
            program.emit_insn(Insn::Concat {
                lhs: policies_reg,
                rhs: oid_reg,
                dest: policies_reg,
            });
            let after_oid_reg = program.emit_string8_new_reg(after_oid);
            program.emit_insn(Insn::Concat {
                lhs: policies_reg,
                rhs: after_oid_reg,
                dest: policies_reg,
            });
        }
    }
    emit_find_class_row(program, cursors, namespace, name, |program, row| {
        let record_reg = emit_class_record(
            program,
            ClassRecordColumn::Literal(name.to_string()),
            ClassRecordColumn::LiteralInt(i64::from(namespace.as_u32())),
            ClassRecordColumn::Register(row.relkind_reg),
            ClassRecordColumn::Register(row.relowner_reg),
            ClassRecordColumn::Register(row.relownedby_reg),
            ClassRecordColumn::Register(row.relacl_reg),
            ClassRecordColumn::Register(row.relrowsecurity_reg),
            ClassRecordColumn::Register(policies_reg),
            ClassRecordColumn::Register(row.relownedbycolumn_reg),
        );
        emit_class_row_overwrite(program, cursors, row.rowid_reg, record_reg);
    });
    PoliciesSet {
        namespace,
        name: name.to_string(),
    }
    .emit(program, policies_reg);
}

/// A class row that the statement inserted.
#[derive(Debug)]
struct ClassCreated {
    namespace: NamespaceId,
    name: String,
    kind: RelationKind,
}

impl ClassCreated {
    /// `oid_reg` holds the oid the row was stored under and `owner_reg` its
    /// `relowner`.
    fn emit(self, program: &mut ProgramBuilder, oid_reg: usize, owner_reg: usize) {
        emit_catalog_apply(program, self, vec![oid_reg, owner_reg]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, values: &[Value]) -> turso_core::Result<()> {
        let [oid, owner] = values else {
            return Err(LimboError::InternalError(
                "a created relation carries its oid and owner".to_string(),
            ));
        };
        classes.add(RelationClass {
            oid: read_back_integer(oid)?,
            relname: self.name.clone(),
            relnamespace: self.namespace,
            relkind: self.kind,
            relowner: RoleId::from_stored(read_back_integer(owner)?),
            relownedby: 0,
            relownedbycolumn: None,
            relacl: None,
            relrowsecurity: false,
            relpolicies: TablePolicies::default(),
        });
        Ok(())
    }
}

/// A class row that the statement deleted.
#[derive(Debug)]
struct ClassDropped {
    namespace: NamespaceId,
    name: String,
}

impl ClassDropped {
    fn emit(self, program: &mut ProgramBuilder) {
        emit_catalog_apply(program, self, vec![]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, _values: &[Value]) -> turso_core::Result<()> {
        classes.remove(self.namespace, &self.name);
        Ok(())
    }
}

/// A class row whose `relname` the statement changed.
#[derive(Debug)]
struct ClassRenamed {
    namespace: NamespaceId,
    old_name: String,
    new_name: String,
}

impl ClassRenamed {
    fn emit(self, program: &mut ProgramBuilder) {
        emit_catalog_apply(program, self, vec![]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, _values: &[Value]) -> turso_core::Result<()> {
        if let Some(mut class) = classes.remove(self.namespace, &self.old_name) {
            class.relname.clone_from(&self.new_name);
            classes.add(class);
        }
        Ok(())
    }
}

/// A class row whose `relownedby` and `relownedbycolumn` the statement set
/// to `owner`, or to no column.
#[derive(Debug)]
struct OwnerColumnSet {
    namespace: NamespaceId,
    name: String,
    owner: Option<OwnerColumn>,
}

impl OwnerColumnSet {
    fn emit(self, program: &mut ProgramBuilder) {
        emit_catalog_apply(program, self, vec![]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, _values: &[Value]) -> turso_core::Result<()> {
        let table_oid = self
            .owner
            .as_ref()
            .and_then(|owner| classes.get_by_name(self.namespace, &owner.table))
            .map_or(0, |table| table.oid);
        if let Some(class) = classes.get_mut_by_name(self.namespace, &self.name) {
            class.relownedby = table_oid;
            class.relownedbycolumn = self.owner.as_ref().map(|owner| owner.column.clone());
        }
        Ok(())
    }
}

/// The class rows that column `old_name` of `table` owned, whose
/// `relownedbycolumn` the statement set to `new_name`.
#[derive(Debug)]
struct OwnerColumnRenamed {
    namespace: NamespaceId,
    table: String,
    old_name: String,
    new_name: String,
}

impl OwnerColumnRenamed {
    fn emit(self, program: &mut ProgramBuilder) {
        emit_catalog_apply(program, self, vec![]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, _values: &[Value]) -> turso_core::Result<()> {
        let Some(table_oid) = classes
            .get_by_name(self.namespace, &self.table)
            .map(|table| table.oid)
        else {
            return Ok(());
        };
        for class in classes.by_oid.values_mut() {
            if class.relownedby == table_oid && class.is_owned_by_column(&self.old_name) {
                class.relownedbycolumn = Some(self.new_name.clone());
            }
        }
        Ok(())
    }
}

/// A class row whose `relowner` and `relacl` the statement set.
#[derive(Debug)]
struct OwnerSet {
    namespace: NamespaceId,
    name: String,
}

impl OwnerSet {
    /// `owner_reg` holds the new `relowner` and `acl_reg` the new `relacl`.
    fn emit(self, program: &mut ProgramBuilder, owner_reg: usize, acl_reg: usize) {
        emit_catalog_apply(program, self, vec![owner_reg, acl_reg]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, values: &[Value]) -> turso_core::Result<()> {
        let [owner, acl] = values else {
            return Err(LimboError::InternalError(
                "an owner change carries the new owner and the list it leaves".to_string(),
            ));
        };
        if let Some(class) = classes.get_mut_by_name(self.namespace, &self.name) {
            class.relowner = RoleId::from_stored(read_back_integer(owner)?);
            class.relacl = read_back_acl(acl)?;
        }
        Ok(())
    }
}

/// A class row whose `relacl` the statement set.
#[derive(Debug)]
struct AclSet {
    namespace: NamespaceId,
    name: String,
}

impl AclSet {
    /// `acl_reg` holds the new `relacl`.
    fn emit(self, program: &mut ProgramBuilder, acl_reg: usize) {
        emit_catalog_apply(program, self, vec![acl_reg]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, values: &[Value]) -> turso_core::Result<()> {
        let [acl] = values else {
            return Err(LimboError::InternalError(
                "a grant carries the list it leaves".to_string(),
            ));
        };
        if let Some(class) = classes.get_mut_by_name(self.namespace, &self.name) {
            class.relacl = read_back_acl(acl)?;
        }
        Ok(())
    }
}

/// A class row whose `relrowsecurity` the statement set to `enabled`.
#[derive(Debug)]
struct RowSecuritySet {
    namespace: NamespaceId,
    name: String,
    enabled: bool,
}

impl RowSecuritySet {
    fn emit(self, program: &mut ProgramBuilder) {
        emit_catalog_apply(program, self, vec![]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, _values: &[Value]) -> turso_core::Result<()> {
        if let Some(class) = classes.get_mut_by_name(self.namespace, &self.name) {
            class.relrowsecurity = self.enabled;
        }
        Ok(())
    }
}

/// A class row whose `relpolicies` the statement set.
#[derive(Debug)]
struct PoliciesSet {
    namespace: NamespaceId,
    name: String,
}

impl PoliciesSet {
    /// `policies_reg` holds the new `relpolicies`.
    fn emit(self, program: &mut ProgramBuilder, policies_reg: usize) {
        emit_catalog_apply(program, self, vec![policies_reg]);
    }

    fn apply_to(&self, classes: &mut ClassCatalog, values: &[Value]) -> turso_core::Result<()> {
        let [relpolicies] = values else {
            return Err(LimboError::InternalError(
                "a policy change carries the policies it leaves".to_string(),
            ));
        };
        if let Some(class) = classes.get_mut_by_name(self.namespace, &self.name) {
            class.relpolicies = read_back_policies(relpolicies)?;
        }
        Ok(())
    }
}

macro_rules! apply_to_class_catalog {
    ($($change:ty),* $(,)?) => {
        $(
            impl CatalogApply for $change {
                fn apply(&self, schema: &mut Schema, values: &[Value]) -> turso_core::Result<()> {
                    change_schema_state(schema, |state| {
                        self.apply_to(Arc::make_mut(&mut state.classes), values)
                    })
                }
            }
        )*
    };
}

apply_to_class_catalog!(
    ClassCreated,
    ClassDropped,
    ClassRenamed,
    OwnerColumnSet,
    OwnerColumnRenamed,
    OwnerSet,
    AclSet,
    RowSecuritySet,
    PoliciesSet,
);

/// An integer the emitted instruction read back from a register.
fn read_back_integer(value: &Value) -> turso_core::Result<i64> {
    match value {
        Value::Numeric(Numeric::Integer(value)) => Ok(*value),
        other => Err(LimboError::InternalError(format!(
            "the relation change read back {other:?} where an integer was written"
        ))),
    }
}

/// An access control list, or NULL, that the emitted instruction read back
/// from a register.
fn read_back_acl(value: &Value) -> turso_core::Result<Option<Acl>> {
    match value {
        Value::Null => Ok(None),
        Value::Text(text) => Acl::parse(text.as_str()).map(Some).ok_or_else(|| {
            LimboError::InternalError(format!(
                "the relation change read back an invalid access control list {text:?}"
            ))
        }),
        other => Err(LimboError::InternalError(format!(
            "the relation change read back {other:?} where an access control list was written"
        ))),
    }
}

fn read_back_policies(value: &Value) -> turso_core::Result<TablePolicies> {
    match value {
        Value::Null => Ok(TablePolicies::default()),
        Value::Text(text) => policies::parse(text.as_str()).ok_or_else(|| {
            LimboError::InternalError(format!(
                "the relation change read back invalid policies {text:?}"
            ))
        }),
        other => Err(LimboError::InternalError(format!(
            "the relation change read back {other:?} where policies were written"
        ))),
    }
}

/// Puts `acl` in its stored form, or NULL for none, in a new register.
fn emit_acl(program: &mut ProgramBuilder, acl: Option<&Acl>) -> usize {
    match acl {
        Some(acl) => program.emit_string8_new_reg(acl.stored_text()),
        None => {
            let acl_reg = program.alloc_register();
            program.emit_insn(Insn::Null {
                dest: acl_reg,
                dest_end: None,
            });
            acl_reg
        }
    }
}

/// The registers that [`emit_find_class_row`] fills for the found row.
struct FoundClassRow {
    rowid_reg: usize,
    relkind_reg: usize,
    relowner_reg: usize,
    relownedby_reg: usize,
    relacl_reg: usize,
    relrowsecurity_reg: usize,
    relpolicies_reg: usize,
    relownedbycolumn_reg: usize,
}

/// Scans the classes table for the row of relation `name` of `namespace` and
/// runs `found` with the cursor on it. Emits nothing after the scan when the
/// row does not exist, so a relation without a row is tolerated.
fn emit_find_class_row(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    found: impl FnOnce(&mut ProgramBuilder, &FoundClassRow),
) {
    let name_reg = program.emit_string8_new_reg(name.to_string());
    let namespace_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        value: i64::from(namespace.as_u32()),
        dest: namespace_reg,
    });

    let loop_end = program.allocate_label();
    let done = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: cursors.table_cursor_id,
        pc_if_empty: loop_end,
    });
    let loop_start = program.allocate_label();
    program.preassign_label_to_next_insn(loop_start);

    let next = program.allocate_label();
    let column_reg = program.alloc_register();
    program.emit_column_or_rowid(cursors.table_cursor_id, 2, column_reg);
    program.emit_insn(Insn::Ne {
        lhs: column_reg,
        rhs: namespace_reg,
        target_pc: next,
        flags: CmpInsFlags::default().jump_if_null(),
        collation: CollationSeq::for_internal_values(),
    });
    program.emit_column_or_rowid(cursors.table_cursor_id, 1, column_reg);
    program.emit_insn(Insn::Ne {
        lhs: column_reg,
        rhs: name_reg,
        target_pc: next,
        flags: CmpInsFlags::default().jump_if_null(),
        collation: CollationSeq::NOCASE,
    });

    let row = FoundClassRow {
        rowid_reg: program.alloc_register(),
        relkind_reg: program.alloc_register(),
        relowner_reg: program.alloc_register(),
        relownedby_reg: program.alloc_register(),
        relacl_reg: program.alloc_register(),
        relrowsecurity_reg: program.alloc_register(),
        relpolicies_reg: program.alloc_register(),
        relownedbycolumn_reg: program.alloc_register(),
    };
    program.emit_insn(Insn::RowId {
        cursor_id: cursors.table_cursor_id,
        dest: row.rowid_reg,
    });
    program.emit_column_or_rowid(cursors.table_cursor_id, 3, row.relkind_reg);
    program.emit_column_or_rowid(cursors.table_cursor_id, 4, row.relowner_reg);
    program.emit_column_or_rowid(cursors.table_cursor_id, 5, row.relownedby_reg);
    program.emit_column_or_rowid(cursors.table_cursor_id, 6, row.relacl_reg);
    program.emit_column_or_rowid(cursors.table_cursor_id, 7, row.relrowsecurity_reg);
    program.emit_column_or_rowid(cursors.table_cursor_id, 8, row.relpolicies_reg);
    program.emit_column_or_rowid(cursors.table_cursor_id, 9, row.relownedbycolumn_reg);
    found(program, &row);
    program.emit_insn(Insn::Goto { target_pc: done });

    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::Next {
        cursor_id: cursors.table_cursor_id,
        pc_if_next: loop_start,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(loop_end);
    program.preassign_label_to_next_insn(done);
}

/// What a column of a rewritten class record is built from.
enum ClassRecordColumn {
    Literal(String),
    LiteralInt(i64),
    Null,
    Register(usize),
}

/// Builds a class record `(NULL, relname, relnamespace, relkind, relowner,
/// relownedby, relacl, relrowsecurity, relpolicies, relownedbycolumn)` and
/// returns its register.
#[allow(clippy::too_many_arguments)]
fn emit_class_record(
    program: &mut ProgramBuilder,
    relname: ClassRecordColumn,
    relnamespace: ClassRecordColumn,
    relkind: ClassRecordColumn,
    relowner: ClassRecordColumn,
    relownedby: ClassRecordColumn,
    relacl: ClassRecordColumn,
    relrowsecurity: ClassRecordColumn,
    relpolicies: ClassRecordColumn,
    relownedbycolumn: ClassRecordColumn,
) -> usize {
    let first_column_reg = program.alloc_registers(CLASS_COLUMN_COUNT);
    program.emit_insn(Insn::Null {
        dest: first_column_reg,
        dest_end: None,
    });
    for (offset, column) in [
        relname,
        relnamespace,
        relkind,
        relowner,
        relownedby,
        relacl,
        relrowsecurity,
        relpolicies,
        relownedbycolumn,
    ]
    .into_iter()
    .enumerate()
    {
        let dest = first_column_reg + 1 + offset;
        match column {
            ClassRecordColumn::Literal(value) => {
                program.emit_insn(Insn::String8 { value, dest });
            }
            ClassRecordColumn::LiteralInt(value) => {
                program.emit_insn(Insn::Integer { value, dest });
            }
            ClassRecordColumn::Null => {
                program.emit_insn(Insn::Null {
                    dest,
                    dest_end: None,
                });
            }
            ClassRecordColumn::Register(src_reg) => {
                program.emit_insn(Insn::Copy {
                    src_reg,
                    dst_reg: dest,
                    extra_amount: 0,
                });
            }
        }
    }
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(first_column_reg),
        count: to_u32(CLASS_COLUMN_COUNT),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    record_reg
}

/// Replaces the class row at `rowid_reg` with `record_reg`. The cursor is on
/// the row. The delete first ends the old version under MVCC, where an
/// update is a delete and an insert.
fn emit_class_row_overwrite(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    rowid_reg: usize,
    record_reg: usize,
) {
    if program.is_mvcc_enabled() {
        program.emit_insn(Insn::Delete {
            cursor_id: cursors.table_cursor_id,
            table_name: CLASSES_TABLE_NAME.to_string(),
            is_part_of_update: true,
        });
    }
    program.emit_insn(Insn::Insert {
        cursor: cursors.table_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new()
            .skip_last_rowid()
            .skip_all_change_counts()
            .require_seek(),
        table_name: CLASSES_TABLE_NAME.to_string(),
    });
}

/// Adds `(namespace, name, rowid)` to the name index, failing if another
/// relation of the namespace has the name. The schema has already rejected a
/// name it knows, so this only fails if the stored table has a relation the
/// schema does not.
fn emit_class_name_index_insert(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    rowid_reg: usize,
) {
    let key_reg = program.alloc_registers(3);
    program.emit_insn(Insn::Integer {
        value: i64::from(namespace.as_u32()),
        dest: key_reg,
    });
    program.emit_insn(Insn::String8 {
        value: name.to_string(),
        dest: key_reg + 1,
    });
    program.emit_insn(Insn::Copy {
        src_reg: rowid_reg,
        dst_reg: key_reg + 2,
        extra_amount: 0,
    });
    emit_unique_index_insert(
        program,
        cursors.index_cursor_id,
        &cursors.index_name,
        key_reg,
        3,
        format!("relation \"{name}\" already has a class row"),
    );
}

/// Removes `(namespace, name, rowid)` from the name index.
fn emit_class_name_index_delete(
    program: &mut ProgramBuilder,
    cursors: &ClassTableCursors,
    namespace: NamespaceId,
    name: &str,
    rowid_reg: usize,
) {
    let key_reg = program.alloc_registers(3);
    program.emit_insn(Insn::Integer {
        value: i64::from(namespace.as_u32()),
        dest: key_reg,
    });
    program.emit_insn(Insn::String8 {
        value: name.to_string(),
        dest: key_reg + 1,
    });
    program.emit_insn(Insn::Copy {
        src_reg: rowid_reg,
        dst_reg: key_reg + 2,
        extra_amount: 0,
    });
    program.emit_insn(Insn::IdxDelete {
        start_reg: key_reg,
        num_regs: 3,
        cursor_id: cursors.index_cursor_id,
        raise_error_if_no_matching_entry: true,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE_OID: i64 = FIRST_NORMAL_OBJECT_ID;
    const SEQUENCE_OID: i64 = FIRST_NORMAL_OBJECT_ID + 1;
    const ALICE: i64 = 7;
    const BOB: i64 = 8;

    #[test]
    fn class_created_adds_a_row_with_the_oid_and_owner_it_reads_back() {
        let classes = table_and_sequence();

        let table = classes.get_by_name(NamespaceId::ROOT, "t").unwrap();
        assert_eq!(table.oid, TABLE_OID);
        assert_eq!(table.relkind, RelationKind::Table);
        assert_eq!(table.relowner, RoleId::from_stored(ALICE));
        assert_eq!(table.relownedby, 0);
        assert_eq!(table.relacl, None);
        assert!(!table.relrowsecurity);
    }

    #[test]
    fn class_created_refuses_values_that_are_not_an_oid_and_an_owner() {
        let created = ClassCreated {
            namespace: NamespaceId::ROOT,
            name: "t".to_string(),
            kind: RelationKind::Table,
        };
        let mut classes = ClassCatalog::default();

        let missing_owner = created.apply_to(&mut classes, &[Value::from_i64(TABLE_OID)]);
        let text_oid = created.apply_to(
            &mut classes,
            &[Value::build_text("t"), Value::from_i64(ALICE)],
        );

        assert!(matches!(missing_owner, Err(LimboError::InternalError(_))));
        assert!(matches!(text_oid, Err(LimboError::InternalError(_))));
        assert_eq!(classes.classes().count(), 0);
    }

    #[test]
    fn class_renamed_keeps_the_oid_under_the_new_name() {
        let mut classes = table_and_sequence();

        ClassRenamed {
            namespace: NamespaceId::ROOT,
            old_name: "t".to_string(),
            new_name: "u".to_string(),
        }
        .apply_to(&mut classes, &[])
        .unwrap();

        assert!(classes.get_by_name(NamespaceId::ROOT, "t").is_none());
        let renamed = classes.get_by_name(NamespaceId::ROOT, "u").unwrap();
        assert_eq!(renamed.oid, TABLE_OID);
        assert_eq!(renamed.relname, "u");
    }

    #[test]
    fn class_dropped_removes_the_row() {
        let mut classes = table_and_sequence();

        ClassDropped {
            namespace: NamespaceId::ROOT,
            name: "t".to_string(),
        }
        .apply_to(&mut classes, &[])
        .unwrap();

        assert!(classes.get_by_name(NamespaceId::ROOT, "t").is_none());
        assert!(classes.get_by_name(NamespaceId::ROOT, "s").is_some());
    }

    #[test]
    fn owner_column_changes_follow_the_table_and_the_column_name() {
        let mut classes = table_and_sequence();
        let owned_by = |owner| OwnerColumnSet {
            namespace: NamespaceId::ROOT,
            name: "s".to_string(),
            owner,
        };

        owned_by(Some(OwnerColumn {
            table: "t".to_string(),
            column: "a".to_string(),
        }))
        .apply_to(&mut classes, &[])
        .unwrap();
        let sequence = classes.get_by_name(NamespaceId::ROOT, "s").unwrap();
        assert_eq!(sequence.relownedby, TABLE_OID);
        assert_eq!(sequence.relownedbycolumn.as_deref(), Some("a"));

        OwnerColumnRenamed {
            namespace: NamespaceId::ROOT,
            table: "t".to_string(),
            old_name: "a".to_string(),
            new_name: "b".to_string(),
        }
        .apply_to(&mut classes, &[])
        .unwrap();
        let sequence = classes.get_by_name(NamespaceId::ROOT, "s").unwrap();
        assert_eq!(sequence.relownedby, TABLE_OID);
        assert_eq!(sequence.relownedbycolumn.as_deref(), Some("b"));

        owned_by(None).apply_to(&mut classes, &[]).unwrap();
        let sequence = classes.get_by_name(NamespaceId::ROOT, "s").unwrap();
        assert_eq!(sequence.relownedby, 0);
        assert_eq!(sequence.relownedbycolumn, None);
    }

    #[test]
    fn owner_set_reads_back_the_owner_and_the_access_control_list() {
        let mut classes = table_and_sequence();
        let acl = Acl::default_for(RelationKind::Table, role_oid(RoleId::from_stored(BOB)));
        let owner_set = OwnerSet {
            namespace: NamespaceId::ROOT,
            name: "t".to_string(),
        };

        owner_set
            .apply_to(
                &mut classes,
                &[Value::from_i64(BOB), Value::build_text(acl.stored_text())],
            )
            .unwrap();
        let table = classes.get_by_name(NamespaceId::ROOT, "t").unwrap();
        assert_eq!(table.relowner, RoleId::from_stored(BOB));
        assert_eq!(table.relacl.as_ref(), Some(&acl));

        owner_set
            .apply_to(&mut classes, &[Value::from_i64(ALICE), Value::Null])
            .unwrap();
        let table = classes.get_by_name(NamespaceId::ROOT, "t").unwrap();
        assert_eq!(table.relowner, RoleId::from_stored(ALICE));
        assert_eq!(table.relacl, None);

        let only_owner = owner_set.apply_to(&mut classes, &[Value::from_i64(BOB)]);
        assert!(matches!(only_owner, Err(LimboError::InternalError(_))));
    }

    #[test]
    fn acl_set_reads_back_the_access_control_list() {
        let mut classes = table_and_sequence();
        let acl = Acl::default_for(RelationKind::Table, role_oid(RoleId::from_stored(ALICE)));
        let acl_set = AclSet {
            namespace: NamespaceId::ROOT,
            name: "t".to_string(),
        };

        acl_set
            .apply_to(&mut classes, &[Value::build_text(acl.stored_text())])
            .unwrap();
        let table = classes.get_by_name(NamespaceId::ROOT, "t").unwrap();
        assert_eq!(table.relacl.as_ref(), Some(&acl));

        let not_a_list = acl_set.apply_to(&mut classes, &[Value::from_i64(1)]);
        assert!(matches!(not_a_list, Err(LimboError::InternalError(_))));
    }

    #[test]
    fn row_security_set_changes_only_the_named_table() {
        let mut classes = table_and_sequence();

        RowSecuritySet {
            namespace: NamespaceId::ROOT,
            name: "t".to_string(),
            enabled: true,
        }
        .apply_to(&mut classes, &[])
        .unwrap();

        assert!(
            classes
                .get_by_name(NamespaceId::ROOT, "t")
                .unwrap()
                .relrowsecurity
        );
        assert!(
            !classes
                .get_by_name(NamespaceId::ROOT, "s")
                .unwrap()
                .relrowsecurity
        );
    }

    #[test]
    fn policies_set_reads_back_the_policies() {
        let mut classes = table_and_sequence();
        let policies_set = PoliciesSet {
            namespace: NamespaceId::ROOT,
            name: "t".to_string(),
        };

        policies_set.apply_to(&mut classes, &[Value::Null]).unwrap();
        let table = classes.get_by_name(NamespaceId::ROOT, "t").unwrap();
        assert!(table.relpolicies.is_empty());

        let not_policies = policies_set.apply_to(&mut classes, &[Value::build_text("?")]);
        let no_value = policies_set.apply_to(&mut classes, &[]);
        assert!(matches!(not_policies, Err(LimboError::InternalError(_))));
        assert!(matches!(no_value, Err(LimboError::InternalError(_))));
    }

    #[test]
    fn a_change_to_a_relation_without_a_row_does_nothing() {
        let mut classes = table_and_sequence();

        RowSecuritySet {
            namespace: NamespaceId::ROOT,
            name: "missing".to_string(),
            enabled: true,
        }
        .apply_to(&mut classes, &[])
        .unwrap();
        AclSet {
            namespace: NamespaceId::ROOT,
            name: "missing".to_string(),
        }
        .apply_to(&mut classes, &[Value::Null])
        .unwrap();

        assert_eq!(classes.classes().count(), 2);
    }

    /// Table `t` and sequence `s`, both owned by `ALICE`.
    fn table_and_sequence() -> ClassCatalog {
        let mut classes = ClassCatalog::default();
        for (name, kind, oid) in [
            ("t", RelationKind::Table, TABLE_OID),
            ("s", RelationKind::Sequence, SEQUENCE_OID),
        ] {
            ClassCreated {
                namespace: NamespaceId::ROOT,
                name: name.to_string(),
                kind,
            }
            .apply_to(
                &mut classes,
                &[Value::from_i64(oid), Value::from_i64(ALICE)],
            )
            .unwrap();
        }
        classes
    }

    /// A table that a user can name in an attached database is in no
    /// schema, as in a file that the SQLite frontend made.
    #[test]
    fn alter_table_in_an_attached_database_refuses_renames_only() {
        use turso_core::{ConnectOpts, SessionRole};

        let dir = tempfile::tempdir().unwrap();
        let attached_path = dir.path().join("attached.db");
        let io = Arc::new(turso_core::PlatformIO::new().unwrap());
        let opts = turso_core::DatabaseOpts::new().with_attach(true);
        let attached = turso_core::Database::open_file_with_flags(
            io.clone(),
            attached_path.to_str().unwrap(),
            turso_core::OpenFlags::default(),
            opts.with_mvcc(true),
            None,
            Arc::new(turso_sqlite::SqliteDialect),
        )
        .unwrap()
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
        attached
            .execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b INTEGER, c INTEGER)")
            .unwrap();
        attached.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        attached.close().unwrap();
        drop(attached);

        let conn = crate::session::open_database_with_io(
            io,
            dir.path().join("main.db").to_str().unwrap(),
            turso_core::OpenFlags::default(),
            opts,
        )
        .unwrap()
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
        conn.execute(format!("ATTACH '{}' AS aux", attached_path.display()))
            .unwrap();

        for rename in [
            "ALTER TABLE aux.t RENAME TO t2",
            "ALTER TABLE aux.t RENAME COLUMN b TO z",
        ] {
            let err = conn.execute(rename).unwrap_err();
            assert_eq!(
                err.to_string(),
                "creating, renaming or dropping relations in an attached database is not supported",
                "{rename}"
            );
        }
        conn.execute("ALTER TABLE aux.t RENAME COLUMN b TO b")
            .unwrap();
        conn.execute("ALTER TABLE aux.t ADD COLUMN d INT").unwrap();
        conn.execute("ALTER TABLE aux.t DROP COLUMN c").unwrap();
        conn.execute("INSERT INTO aux.t (a, b, d) VALUES (1, 2, 3)")
            .unwrap();
    }
}
