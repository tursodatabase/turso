//! How a dialect stores the objects of a database: the [`Catalog`] that
//! DDL translation writes through, what translation tells the dialect about
//! a statement that changes the dialect's catalog of the main database, and
//! what the dialect's bytecode tells the in-memory schema once the change
//! is written.

use crate::{
    namespace::NamespaceId,
    schema::{load::LoadStep, RelationKind, Schema},
    translate::{
        emitter::Resolver,
        schema::{
            alloc_sqlite_schema_cursor, emit_open_sqlite_schema_cursor,
            open_sqlite_schema_for_write,
        },
    },
    vdbe::{builder::ProgramBuilder, insn::Insn},
    Connection, LimboError, Result, Value,
};
use std::{borrow::Cow, cell::Cell, sync::Arc};
use turso_parser::ast::{self, Grantee, Privilege, RoleSpec};

/// Where a dialect stores the objects of a database. DDL translation
/// creates and fills the B-trees; the catalog stores what describes them.
pub trait Catalog: Send + Sync {
    /// Emits the bytecode that stores `change`, in the transaction of the
    /// statement being compiled. Returns what the statement emits after its
    /// other schema writes, which puts the change into the schema in memory.
    fn write(
        &self,
        program: &mut ProgramBuilder,
        ctx: &WriteContext<'_>,
        change: &CatalogWrite<'_>,
    ) -> Result<AfterWrites>;

    /// The steps that read what this catalog stores besides the engine's own
    /// tables. They run after the engine's steps, when a database is opened
    /// and when a connection reads its schema again.
    fn load_steps(&self) -> Vec<Box<dyn LoadStep>> {
        Vec::new()
    }
}

/// The statement that a [`Catalog`] writes for.
pub struct WriteContext<'a> {
    pub connection: &'a Arc<Connection>,
    /// The schema of the main database that the statement is compiled
    /// against.
    pub schema: &'a Schema,
    /// The database the statement changes.
    pub database_id: usize,
    pub origin: WriteOrigin,
    pub(crate) resolver: &'a Resolver<'a>,
    sqlite_schema_cursor: Cell<Option<usize>>,
    /// A cursor for `sqlite_schema` that has a number and is not open yet.
    unopened_sqlite_schema_cursor: Cell<Option<usize>>,
    sqlite_schema_rowid_reg: Cell<Option<usize>>,
}

impl<'a> WriteContext<'a> {
    pub(crate) fn new(
        connection: &'a Arc<Connection>,
        resolver: &'a Resolver<'a>,
        database_id: usize,
    ) -> Self {
        let origin = if connection.is_nested_stmt() || connection.is_mvcc_bootstrap_connection() {
            WriteOrigin::Engine
        } else {
            WriteOrigin::User
        };
        Self {
            connection,
            schema: resolver.schema(),
            database_id,
            origin,
            resolver,
            sqlite_schema_cursor: Cell::new(None),
            unopened_sqlite_schema_cursor: Cell::new(None),
            sqlite_schema_rowid_reg: Cell::new(None),
        }
    }

    /// The statement's cursor for writing `sqlite_schema` of the database
    /// it changes. The first call opens it.
    pub(crate) fn sqlite_schema_cursor(&self, program: &mut ProgramBuilder) -> usize {
        if let Some(cursor_id) = self.sqlite_schema_cursor.get() {
            return cursor_id;
        }
        let cursor_id = match self.unopened_sqlite_schema_cursor.take() {
            Some(cursor_id) => {
                emit_open_sqlite_schema_cursor(program, cursor_id, self.database_id);
                cursor_id
            }
            None => open_sqlite_schema_for_write(program, self.resolver, self.database_id),
        };
        self.sqlite_schema_cursor.set(Some(cursor_id));
        cursor_id
    }

    /// Gives the statement's `sqlite_schema` cursor its number now, for a
    /// statement whose cursor numbers depend on it, without opening it.
    /// [`Self::sqlite_schema_cursor`] opens it on its first call.
    pub(crate) fn alloc_sqlite_schema_cursor(&self, program: &mut ProgramBuilder) {
        assert!(
            self.sqlite_schema_cursor.get().is_none()
                && self.unopened_sqlite_schema_cursor.get().is_none(),
            "the statement already has a sqlite_schema cursor"
        );
        let cursor_id = alloc_sqlite_schema_cursor(program, self.resolver, self.database_id);
        self.unopened_sqlite_schema_cursor.set(Some(cursor_id));
    }

    /// The statement's `sqlite_schema` cursor, if a write opened it.
    pub(crate) fn open_sqlite_schema_cursor(&self) -> Option<usize> {
        self.sqlite_schema_cursor.get()
    }

    /// The statement's register for the rowid of a `sqlite_schema` row it
    /// deletes. The first call allocates it.
    pub(crate) fn sqlite_schema_rowid_reg(&self, program: &mut ProgramBuilder) -> usize {
        if let Some(reg) = self.sqlite_schema_rowid_reg.get() {
            return reg;
        }
        let reg = program.alloc_register();
        self.sqlite_schema_rowid_reg.set(Some(reg));
        reg
    }
}

/// Who runs a DDL statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOrigin {
    /// A user of the connection.
    User,
    /// The engine, for itself: a frontend creating its own tables when it
    /// opens a database, VACUUM copying a database from its stored DDL, or
    /// the MVCC bootstrap connection replaying its log.
    Engine,
}

/// A change to the objects of a database.
#[derive(Debug, Clone, Copy)]
pub enum CatalogWrite<'a> {
    /// `CREATE TABLE`, or `CREATE TABLE ... AS SELECT` before its rows are
    /// inserted.
    CreateTable(CreateTable<'a>),
    /// `CREATE INDEX`, before the index is filled.
    CreateIndex(CreateIndex<'a>),
    /// `DROP INDEX`, before the index's B-tree is destroyed.
    DropIndex(DropIndex<'a>),
    /// `CREATE SEQUENCE`.
    CreateSequence(CreateSequence<'a>),
    /// A sequence that goes away: `DROP SEQUENCE`, or `ALTER TABLE ... DROP
    /// COLUMN` of the column that owns the sequence.
    DropSequence(DropSequence<'a>),
    /// `DROP TABLE`, of a virtual table too.
    DropTable(DropTable<'a>),
    /// `CREATE VIEW`.
    CreateView(CreateView<'a>),
    /// `CREATE MATERIALIZED VIEW`, before its rows are computed.
    CreateMaterializedView(CreateMaterializedView<'a>),
    /// `DROP VIEW` of a view or a materialized view.
    DropView(DropView<'a>),
}

/// A new table whose B-tree, and the B-trees of its automatic indexes, the
/// statement has already emitted the creation of.
#[derive(Debug, Clone, Copy)]
pub struct CreateTable<'a> {
    /// The namespace of the table in the database the statement changes.
    pub namespace: NamespaceId,
    /// The normalized name of the table in its namespace.
    pub name: &'a str,
    /// The name as the statement spells it.
    pub written_name: &'a ast::Name,
    /// Whether the statement says `TEMP`.
    pub temporary: bool,
    /// Whether the statement says `IF NOT EXISTS`.
    pub if_not_exists: bool,
    /// The columns, constraints and options of the table. For `CREATE TABLE
    /// ... AS SELECT`, the columns that the `SELECT` gives.
    pub body: &'a ast::CreateTableBody,
    pub text: TableText<'a>,
    /// Register that holds the root page of the table's B-tree.
    pub root_reg: usize,
    /// The indexes of the table's PRIMARY KEY and UNIQUE constraints.
    pub automatic_indexes: &'a [AutomaticIndex],
}

/// The SQL text that describes a new table.
#[derive(Debug, Clone, Copy)]
pub enum TableText<'a> {
    /// The statement, as the user wrote it in the dialect's SQL.
    Statement(&'a str),
    /// No text: translation changed the table after the statement was
    /// parsed, so only [`CreateTable::body`] describes it.
    Rewritten,
    /// The `CREATE TABLE` text that the engine wrote for the columns of a
    /// `CREATE TABLE ... AS SELECT`.
    FromSelect(&'a str),
}

/// An index that the engine creates for a PRIMARY KEY or UNIQUE constraint.
#[derive(Debug, Clone)]
pub struct AutomaticIndex {
    /// The name the engine gave the index, unique in the database.
    pub name: String,
    /// Register that holds the root page of the index's B-tree.
    pub root_reg: usize,
}

/// A new index whose B-tree, or whose index method's storage, the statement
/// has already emitted the creation of.
#[derive(Debug, Clone, Copy)]
pub struct CreateIndex<'a> {
    /// The namespace of the index and of its table in the database the
    /// statement changes.
    pub namespace: NamespaceId,
    /// The normalized name of the index in its namespace.
    pub name: &'a str,
    /// The index name as the statement spells it.
    pub written_name: &'a ast::Name,
    /// The normalized name of the table the index is on.
    pub table: &'a str,
    /// The table name as the statement spells it.
    pub written_table: &'a ast::Name,
    /// The `CREATE INDEX` statement.
    pub statement: &'a ast::Stmt,
    /// Register that holds the root page of the index's B-tree, or 0 for an
    /// index whose index method stores it elsewhere.
    pub root_reg: usize,
}

/// An index that the statement removes from the schema in memory itself.
#[derive(Debug, Clone, Copy)]
pub struct DropIndex<'a> {
    /// The namespace of the index in the database the statement changes.
    pub namespace: NamespaceId,
    /// The normalized name of the index in its namespace.
    pub name: &'a str,
}

/// A new sequence. Its values are in a backing table, which is an engine
/// table that the statement creates itself.
#[derive(Debug, Clone, Copy)]
pub struct CreateSequence<'a> {
    /// The namespace of the sequence in the database the statement changes.
    pub namespace: NamespaceId,
    /// The normalized name of the sequence in its namespace.
    pub name: &'a str,
}

/// A sequence whose backing table the statement has already emitted the
/// removal of.
#[derive(Debug, Clone, Copy)]
pub struct DropSequence<'a> {
    /// The namespace of the sequence in the database the statement changes.
    pub namespace: NamespaceId,
    /// The normalized name of the sequence in its namespace.
    pub name: &'a str,
}

/// A table that the statement drops, before the statement destroys the
/// B-trees of the table and of its indexes.
#[derive(Debug, Clone, Copy)]
pub struct DropTable<'a> {
    /// The namespace of the table in the database the statement changes.
    pub namespace: NamespaceId,
    /// The normalized name of the table in its namespace.
    pub name: &'a str,
    /// Whether the table is a virtual table, which has no B-tree.
    pub is_virtual: bool,
    /// The names of the table's indexes, the automatic ones among them.
    /// They are dropped with the table.
    pub indexes: &'a [&'a str],
    /// The names of the sequences in the table's namespace that the
    /// table's columns own. They are dropped with the table.
    pub owned_sequences: &'a [String],
    /// Two registers that the catalog may overwrite while it deletes its
    /// rows. The statement reads neither before it writes it again.
    pub scratch_regs: [usize; 2],
}

/// A new view. It has no B-tree.
#[derive(Debug, Clone, Copy)]
pub struct CreateView<'a> {
    /// The namespace of the view in the database the statement changes.
    pub namespace: NamespaceId,
    /// The normalized name of the view in its namespace.
    pub name: &'a str,
    /// The name as the statement spells it.
    pub written_name: &'a ast::Name,
    /// The names the statement gives the view's columns. Empty when it
    /// gives none.
    pub columns: &'a [ast::IndexedColumn],
    pub select: &'a ast::Select,
}

/// A new materialized view whose B-tree the statement has already emitted
/// the creation of. The tables that hold the state of its maintenance are
/// the engine's, and the statement writes their rows itself.
#[derive(Debug, Clone, Copy)]
pub struct CreateMaterializedView<'a> {
    /// The namespace of the view in the database the statement changes.
    pub namespace: NamespaceId,
    /// The normalized name of the view in its namespace.
    pub name: &'a str,
    /// The name as the statement spells it.
    pub written_name: &'a ast::Name,
    /// The names the statement gives the view's columns. Empty when it
    /// gives none.
    pub columns: &'a [ast::IndexedColumn],
    pub select: &'a ast::Select,
    /// Register that holds the root page of the B-tree with the view's
    /// rows.
    pub root_reg: usize,
}

/// A view or materialized view that the statement drops. The statement
/// destroys the B-trees of a materialized view and removes the engine
/// tables with the state of its maintenance itself.
#[derive(Debug, Clone, Copy)]
pub struct DropView<'a> {
    /// The namespace of the view in the database the statement changes.
    pub namespace: NamespaceId,
    /// The normalized name of the view in its namespace.
    pub name: &'a str,
}

/// What a statement emits after all of its schema writes, so that the
/// schema in memory has what a [`Catalog`] stored.
#[must_use]
#[derive(Debug, Default)]
pub struct AfterWrites {
    /// The `WHERE` clause that selects the stored `sqlite_schema` rows to
    /// parse into the schema.
    sqlite_schema_rows: Option<String>,
}

impl AfterWrites {
    /// Nothing to emit.
    pub fn none() -> Self {
        Self::default()
    }

    pub(crate) fn parse_sqlite_schema_rows(where_clause: String) -> Self {
        Self {
            sqlite_schema_rows: Some(where_clause),
        }
    }

    /// Emits the instructions. `engine_rows` selects the `sqlite_schema`
    /// rows of the engine tables that the statement created itself.
    pub(crate) fn emit(
        self,
        program: &mut ProgramBuilder,
        database_id: usize,
        engine_rows: Option<&str>,
    ) {
        let where_clause = match (self.sqlite_schema_rows, engine_rows) {
            (Some(rows), Some(engine_rows)) => format!("{rows} OR {engine_rows}"),
            (Some(rows), None) => rows,
            (None, Some(engine_rows)) => engine_rows.to_string(),
            (None, None) => return,
        };
        program.emit_insn(Insn::ParseSchema {
            db: database_id,
            where_clause: Some(where_clause),
            trigger_target_database_id: None,
        });
    }
}

/// A relation that a statement created, dropped or renamed. The engine makes
/// the change itself; the dialect records it in its own catalog through
/// [`Dialect::translate_relation_changes`](super::Dialect::translate_relation_changes).
#[derive(Debug, Clone)]
pub enum RelationChange<'a> {
    Created {
        namespace: NamespaceId,
        name: &'a str,
        kind: RelationKind,
        /// For an index, the table it is on, in the same namespace. Its
        /// owner owns the index.
        table: Option<&'a str>,
    },
    Dropped {
        namespace: NamespaceId,
        name: &'a str,
    },
    Renamed {
        namespace: NamespaceId,
        old_name: &'a str,
        /// Owned when the statement had to build the name, as for an
        /// automatic index that is renamed with its table.
        new_name: Cow<'a, str>,
    },
    /// `ALTER TABLE table RENAME COLUMN old_name TO new_name`.
    ColumnRenamed {
        namespace: NamespaceId,
        table: &'a str,
        old_name: &'a str,
        new_name: &'a str,
    },
}

/// A statement about roles, ownership, privileges or row-level security.
/// The engine stores none of these; the dialect does all of the work in
/// [`Dialect::translate_access_statement`](super::Dialect::translate_access_statement).
#[derive(Debug, Clone, Copy)]
pub enum AccessStatement<'a> {
    /// Whom the relation belongs to from now on.
    ChangeOwner {
        namespace: NamespaceId,
        name: &'a str,
        owner: &'a NewOwner,
    },
    /// `GRANT privileges ON relations TO grantees`, or `REVOKE privileges ON
    /// relations FROM grantees` when `revoke` is set.
    Grant {
        revoke: bool,
        relations: &'a [RelationName],
        privileges: &'a [Privilege],
        grantees: &'a [Grantee],
    },
    /// `ALTER TABLE ... ENABLE ROW LEVEL SECURITY`, or `DISABLE` when
    /// `enabled` is false.
    SetRowSecurity {
        namespace: NamespaceId,
        name: &'a str,
        enabled: bool,
    },
    /// `CREATE POLICY` on table `name`. Its expressions refer to the
    /// table's columns by their unqualified names.
    CreatePolicy {
        namespace: NamespaceId,
        name: &'a str,
        policy: &'a ast::CreatePolicy,
    },
    /// `DROP POLICY policy_name ON name`.
    DropPolicy {
        namespace: NamespaceId,
        name: &'a str,
        policy_name: &'a str,
        if_exists: bool,
    },
    /// `CREATE ROLE name`: a role that is not a superuser and cannot log in.
    CreateRole {
        name: &'a str,
        options: &'a ast::RoleOptions,
    },
    /// `GRANT roles TO grantees`, which makes each grantee a member of each
    /// role, or `REVOKE roles FROM grantees` when `revoke` is set.
    GrantRole {
        revoke: bool,
        roles: &'a [String],
        grantees: &'a [Grantee],
    },
}

impl AccessStatement<'_> {
    /// The error of a dialect that stores no roles, owners, privileges or
    /// policies.
    pub fn not_supported(&self) -> LimboError {
        let message = match self {
            Self::ChangeOwner {
                owner: NewOwner::Role(_),
                ..
            } => "ALTER ... OWNER TO is not supported".to_string(),
            Self::ChangeOwner {
                owner: NewOwner::Column(_),
                ..
            } => "ALTER SEQUENCE ... OWNED BY is not supported".to_string(),
            Self::Grant { revoke, .. } => {
                format!("{} is not supported", grant_or_revoke(*revoke))
            }
            Self::SetRowSecurity { .. } | Self::CreatePolicy { .. } | Self::DropPolicy { .. } => {
                "row-level security is not supported".to_string()
            }
            Self::CreateRole { .. } => "CREATE ROLE is not supported".to_string(),
            Self::GrantRole { revoke, .. } => format!(
                "{} of role membership is not supported",
                grant_or_revoke(*revoke)
            ),
        };
        LimboError::ParseError(message)
    }
}

fn grant_or_revoke(revoke: bool) -> &'static str {
    if revoke {
        "REVOKE"
    } else {
        "GRANT"
    }
}

/// A relation of the main database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationName {
    pub namespace: NamespaceId,
    pub name: String,
}

/// What a statement's bytecode did to the dialect's tables. The dialect
/// puts one in [`Insn::CatalogApply`] after the
/// instructions that write the rows.
pub trait CatalogApply: std::fmt::Debug + Send + Sync {
    /// Makes the same change to the dialect's state in `schema`. `values`
    /// are the values of the registers the instruction names, in the same
    /// order.
    fn apply(&self, schema: &mut Schema, values: &[Value]) -> Result<()>;
}

/// Whom a relation belongs to after an [`AccessStatement::ChangeOwner`].
#[derive(Debug, Clone)]
pub enum NewOwner {
    /// A column of a table of the same namespace, as `ALTER SEQUENCE ...
    /// OWNED BY table.column` names one for a sequence, or no column. What a
    /// table owns changes owner with it, and not on its own.
    Column(Option<OwnerColumn>),
    /// A role, as `ALTER ... OWNER TO` names one.
    Role(RoleSpec),
}

/// The column that owns a sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerColumn {
    pub table: String,
    pub column: String,
}
