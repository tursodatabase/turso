//! How a dialect stores the objects of a database: the [`Catalog`] that DDL
//! translation writes through, and what the dialect's bytecode tells the
//! in-memory schema once the command is stored.

use crate::{
    namespace::NamespaceId,
    schema::{Index, Schema},
    translate::{
        emitter::Resolver,
        schema::{
            alloc_sqlite_schema_cursor, emit_open_sqlite_schema_cursor,
            open_sqlite_schema_for_write,
        },
    },
    util::renamed_automatic_index_name,
    vdbe::{builder::ProgramBuilder, insn::Insn},
    Connection, LimboError, Result, Value,
};
use std::{cell::Cell, sync::Arc};
use turso_parser::ast::{self, Grantee, Privilege, RoleSpec};

/// Where a dialect stores the objects of a database. DDL translation
/// creates and fills the B-trees; the catalog stores what describes them.
pub trait Catalog: Send + Sync {
    /// Emits the bytecode that stores `cmd`, in the transaction of the
    /// statement being compiled.
    fn emit(
        &self,
        program: &mut ProgramBuilder,
        ctx: &CmdContext<'_>,
        cmd: &CatalogCmd<'_>,
    ) -> Result<()>;

    /// Emits the bytecode that stores what `statement` changes: roles,
    /// owners, privileges or policies. A catalog that stores none refuses
    /// `CREATE ROLE`, `DROP ROLE`, `GRANT role TO role`, `ALTER ... OWNER TO`,
    /// `ALTER SEQUENCE ... OWNED BY`, `GRANT`, `REVOKE`,
    /// `ALTER TABLE ... ENABLE ROW LEVEL SECURITY`, `CREATE POLICY` and
    /// `DROP POLICY`.
    fn emit_access(
        &self,
        program: &mut ProgramBuilder,
        ctx: &CmdContext<'_>,
        statement: &AccessStatement<'_>,
    ) -> Result<()> {
        let _ = (program, ctx);
        Err(statement.not_supported())
    }
}

/// The statement that a [`Catalog`] emits bytecode for. `schema` is the schema of
/// the main database, and `database_id` the database the statement changes.
pub struct CmdContext<'a> {
    pub connection: &'a Arc<Connection>,
    pub schema: &'a Schema,
    pub database_id: usize,
    pub origin: CmdOrigin,
    pub(crate) resolver: &'a Resolver<'a>,
    sqlite_schema_cursor: Cell<SqliteSchemaCursor>,
}

impl<'a> CmdContext<'a> {
    pub(crate) fn new(
        connection: &'a Arc<Connection>,
        resolver: &'a Resolver<'a>,
        database_id: usize,
    ) -> Self {
        let origin = if connection.is_nested_stmt() || connection.is_mvcc_bootstrap_connection() {
            CmdOrigin::Engine
        } else {
            CmdOrigin::User
        };
        Self {
            connection,
            schema: resolver.schema(),
            database_id,
            origin,
            resolver,
            sqlite_schema_cursor: Cell::new(SqliteSchemaCursor::Unallocated),
        }
    }

    /// The statement's cursor for writing `sqlite_schema` of the database
    /// it changes. The first call opens it.
    pub(crate) fn sqlite_schema_cursor(&self, program: &mut ProgramBuilder) -> usize {
        let cursor_id = match self.sqlite_schema_cursor.get() {
            SqliteSchemaCursor::Open(cursor_id) => return cursor_id,
            SqliteSchemaCursor::Allocated(cursor_id) => {
                emit_open_sqlite_schema_cursor(program, cursor_id, self.database_id);
                cursor_id
            }
            SqliteSchemaCursor::Unallocated => {
                open_sqlite_schema_for_write(program, self.resolver, self.database_id)
            }
        };
        self.sqlite_schema_cursor
            .set(SqliteSchemaCursor::Open(cursor_id));
        cursor_id
    }

    /// Gives the statement's `sqlite_schema` cursor its number now, for a
    /// statement whose cursor numbers depend on it, without opening it.
    /// [`Self::sqlite_schema_cursor`] opens it on its first call.
    pub(crate) fn alloc_sqlite_schema_cursor(&self, program: &mut ProgramBuilder) {
        assert!(
            self.sqlite_schema_cursor.get() == SqliteSchemaCursor::Unallocated,
            "the statement already has a sqlite_schema cursor"
        );
        let cursor_id = alloc_sqlite_schema_cursor(program, self.resolver, self.database_id);
        self.sqlite_schema_cursor
            .set(SqliteSchemaCursor::Allocated(cursor_id));
    }

    /// Closes the statement's `sqlite_schema` cursor. Emits nothing when no
    /// write opened it, which is the case for a catalog that stores no
    /// `sqlite_schema` row.
    pub(crate) fn close_sqlite_schema_cursor(&self, program: &mut ProgramBuilder) {
        if let SqliteSchemaCursor::Open(cursor_id) = self.sqlite_schema_cursor.get() {
            program.emit_insn(Insn::Close { cursor_id });
            self.sqlite_schema_cursor
                .set(SqliteSchemaCursor::Allocated(cursor_id));
        }
    }
}

/// The statement's `sqlite_schema` cursor. CREATE INDEX gives it its number
/// before the index cursor and opens it after CreateBtree, so a cursor can
/// have a number and no instruction that opens it yet.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SqliteSchemaCursor {
    Unallocated,
    Allocated(usize),
    Open(usize),
}

/// Whether a user runs the statement, or the engine for itself: a frontend
/// creating its own tables, VACUUM, the MVCC bootstrap connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmdOrigin {
    User,
    Engine,
}

/// What a DDL statement asks the catalog to store.
#[derive(Debug, Clone, Copy)]
pub enum CatalogCmd<'a> {
    CreateTable(CreateTable<'a>),
    CreateIndex(CreateIndex<'a>),
    DropIndex(DropIndex<'a>),
    CreateSequence(CreateSequence<'a>),
    DropSequence(DropSequence<'a>),
    DropTable(DropTable<'a>),
    CreateView(CreateView<'a>),
    CreateMaterializedView(CreateMaterializedView<'a>),
    DropView(DropView<'a>),
    RenameTable(RenameTable<'a>),
    RenameColumn(RenameColumn<'a>),
    AlterColumn(AlterColumn<'a>),
}

/// A new table. The statement has already emitted the creation of its
/// B-tree and of the B-trees of its automatic indexes.
#[derive(Debug, Clone, Copy)]
pub struct CreateTable<'a> {
    pub namespace: NamespaceId,
    pub name: &'a str,
    pub written_name: &'a ast::Name,
    pub temporary: bool,
    pub if_not_exists: bool,
    pub body: &'a ast::CreateTableBody,
    pub text: TableText<'a>,
    pub root_reg: usize,
    pub automatic_indexes: &'a [AutomaticIndex],
}

/// The SQL text of a new table: the statement's, none because translation
/// changed the table (`Rewritten`), or the engine's for `AS SELECT`.
#[derive(Debug, Clone, Copy)]
pub enum TableText<'a> {
    Statement(&'a str),
    Rewritten,
    FromSelect(&'a str),
}

/// An index of a PRIMARY KEY or UNIQUE constraint, with the register that
/// holds its root page.
#[derive(Debug, Clone)]
pub struct AutomaticIndex {
    pub name: String,
    pub root_reg: usize,
}

/// A new index. The statement has already emitted the creation of its
/// storage. `root_reg` has the root page of its B-tree, or 0 for an index
/// method that stores the index elsewhere.
#[derive(Debug, Clone, Copy)]
pub struct CreateIndex<'a> {
    pub namespace: NamespaceId,
    pub name: &'a str,
    pub written_name: &'a ast::Name,
    pub table: &'a str,
    pub written_table: &'a ast::Name,
    pub statement: &'a ast::Stmt,
    pub root_reg: usize,
}

/// An index that the statement drops.
#[derive(Debug, Clone, Copy)]
pub struct DropIndex<'a> {
    pub namespace: NamespaceId,
    pub name: &'a str,
}

/// A new sequence. The statement creates its backing table itself.
#[derive(Debug, Clone, Copy)]
pub struct CreateSequence<'a> {
    pub namespace: NamespaceId,
    pub name: &'a str,
}

/// A sequence that is dropped, by `DROP SEQUENCE` or with the column that
/// owns it.
#[derive(Debug, Clone, Copy)]
pub struct DropSequence<'a> {
    pub namespace: NamespaceId,
    pub name: &'a str,
}

/// A table that the statement drops, with its indexes and the sequences
/// that its columns own, before the statement destroys their B-trees.
/// `scratch_regs` are two registers for the catalog to use. The statement
/// allocates them because Destroy later reuses the second, as in SQLite's
/// bytecode.
#[derive(Debug, Clone, Copy)]
pub struct DropTable<'a> {
    pub namespace: NamespaceId,
    pub name: &'a str,
    pub is_virtual: bool,
    pub indexes: &'a [&'a str],
    pub owned_sequences: &'a [String],
    pub scratch_regs: [usize; 2],
}

/// A new view.
#[derive(Debug, Clone, Copy)]
pub struct CreateView<'a> {
    pub namespace: NamespaceId,
    pub name: &'a str,
    pub written_name: &'a ast::Name,
    pub columns: &'a [ast::IndexedColumn],
    pub select: &'a ast::Select,
}

/// A new materialized view. The statement writes the engine tables that
/// hold the state of its maintenance itself.
#[derive(Debug, Clone, Copy)]
pub struct CreateMaterializedView<'a> {
    pub namespace: NamespaceId,
    pub name: &'a str,
}

/// A view or materialized view that the statement drops.
#[derive(Debug, Clone, Copy)]
pub struct DropView<'a> {
    pub namespace: NamespaceId,
    pub name: &'a str,
}

/// A table that gets a new name in the same namespace. The `stored_` names
/// are the names in the table's schema rows.
#[derive(Debug, Clone, Copy)]
pub struct RenameTable<'a> {
    pub namespace: NamespaceId,
    pub old_name: &'a str,
    pub new_name: &'a str,
    pub(crate) indexes: &'a [Arc<Index>],
    pub(crate) stored_old_name: &'a str,
    pub(crate) stored_new_name: &'a str,
}

impl<'a> RenameTable<'a> {
    /// The old and the new name of each automatic index of the table.
    pub fn renamed_automatic_indexes(&self) -> impl Iterator<Item = (&'a str, String)> + '_ {
        self.indexes.iter().filter_map(|index| {
            let new_name = renamed_automatic_index_name(
                &index.name,
                self.stored_old_name,
                self.stored_new_name,
            )?;
            Some((index.name.as_str(), new_name))
        })
    }
}

/// A column that gets a new name.
#[derive(Debug, Clone, Copy)]
pub struct RenameColumn<'a> {
    pub namespace: NamespaceId,
    pub table: &'a str,
    pub old_name: &'a str,
    pub new_name: &'a ast::Name,
}

/// A column that gets a new definition, and with it possibly a new name.
#[derive(Debug, Clone, Copy)]
pub struct AlterColumn<'a> {
    pub namespace: NamespaceId,
    pub table: &'a str,
    pub old_name: &'a str,
    pub definition: &'a ast::ColumnDefinition,
}

/// A statement about roles, ownership, privileges or row-level security.
/// The engine stores none of these; the dialect's catalog does all of the
/// work in [`Catalog::emit_access`].
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
    /// `DROP ROLE roles`, which also ends the roles' memberships.
    /// `if_exists` skips a role that does not exist.
    DropRole {
        if_exists: bool,
        roles: &'a [Grantee],
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
            Self::DropRole { .. } => "DROP ROLE is not supported".to_string(),
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
/// puts one in [`Insn::CatalogApply`](crate::Insn::CatalogApply) after the
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
