//! How a dialect stores the objects of a database: the [`Catalog`] that DDL
//! translation writes through, and what the dialect's bytecode tells the
//! in-memory schema once the command is stored.

use crate::ast::{self, Grantee, Privilege, RoleSpec};
use crate::{
    namespace::{NamespaceId, NamespaceRef},
    schema::{load::LoadStep, BTreeTable, Index, Schema},
    translate::{
        emitter::Resolver,
        schema::{
            alloc_sqlite_schema_cursor, emit_open_sqlite_schema_cursor,
            open_seeded_table_for_write, open_sqlite_schema_for_write, SeededTableWriter,
        },
    },
    util::renamed_automatic_index_name,
    vdbe::{builder::ProgramBuilder, insn::Insn},
    Connection, LimboError, Result, Value,
};
use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

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

    /// Steps that read this catalog's own tables while a schema is loaded.
    /// The loader runs them after its own steps, at open and at reload.
    fn load_steps(&self) -> Vec<Box<dyn LoadStep>> {
        Vec::new()
    }

    /// The tables of this catalog that have rows before anything is
    /// stored. Core serves the rows, and stores them on the first write.
    ///
    /// The list is static, because the schema of a database needs it before
    /// a statement compiles. The main schema has a read-only table for each
    /// seeded table whose name no stored object has.
    fn seeded_tables(&self) -> &'static [SeededTable] {
        &[]
    }
}

/// A table that has a fixed set of rows, the seed, before anything is
/// stored. Until a statement stores the table, core serves the seed rows
/// through a read-only table named `name`.
pub struct SeededTable {
    /// The name of the table, such as `pg_proc`.
    pub name: &'static str,
    /// The `CREATE TABLE` statement of the table, in SQLite syntax. It gives
    /// the columns of the read-only table and of the stored table. The table
    /// must have a rowid.
    pub create_sql: &'static str,
    /// The `CREATE INDEX` statements of the stored table, in SQLite syntax.
    pub index_sql: &'static [&'static str],
    /// The rows of the table before it is stored.
    pub seed: &'static dyn Seed,
    /// The hidden columns of the read-only table, after the columns of
    /// `create_sql`. `SELECT *` does not show them. The stored table does not
    /// have them, so core refuses to store a seeded table that has hidden
    /// columns.
    pub hidden_columns: &'static [HiddenColumn],
    /// The stored rows that belong to the seed. Each seeded table of a
    /// catalog must have a range, also when its seed is empty, so that a
    /// later seed with rows does not take rows that the catalog wrote. Only
    /// the seeded tables of core, whose seeds stay empty, have none.
    pub seed_rows: Option<SeedRowRange>,
}

/// The stored rows of a [`SeededTable`] that belong to its seed: the rows
/// whose `column` has an integer value in `range`.
///
/// The seed owns the rows inside the range. When the seed in the binary has
/// another [`Seed::version`] than the seed that filled the stored table,
/// core deletes the rows inside the range and copies the seed rows in
/// again. Core never deletes or changes a row outside the range. Thus each
/// seed row must be inside the range, and each row that the catalog writes
/// must be outside it. A statement that writes a row that breaks one of
/// these rules fails.
#[derive(Debug, Clone)]
pub struct SeedRowRange {
    /// The name of an `INTEGER` column of `create_sql`, which can be the
    /// `INTEGER PRIMARY KEY` column. A NULL value is outside the range.
    pub column: &'static str,
    /// The values of `column` that the seed owns.
    pub range: std::ops::Range<i64>,
}

/// A hidden `INTEGER` column of the read-only table of a [`SeededTable`],
/// with the same value in each row. An example is the `tableoid` system
/// column of a PostgreSQL catalog.
pub struct HiddenColumn {
    /// The name of the column.
    pub name: &'static str,
    /// The value of the column in each row.
    pub value: i64,
}

/// The rows of a [`SeededTable`] before the table is stored. A row has an
/// index from 0 to `len() - 1`, and the rows are in index order.
///
/// The rowid of a row is the value of the column that the table's
/// `create_sql` declares `INTEGER PRIMARY KEY`. When no column is declared
/// so, the rowid of the row at `index` is `index + 1`. Rowids are unique.
pub trait Seed: Send + Sync {
    /// The number of rows.
    fn len(&self) -> usize;

    /// Whether the seed has no rows.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Writes the values of the row at `index`, in column order, to the end
    /// of `out`. The caller gives an empty `out`. When the table has an
    /// `INTEGER PRIMARY KEY` column, the value of that column is an integer.
    fn row(&self, index: usize, out: &mut Vec<Value>);

    /// The index of the row whose rowid is `rowid`, or None when no row has
    /// it, so that `WHERE oid = ...` does not scan.
    fn find(&self, rowid: i64) -> Option<usize>;

    /// Changes when the rows change.
    fn version(&self) -> u32;
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
    sqlite_schema_rowid_reg: Cell<Option<usize>>,
    /// The statement's cursor for writing `sqlite_schema` of the main
    /// database, when the statement changes another database.
    main_sqlite_schema_cursor: Cell<Option<usize>>,
    /// The seeded tables that the statement writes, under their normalized
    /// names.
    seeded_table_writers: RefCell<Vec<(String, SeededTableWriter)>>,
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
            sqlite_schema_rowid_reg: Cell::new(None),
            main_sqlite_schema_cursor: Cell::new(None),
            seeded_table_writers: RefCell::new(Vec::new()),
        }
    }

    /// Opens the seeded table `name` of the main database for writing, and
    /// returns its writer. `name` is the name of a seeded table of the
    /// catalog or of core.
    ///
    /// When the schema of the statement has the table stored, the writer
    /// opens it. Otherwise this is the first write: the writer creates the
    /// table and its indexes in the transaction of the statement, and copies
    /// the seed rows into it. From then on the name is the stored table's.
    /// When the statement asks again for the same table, it gets the same
    /// writer.
    pub fn open_seeded_table_for_write(
        &self,
        program: &mut ProgramBuilder,
        name: &str,
    ) -> Result<SeededTableWriter> {
        let name = crate::util::normalize_ident(name);
        if let Some((_, writer)) = self
            .seeded_table_writers
            .borrow()
            .iter()
            .find(|(other, _)| *other == name)
        {
            return Ok(writer.clone());
        }
        let writer = open_seeded_table_for_write(self, program, &name)?;
        self.seeded_table_writers
            .borrow_mut()
            .push((name, writer.clone()));
        Ok(writer)
    }

    /// The statement's cursor for writing `sqlite_schema` of the main
    /// database. It is [`Self::sqlite_schema_cursor`] when the statement
    /// changes the main database. The first call opens it.
    pub(crate) fn main_sqlite_schema_cursor(&self, program: &mut ProgramBuilder) -> usize {
        if self.database_id == crate::MAIN_DB_ID {
            return self.sqlite_schema_cursor(program);
        }
        if let Some(cursor_id) = self.main_sqlite_schema_cursor.get() {
            return cursor_id;
        }
        let cursor_id = open_sqlite_schema_for_write(program, self.resolver, crate::MAIN_DB_ID);
        self.main_sqlite_schema_cursor.set(Some(cursor_id));
        cursor_id
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
    AddColumn(AddColumn<'a>),
    DropColumn(DropColumn<'a>),
    CreateTrigger(CreateTrigger<'a>),
    DropTrigger(DropTrigger<'a>),
    CreateVirtualTable(CreateVirtualTable<'a>),
}

/// A new table. The statement has already emitted the creation of its
/// B-tree and of the B-trees of its automatic indexes. `table` is the table
/// that the statement creates, as the schema will hold it after loading it,
/// with two differences: it has no root page yet, and its name still has the
/// prefix of its namespace. Use `name` for the table's name.
#[derive(Debug, Clone, Copy)]
pub struct CreateTable<'a> {
    pub namespace: NamespaceId,
    pub name: &'a str,
    pub written_name: &'a ast::Name,
    pub temporary: bool,
    pub if_not_exists: bool,
    pub body: &'a ast::CreateTableBody,
    pub table: &'a BTreeTable,
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

/// An object that depends on a relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dependent {
    pub object: DependentObject,
    /// The namespace and name of the relation that `object` depends on.
    pub depends_on: (NamespaceId, String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DependentObject {
    /// A view that reads the relation.
    View {
        namespace: NamespaceId,
        name: String,
    },
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

/// A new sequence. The statement has already emitted the creation of the
/// B-tree of its backing table, whose root page is in `root_reg`, and
/// inserts the table's initial row itself.
#[derive(Debug, Clone, Copy)]
pub struct CreateSequence<'a> {
    pub namespace: NamespaceId,
    pub name: &'a str,
    pub root_reg: usize,
}

/// A sequence that is dropped, by `DROP SEQUENCE` or with the column that
/// owns it, before the statement destroys the B-tree of its backing table.
#[derive(Debug, Clone, Copy)]
pub struct DropSequence<'a> {
    pub namespace: NamespaceId,
    pub name: &'a str,
}

/// A table that the statement drops, with its indexes, before the
/// statement destroys their B-trees. A sequence that a column of the table
/// owns gets a [`DropSequence`] of its own. `scratch_regs` are two
/// registers for the catalog to use. The statement allocates them because
/// Destroy later reuses the second, as in SQLite's bytecode.
#[derive(Debug, Clone, Copy)]
pub struct DropTable<'a> {
    pub namespace: NamespaceId,
    pub name: &'a str,
    pub is_virtual: bool,
    pub indexes: &'a [&'a str],
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
    pub written_name: &'a ast::Name,
    pub columns: &'a [ast::IndexedColumn],
    pub select: &'a ast::Select,
    pub root_reg: usize,
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
    pub written_old_name: &'a str,
    pub written_new_name: &'a str,
    pub is_virtual: bool,
    pub(crate) indexes: &'a [Arc<Index>],
    pub(crate) stored_old_name: &'a str,
    pub(crate) stored_new_name: &'a str,
    pub temp_triggers: &'a [TempTriggerOfRenamedTable],
    pub statement: &'a str,
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

/// A trigger of the temp database whose text names a renamed table, with
/// its new text.
#[derive(Debug, Clone)]
pub struct TempTriggerOfRenamedTable {
    pub name: String,
    pub sql: String,
    pub is_on_the_table: bool,
}

/// A column that gets a new name.
#[derive(Debug, Clone, Copy)]
pub struct RenameColumn<'a> {
    pub namespace: NamespaceId,
    pub table: &'a str,
    pub written_table_name: &'a str,
    pub old_name: &'a str,
    pub written_old_name: &'a str,
    pub new_name: &'a ast::Name,
    pub triggers: &'a [RewrittenTrigger],
    pub views: &'a [RewrittenView],
    pub statement: &'a str,
}

/// A trigger whose text names a renamed column, with its new text.
#[derive(Debug, Clone)]
pub struct RewrittenTrigger {
    pub namespace: NamespaceRef,
    pub name: String,
    pub sql: String,
}

/// A view whose text names a renamed column, with its new text.
#[derive(Debug, Clone)]
pub struct RewrittenView {
    pub namespace: NamespaceRef,
    pub name: String,
    pub sql: String,
}

/// A column that gets a new definition, and with it possibly a new name.
#[derive(Debug, Clone, Copy)]
pub struct AlterColumn<'a> {
    pub namespace: NamespaceId,
    pub table: &'a str,
    pub written_table_name: &'a str,
    pub old_name: &'a str,
    pub written_old_name: &'a str,
    pub definition: &'a ast::ColumnDefinition,
}

/// A table that gets a new last column. `table` is the table with it.
#[derive(Debug, Clone, Copy)]
pub struct AddColumn<'a> {
    pub namespace: NamespaceId,
    pub name: &'a str,
    pub table: &'a BTreeTable,
    pub statement: &'a str,
}

/// A table that loses a column. `table` is the table without it.
#[derive(Debug, Clone, Copy)]
pub struct DropColumn<'a> {
    pub namespace: NamespaceId,
    pub name: &'a str,
    pub table: &'a BTreeTable,
    pub statement: &'a str,
}

/// A new trigger on `table`. A trigger of the temp database can be on a
/// table of another database, so the table comes with its own database.
#[derive(Debug, Clone, Copy)]
pub struct CreateTrigger<'a> {
    pub namespace: NamespaceId,
    pub name: &'a str,
    pub table_namespace: NamespaceRef,
    pub table: &'a str,
    pub sql: &'a str,
}

/// A trigger that the statement drops.
#[derive(Debug, Clone, Copy)]
pub struct DropTrigger<'a> {
    pub namespace: NamespaceId,
    pub name: &'a str,
}

/// A new virtual table of the main database. It has no B-tree: its module
/// stores it.
#[derive(Debug, Clone, Copy)]
pub struct CreateVirtualTable<'a> {
    pub name: &'a str,
    pub sql: &'a str,
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
