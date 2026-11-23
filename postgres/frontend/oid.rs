//! The oids of the objects that users create: one counter for the whole
//! database, as in PostgreSQL, so that no two objects share an oid, whatever
//! catalog stores them.
//!
//! The counter is the highest oid ever given out. It is the `seq` of one
//! `sqlite_sequence` row of the main database. An oid is never given out
//! twice, even after its object is dropped.
//!
//! A statement that creates an object opens an [`OidAllocator`] while it is
//! compiled and calls [`OidAllocator::emit_allocate`] once per oid:
//!
//! ```ignore
//! let oids = OidAllocator::open(program, ctx.schema)?;
//! // An oid that the statement stores as a value:
//! let oid_reg = oids.emit_allocate(program);
//! // An oid that is the rowid of the row the statement inserts next:
//! let oid_reg = oids.emit_allocate_rowid(program, table_cursor_id, TABLE_NAME);
//! ```
//!
//! The bytecode reads and writes the counter in the statement's own
//! transaction. This differs from PostgreSQL, whose counter is outside any
//! transaction: here a statement that rolls back gives its oids back. No
//! committed row ever held them, so the next statement can take them.

use std::sync::Arc;

use turso_core::namespace::NamespaceId;
use turso_core::schema::{BTreeTable, Schema, SQLITE_SEQUENCE_TABLE_NAME};
use turso_core::{
    emit_sqlite_sequence_row_read, emit_sqlite_sequence_row_write, CursorType, Insn, LimboError,
    ProgramBuilder, MAIN_DB_ID, SQLITE_ERROR,
};

/// PostgreSQL's `FirstNormalObjectId`: the first oid given to an object
/// that a user creates. Smaller oids belong to built-in catalog objects.
pub(crate) const FIRST_NORMAL_OBJECT_ID: i64 = 16384;

/// The name of the `sqlite_sequence` row that holds the counter. It is the
/// name of the relation catalog's table, which was the only user of the
/// counter when databases first stored it.
const COUNTER_ROW_NAME: &str = "__turso_internal_classes";

/// The statements that create the counter in a database that has none.
/// A database that the SQLite frontend made can have `sqlite_sequence`
/// already. No oid is given out yet, so the counter starts at the oid before
/// [`FIRST_NORMAL_OBJECT_ID`].
pub(crate) const COUNTER_INIT_STATEMENTS: [&str; 2] = [
    "CREATE TABLE IF NOT EXISTS sqlite_sequence(name,seq)",
    "INSERT INTO sqlite_sequence (name, seq) VALUES ('__turso_internal_classes', 16383)",
];

// The statements above spell the row name and the oid so that they can be
// constants.
const _: () = assert!(FIRST_NORMAL_OBJECT_ID - 1 == 16383);

/// Gives out oids in the program of one statement.
pub(crate) struct OidAllocator {
    counter_cursor_id: usize,
    sequence_table: Arc<BTreeTable>,
}

impl OidAllocator {
    /// Opens the counter for the statement that `program` compiles. Fails
    /// for a database without `sqlite_sequence`, which the PostgreSQL
    /// opening functions always create.
    pub(crate) fn open(program: &mut ProgramBuilder, schema: &Schema) -> turso_core::Result<Self> {
        let Some(sequence_table) =
            schema.get_btree_table(NamespaceId::ROOT, SQLITE_SEQUENCE_TABLE_NAME)
        else {
            return Err(LimboError::Corrupt(format!(
                "the database has no {SQLITE_SEQUENCE_TABLE_NAME}, which holds the oid counter"
            )));
        };
        let counter_cursor_id =
            program.alloc_cursor_id(CursorType::BTreeTable(sequence_table.clone()));
        program.emit_insn(Insn::OpenRead {
            cursor_id: counter_cursor_id,
            root_page: sequence_table.root_page,
            db: MAIN_DB_ID,
        });
        Ok(Self {
            counter_cursor_id,
            sequence_table,
        })
    }

    /// Puts the next oid in the returned register and stores it as the
    /// counter. Each call gives a new oid.
    pub(crate) fn emit_allocate(&self, program: &mut ProgramBuilder) -> usize {
        let oid_reg = self.emit_read_counter(program);
        program.emit_insn(Insn::AddImm {
            register: oid_reg,
            value: 1,
        });
        emit_sqlite_sequence_row_write(
            program,
            self.sequence_table.clone(),
            MAIN_DB_ID,
            COUNTER_ROW_NAME,
            oid_reg,
        );
        oid_reg
    }

    /// Puts the next oid in the returned register, for a row that a table
    /// stores under that oid as its rowid. Leaves `table_cursor_id` where
    /// `Insn::Insert` writes the row, which that instruction needs. The
    /// statement fails when `table_name` already has a row with the oid:
    /// the counter is behind the table.
    pub(crate) fn emit_allocate_rowid(
        &self,
        program: &mut ProgramBuilder,
        table_cursor_id: usize,
        table_name: &str,
    ) -> usize {
        let oid_reg = self.emit_allocate(program);
        let unused = program.allocate_label();
        program.emit_insn(Insn::NotExists {
            cursor: table_cursor_id,
            rowid_reg: oid_reg,
            target_pc: unused,
        });
        program.emit_insn(Insn::Halt {
            constraint_info: None,
            err_code: SQLITE_ERROR,
            description: format!(
                "the oid counter gave out an oid that a row of {table_name} already has"
            ),
            on_error: None,
            description_reg: None,
        });
        program.preassign_label_to_next_insn(unused);
        oid_reg
    }

    /// Reads the counter into the returned register. The statement fails
    /// when the counter's row is missing: starting again at
    /// [`FIRST_NORMAL_OBJECT_ID`] would give out oids that objects already
    /// have.
    fn emit_read_counter(&self, program: &mut ProgramBuilder) -> usize {
        let name_reg = program.emit_string8_new_reg(COUNTER_ROW_NAME.to_string());
        let counter_reg = program.alloc_register();
        let rowid_reg = program.alloc_register();
        emit_sqlite_sequence_row_read(
            program,
            self.counter_cursor_id,
            name_reg,
            counter_reg,
            rowid_reg,
        );
        let found = program.allocate_label();
        program.emit_insn(Insn::NotNull {
            reg: rowid_reg,
            target_pc: found,
        });
        program.emit_insn(Insn::Halt {
            constraint_info: None,
            err_code: SQLITE_ERROR,
            description: format!(
                "{SQLITE_SEQUENCE_TABLE_NAME} has no row for the oid counter \
                 \"{COUNTER_ROW_NAME}\""
            ),
            on_error: None,
            description_reg: None,
        });
        program.preassign_label_to_next_insn(found);
        counter_reg
    }
}
