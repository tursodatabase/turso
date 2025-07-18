use std::{
    num::{NonZero, NonZeroUsize},
    rc::Rc,
    sync::Arc,
};

use super::{execute, AggFunc, BranchOffset, CursorID, FuncCtx, InsnFunction, PageIdx};
use crate::{
    schema::{Affinity, BTreeTable, Index},
    storage::{pager::CreateBTreeFlags, wal::CheckpointMode},
    translate::collate::CollationSeq,
    Value,
};
use turso_macros::Description;
use turso_sqlite3_parser::ast::SortOrder;

/// Flags provided to comparison instructions (e.g. Eq, Ne) which determine behavior related to NULL values.
#[derive(Clone, Copy, Debug, Default)]
pub struct CmpInsFlags(usize);

impl CmpInsFlags {
    const NULL_EQ: usize = 0x80;
    const JUMP_IF_NULL: usize = 0x10;
    const AFFINITY_MASK: usize = 0x47;

    fn has(&self, flag: usize) -> bool {
        (self.0 & flag) != 0
    }

    pub fn null_eq(mut self) -> Self {
        self.0 |= CmpInsFlags::NULL_EQ;
        self
    }

    pub fn jump_if_null(mut self) -> Self {
        self.0 |= CmpInsFlags::JUMP_IF_NULL;
        self
    }

    pub fn has_jump_if_null(&self) -> bool {
        self.has(CmpInsFlags::JUMP_IF_NULL)
    }

    pub fn has_nulleq(&self) -> bool {
        self.has(CmpInsFlags::NULL_EQ)
    }

    pub fn with_affinity(mut self, affinity: Affinity) -> Self {
        let aff_code = affinity.as_char_code() as usize;
        self.0 = (self.0 & !Self::AFFINITY_MASK) | aff_code;
        self
    }

    pub fn get_affinity(&self) -> Affinity {
        let aff_code = (self.0 & Self::AFFINITY_MASK) as u8;
        Affinity::from_char_code(aff_code).unwrap_or(Affinity::Blob)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct IdxInsertFlags(pub u8);
impl IdxInsertFlags {
    pub const APPEND: u8 = 0x01; // Hint: insert likely at the end
    pub const NCHANGE: u8 = 0x02; // Increment the change counter
    pub const USE_SEEK: u8 = 0x04; // Skip seek if last one was same key
    pub const NO_OP_DUPLICATE: u8 = 0x08; // Do not error on duplicate key
    pub fn new() -> Self {
        IdxInsertFlags(0)
    }
    pub fn has(&self, flag: u8) -> bool {
        (self.0 & flag) != 0
    }
    pub fn append(mut self, append: bool) -> Self {
        if append {
            self.0 |= IdxInsertFlags::APPEND;
        } else {
            self.0 &= !IdxInsertFlags::APPEND;
        }
        self
    }
    pub fn use_seek(mut self, seek: bool) -> Self {
        if seek {
            self.0 |= IdxInsertFlags::USE_SEEK;
        } else {
            self.0 &= !IdxInsertFlags::USE_SEEK;
        }
        self
    }
    pub fn nchange(mut self, change: bool) -> Self {
        if change {
            self.0 |= IdxInsertFlags::NCHANGE;
        } else {
            self.0 &= !IdxInsertFlags::NCHANGE;
        }
        self
    }
    /// If this is set, we will not error on duplicate key.
    /// This is a bit of a hack we use to make ephemeral indexes for UNION work --
    /// instead we should allow overwriting index interior cells, which we currently don't;
    /// this should (and will) be fixed in a future PR.
    pub fn no_op_duplicate(mut self) -> Self {
        self.0 |= IdxInsertFlags::NO_OP_DUPLICATE;
        self
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct InsertFlags(pub u8);

impl InsertFlags {
    pub const UPDATE: u8 = 0x01; // Flag indicating this is part of an UPDATE statement

    pub fn new() -> Self {
        InsertFlags(0)
    }

    pub fn has(&self, flag: u8) -> bool {
        (self.0 & flag) != 0
    }

    pub fn update(mut self, is_update: bool) -> Self {
        if is_update {
            self.0 |= InsertFlags::UPDATE;
        } else {
            self.0 &= !InsertFlags::UPDATE;
        }
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub enum RegisterOrLiteral<T: Copy + std::fmt::Display> {
    Register(usize),
    Literal(T),
}

impl From<PageIdx> for RegisterOrLiteral<PageIdx> {
    fn from(value: PageIdx) -> Self {
        RegisterOrLiteral::Literal(value)
    }
}

impl<T: Copy + std::fmt::Display> std::fmt::Display for RegisterOrLiteral<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Literal(lit) => lit.fmt(f),
            Self::Register(reg) => reg.fmt(f),
        }
    }
}

#[derive(Description, Debug)]
pub enum Insn {
    /// Initialize the program state and jump to the given PC.
    Init {
        target_pc: BranchOffset,
    },
    /// Write a NULL into register dest. If dest_end is Some, then also write NULL into register dest_end and every register in between dest and dest_end. If dest_end is not set, then only register dest is set to NULL.
    Null {
        dest: usize,
        dest_end: Option<usize>,
    },
    /// Mark the beginning of a subroutine tha can be entered in-line. This opcode is identical to Null
    /// it has a different name only to make the byte code easier to read and verify
    BeginSubrtn {
        dest: usize,
        dest_end: Option<usize>,
    },
    /// Move the cursor P1 to a null row. Any Column operations that occur while the cursor is on the null row will always write a NULL.
    NullRow {
        cursor_id: CursorID,
    },
    /// Add two registers and store the result in a third register.
    Add {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Subtract rhs from lhs and store in dest
    Subtract {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Multiply two registers and store the result in a third register.
    Multiply {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Divide lhs by rhs and store the result in a third register.
    Divide {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Compare two vectors of registers in reg(P1)..reg(P1+P3-1) (call this vector "A") and in reg(P2)..reg(P2+P3-1) ("B"). Save the result of the comparison for use by the next Jump instruct.
    Compare {
        start_reg_a: usize,
        start_reg_b: usize,
        count: usize,
        collation: Option<CollationSeq>,
    },
    /// Place the result of rhs bitwise AND lhs in third register.
    BitAnd {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Place the result of rhs bitwise OR lhs in third register.
    BitOr {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Place the result of bitwise NOT register P1 in dest register.
    BitNot {
        reg: usize,
        dest: usize,
    },
    /// Checkpoint the database (applying wal file content to database file).
    Checkpoint {
        database: usize,                 // checkpoint database P1
        checkpoint_mode: CheckpointMode, // P2 checkpoint mode
        dest: usize,                     // P3 checkpoint result
    },
    /// Divide lhs by rhs and place the remainder in dest register.
    Remainder {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Jump to the instruction at address P1, P2, or P3 depending on whether in the most recent Compare instruction the P1 vector was less than, equal to, or greater than the P2 vector, respectively.
    Jump {
        target_pc_lt: BranchOffset,
        target_pc_eq: BranchOffset,
        target_pc_gt: BranchOffset,
    },
    /// Move the P3 values in register P1..P1+P3-1 over into registers P2..P2+P3-1. Registers P1..P1+P3-1 are left holding a NULL. It is an error for register ranges P1..P1+P3-1 and P2..P2+P3-1 to overlap. It is an error for P3 to be less than 1.
    Move {
        source_reg: usize,
        dest_reg: usize,
        count: usize,
    },
    /// If the given register is a positive integer, decrement it by decrement_by and jump to the given PC.
    IfPos {
        reg: usize,
        target_pc: BranchOffset,
        decrement_by: usize,
    },
    /// If the given register is not NULL, jump to the given PC.
    NotNull {
        reg: usize,
        target_pc: BranchOffset,
    },
    /// Compare two registers and jump to the given PC if they are equal.
    Eq {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
        /// CmpInsFlags are nulleq (null = null) or jump_if_null.
        ///
        /// jump_if_null jumps if either of the operands is null. Used for "jump when false" logic.
        /// Eg. "SELECT * FROM users WHERE id = NULL" becomes:
        /// <JUMP TO NEXT ROW IF id != NULL>
        /// Without the jump_if_null flag it would not jump because the logical comparison "id != NULL" is never true.
        /// This flag indicates that if either is null we should still jump.
        flags: CmpInsFlags,
        collation: Option<CollationSeq>,
    },
    /// Compare two registers and jump to the given PC if they are not equal.
    Ne {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
        /// CmpInsFlags are nulleq (null = null) or jump_if_null.
        ///
        /// jump_if_null jumps if either of the operands is null. Used for "jump when false" logic.
        flags: CmpInsFlags,
        collation: Option<CollationSeq>,
    },
    /// Compare two registers and jump to the given PC if the left-hand side is less than the right-hand side.
    Lt {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
        /// jump_if_null: Jump if either of the operands is null. Used for "jump when false" logic.
        flags: CmpInsFlags,
        collation: Option<CollationSeq>,
    },
    // Compare two registers and jump to the given PC if the left-hand side is less than or equal to the right-hand side.
    Le {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
        /// jump_if_null: Jump if either of the operands is null. Used for "jump when false" logic.
        flags: CmpInsFlags,
        collation: Option<CollationSeq>,
    },
    /// Compare two registers and jump to the given PC if the left-hand side is greater than the right-hand side.
    Gt {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
        /// jump_if_null: Jump if either of the operands is null. Used for "jump when false" logic.
        flags: CmpInsFlags,
        collation: Option<CollationSeq>,
    },
    /// Compare two registers and jump to the given PC if the left-hand side is greater than or equal to the right-hand side.
    Ge {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
        /// jump_if_null: Jump if either of the operands is null. Used for "jump when false" logic.
        flags: CmpInsFlags,
        collation: Option<CollationSeq>,
    },
    /// Jump to target_pc if r\[reg\] != 0 or (r\[reg\] == NULL && r\[jump_if_null\] != 0)
    If {
        reg: usize,              // P1
        target_pc: BranchOffset, // P2
        /// P3. If r\[reg\] is null, jump iff r\[jump_if_null\] != 0
        jump_if_null: bool,
    },
    /// Jump to target_pc if r\[reg\] != 0 or (r\[reg\] == NULL && r\[jump_if_null\] != 0)
    IfNot {
        reg: usize,              // P1
        target_pc: BranchOffset, // P2
        /// P3. If r\[reg\] is null, jump iff r\[jump_if_null\] != 0
        jump_if_null: bool,
    },
    /// Open a cursor for reading.
    OpenRead {
        cursor_id: CursorID,
        root_page: PageIdx,
    },

    /// Open a cursor for a virtual table.
    VOpen {
        cursor_id: CursorID,
    },

    /// Create a new virtual table.
    VCreate {
        module_name: usize, // P1: Name of the module that contains the virtual table implementation
        table_name: usize,  // P2: Name of the virtual table
        args_reg: Option<usize>,
    },

    /// Initialize the position of the virtual table cursor.
    VFilter {
        cursor_id: CursorID,
        pc_if_empty: BranchOffset,
        arg_count: usize,
        args_reg: usize,
        idx_str: Option<usize>,
        idx_num: usize,
    },

    /// Read a column from the current row of the virtual table cursor.
    VColumn {
        cursor_id: CursorID,
        column: usize,
        dest: usize,
    },

    /// `VUpdate`: Virtual Table Insert/Update/Delete Instruction
    VUpdate {
        cursor_id: usize,     // P1: Virtual table cursor number
        arg_count: usize,     // P2: Number of arguments in argv[]
        start_reg: usize,     // P3: Start register for argv[]
        conflict_action: u16, // P4: Conflict resolution flags
    },

    /// Advance the virtual table cursor to the next row.
    /// TODO: async
    VNext {
        cursor_id: CursorID,
        pc_if_next: BranchOffset,
    },

    /// P4 is the name of a virtual table in database P1. Call the xDestroy method of that table.
    VDestroy {
        /// Name of a virtual table being destroyed
        table_name: String,
        ///  The database within which this virtual table needs to be destroyed (P1).
        db: usize,
    },

    /// Open a cursor for a pseudo-table that contains a single row.
    OpenPseudo {
        cursor_id: CursorID,
        content_reg: usize,
        num_fields: usize,
    },

    /// Rewind the cursor to the beginning of the B-Tree.
    Rewind {
        cursor_id: CursorID,
        pc_if_empty: BranchOffset,
    },

    Last {
        cursor_id: CursorID,
        pc_if_empty: BranchOffset,
    },

    /// Read a column from the current row of the cursor.
    Column {
        cursor_id: CursorID,
        column: usize,
        dest: usize,
        default: Option<Value>,
    },

    TypeCheck {
        start_reg: usize, // P1
        count: usize,     // P2
        /// GENERATED ALWAYS AS ... STATIC columns are only checked if P3 is zero.
        /// When P3 is non-zero, no type checking occurs for static generated columns.
        check_generated: bool, // P3
        table_reference: Rc<BTreeTable>, // P4
    },

    // Make a record and write it to destination register.
    MakeRecord {
        start_reg: usize, // P1
        count: usize,     // P2
        dest_reg: usize,  // P3
        index_name: Option<String>,
    },

    /// Emit a row of results.
    ResultRow {
        start_reg: usize, // P1
        count: usize,     // P2
    },

    /// Advance the cursor to the next row.
    Next {
        cursor_id: CursorID,
        pc_if_next: BranchOffset,
    },

    Prev {
        cursor_id: CursorID,
        pc_if_prev: BranchOffset,
    },

    /// Halt the program.
    Halt {
        err_code: usize,
        description: String,
    },

    /// Halt the program if P3 is null.
    HaltIfNull {
        target_reg: usize,   // P3
        description: String, // p4
        err_code: usize,     // p1
    },

    /// Start a transaction.
    Transaction {
        write: bool,
    },

    /// Set database auto-commit mode and potentially rollback.
    AutoCommit {
        auto_commit: bool,
        rollback: bool,
    },

    /// Branch to the given PC.
    Goto {
        target_pc: BranchOffset,
    },

    /// Stores the current program counter into register 'return_reg' then jumps to address target_pc.
    Gosub {
        target_pc: BranchOffset,
        return_reg: usize,
    },

    /// Returns to the program counter stored in register 'return_reg'.
    /// If can_fallthrough is true, fall through to the next instruction
    /// if return_reg does not contain an integer value. Otherwise raise an error.
    Return {
        return_reg: usize,
        can_fallthrough: bool,
    },

    /// Write an integer value into a register.
    Integer {
        value: i64,
        dest: usize,
    },

    /// Write a float value into a register
    Real {
        value: f64,
        dest: usize,
    },

    /// If register holds an integer, transform it to a float
    RealAffinity {
        register: usize,
    },

    // Write a string value into a register.
    String8 {
        value: String,
        dest: usize,
    },

    /// Write a blob value into a register.
    Blob {
        value: Vec<u8>,
        dest: usize,
    },

    /// Read a complete row of data from the current cursor and write it to the destination register.
    RowData {
        cursor_id: CursorID,
        dest: usize,
    },

    /// Read the rowid of the current row.
    RowId {
        cursor_id: CursorID,
        dest: usize,
    },
    /// Read the rowid of the current row from an index cursor.
    IdxRowId {
        cursor_id: CursorID,
        dest: usize,
    },

    /// Seek to a rowid in the cursor. If not found, jump to the given PC. Otherwise, continue to the next instruction.
    SeekRowid {
        cursor_id: CursorID,
        src_reg: usize,
        target_pc: BranchOffset,
    },
    SeekEnd {
        cursor_id: CursorID,
    },

    /// P1 is an open index cursor and P3 is a cursor on the corresponding table. This opcode does a deferred seek of the P3 table cursor to the row that corresponds to the current row of P1.
    /// This is a deferred seek. Nothing actually happens until the cursor is used to read a record. That way, if no reads occur, no unnecessary I/O happens.
    DeferredSeek {
        index_cursor_id: CursorID,
        table_cursor_id: CursorID,
    },

    /// If cursor_id refers to an SQL table (B-Tree that uses integer keys), use the value in start_reg as the key.
    /// If cursor_id refers to an SQL index, then start_reg is the first in an array of num_regs registers that are used as an unpacked index key.
    /// Seek to the first index entry that is greater than or equal to the given key. If not found, jump to the given PC. Otherwise, continue to the next instruction.
    SeekGE {
        is_index: bool,
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
        eq_only: bool,
    },

    /// If cursor_id refers to an SQL table (B-Tree that uses integer keys), use the value in start_reg as the key.
    /// If cursor_id refers to an SQL index, then start_reg is the first in an array of num_regs registers that are used as an unpacked index key.
    /// Seek to the first index entry that is greater than the given key. If not found, jump to the given PC. Otherwise, continue to the next instruction.
    SeekGT {
        is_index: bool,
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
    },

    /// cursor_id is a cursor pointing to a B-Tree index that uses integer keys, this op writes the value obtained from MakeRecord into the index.
    /// P3 + P4 are for the original column values that make up that key in unpacked (pre-serialized) form.
    /// If P5 has the OPFLAG_APPEND bit set, that is a hint to the b-tree layer that this insert is likely to be an append.
    /// OPFLAG_NCHANGE bit set, then the change counter is incremented by this instruction. If the OPFLAG_NCHANGE bit is clear, then the change counter is unchanged
    IdxInsert {
        cursor_id: CursorID,
        record_reg: usize, // P2 the register containing the record to insert
        unpacked_start: Option<usize>, // P3 the index of the first register for the unpacked key
        unpacked_count: Option<u16>, // P4 # of unpacked values in the key in P2
        flags: IdxInsertFlags, // TODO: optimization
    },

    /// The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end.
    /// If the P1 index entry is greater or equal than the key value then jump to P2. Otherwise fall through to the next instruction.
    // If cursor_id refers to an SQL table (B-Tree that uses integer keys), use the value in start_reg as the key.
    // If cursor_id refers to an SQL index, then start_reg is the first in an array of num_regs registers that are used as an unpacked index key.
    // Seek to the first index entry that is less than or equal to the given key. If not found, jump to the given PC. Otherwise, continue to the next instruction.
    SeekLE {
        is_index: bool,
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
        eq_only: bool,
    },

    // If cursor_id refers to an SQL table (B-Tree that uses integer keys), use the value in start_reg as the key.
    // If cursor_id refers to an SQL index, then start_reg is the first in an array of num_regs registers that are used as an unpacked index key.
    // Seek to the first index entry that is less than the given key. If not found, jump to the given PC. Otherwise, continue to the next instruction.
    SeekLT {
        is_index: bool,
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
    },

    // The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end.
    // If the P1 index entry is greater or equal than the key value then jump to P2. Otherwise fall through to the next instruction.
    IdxGE {
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
    },

    /// The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end.
    /// If the P1 index entry is greater than the key value then jump to P2. Otherwise fall through to the next instruction.
    IdxGT {
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
    },

    /// The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end.
    /// If the P1 index entry is lesser or equal than the key value then jump to P2. Otherwise fall through to the next instruction.
    IdxLE {
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
    },

    /// The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end.
    /// If the P1 index entry is lesser than the key value then jump to P2. Otherwise fall through to the next instruction.
    IdxLT {
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
    },

    /// Decrement the given register and jump to the given PC if the result is zero.
    DecrJumpZero {
        reg: usize,
        target_pc: BranchOffset,
    },

    AggStep {
        acc_reg: usize,
        col: usize,
        delimiter: usize,
        func: AggFunc,
    },

    AggFinal {
        register: usize,
        func: AggFunc,
    },

    /// Open a sorter.
    SorterOpen {
        cursor_id: CursorID,                   // P1
        columns: usize,                        // P2
        order: Vec<SortOrder>,                 // P4.
        collations: Vec<Option<CollationSeq>>, // The only reason for using Option<CollationSeq> is so the explain message is the same as in SQLite
    },

    /// Insert a row into the sorter.
    SorterInsert {
        cursor_id: CursorID,
        record_reg: usize,
    },

    /// Sort the rows in the sorter.
    SorterSort {
        cursor_id: CursorID,
        pc_if_empty: BranchOffset,
    },

    /// Retrieve the next row from the sorter.
    SorterData {
        cursor_id: CursorID,  // P1
        dest_reg: usize,      // P2
        pseudo_cursor: usize, // P3
    },

    /// Advance to the next row in the sorter.
    SorterNext {
        cursor_id: CursorID,
        pc_if_next: BranchOffset,
    },

    /// Function
    Function {
        constant_mask: i32, // P1
        start_reg: usize,   // P2, start of argument registers
        dest: usize,        // P3
        func: FuncCtx,      // P4
    },

    InitCoroutine {
        yield_reg: usize,
        jump_on_definition: BranchOffset,
        start_offset: BranchOffset,
    },

    EndCoroutine {
        yield_reg: usize,
    },

    Yield {
        yield_reg: usize,
        end_offset: BranchOffset,
    },

    Insert {
        cursor: CursorID,
        key_reg: usize,    // Must be int.
        record_reg: usize, // Blob of record data.
        flag: InsertFlags, // Flags used by insert, for now not used.
        table_name: String,
    },

    Int64 {
        _p1: usize,     //  unused
        out_reg: usize, // the output register
        _p3: usize,     // unused
        value: i64,     //  the value being written into the output register
    },

    Delete {
        cursor_id: CursorID,
    },

    /// If P5 is not zero, then raise an SQLITE_CORRUPT_INDEX error if no matching index entry
    /// is found. This happens when running an UPDATE or DELETE statement and the index entry to
    /// be updated or deleted is not found. For some uses of IdxDelete (example: the EXCEPT operator)
    /// it does not matter that no matching entry is found. For those cases, P5 is zero.
    IdxDelete {
        start_reg: usize,
        num_regs: usize,
        cursor_id: CursorID,
        raise_error_if_no_matching_entry: bool, // P5
    },

    NewRowid {
        cursor: CursorID,        // P1
        rowid_reg: usize,        // P2  Destination register to store the new rowid
        prev_largest_reg: usize, // P3 Previous largest rowid in the table (Not used for now)
    },

    MustBeInt {
        reg: usize,
    },

    SoftNull {
        reg: usize,
    },

    /// If P4==0 then register P3 holds a blob constructed by [MakeRecord](https://sqlite.org/opcode.html#MakeRecord). If P4>0 then register P3 is the first of P4 registers that form an unpacked record.\
    ///
    /// Cursor P1 is on an index btree. If the record identified by P3 and P4 contains any NULL value, jump immediately to P2. If all terms of the record are not-NULL then a check is done to determine if any row in the P1 index btree has a matching key prefix. If there are no matches, jump immediately to P2. If there is a match, fall through and leave the P1 cursor pointing to the matching row.\
    ///
    /// This opcode is similar to [NotFound](https://sqlite.org/opcode.html#NotFound) with the exceptions that the branch is always taken if any part of the search key input is NULL.
    NoConflict {
        cursor_id: CursorID,     // P1 index cursor
        target_pc: BranchOffset, // P2 jump target
        record_reg: usize,
        num_regs: usize,
    },

    NotExists {
        cursor: CursorID,
        rowid_reg: usize,
        target_pc: BranchOffset,
    },

    OffsetLimit {
        limit_reg: usize,
        combined_reg: usize,
        offset_reg: usize,
    },

    OpenWrite {
        cursor_id: CursorID,
        root_page: RegisterOrLiteral<PageIdx>,
        name: String,
    },

    Copy {
        src_reg: usize,
        dst_reg: usize,
        extra_amount: usize, // 0 extra_amount means we include src_reg, dst_reg..=dst_reg+amount = src_reg..=src_reg+amount
    },

    /// Allocate a new b-tree.
    CreateBtree {
        /// Allocate b-tree in main database if zero or in temp database if non-zero (P1).
        db: usize,
        /// The root page of the new b-tree (P2).
        root: usize,
        /// Flags (P3).
        flags: CreateBTreeFlags,
    },

    /// Deletes an entire database table or index whose root page in the database file is given by P1.
    Destroy {
        /// The root page of the table/index to destroy
        root: usize,
        /// Register to store the former value of any moved root page (for AUTOVACUUM)
        former_root_reg: usize,
        /// Whether this is a temporary table (1) or main database table (0)
        is_temp: usize,
    },

    ///  Drop a table
    DropTable {
        ///  The database within which this b-tree needs to be dropped (P1).
        db: usize,
        ///  unused register p2
        _p2: usize,
        ///  unused register p3
        _p3: usize,
        //  The name of the table being dropped
        table_name: String,
    },
    DropIndex {
        ///  The database within which this index needs to be dropped (P1).
        db: usize,
        //  The name of the index being dropped
        index: Arc<Index>,
    },

    /// Close a cursor.
    Close {
        cursor_id: CursorID,
    },

    /// Check if the register is null.
    IsNull {
        /// Source register (P1).
        reg: usize,

        /// Jump to this PC if the register is null (P2).
        target_pc: BranchOffset,
    },
    ParseSchema {
        db: usize,
        where_clause: Option<String>,
    },

    /// Place the result of lhs >> rhs in dest register.
    ShiftRight {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },

    /// Place the result of lhs << rhs in dest register.
    ShiftLeft {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },

    /// Get parameter variable.
    Variable {
        index: NonZero<usize>,
        dest: usize,
    },
    /// If either register is null put null else put 0
    ZeroOrNull {
        /// Source register (P1).
        rg1: usize,
        rg2: usize,
        dest: usize,
    },
    /// Interpret the value in reg as boolean and store its compliment in destination
    Not {
        reg: usize,
        dest: usize,
    },
    /// Concatenates the `rhs` and `lhs` values and stores the result in the third register.
    Concat {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Take the logical AND of the values in registers P1 and P2 and write the result into register P3.
    And {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Take the logical OR of the values in register P1 and P2 and store the answer in register P3.
    Or {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Do nothing. Continue downward to the next opcode.
    Noop,
    /// Write the current number of pages in database P1 to memory cell P2.
    PageCount {
        db: usize,
        dest: usize,
    },
    /// Read cookie number P3 from database P1 and write it into register P2
    ReadCookie {
        db: usize,
        dest: usize,
        cookie: Cookie,
    },
    /// Write the value in register P3 into cookie number P2 of database P1.
    /// If P2 is the SCHEMA_VERSION cookie (cookie number 1) then the internal schema version is set to P3-P5
    SetCookie {
        db: usize,
        cookie: Cookie,
        value: i32,
        p5: u16,
    },
    /// Open a new cursor P1 to a transient table.
    OpenEphemeral {
        cursor_id: usize,
        is_table: bool,
    },
    /// Works the same as OpenEphemeral, name just distinguishes its use; used for transient indexes in joins.
    OpenAutoindex {
        cursor_id: usize,
    },
    /// Fall through to the next instruction on the first invocation, otherwise jump to target_pc
    Once {
        target_pc_when_reentered: BranchOffset,
    },
    /// Search for a record in the index cursor.
    /// If any entry for which the key is a prefix exists, jump to target_pc.
    /// Otherwise, continue to the next instruction.
    Found {
        cursor_id: CursorID,
        target_pc: BranchOffset,
        record_reg: usize,
        num_regs: usize,
    },
    /// Search for record in the index cusor, if any entry for which the key is a prefix exists
    /// is a no-op, otherwise go to target_pc
    /// Example =>
    /// For a index key (1,2,3):
    /// NotFound((1,2,3)) => No-op
    /// NotFound((1,2)) => No-op
    /// NotFound((2,2, 1)) => Jump
    NotFound {
        cursor_id: CursorID,
        target_pc: BranchOffset,
        record_reg: usize,
        num_regs: usize,
    },
    /// Apply affinities to a range of registers. Affinities must have the same size of count
    Affinity {
        start_reg: usize,
        count: NonZeroUsize,
        affinities: String,
    },

    /// Store the number of entries (an integer value) in the table or index opened by cursor P1 in register P2.
    ///
    /// If P3==0, then an exact count is obtained, which involves visiting every btree page of the table.
    /// But if P3 is non-zero, an estimate is returned based on the current cursor position.
    Count {
        cursor_id: CursorID,
        target_reg: usize,
        exact: bool,
    },

    /// Do an analysis of the currently open database. Store in register (P1+1) the text of an error message describing any problems.
    /// If no problems are found, store a NULL in register (P1+1).
    /// The register (P1) contains one less than the maximum number of allowed errors.
    /// At most reg(P1) errors will be reported. In other words, the analysis stops as soon as reg(P1) errors are seen.
    /// Reg(P1) is updated with the number of errors remaining. The root page numbers of all tables in the database are integers
    /// stored in P4_INTARRAY argument. If P5 is not zero, the check is done on the auxiliary database file, not the main database file. This opcode is used to implement the integrity_check pragma.
    IntegrityCk {
        max_errors: usize,
        roots: Vec<usize>,
        message_register: usize,
    },
}

impl Insn {
    pub fn to_function(&self) -> InsnFunction {
        match self {
            Insn::Init { .. } => execute::op_init,
            Insn::Null { .. } => execute::op_null,
            Insn::BeginSubrtn { .. } => execute::op_null,
            Insn::NullRow { .. } => execute::op_null_row,
            Insn::Add { .. } => execute::op_add,
            Insn::Subtract { .. } => execute::op_subtract,
            Insn::Multiply { .. } => execute::op_multiply,
            Insn::Divide { .. } => execute::op_divide,
            Insn::DropIndex { .. } => execute::op_drop_index,
            Insn::Compare { .. } => execute::op_compare,
            Insn::BitAnd { .. } => execute::op_bit_and,
            Insn::BitOr { .. } => execute::op_bit_or,
            Insn::BitNot { .. } => execute::op_bit_not,
            Insn::Checkpoint { .. } => execute::op_checkpoint,
            Insn::Remainder { .. } => execute::op_remainder,
            Insn::Jump { .. } => execute::op_jump,
            Insn::Move { .. } => execute::op_move,
            Insn::IfPos { .. } => execute::op_if_pos,
            Insn::NotNull { .. } => execute::op_not_null,
            Insn::Eq { .. }
            | Insn::Ne { .. }
            | Insn::Lt { .. }
            | Insn::Le { .. }
            | Insn::Gt { .. }
            | Insn::Ge { .. } => execute::op_comparison,
            Insn::If { .. } => execute::op_if,
            Insn::IfNot { .. } => execute::op_if_not,
            Insn::OpenRead { .. } => execute::op_open_read,
            Insn::VOpen { .. } => execute::op_vopen,
            Insn::VCreate { .. } => execute::op_vcreate,
            Insn::VFilter { .. } => execute::op_vfilter,
            Insn::VColumn { .. } => execute::op_vcolumn,
            Insn::VUpdate { .. } => execute::op_vupdate,
            Insn::VNext { .. } => execute::op_vnext,
            Insn::VDestroy { .. } => execute::op_vdestroy,

            Insn::OpenPseudo { .. } => execute::op_open_pseudo,
            Insn::Rewind { .. } => execute::op_rewind,
            Insn::Last { .. } => execute::op_last,
            Insn::Column { .. } => execute::op_column,
            Insn::TypeCheck { .. } => execute::op_type_check,
            Insn::MakeRecord { .. } => execute::op_make_record,
            Insn::ResultRow { .. } => execute::op_result_row,
            Insn::Next { .. } => execute::op_next,
            Insn::Prev { .. } => execute::op_prev,
            Insn::Halt { .. } => execute::op_halt,
            Insn::HaltIfNull { .. } => execute::op_halt_if_null,
            Insn::Transaction { .. } => execute::op_transaction,
            Insn::AutoCommit { .. } => execute::op_auto_commit,
            Insn::Goto { .. } => execute::op_goto,
            Insn::Gosub { .. } => execute::op_gosub,
            Insn::Return { .. } => execute::op_return,
            Insn::Integer { .. } => execute::op_integer,
            Insn::Real { .. } => execute::op_real,
            Insn::RealAffinity { .. } => execute::op_real_affinity,
            Insn::String8 { .. } => execute::op_string8,
            Insn::Blob { .. } => execute::op_blob,
            Insn::RowData { .. } => execute::op_row_data,
            Insn::RowId { .. } => execute::op_row_id,
            Insn::IdxRowId { .. } => execute::op_idx_row_id,
            Insn::SeekRowid { .. } => execute::op_seek_rowid,
            Insn::DeferredSeek { .. } => execute::op_deferred_seek,
            Insn::SeekGE { .. }
            | Insn::SeekGT { .. }
            | Insn::SeekLE { .. }
            | Insn::SeekLT { .. } => execute::op_seek,
            Insn::SeekEnd { .. } => execute::op_seek_end,
            Insn::IdxGE { .. } => execute::op_idx_ge,
            Insn::IdxGT { .. } => execute::op_idx_gt,
            Insn::IdxLE { .. } => execute::op_idx_le,
            Insn::IdxLT { .. } => execute::op_idx_lt,
            Insn::DecrJumpZero { .. } => execute::op_decr_jump_zero,
            Insn::AggStep { .. } => execute::op_agg_step,
            Insn::AggFinal { .. } => execute::op_agg_final,
            Insn::SorterOpen { .. } => execute::op_sorter_open,
            Insn::SorterInsert { .. } => execute::op_sorter_insert,
            Insn::SorterSort { .. } => execute::op_sorter_sort,
            Insn::SorterData { .. } => execute::op_sorter_data,
            Insn::SorterNext { .. } => execute::op_sorter_next,
            Insn::Function { .. } => execute::op_function,
            Insn::InitCoroutine { .. } => execute::op_init_coroutine,
            Insn::EndCoroutine { .. } => execute::op_end_coroutine,
            Insn::Yield { .. } => execute::op_yield,
            Insn::Insert { .. } => execute::op_insert,
            Insn::Int64 { .. } => execute::op_int_64,
            Insn::IdxInsert { .. } => execute::op_idx_insert,
            Insn::Delete { .. } => execute::op_delete,
            Insn::NewRowid { .. } => execute::op_new_rowid,
            Insn::MustBeInt { .. } => execute::op_must_be_int,
            Insn::SoftNull { .. } => execute::op_soft_null,
            Insn::NoConflict { .. } => execute::op_no_conflict,
            Insn::NotExists { .. } => execute::op_not_exists,
            Insn::OffsetLimit { .. } => execute::op_offset_limit,
            Insn::OpenWrite { .. } => execute::op_open_write,
            Insn::Copy { .. } => execute::op_copy,
            Insn::CreateBtree { .. } => execute::op_create_btree,
            Insn::Destroy { .. } => execute::op_destroy,

            Insn::DropTable { .. } => execute::op_drop_table,
            Insn::Close { .. } => execute::op_close,
            Insn::IsNull { .. } => execute::op_is_null,
            Insn::ParseSchema { .. } => execute::op_parse_schema,
            Insn::ShiftRight { .. } => execute::op_shift_right,
            Insn::ShiftLeft { .. } => execute::op_shift_left,
            Insn::Variable { .. } => execute::op_variable,
            Insn::ZeroOrNull { .. } => execute::op_zero_or_null,
            Insn::Not { .. } => execute::op_not,
            Insn::Concat { .. } => execute::op_concat,
            Insn::And { .. } => execute::op_and,
            Insn::Or { .. } => execute::op_or,
            Insn::Noop => execute::op_noop,
            Insn::PageCount { .. } => execute::op_page_count,
            Insn::ReadCookie { .. } => execute::op_read_cookie,
            Insn::SetCookie { .. } => execute::op_set_cookie,
            Insn::OpenEphemeral { .. } | Insn::OpenAutoindex { .. } => execute::op_open_ephemeral,
            Insn::Once { .. } => execute::op_once,
            Insn::Found { .. } | Insn::NotFound { .. } => execute::op_found,
            Insn::Affinity { .. } => execute::op_affinity,
            Insn::IdxDelete { .. } => execute::op_idx_delete,
            Insn::Count { .. } => execute::op_count,
            Insn::IntegrityCk { .. } => execute::op_integrity_check,
        }
    }
}

// TODO: Add remaining cookies.
#[derive(Description, Debug, Clone, Copy)]
pub enum Cookie {
    /// The schema cookie.
    SchemaVersion = 1,
    /// The schema format number. Supported schema formats are 1, 2, 3, and 4.
    DatabaseFormat = 2,
    /// Default page cache size.
    DefaultPageCacheSize = 3,
    /// The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
    LargestRootPageNumber = 4,
    /// The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
    DatabaseTextEncoding = 5,
    /// The "user version" as read and set by the user_version pragma.
    UserVersion = 6,
    /// The auto-vacuum mode setting.
    IncrementalVacuum = 7,
}
