//! The virtual database engine (VDBE).
//!
//! The VDBE is a register-based virtual machine that execute bytecode
//! instructions that represent SQL statements. When an application prepares
//! an SQL statement, the statement is compiled into a sequence of bytecode
//! instructions that perform the needed operations, such as reading or
//! writing to a b-tree, sorting, or aggregating data.
//!
//! The instruction set of the VDBE is similar to SQLite's instruction set,
//! but with the exception that bytecodes that perform I/O operations are
//! return execution back to the caller instead of blocking. This is because
//! Limbo is designed for applications that need high concurrency such as
//! serverless runtimes. In addition, asynchronous I/O makes storage
//! disaggregation easier.
//!
//! You can find a full list of SQLite opcodes at:
//!
//! https://www.sqlite.org/opcode.html

pub mod builder;
pub mod explain;
pub mod sorter;

mod datetime;

use crate::error::{LimboError, SQLITE_CONSTRAINT_PRIMARYKEY};
use crate::function::{AggFunc, FuncCtx, MathFunc, MathFuncArity, ScalarFunc};
use crate::pseudo::PseudoCursor;
use crate::schema::Table;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::storage::{btree::BTreeCursor, pager::Pager};
use crate::types::{
    AggContext, Cursor, CursorResult, OwnedRecord, OwnedValue, Record, SeekKey, SeekOp,
};
use crate::util::parse_schema_rows;
#[cfg(feature = "json")]
use crate::{function::JsonFunc, json::get_json, json::json_array};
use crate::{Connection, Result, TransactionState};
use crate::{Rows, DATABASE_VERSION};

use limbo_macros::Explain;

use datetime::{exec_date, exec_time, exec_unixepoch};

use rand::distributions::{Distribution, Uniform};
use rand::{thread_rng, Rng};
use regex::Regex;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;
use std::rc::{Rc, Weak};
pub type BranchOffset = i64;
pub type CursorID = usize;

pub type PageIdx = usize;

#[allow(dead_code)]
#[derive(Debug)]
pub enum Func {
    Scalar(ScalarFunc),
    #[cfg(feature = "json")]
    Json(JsonFunc),
}

impl Display for Func {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Func::Scalar(scalar_func) => scalar_func.to_string(),
            #[cfg(feature = "json")]
            Func::Json(json_func) => json_func.to_string(),
        };
        write!(f, "{}", str)
    }
}

#[derive(Debug, Explain)]
pub enum Insn {
    #[desc = "Add the value in register P1 to the value in register P2 and store the result in register P3. If either input is NULL, the result is NULL."]
    Add {
        #[p1]
        lhs: usize,
        #[p2]
        rhs: usize,
        #[p3]
        dest: usize,
    },

    #[desc = "Execute the xStep function for an aggregate. The function has P5 arguments. P4 is a pointer to the FuncDef structure that specifies the function. Register P3 is the accumulator. The P5 arguments are taken from register P2 and its successors."]
    AggStep {
        #[p2]
        col: usize,
        #[p3]
        acc_reg: usize,
        delimiter: usize,
        func: AggFunc,
    },

    #[desc = "P1 is the memory location that is the accumulator for an aggregate or window function. Execute the finalizer function for an aggregate and store the result in P1. P2 is the number of arguments that the step function takes and P4 is a pointer to the FuncDef for this function. The P2 argument is not used by this opcode. It is only there to disambiguate functions that can take varying numbers of arguments. The P4 argument is only needed for the case where the step function was not previously called."]
    AggFinal {
        #[p2]
        register: usize,
        func: AggFunc,
    },

    #[desc = "Take the bit-wise AND of the values in register P1 and P2 and store the result in register P3. If either input is NULL, the result is NULL."]
    BitAnd {
        #[p1]
        lhs: usize,
        #[p2]
        rhs: usize,
        #[p3]
        dest: usize,
    },

    #[desc = "Interpret the content of register P1 as an integer. Store the ones-complement of the P1 value into register P2. If P1 holds a NULL then store a NULL in P2."]
    BitNot {
        #[p1]
        reg: usize,
        #[p2]
        dest: usize,
    },

    #[desc = "Take the bit-wise OR of the values in register P1 and P2 and store the result in register P3. If either input is NULL, the result is NULL."]
    BitOr {
        #[p1]
        lhs: usize,
        #[p2]
        rhs: usize,
        #[p3]
        dest: usize,
    },

    #[desc = "P4 points to a blob of data P1 bytes long. Store this blob in register P2. If P4 is a NULL pointer, then construct a zero-filled blob that is P1 bytes long in P2."]
    Blob {
        value: Vec<u8>,
        #[p2]
        dest: usize,
    },

    #[desc = "Close a cursor previously opened as P1. If P1 is not currently open, this instruction is a no-op."]
    Close {
        #[p1]
        cursor_id: CursorID,
    },

    #[desc = "Interpret the data that cursor P1 points to as a structure built using the MakeRecord instruction. (See the MakeRecord opcode for additional information about the format of the data.) Extract the P2-th column from this record. If there are less than (P2+1) values in the record, extract a NULL. The value extracted is stored in register P3. If the record contains fewer than P2 fields, then extract a NULL. Or, if the P4 argument is a P4_MEM use the value of the P4 argument as the result. If the OPFLAG_LENGTHARG bit is set in P5 then the result is guaranteed to only be used by the length() function or the equivalent. The content of large blobs is not loaded, thus saving CPU cycles. If the OPFLAG_TYPEOFARG bit is set then the result will only be used by the typeof() function or the IS NULL or IS NOT NULL operators or the equivalent. In this case, all content loading can be omitted."]
    Column {
        #[p1]
        cursor_id: CursorID,
        #[p2]
        column: usize,
        #[p3]
        dest: usize,
    },

    #[desc = "Compare two vectors of registers in reg(P1)..reg(P1+P3-1) (call this vector \"A\") and in reg(P2)..reg(P2+P3-1) (\"B\"). Save the result of the comparison for use by the next Jump instruct. If P5 has the OPFLAG_PERMUTE bit set, then the order of comparison is determined by the most recent Permutation operator. If the OPFLAG_PERMUTE bit is clear, then register are compared in sequential order. P4 is a KeyInfo structure that defines collating sequences and sort orders for the comparison. The permutation applies to registers only. The KeyInfo elements are used sequentially. The comparison is a sort comparison, so NULLs compare equal, NULLs are less than numbers, numbers are less than strings, and strings are less than blobs. This opcode must be immediately followed by an Jump opcode."]
    Compare {
        #[p1]
        start_reg_a: usize,
        #[p2]
        start_reg_b: usize,
        #[p3]
        count: usize,
    },

    #[desc = "Make a copy of registers P1..P1+P3 into registers P2..P2+P3. If the 0x0002 bit of P5 is set then also clear the MEM_Subtype flag in the destination. The 0x0001 bit of P5 indicates that this Copy opcode cannot be merged. The 0x0001 bit is used by the query planner and does not come into play during query execution. This instruction makes a deep copy of the value. A duplicate is made of any string or blob constant. See also SCopy."]
    Copy {
        #[p1]
        src_reg: usize,
        #[p2]
        dst_reg: usize,
        #[p3]
        amount: usize,
    },

    #[desc = "Allocate a new b-tree in the main database file if P1==0 or in the TEMP database file if P1==1 or in an attached database if P1>1. The P3 argument must be 1 (BTREE_INTKEY) for a rowid table it must be 2 (BTREE_BLOBKEY) for an index or WITHOUT ROWID table. The root page number of the new b-tree is stored in register P2."]
    CreateBtree {
        #[p1]
        db: usize,
        #[p2]
        root: usize,
        #[p3]
        flags: usize,
    },

    #[desc = "Register P1 must hold an integer. Decrement the value in P1 and jump to P2 if the new value is exactly zero."]
    DecrJumpZero {
        #[p1]
        reg: usize,
        #[p2]
        target_pc: BranchOffset,
    },

    #[desc = "P1 is an open index cursor and P3 is a cursor on the corresponding table. This opcode does a deferred seek of the P3 table cursor to the row that corresponds to the current row of P1. This is a deferred seek. Nothing actually happens until the cursor is used to read a record. That way, if no reads occur, no unnecessary I/O happens. P4 may be an array of integers (type P4_INTARRAY) containing one entry for each column in the P3 table. If array entry a(i) is non-zero, then reading column a(i)-1 from cursor P3 is equivalent to performing the deferred seek and then reading column i from P1. This information is stored in P3 and used to redirect reads against P3 over to P1, thus possibly avoiding the need to seek and read cursor P3."]
    DeferredSeek {
        #[p1]
        index_cursor_id: CursorID,
        #[p2]
        table_cursor_id: CursorID,
    },

    #[desc = "Divide the value in register P1 by the value in register P2 and store the result in register P3 (P3=P2/P1). If the value in register P1 is zero, then the result is NULL. If either input is NULL, the result is NULL."]
    Divide {
        #[p1]
        lhs: usize,
        #[p2]
        rhs: usize,
        #[p3]
        dest: usize,
    },

    #[desc = "The instruction at the address in register P1 is a Yield. Jump to the P2 parameter of that Yield. After the jump, the value register P1 is left with a value such that subsequent OP_Yields go back to the this same EndCoroutine instruction. See also: InitCoroutine"]
    EndCoroutine {
        #[p1]
        yield_reg: usize,
    },

    #[desc = "Compare the values in register P1 and P3. If reg(P3)==reg(P1) then jump to address P2. The SQLITE_AFF_MASK portion of P5 must be an affinity character - SQLITE_AFF_TEXT, SQLITE_AFF_INTEGER, and so forth. An attempt is made to coerce both inputs according to this affinity before the comparison is made. If the SQLITE_AFF_MASK is 0x00, then numeric affinity is used. Note that the affinity conversions are stored back into the input registers P1 and P3. So this opcode can cause persistent changes to registers P1 and P3. Once any conversions have taken place, and neither value is NULL, the values are compared. If both values are blobs then memcmp() is used to determine the results of the comparison. If both values are text, then the appropriate collating function specified in P4 is used to do the comparison. If P4 is not specified then memcmp() is used to compare text string. If both values are numeric, then a numeric comparison is used. If the two values are of different types, then numbers are considered less than strings and strings are considered less than blobs. If SQLITE_NULLEQ is set in P5 then the result of comparison is always either true or false and is never NULL. If both operands are NULL then the result of comparison is true. If either operand is NULL then the result is false. If neither operand is NULL the result is the same as it would be if the SQLITE_NULLEQ flag were omitted from P5. This opcode saves the result of comparison for use by the new Jump opcode."]
    Eq {
        #[p1]
        lhs: usize,
        #[p2]
        rhs: usize,
        #[p3]
        target_pc: BranchOffset,
    },

    #[desc = "Invoke a user function (P4 is a pointer to an sqlite3_context object that contains a pointer to the function to be run) with arguments taken from register P2 and successors. The number of arguments is in the sqlite3_context object that P4 points to. The result of the function is stored in register P3. Register P3 must not be one of the function inputs. P1 is a 32-bit bitmask indicating whether or not each argument to the function was determined to be constant at compile time. If the first argument was constant then bit 0 of P1 is set. This is used to determine whether meta data associated with a user function argument using the sqlite3_set_auxdata() API may be safely retained until the next invocation of this opcode. See also: AggStep, AggFinal, PureFunc"]
    Function {
        #[p1]
        constant_mask: i32,
        #[p2]
        start_reg: usize,
        #[p3]
        dest: usize,
        func: FuncCtx,
    },

    #[desc = "This works just like the Lt opcode except that the jump is taken if the content of register P3 is greater than or equal to the content of register P1. See the Lt opcode for additional information."]
    Ge {
        #[p1]
        lhs: usize,
        #[p2]
        rhs: usize,
        #[p3]
        target_pc: BranchOffset,
    },

    #[desc = "This works just like the Lt opcode except that the jump is taken if the content of register P3 is greater than the content of register P1. See the Lt opcode for additional information."]
    Gt {
        #[p1]
        lhs: usize,
        #[p2]
        rhs: usize,
        #[p3]
        target_pc: BranchOffset,
    },

    #[desc = "Write the current address onto register P1 and then jump to address P2."]
    Gosub {
        #[p1]
        target_pc: BranchOffset,
        #[p2]
        return_reg: usize,
    },

    #[desc = "An unconditional jump to address P2. The next instruction executed will be the one at index P2 from the beginning of the program. The P1 parameter is not actually used by this opcode. However, it is sometimes set to 1 instead of 0 as a hint to the command-line shell that this Goto is the bottom of a loop and that the lines from P2 down to the current line should be indented for EXPLAIN output."]
    Goto {
        #[p2]
        target_pc: BranchOffset,
    },

    #[desc = "Exit immediately. All open cursors, etc are closed automatically. P1 is the result code returned by sqlite3_exec(), sqlite3_reset(), or sqlite3_finalize(). For a normal halt, this should be SQLITE_OK (0). For errors, it can be some other value. If P1!=0 then P2 will determine whether or not to rollback the current transaction. Do not rollback if P2==OE_Fail. Do the rollback if P2==OE_Rollback. If P2==OE_Abort, then back out all changes that have occurred during this execution of the VDBE, but do not rollback the transaction. If P4 is not null then it is an error message string. P5 is a value between 0 and 4, inclusive, that modifies the P4 string. 0: (no change) 1: NOT NULL constraint failed: P4 2: UNIQUE constraint failed: P4 3: CHECK constraint failed: P4 4: FOREIGN KEY constraint failed: P4 If P5 is not zero and P4 is NULL, then everything after the \":\" is omitted. There is an implied \"Halt 0 0 0\" instruction inserted at the very end of every program. So a jump past the last instruction of the program is the same as executing Halt."]
    Halt {
        #[p1]
        err_code: usize,
        description: String,
    },

    #[desc = "The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end. If the P1 index entry is greater than or equal to the key value then jump to P2. Otherwise fall through to the next instruction."]
    IdxGE {
        #[p1]
        cursor_id: CursorID,
        #[p2]
        target_pc: BranchOffset,
        #[p3]
        start_reg: usize,
        num_regs: usize,
    },

    #[desc = "The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end. If the P1 index entry is greater than the key value then jump to P2. Otherwise fall through to the next instruction."]
    IdxGT {
        #[p1]
        cursor_id: CursorID,
        #[p2]
        target_pc: BranchOffset,
        #[p3]
        start_reg: usize,
        num_regs: usize,
    },

    #[desc = "Jump to P2 if the value in register P1 is true. The value is considered true if it is numeric and non-zero. If the value in P1 is NULL then take the jump if and only if P3 is non-zero."]
    If {
        #[p1]
        reg: usize,
        #[p2]
        target_pc: BranchOffset,
        #[p3]
        null_reg: usize,
    },

    #[desc = "Jump to P2 if the value in register P1 is False. The value is considered false if it has a numeric value of zero. If the value in P1 is NULL then take the jump if and only if P3 is non-zero."]
    IfNot {
        #[p1]
        reg: usize,
        #[p2]
        target_pc: BranchOffset,
        #[p3]
        null_reg: usize,
    },

    #[desc = "Register P1 must contain an integer. If the value of register P1 is 1 or greater, subtract P3 from the value in P1 and jump to P2. If the initial value of register P1 is less than 1, then the value is unchanged and control passes through to the next instruction."]
    IfPos {
        #[p1]
        reg: usize,
        #[p2]
        target_pc: BranchOffset,
        decrement_by: usize,
    },

    #[desc = "Programs contain a single instance of this opcode as the very first opcode. If tracing is enabled (by the sqlite3_trace()) interface, then the UTF-8 string contained in P4 is emitted on the trace callback. Or if P4 is blank, use the string returned by sqlite3_sql(). If P2 is not zero, jump to instruction P2. Increment the value of P1 so that Once opcodes will jump the first time they are evaluated for this run. If P3 is not zero, then it is an address to jump to if an SQLITE_CORRUPT error is encountered."]
    Init {
        #[p2]
        target_pc: BranchOffset,
    },

    #[desc = "Set up register P1 so that it will Yield to the coroutine located at address P3. If P2!=0 then the coroutine implementation immediately follows this opcode. So jump over the coroutine implementation to address P2. See also: EndCoroutine"]
    InitCoroutine {
        #[p1]
        yield_reg: usize,
        #[p2]
        jump_on_definition: BranchOffset,
        #[p3]
        start_offset: BranchOffset,
    },

    #[desc = "The 32-bit integer value P1 is written into register P2."]
    Integer {
        #[p1]
        value: i64,
        #[p2]
        dest: usize,
    },

    InsertAsync {
        #[p1]
        cursor: CursorID,
        #[p2]
        record_reg: usize,
        #[p3]
        key_reg: usize,
        flag: usize,
    },

    InsertAwait {
        #[p1]
        cursor_id: usize,
    },

    #[desc = "Jump to P2 if the value in register P1 is NULL."]
    IsNull {
        #[p1]
        src: usize,
        #[p2]
        target_pc: BranchOffset,
    },

    #[desc = "Jump to the instruction at address P1, P2, or P3 depending on whether in the most recent Compare instruction the P1 vector was less than, equal to, or greater than the P2 vector, respectively. This opcode must immediately follow an Compare opcode."]
    Jump {
        #[p1]
        target_pc_lt: BranchOffset,
        #[p2]
        target_pc_eq: BranchOffset,
        #[p3]
        target_pc_gt: BranchOffset,
    },

    #[desc = "TODO"]
    LastAsync { cursor_id: CursorID },

    #[desc = "TODO"]
    LastAwait {
        cursor_id: CursorID,
        pc_if_empty: BranchOffset,
    },

    #[desc = "This works just like the Lt opcode except that the jump is taken if the content of register P3 is less than or equal to the content of register P1. See the Lt opcode for additional information."]
    Le {
        #[p1]
        lhs: usize,
        #[p2]
        rhs: usize,
        #[p3]
        target_pc: BranchOffset,
    },

    #[desc = "Compare the values in register P1 and P3. If reg(P3)<reg(P1) then jump to address P2. If the SQLITE_JUMPIFNULL bit of P5 is set and either reg(P1) or reg(P3) is NULL then the take the jump. If the SQLITE_JUMPIFNULL bit is clear then fall through if either operand is NULL. The SQLITE_AFF_MASK portion of P5 must be an affinity character - SQLITE_AFF_TEXT, SQLITE_AFF_INTEGER, and so forth. An attempt is made to coerce both inputs according to this affinity before the comparison is made. If the SQLITE_AFF_MASK is 0x00, then numeric affinity is used. Note that the affinity conversions are stored back into the input registers P1 and P3. So this opcode can cause persistent changes to registers P1 and P3. Once any conversions have taken place, and neither value is NULL, the values are compared. If both values are blobs then memcmp() is used to determine the results of the comparison. If both values are text, then the appropriate collating function specified in P4 is used to do the comparison. If P4 is not specified then memcmp() is used to compare text string. If both values are numeric, then a numeric comparison is used. If the two values are of different types, then numbers are considered less than strings and strings are considered less than blobs. This opcode saves the result of comparison for use by the new Jump opcode."]
    Lt {
        #[p1]
        lhs: usize,
        #[p2]
        rhs: usize,
        #[p3]
        target_pc: BranchOffset,
    },

    #[desc = "Convert P2 registers beginning with P1 into the record format use as a data record in a database table or as a key in an index. The Column opcode can decode the record later. P4 may be a string that is P2 characters long. The N-th character of the string indicates the column affinity that should be used for the N-th field of the index key. The mapping from character to affinity is given by the SQLITE_AFF_ macros defined in sqliteInt.h. If P4 is NULL then all index fields have the affinity BLOB. The meaning of P5 depends on whether or not the SQLITE_ENABLE_NULL_TRIM compile-time option is enabled: * If SQLITE_ENABLE_NULL_TRIM is enabled, then the P5 is the index of the right-most table that can be null-trimmed. * If SQLITE_ENABLE_NULL_TRIM is omitted, then P5 has the value OPFLAG_NOCHNG_MAGIC if the MakeRecord opcode is allowed to accept no-change records with serial_type 10. This value is only used inside an assert() and does not affect the end result."]
    MakeRecord {
        #[p1]
        start_reg: usize,
        #[p2]
        count: usize,
        #[p3]
        dest_reg: usize,
    },

    #[desc = "Move the P3 values in register P1..P1+P3-1 over into registers P2..P2+P3-1. Registers P1..P1+P3-1 are left holding a NULL. It is an error for register ranges P1..P1+P3-1 and P2..P2+P3-1 to overlap. It is an error for P3 to be less than 1."]
    Move {
        #[p1]
        source_reg: usize,
        #[p2]
        dest_reg: usize,
        #[p3]
        count: usize,
    },

    #[desc = "Multiply the value in register P1 by the value in register P2 and store the result in register P3. If either input is NULL, the result is NULL."]
    Multiply {
        #[p1]
        lhs: usize,
        #[p2]
        rhs: usize,
        #[p3]
        dest: usize,
    },

    #[desc = "Force the value in register P1 to be an integer. If the value in P1 is not an integer and cannot be converted into an integer without data loss, then jump immediately to P2, or if P2==0 raise an SQLITE_MISMATCH exception."]
    MustBeInt {
        #[p1]
        reg: usize,
    },

    #[desc = "This works just like the Eq opcode except that the jump is taken if the operands in registers P1 and P3 are not equal. See the Eq opcode for additional information."]
    Ne {
        #[p1]
        lhs: usize,
        #[p2]
        rhs: usize,
        #[p3]
        target_pc: BranchOffset,
    },

    #[desc = "Get a new integer record number (a.k.a \"rowid\") used as the key to a table. The record number is not previously used as a key in the database table that cursor P1 points to. The new record number is written written to register P2. If P3>0 then P3 is a register in the root frame of this VDBE that holds the largest previously generated record number. No new record numbers are allowed to be less than this value. When this value reaches its maximum, an SQLITE_FULL error is generated. The P3 register is updated with the ' generated record number. This P3 mechanism is used to help implement the AUTOINCREMENT feature."]
    NewRowid {
        #[p1]
        cursor: CursorID,
        #[p2]
        rowid_reg: usize,
        #[p3]
        prev_largest_reg: usize,
    },

    #[desc = "TODO"]
    NextAsync {
        #[p1]
        cursor_id: CursorID,
    },

    #[desc = "TODO"]
    NextAwait {
        #[p1]
        cursor_id: CursorID,
        #[p2]
        pc_if_next: BranchOffset,
    },

    #[desc = "P1 is the index of a cursor open on an SQL table btree (with integer keys). P3 is an integer rowid. If P1 does not contain a record with rowid P3 then jump immediately to P2. Or, if P2 is 0, raise an SQLITE_CORRUPT error. If P1 does contain a record with rowid P3 then leave the cursor pointing at that record and fall through to the next instruction. The SeekRowid opcode performs the same operation but also allows the P3 register to contain a non-integer value, in which case the jump is always taken. This opcode requires that P3 always contain an integer. The NotFound opcode performs the same operation on index btrees (with arbitrary multi-value keys). This opcode leaves the cursor in a state where it cannot be advanced in either direction. In other words, the Next and Prev opcodes will not work following this opcode. See also: Found, NotFound, NoConflict, SeekRowid"]
    NotExists {
        #[p1]
        cursor: CursorID,
        #[p2]
        target_pc: BranchOffset,
        #[p3]
        rowid_reg: usize,
    },

    #[desc = "Jump to P2 if the value in register P1 is not NULL."]
    NotNull {
        #[p1]
        reg: usize,
        #[p2]
        target_pc: BranchOffset,
    },

    #[desc = "Write a NULL into registers P2. If P3 greater than P2, then also write NULL into register P3 and every register in between P2 and P3. If P3 is less than P2 (typically P3 is zero) then only register P2 is set to NULL. If the P1 value is non-zero, then also set the MEM_Cleared flag so that NULL values will not compare equal even if SQLITE_NULLEQ is set on Ne or Eq."]
    Null {
        #[p2]
        dest: usize,
        // TODO: Needs handler
        dest_end: Option<usize>,
    },

    #[desc = "Move the cursor P1 to a null row. Any Column operations that occur while the cursor is on the null row will always write a NULL. If cursor P1 is not previously opened, open it now to a special pseudo-cursor that always returns NULL for every column."]
    NullRow {
        #[p1]
        cursor_id: CursorID,
    },

    #[desc = "Open a new cursor that points to a fake table that contains a single row of data. The content of that one row is the content of memory register P2. In other words, cursor P1 becomes an alias for the MEM_Blob content contained in register P2. A pseudo-table created by this opcode is used to hold a single row output from the sorter so that the row can be decomposed into individual columns using the Column opcode. The Column opcode is the only cursor opcode that works with a pseudo-table. P3 is the number of fields in the records that will be stored by the pseudo-table. If P2 is 0 or negative then the pseudo-cursor will return NULL for every column."]
    OpenPseudo {
        #[p1]
        cursor_id: CursorID,
        #[p2]
        content_reg: usize,
        #[p3]
        num_fields: usize,
    },

    #[desc = "TODO"]
    OpenReadAsync {
        #[p1]
        cursor_id: CursorID,
        #[p2]
        root_page: PageIdx,
    },

    #[desc = "TODO"]
    OpenReadAwait,

    #[desc = "TODO"]
    OpenWriteAsync {
        #[p1]
        cursor_id: CursorID,
        #[p2]
        root_page: PageIdx,
    },

    #[desc = "TODO"]
    OpenWriteAwait {},

    #[desc = "TODO"]
    PrevAsync { cursor_id: CursorID },

    #[desc = "TODO"]
    PrevAwait {
        cursor_id: CursorID,
        pc_if_next: BranchOffset,
    },

    #[desc = "Read and parse all entries from the schema table of database P1 that match the WHERE clause P4. If P4 is a NULL pointer, then the entire schema for P1 is reparsed. This opcode invokes the parser to create a new virtual machine, then runs the new virtual machine. It is thus a re-entrant opcode."]
    ParseSchema {
        #[p1]
        db: usize,
        where_clause: String,
    },

    #[desc = "P4 is a pointer to a 64-bit floating point value. Write that value into register P2."]
    Real {
        value: f64,
        #[p2]
        dest: usize,
    },

    #[desc = "If register P1 holds an integer convert it to a real value. This opcode is used when extracting information from a column that has REAL affinity. Such column values may still be stored as integers, for space efficiency, but after extraction we want them to have only a real value."]
    RealAffinity {
        #[p1]
        register: usize,
    },

    #[desc = "The registers P1 through P1+P2-1 contain a single row of results. This opcode causes the sqlite3_step() call to terminate with an SQLITE_ROW return code and it sets up the sqlite3_stmt structure to provide access to the r(P1)..r(P1+P2-1) values as the result row."]
    ResultRow {
        #[p1]
        start_reg: usize,
        #[p2]
        count: usize,
    },

    #[desc = "Jump to the address stored in register P1. If P1 is a return address register, then this accomplishes a return from a subroutine. If P3 is 1, then the jump is only taken if register P1 holds an integer values, otherwise execution falls through to the next opcode, and the Return becomes a no-op. If P3 is 0, then register P1 must hold an integer or else an assert() is raised. P3 should be set to 1 when this opcode is used in combination with BeginSubrtn, and set to 0 otherwise. The value in register P1 is unchanged by this opcode. P2 is not used by the byte-code engine. However, if P2 is positive and also less than the current address, then the \"EXPLAIN\" output formatter in the CLI will indent all opcodes from the P2 opcode up to be not including the current Return. P2 should be the first opcode in the subroutine from which this opcode is returning. Thus the P2 value is a byte-code indentation hint. See tag-20220407a in wherecode.c and shell.c."]
    Return {
        #[p1]
        return_reg: usize,
    },

    #[desc = ""]
    RewindAsync {
        #[p1]
        cursor_id: CursorID,
    },

    #[desc = ""]
    RewindAwait {
        #[p1]
        cursor_id: CursorID,
        #[p2]
        pc_if_empty: BranchOffset,
    },

    #[desc = "Store in register P2 an integer which is the key of the table entry that P1 is currently point to. P1 can be either an ordinary table or a virtual table. There used to be a separate OP_VRowid opcode for use with virtual tables, but this one opcode now works for both table types."]
    RowId {
        #[p1]
        cursor_id: CursorID,
        #[p2]
        dest: usize,
    },

    #[desc = "If cursor P1 refers to an SQL table (B-Tree that uses integer keys), use the value in register P3 as the key. If cursor P1 refers to an SQL index, then P3 is the first in an array of P4 registers that are used as an unpacked index key. Reposition cursor P1 so that it points to the smallest entry that is greater than or equal to the key value. If there are no records greater than or equal to the key and P2 is not zero, then jump to P2. If the cursor P1 was opened using the OPFLAG_SEEKEQ flag, then this opcode will either land on a record that exactly matches the key, or else it will cause a jump to P2. When the cursor is OPFLAG_SEEKEQ, this opcode must be followed by an IdxLE opcode with the same arguments. The IdxGT opcode will be skipped if this opcode succeeds, but the IdxGT opcode will be used on subsequent loop iterations. The OPFLAG_SEEKEQ flags is a hint to the btree layer to say that this is an equality search. This opcode leaves the cursor configured to move in forward order, from the beginning toward the end. In other words, the cursor is configured to use Next, not Prev. See also: Found, NotFound, SeekLt, SeekGt, SeekLe"]
    SeekGE {
        #[p1]
        cursor_id: CursorID,
        #[p2]
        target_pc: BranchOffset,
        #[p3]
        start_reg: usize,
        num_regs: usize,
        is_index: bool,
    },

    #[desc = "If cursor P1 refers to an SQL table (B-Tree that uses integer keys), use the value in register P3 as a key. If cursor P1 refers to an SQL index, then P3 is the first in an array of P4 registers that are used as an unpacked index key. Reposition cursor P1 so that it points to the smallest entry that is greater than the key value. If there are no records greater than the key and P2 is not zero, then jump to P2. This opcode leaves the cursor configured to move in forward order, from the beginning toward the end. In other words, the cursor is configured to use Next, not Prev. See also: Found, NotFound, SeekLt, SeekGe, SeekLe"]
    SeekGT {
        #[p1]
        cursor_id: CursorID,
        #[p2]
        target_pc: BranchOffset,
        #[p3]
        start_reg: usize,
        num_regs: usize,
        is_index: bool,
    },

    #[desc = "P1 is the index of a cursor open on an SQL table btree (with integer keys). If register P3 does not contain an integer or if P1 does not contain a record with rowid P3 then jump immediately to P2. Or, if P2 is 0, raise an SQLITE_CORRUPT error. If P1 does contain a record with rowid P3 then leave the cursor pointing at that record and fall through to the next instruction. The NotExists opcode performs the same operation, but with NotExists the P3 register must be guaranteed to contain an integer value. With this opcode, register P3 might not contain an integer. The NotFound opcode performs the same operation on index btrees (with arbitrary multi-value keys). This opcode leaves the cursor in a state where it cannot be advanced in either direction. In other words, the Next and Prev opcodes will not work following this opcode. See also: Found, NotFound, NoConflict, SeekRowid"]
    SeekRowid {
        #[p1]
        cursor_id: CursorID,
        #[p2]
        src_reg: usize,
        #[p3]
        target_pc: BranchOffset,
    },

    #[desc = "Set register P1 to have the value NULL as seen by the MakeRecord instruction, but do not free any string or blob memory associated with the register, so that if the value was a string or blob that was previously copied using SCopy, the copies will continue to be valid."]
    SoftNull {
        #[p1]
        reg: usize,
    },

    #[desc = "Write into register P2 the current sorter data for sorter cursor P1. Then clear the column header cache on cursor P3. This opcode is normally used to move a record out of the sorter and into a register that is the source for a pseudo-table cursor created using OpenPseudo. That pseudo-table cursor is the one that is identified by parameter P3. Clearing the P3 column cache as part of this opcode saves us from having to issue a separate NullRow instruction to clear that cache."]
    SorterData {
        #[p1]
        cursor_id: CursorID,
        #[p2]
        dest_reg: usize,
        #[p3]
        pseudo_cursor: usize,
    },

    #[desc = "Register P2 holds an SQL index key made using the MakeRecord instructions. This opcode writes that key into the sorter P1. Data for the entry is nil."]
    SorterInsert {
        #[p1]
        cursor_id: CursorID,
        #[p2]
        record_reg: usize,
    },

    #[desc = "This opcode works just like Next except that P1 must be a sorter object for which the SorterSort opcode has been invoked. This opcode advances the cursor to the next sorted record, or jumps to P2 if there are no more sorted records."]
    SorterNext {
        #[p1]
        cursor_id: CursorID,
        #[p2]
        pc_if_next: BranchOffset,
    },

    #[desc = "This opcode works like OpenEphemeral except that it opens a transient index that is specifically designed to sort large tables using an external merge-sort algorithm. If argument P3 is non-zero, then it indicates that the sorter may assume that a stable sort considering the first P3 fields of each key is sufficient to produce the required results."]
    SorterOpen {
        #[p1]
        cursor_id: CursorID,
        #[p2]
        columns: usize,
        order: OwnedRecord,
    },

    #[desc = "After all records have been inserted into the Sorter object identified by P1, invoke this opcode to actually do the sorting. Jump to P2 if there are no records to be sorted. This opcode is an alias for Sort and Rewind that is used for Sorter objects."]
    SorterSort {
        #[p1]
        cursor_id: CursorID,
        #[p2]
        pc_if_empty: BranchOffset,
    },

    #[desc = "P4 points to a nul terminated UTF-8 string. This opcode is transformed into a String opcode before it is executed for the first time. During this transformation, the length of string P4 is computed and stored as the P1 parameter."]
    String8 {
        value: String,
        #[p2]
        dest: usize,
    },

    #[desc = "Subtract the value in register P1 from the value in register P2 and store the result in register P3. If either input is NULL, the result is NULL."]
    Subtract {
        #[p1]
        lhs: usize,
        #[p2]
        rhs: usize,
        #[p3]
        dest: usize,
    },

    #[desc = "Begin a transaction on database P1 if a transaction is not already active. If P2 is non-zero, then a write-transaction is started, or if a read-transaction is already active, it is upgraded to a write-transaction. If P2 is zero, then a read-transaction is started. If P2 is 2 or more then an exclusive transaction is started. P1 is the index of the database file on which the transaction is started. Index 0 is the main database file and index 1 is the file used for temporary tables. Indices of 2 or more are used for attached databases. If a write-transaction is started and the Vdbe.usesStmtJournal flag is true (this flag is set if the Vdbe may modify more than one row and may throw an ABORT exception), a statement transaction may also be opened. More specifically, a statement transaction is opened iff the database connection is currently not in autocommit mode, or if there are other active statements. A statement transaction allows the changes made by this VDBE to be rolled back after an error without having to roll back the entire transaction. If no error is encountered, the statement transaction will automatically commit when the VDBE halts. If P5!=0 then this opcode also checks the schema cookie against P3 and the schema generation counter against P4. The cookie changes its value whenever the database schema changes. This operation is used to detect when that the cookie has changed and that the current process needs to reread the schema. If the schema cookie in P3 differs from the schema cookie in the database header or if the schema generation counter in P4 differs from the current generation counter, then an SQLITE_SCHEMA error is raised and execution halts. The sqlite3_step() wrapper function might then reprepare the statement and rerun it from the beginning."]
    Transaction {
        #[p2]
        write: bool,
    },

    #[desc = "Swap the program counter with the value in register P1. This has the effect of yielding to a coroutine. If the coroutine that is launched by this instruction ends with Yield or Return then continue to the next instruction. But if the coroutine launched by this instruction ends with EndCoroutine, then jump to P2 rather than continuing with the next instruction. See also: InitCoroutine"]
    Yield {
        #[p1]
        yield_reg: usize,
        #[p2]
        end_offset: BranchOffset,
    },
    // }

    // The big fsckin todo list!
    // enum InsnTodo {
    #[desc = "Verify that an Abort can happen. Assert if an Abort at this point might cause database corruption. This opcode only appears in debugging builds. An Abort is safe if either there have been no writes, or if there is an active statement journal."]
    Abortable,

    #[desc = "Add the constant P2 to the value in register P1. The result is always an integer. To force any register to be an integer, just add 0."]
    AddImm,

    #[desc = "Apply affinities to a range of P2 registers starting with P1. P4 is a string that is P2 characters long. The N-th character of the string indicates the column affinity that should be used for the N-th memory cell in the range."]
    Affinity,

    #[desc = "Execute the xInverse function for an aggregate. The function has P5 arguments. P4 is a pointer to the FuncDef structure that specifies the function. Register P3 is the accumulator. The P5 arguments are taken from register P2 and its successors."]
    AggInverse,

    #[desc = "Execute the xStep (if P1==0) or xInverse (if P1!=0) function for an aggregate. The function has P5 arguments. P4 is a pointer to the FuncDef structure that specifies the function. Register P3 is the accumulator. The P5 arguments are taken from register P2 and its successors. This opcode is initially coded as OP_AggStep0. On first evaluation, the FuncDef stored in P4 is converted into an sqlite3_context and the opcode is changed. In this way, the initialization of the sqlite3_context only happens once, instead of on each call to the step function."]
    AggStep1,

    #[desc = "Invoke the xValue() function and store the result in register P3. P2 is the number of arguments that the step function takes and P4 is a pointer to the FuncDef for this function. The P2 argument is not used by this opcode. It is only there to disambiguate functions that can take varying numbers of arguments. The P4 argument is only needed for the case where the step function was not previously called."]
    AggValue,

    #[desc = "Take the logical AND of the values in registers P1 and P2 and write the result into register P3. If either P1 or P2 is 0 (false) then the result is 0 even if the other input is NULL. A NULL and true or two NULLs give a NULL output."]
    And,

    #[desc = "Set the database auto-commit flag to P1 (1 or 0). If P2 is true, roll back any currently active btree transactions. If there are any active VMs (apart from this one), then a ROLLBACK fails. A COMMIT fails if there are active writing VMs or active VMs that use shared cache. This instruction causes the VM to halt."]
    AutoCommit,

    #[desc = "Mark the beginning of a subroutine that can be entered in-line or that can be called using Gosub. The subroutine should be terminated by an Return instruction that has a P1 operand that is the same as the P2 operand to this opcode and that has P3 set to 1. If the subroutine is entered in-line, then the Return will simply fall through. But if the subroutine is entered using Gosub, then the Return will jump back to the first instruction after the Gosub. This routine works by loading a NULL into the P2 register. When the return address register contains a NULL, the Return instruction is a no-op that simply falls through to the next instruction (assuming that the Return opcode has a P3 value of 1). Thus if the subroutine is entered in-line, then the Return will cause in-line execution to continue. But if the subroutine is entered via Gosub, then the Return will cause a return to the address following the Gosub. This opcode is identical to Null. It has a different name only to make the byte code easier to read and verify."]
    BeginSubrtn,

    #[desc = "Force the value in register P1 to be the type defined by P2.
P2=='A' → BLOB
P2=='B' → TEXT
P2=='C' → NUMERIC
P2=='D' → INTEGER
P2=='E' → REAL A NULL value is not changed by this routine. It remains NULL."]
    Cast,

    #[desc = "Checkpoint database P1. This is a no-op if P1 is not currently in WAL mode. Parameter P2 is one of SQLITE_CHECKPOINT_PASSIVE, FULL, RESTART, or TRUNCATE. Write 1 or 0 into mem[P3] if the checkpoint returns SQLITE_BUSY or not, respectively. Write the number of pages in the WAL after the checkpoint into mem[P3+1] and the number of pages in the WAL that have been checkpointed after the checkpoint completes into mem[P3+2]. However on an error, mem[P3+1] and mem[P3+2] are initialized to -1."]
    Checkpoint,

    #[desc = "Delete all contents of the database table or index whose root page in the database file is given by P1. But, unlike Destroy, do not remove the table or index from the database file. The table being cleared is in the main database file if P2==0. If P2==1 then the table to be cleared is in the auxiliary database file that is used to store tables create using CREATE TEMPORARY TABLE. If the P3 value is non-zero, then the row change count is incremented by the number of rows in the table being cleared. If P3 is greater than zero, then the value stored in register P3 is also incremented by the number of rows in the table being cleared. See also: Destroy"]
    Clear,

    #[desc = "Clear the subtype from register P1."]
    ClrSubtype,

    #[desc = "P4 is a pointer to a CollSeq object. If the next call to a user function or aggregate calls sqlite3GetFuncCollSeq(), this collation sequence will be returned. This is used by the built-in min(), max() and nullif() functions. If P1 is not zero, then it is a register that a subsequent min() or max() aggregate will set to 1 if the current row is not the minimum or maximum. The P1 register is initialized to 0 by this instruction. The interface used by the implementation of the aforementioned functions to retrieve the collation sequence set by this opcode is not available publicly. Only built-in functions have access to this feature."]
    CollSeq,

    #[desc = "This opcode (which only exists if SQLite was compiled with SQLITE_ENABLE_COLUMN_USED_MASK) identifies which columns of the table or index for cursor P1 are used. P4 is a 64-bit integer (P4_INT64) in which the first 63 bits are one for each of the first 63 columns of the table or index that are actually used by the cursor. The high-order bit is set if any column after the 64th is used."]
    ColumnsUsed,

    #[desc = "Add the text in register P1 onto the end of the text in register P2 and store the result in register P3. If either the P1 or P2 text are NULL then store NULL in P3. P3 = P2 || P1 It is illegal for P1 and P3 to be the same register. Sometimes, if P3 is the same register as P2, the implementation is able to avoid a memcpy()."]
    Concat,

    #[desc = "Store the number of entries (an integer value) in the table or index opened by cursor P1 in register P2. If P3==0, then an exact count is obtained, which involves visiting every btree page of the table. But if P3 is non-zero, an estimate is returned based on the current cursor position."]
    Count,

    #[desc = "Provide a hint to cursor P1 that it only needs to return rows that satisfy the Expr in P4. TK_REGISTER terms in the P4 expression refer to values currently held in registers. TK_COLUMN terms in the P4 expression refer to columns in the b-tree to which cursor P1 is pointing."]
    CursorHint,

    #[desc = "Lock the btree to which cursor P1 is pointing so that the btree cannot be written by an other cursor."]
    CursorLock,

    #[desc = "Unlock the btree to which cursor P1 is pointing so that it can be written by other cursors."]
    CursorUnlock,

    #[desc = "Delete the record at which the P1 cursor is currently pointing. If the OPFLAG_SAVEPOSITION bit of the P5 parameter is set, then the cursor will be left pointing at either the next or the previous record in the table. If it is left pointing at the next record, then the next Next instruction will be a no-op. As a result, in this case it is ok to delete a record from within a Next loop. If OPFLAG_SAVEPOSITION bit of P5 is clear, then the cursor will be left in an undefined state. If the OPFLAG_AUXDELETE bit is set on P5, that indicates that this delete is one of several associated with deleting a table row and all its associated index entries. Exactly one of those deletes is the \"primary\" delete. The others are all on OPFLAG_FORDELETE cursors or else are marked with the AUXDELETE flag. If the OPFLAG_NCHANGE (0x01) flag of P2 (NB: P2 not P5) is set, then the row change count is incremented (otherwise not). If the OPFLAG_ISNOOP (0x40) flag of P2 (not P5!) is set, then the pre-update-hook for deletes is run, but the btree is otherwise unchanged. This happens when the Delete is to be shortly followed by an Insert with the same key, causing the btree entry to be overwritten. P1 must not be pseudo-table. It has to be a real table with multiple rows. If P4 is not NULL then it points to a Table object. In this case either the update or pre-update hook, or both, may be invoked. The P1 cursor must have been positioned using NotFound prior to invoking this opcode in this case. Specifically, if one is configured, the pre-update hook is invoked if P4 is not NULL. The update-hook is invoked if one is configured, P4 is not NULL, and the OPFLAG_NCHANGE flag is set in P2. If the OPFLAG_ISUPDATE flag is set in P2, then P3 contains the address of the memory cell that contains the value that the rowid of the row will be set to by the update."]
    Delete,

    #[desc = "Delete an entire database table or index whose root page in the database file is given by P1. The table being destroyed is in the main database file if P3==0. If P3==1 then the table to be destroyed is in the auxiliary database file that is used to store tables create using CREATE TEMPORARY TABLE. If AUTOVACUUM is enabled then it is possible that another root page might be moved into the newly deleted root page in order to keep all root pages contiguous at the beginning of the database. The former value of the root page that moved - its value before the move occurred - is stored in register P2. If no page movement was required (because the table being dropped was already the last one in the database) then a zero is stored in register P2. If AUTOVACUUM is disabled then a zero is stored in register P2. This opcode throws an error if there are any active reader VMs when it is invoked. This is done to avoid the difficulty associated with updating existing cursors when a root page is moved in an AUTOVACUUM database. This error is thrown even if the database is not an AUTOVACUUM db in order to avoid introducing an incompatibility between autovacuum and non-autovacuum modes. See also: Clear"]
    Destroy,

    #[desc = "Remove the internal (in-memory) data structures that describe the index named P4 in database P1. This is called after an index is dropped from disk (using the Destroy opcode) in order to keep the internal representation of the schema consistent with what is on disk."]
    DropIndex,

    #[desc = "Remove the internal (in-memory) data structures that describe the table named P4 in database P1. This is called after a table is dropped from disk (using the Destroy opcode) in order to keep the internal representation of the schema consistent with what is on disk."]
    DropTable,

    #[desc = "Remove the internal (in-memory) data structures that describe the trigger named P4 in database P1. This is called after a trigger is dropped from disk (using the Destroy opcode) in order to keep the internal representation of the schema consistent with what is on disk."]
    DropTrigger,

    #[desc = "This opcode must follow an Lt or Gt comparison operator. There can be zero or more OP_ReleaseReg opcodes intervening, but no other opcodes are allowed to occur between this instruction and the previous Lt or Gt. If the result of an Eq comparison on the same two operands as the prior Lt or Gt would have been true, then jump to P2. If the result of an Eq comparison on the two previous operands would have been false or NULL, then fall through."]
    ElseEq,

    #[desc = "Cause precompiled statements to expire. When an expired statement is executed using sqlite3_step() it will either automatically reprepare itself (if it was originally created using sqlite3_prepare_v2()) or it will fail with SQLITE_SCHEMA. If P1 is 0, then all SQL statements become expired. If P1 is non-zero, then only the currently executing statement is expired. If P2 is 0, then SQL statements are expired immediately. If P2 is 1, then running SQL statements are allowed to continue to run to completion. The P2==1 case occurs when a CREATE INDEX or similar schema change happens that might help the statement run faster but which does not affect the correctness of operation."]
    Expire,

    #[desc = "Compute a hash on the key contained in the P4 registers starting with r[P3]. Check to see if that hash is found in the bloom filter hosted by register P1. If it is not present then maybe jump to P2. Otherwise fall through. False negatives are harmless. It is always safe to fall through, even if the value is in the bloom filter. A false negative causes more CPU cycles to be used, but it should still yield the correct answer. However, an incorrect answer may well arise from a false positive - if the jump is taken when it should fall through."]
    Filter,

    #[desc = "Compute a hash on the P4 registers starting with r[P3] and add that hash to the bloom filter contained in r[P1]."]
    FilterAdd,

    #[desc = "If cursor P1 was previously moved via DeferredSeek, complete that seek operation now, without further delay. If the cursor seek has already occurred, this instruction is a no-op."]
    FinishSeek,

    #[desc = "Halt with an SQLITE_CONSTRAINT error if there are any unresolved foreign key constraint violations. If there are no foreign key constraint violations, this is a no-op. FK constraint violations are also checked when the prepared statement exits. This opcode is used to raise foreign key constraint errors prior to returning results such as a row change count or the result of a RETURNING clause."]
    FkCheck,

    #[desc = "Increment a \"constraint counter\" by P2 (P2 may be negative or positive). If P1 is non-zero, the database constraint counter is incremented (deferred foreign key constraints). Otherwise, if P1 is zero, the statement counter is incremented (immediate foreign key constraints)."]
    FkCounter,

    #[desc = "This opcode tests if a foreign key constraint-counter is currently zero. If so, jump to instruction P2. Otherwise, fall through to the next instruction. If P1 is non-zero, then the jump is taken if the database constraint-counter is zero (the one that counts deferred constraint violations). If P1 is zero, the jump is taken if the statement constraint-counter is zero (immediate foreign key constraint violations)."]
    FkIfZero,

    #[desc = "If P4==0 then register P3 holds a blob constructed by MakeRecord. If P4>0 then register P3 is the first of P4 registers that form an unpacked record. Cursor P1 is on an index btree. If the record identified by P3 and P4 is a prefix of any entry in P1 then a jump is made to P2 and P1 is left pointing at the matching entry. This operation leaves the cursor in a state where it can be advanced in the forward direction. The Next instruction will work, but not the Prev instruction. See also: NotFound, NoConflict, NotExists. SeekGe"]
    Found,

    #[desc = "Extract the subtype value from register P1 and write that subtype into register P2. If P1 has no subtype, then P1 gets a NULL."]
    GetSubtype,

    #[desc = "Check the value in register P3. If it is NULL then Halt using parameter P1, P2, and P4 as if this were a Halt instruction. If the value in register P3 is not NULL, then this routine is a no-op. The P5 parameter should be 1."]
    HaltIfNull,

    #[desc = "The content of P3 registers starting at register P2 form an unpacked index key. This opcode removes that entry from the index opened by cursor P1. If P5 is not zero, then raise an SQLITE_CORRUPT_INDEX error if no matching index entry is found. This happens when running an UPDATE or DELETE statement and the index entry to be updated or deleted is not found. For some uses of IdxDelete (example: the EXCEPT operator) it does not matter that no matching entry is found. For those cases, P5 is zero. Also, do not raise this (self-correcting and non-critical) error if in writable_schema mode."]
    IdxDelete,

    #[desc = "Register P2 holds an SQL index key made using the MakeRecord instructions. This opcode writes that key into the index P1. Data for the entry is nil. If P4 is not zero, then it is the number of values in the unpacked key of reg(P2). In that case, P3 is the index of the first register for the unpacked key. The availability of the unpacked key can sometimes be an optimization. If P5 has the OPFLAG_APPEND bit set, that is a hint to the b-tree layer that this insert is likely to be an append. If P5 has the OPFLAG_NCHANGE bit set, then the change counter is incremented by this instruction. If the OPFLAG_NCHANGE bit is clear, then the change counter is unchanged. If the OPFLAG_USESEEKRESULT flag of P5 is set, the implementation might run faster by avoiding an unnecessary seek on cursor P1. However, the OPFLAG_USESEEKRESULT flag must only be set if there have been no prior seeks on the cursor or if the most recent seek used a key equivalent to P2. This instruction only works for indices. The equivalent instruction for tables is Insert."]
    IdxInsert,

    #[desc = "The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY or ROWID. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID on the P1 index. If the P1 index entry is less than or equal to the key value then jump to P2. Otherwise fall through to the next instruction."]
    IdxLE,

    #[desc = "The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY or ROWID. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID on the P1 index. If the P1 index entry is less than the key value then jump to P2. Otherwise fall through to the next instruction."]
    IdxLT,

    #[desc = "Write into register P2 an integer which is the last entry in the record at the end of the index key pointed to by cursor P1. This integer should be the rowid of the table entry to which this index entry points. See also: Rowid, MakeRecord."]
    IdxRowid,

    #[desc = "Register P3 is the first of P4 registers that form an unpacked record. Cursor P1 is an index btree. P2 is a jump destination. In other words, the operands to this opcode are the same as the operands to NotFound and IdxGT. This opcode is an optimization attempt only. If this opcode always falls through, the correct answer is still obtained, but extra work is performed. A value of N in the seekHit flag of cursor P1 means that there exists a key P3:N that will match some record in the index. We want to know if it is possible for a record P3:P4 to match some record in the index. If it is not possible, we can skip some work. So if seekHit is less than P4, attempt to find out if a match is possible by running NotFound. This opcode is used in IN clause processing for a multi-column key. If an IN clause is attached to an element of the key other than the left-most element, and if there are no matches on the most recent seek over the whole key, then it might be that one of the key element to the left is prohibiting a match, and hence there is \"no hope\" of any match regardless of how many IN clause elements are checked. In such a case, we abandon the IN clause search early, using this opcode. The opcode name comes from the fact that the jump is taken if there is \"no hope\" of achieving a match. See also: NotFound, SeekHit"]
    IfNoHope,

    #[desc = "If cursor P1 is not open or if P1 is set to a NULL row using the NullRow opcode, then jump to instruction P2. Otherwise, fall through."]
    IfNotOpen,

    #[desc = "Register P1 must contain an integer. If the content of register P1 is initially greater than zero, then decrement the value in register P1. If it is non-zero (negative or positive) and then also jump to P2. If register P1 is initially zero, leave it unchanged and fall through."]
    IfNotZero,

    #[desc = "Check the cursor P1 to see if it is currently pointing at a NULL row. If it is, then set register P3 to NULL and jump immediately to P2. If P1 is not on a NULL row, then fall through without making any changes. If P1 is not an open cursor, then this opcode is a no-op."]
    IfNullRow,

    #[desc = "Let N be the approximate number of rows in the table or index with cursor P1 and let X be 10*log2(N) if N is positive or -1 if N is zero. Jump to P2 if X is in between P3 and P4, inclusive."]
    IfSizeBetween,

    #[desc = "Perform a single step of the incremental vacuum procedure on the P1 database. If the vacuum has finished, jump to instruction P2. Otherwise, fall through to the next instruction."]
    IncrVacuum,

    #[desc = "Write an entry into the table of cursor P1. A new entry is created if it doesn't already exist or the data for an existing entry is overwritten. The data is the value MEM_Blob stored in register number P2. The key is stored in register P3. The key must be a MEM_Int. If the OPFLAG_NCHANGE flag of P5 is set, then the row change count is incremented (otherwise not). If the OPFLAG_LASTROWID flag of P5 is set, then rowid is stored for subsequent return by the sqlite3_last_insert_rowid() function (otherwise it is unmodified). If the OPFLAG_USESEEKRESULT flag of P5 is set, the implementation might run faster by avoiding an unnecessary seek on cursor P1. However, the OPFLAG_USESEEKRESULT flag must only be set if there have been no prior seeks on the cursor or if the most recent seek used a key equal to P3. If the OPFLAG_ISUPDATE flag is set, then this opcode is part of an UPDATE operation. Otherwise (if the flag is clear) then this opcode is part of an INSERT operation. The difference is only important to the update hook. Parameter P4 may point to a Table structure, or may be NULL. If it is not NULL, then the update-hook (sqlite3.xUpdateCallback) is invoked following a successful insert. (WARNING/TODO: If P1 is a pseudo-cursor and P2 is dynamically allocated, then ownership of P2 is transferred to the pseudo-cursor and register P2 becomes ephemeral. If the cursor is changed, the value of register P2 will then change. Make sure this does not cause any problems.) This instruction only works on tables. The equivalent instruction for indices is IdxInsert."]
    Insert,

    #[desc = "P4 is a pointer to a 64-bit integer value. Write that value into register P2."]
    Int64,

    #[desc = "Transfer the integer value held in register P1 into register P2. This is an optimized version of SCopy that works only for integer values."]
    IntCopy,

    #[desc = "Do an analysis of the currently open database. Store in register (P1+1) the text of an error message describing any problems. If no problems are found, store a NULL in register (P1+1). The register (P1) contains one less than the maximum number of allowed errors. At most reg(P1) errors will be reported. In other words, the analysis stops as soon as reg(P1) errors are seen. Reg(P1) is updated with the number of errors remaining. The root page numbers of all tables in the database are integers stored in P4_INTARRAY argument. If P5 is not zero, the check is done on the auxiliary database file, not the main database file. This opcode is used to implement the integrity_check pragma."]
    IntegrityCk,

    #[desc = "This opcode implements the IS TRUE, IS FALSE, IS NOT TRUE, and IS NOT FALSE operators. Interpret the value in register P1 as a boolean value. Store that boolean (a 0 or 1) in register P2. Or if the value in register P1 is NULL, then the P3 is stored in register P2. Invert the answer if P4 is 1. The logic is summarized like this:
If P3==0 and P4==0 then r[P2] := r[P1] IS TRUE
If P3==1 and P4==1 then r[P2] := r[P1] IS FALSE
If P3==0 and P4==1 then r[P2] := r[P1] IS NOT TRUE
If P3==1 and P4==0 then r[P2] := r[P1] IS NOT FALSE"]
    IsTrue,

    #[desc = "Jump to P2 if the type of a column in a btree is one of the types specified by the P5 bitmask. P1 is normally a cursor on a btree for which the row decode cache is valid through at least column P3. In other words, there should have been a prior Column for column P3 or greater. If the cursor is not valid, then this opcode might give spurious results. The the btree row has fewer than P3 columns, then use P4 as the datatype. If P1 is -1, then P3 is a register number and the datatype is taken from the value in that register. P5 is a bitmask of data types. SQLITE_INTEGER is the least significant (0x01) bit. SQLITE_FLOAT is the 0x02 bit. SQLITE_TEXT is 0x04. SQLITE_BLOB is 0x08. SQLITE_NULL is 0x10. WARNING: This opcode does not reliably distinguish between NULL and REAL when P1>=0. If the database contains a NaN value, this opcode will think that the datatype is REAL when it should be NULL. When P1<0 and the value is already stored in register P3, then this opcode does reliably distinguish between NULL and REAL. The problem only arises then P1>=0. Take the jump to address P2 if and only if the datatype of the value determined by P1 and P3 corresponds to one of the bits in the P5 bitmask."]
    IsType,

    #[desc = "Change the journal mode of database P1 to P3. P3 must be one of the PAGER_JOURNALMODE_XXX values. If changing between the various rollback modes (delete, truncate, persist, off and memory), this is a simple operation. No IO is required. If changing into or out of WAL mode the procedure is more complicated. Write a string containing the final journal-mode to register P2."]
    JournalMode,

    #[desc = "The next use of the Rowid or Column or Prev instruction for P1 will refer to the last entry in the database table or index. If the table or index is empty and P2>0, then jump immediately to P2. If P2 is 0 or if the table or index is not empty, fall through to the following instruction. This opcode leaves the cursor configured to move in reverse order, from the end toward the beginning. In other words, the cursor is configured to use Prev, not Next."]
    Last,

    #[desc = "Read the sqlite_stat1 table for database P1 and load the content of that table into the internal index hash table. This will cause the analysis to be used when preparing all subsequent queries."]
    LoadAnalysis,

    #[desc = "Try to set the maximum page count for database P1 to the value in P3. Do not let the maximum page count fall below the current page count and do not change the maximum page count value if P3==0. Store the maximum page count after the change in register P2."]
    MaxPgcnt,

    #[desc = "P1 is a register in the root frame of this VM (the root frame is different from the current frame if this instruction is being executed within a sub-program). Set the value of register P1 to the maximum of its current value and the value in register P2. This instruction throws an error if the memory cell is not initially an integer."]
    MemMax,

    #[desc = "Advance cursor P1 so that it points to the next key/data pair in its table or index. If there are no more key/value pairs then fall through to the following instruction. But if the cursor advance was successful, jump immediately to P2. The Next opcode is only valid following an SeekGT, SeekGE, or Rewind opcode used to position the cursor. Next is not allowed to follow SeekLT, SeekLE, or Last. The P1 cursor must be for a real table, not a pseudo-table. P1 must have been opened prior to this opcode or the program will segfault. The P3 value is a hint to the btree implementation. If P3==1, that means P1 is an SQL index and that this instruction could have been omitted if that index had been unique. P3 is usually 0. P3 is always either 0 or 1. If P5 is positive and the jump is taken, then event counter number P5-1 in the prepared statement is incremented. See also: Prev"]
    Next,

    #[desc = "If P4==0 then register P3 holds a blob constructed by MakeRecord. If P4>0 then register P3 is the first of P4 registers that form an unpacked record. Cursor P1 is on an index btree. If the record identified by P3 and P4 contains any NULL value, jump immediately to P2. If all terms of the record are not-NULL then a check is done to determine if any row in the P1 index btree has a matching key prefix. If there are no matches, jump immediately to P2. If there is a match, fall through and leave the P1 cursor pointing to the matching row. This opcode is similar to NotFound with the exceptions that the branch is always taken if any part of the search key input is NULL. This operation leaves the cursor in a state where it cannot be advanced in either direction. In other words, the Next and Prev opcodes do not work after this operation. See also: NotFound, Found, NotExists"]
    NoConflict,

    #[desc = "Do nothing. This instruction is often useful as a jump destination."]
    Noop,

    #[desc = "Interpret the value in register P1 as a boolean value. Store the boolean complement in register P2. If the value in register P1 is NULL, then a NULL is stored in P2."]
    Not,

    #[desc = "If P4==0 then register P3 holds a blob constructed by MakeRecord. If P4>0 then register P3 is the first of P4 registers that form an unpacked record. Cursor P1 is on an index btree. If the record identified by P3 and P4 is not the prefix of any entry in P1 then a jump is made to P2. If P1 does contain an entry whose prefix matches the P3/P4 record then control falls through to the next instruction and P1 is left pointing at the matching entry. This operation leaves the cursor in a state where it cannot be advanced in either direction. In other words, the Next and Prev opcodes do not work after this operation. See also: Found, NotExists, NoConflict, IfNoHope"]
    NotFound,

    #[desc = "Store in register r[P3] the byte offset into the database file that is the start of the payload for the record at which that cursor P1 is currently pointing. P2 is the column number for the argument to the sqlite_offset() function. This opcode does not use P2 itself, but the P2 value is used by the code generator. The P1, P2, and P3 operands to this opcode are the same as for Column. This opcode is only available if SQLite is compiled with the -DSQLITE_ENABLE_OFFSET_SQL_FUNC option."]
    Offset,

    #[desc = "This opcode performs a commonly used computation associated with LIMIT and OFFSET processing. r[P1] holds the limit counter. r[P3] holds the offset counter. The opcode computes the combined value of the LIMIT and OFFSET and stores that value in r[P2]. The r[P2] value computed is the total number of rows that will need to be visited in order to complete the query. If r[P3] is zero or negative, that means there is no OFFSET and r[P2] is set to be the value of the LIMIT, r[P1]. if r[P1] is zero or negative, that means there is no LIMIT and r[P2] is set to -1. Otherwise, r[P2] is set to the sum of r[P1] and r[P3]."]
    OffsetLimit,

    #[desc = "Fall through to the next instruction the first time this opcode is encountered on each invocation of the byte-code program. Jump to P2 on the second and all subsequent encounters during the same invocation. Top-level programs determine first invocation by comparing the P1 operand against the P1 operand on the Init opcode at the beginning of the program. If the P1 values differ, then fall through and make the P1 of this opcode equal to the P1 of Init. If P1 values are the same then take the jump. For subprograms, there is a bitmask in the VdbeFrame that determines whether or not the jump should be taken. The bitmask is necessary because the self-altering code trick does not work for recursive triggers."]
    Once,

    #[desc = "This opcode works the same as OpenEphemeral. It has a different name to distinguish its use. Tables created using by this opcode will be used for automatically created transient indices in joins."]
    OpenAutoindex,

    #[desc = "Open a new cursor P1 that points to the same ephemeral table as cursor P2. The P2 cursor must have been opened by a prior OpenEphemeral opcode. Only ephemeral cursors may be duplicated. Duplicate ephemeral cursors are used for self-joins of materialized views."]
    OpenDup,

    #[desc = "Open a new cursor P1 to a transient table. The cursor is always opened read/write even if the main database is read-only. The ephemeral table is deleted automatically when the cursor is closed. If the cursor P1 is already opened on an ephemeral table, the table is cleared (all content is erased). P2 is the number of columns in the ephemeral table. The cursor points to a BTree table if P4==0 and to a BTree index if P4 is not 0. If P4 is not NULL, it points to a KeyInfo structure that defines the format of keys in the index. The P5 parameter can be a mask of the BTREE_* flags defined in btree.h. These flags control aspects of the operation of the btree. The BTREE_OMIT_JOURNAL and BTREE_SINGLE flags are added automatically. If P3 is positive, then reg[P3] is modified slightly so that it can be used as zero-length data for Insert. This is an optimization that avoids an extra Blob opcode to initialize that register."]
    OpenEphemeral,

    #[desc = "Open a read-only cursor for the database table whose root page is P2 in a database file. The database file is determined by P3. P3==0 means the main database, P3==1 means the database used for temporary tables, and P3>1 means used the corresponding attached database. Give the new cursor an identifier of P1. The P1 values need not be contiguous but all P1 values should be small integers. It is an error for P1 to be negative. Allowed P5 bits: 0x02 OPFLAG_SEEKEQ: This cursor will only be used for equality lookups (implemented as a pair of opcodes SeekGE/IdxGT of SeekLE/IdxLT) The P4 value may be either an integer (P4_INT32) or a pointer to a KeyInfo structure (P4_KEYINFO). If it is a pointer to a KeyInfo object,     #[desc( then table being opened must be an index b-tree where the KeyInfo object defines the content and collating sequence of that index b-tree. Otherwise, if P4 is an integer value, then the table being opened must be a table b-tree with a number of columns no less than the value of P4. See also: OpenWrite, ReopenIdx"]
    OpenRead,

    #[desc = "Open a read/write cursor named P1 on the table or index whose root page is P2 (or whose root page is held in register P2 if the OPFLAG_P2ISREG bit is set in P5 - see below). The P4 value may be either an integer (P4_INT32) or a pointer to a KeyInfo structure (P4_KEYINFO). If it is a pointer to a KeyInfo object, then table being opened must be an index b-tree where the KeyInfo object defines the content and collating sequence of that index b-tree. Otherwise, if P4 is an integer value, then the table being opened must be a table b-tree with a number of columns no less than the value of P4. Allowed P5 bits:
0x02 OPFLAG_SEEKEQ: This cursor will only be used for equality lookups (implemented as a pair of opcodes SeekGE/IdxGT of SeekLE/IdxLT)
0x08 OPFLAG_FORDELETE: This cursor is used only to seek and subsequently delete entries in an index btree. This is a hint to the storage engine that the storage engine is allowed to ignore. The hint is not used by the official SQLite b*tree storage engine, but is used by COMDB2.
0x10 OPFLAG_P2ISREG: Use the content of register P2 as the root page, not the value of P2 itself. This instruction works like OpenRead except that it opens the cursor in read/write mode. See also: OpenRead, ReopenIdx"]
    OpenWrite,

    #[desc = "Take the logical OR of the values in register P1 and P2 and store the answer in register P3. If either P1 or P2 is nonzero (true) then the result is 1 (true) even if the other input is NULL. A NULL and false or two NULLs give a NULL output."]
    Or,

    #[desc = "Write the current number of pages in database P1 to memory cell P2."]
    Pagecount,

    #[desc = "This opcode is only ever present in sub-programs called via the Program instruction. Copy a value currently stored in a memory cell of the calling (parent) frame to cell P2 in the current frames address space. This is used by trigger programs to access the new.* and old.* values. The address of the cell in the parent frame is determined by adding the value of the P1 argument to the value of the P1 argument to the calling Program instruction."]
    Param,

    #[desc = "Set the permutation used by the Compare operator in the next instruction. The permutation is stored in the P4 operand. The permutation is only valid for the next opcode which must be an Compare that has the OPFLAG_PERMUTE bit set in P5. The first integer in the P4 integer array is the length of the array and does not become part of the permutation."]
    Permutation,

    #[desc = "Back up cursor P1 so that it points to the previous key/data pair in its table or index. If there is no previous key/value pairs then fall through to the following instruction. But if the cursor backup was successful, jump immediately to P2. The Prev opcode is only valid following an SeekLT, SeekLE, or Last opcode used to position the cursor. Prev is not allowed to follow SeekGT, SeekGE, or Rewind. The P1 cursor must be for a real table, not a pseudo-table. If P1 is not open then the behavior is undefined. The P3 value is a hint to the btree implementation. If P3==1, that means P1 is an SQL index and that this instruction could have been omitted if that index had been unique. P3 is usually 0. P3 is always either 0 or 1. If P5 is positive and the jump is taken, then event counter number P5-1 in the prepared statement is incremented."]
    Prev,

    #[desc = "Execute the trigger program passed as P4 (type P4_SUBPROGRAM). P1 contains the address of the memory cell that contains the first memory cell in an array of values used as arguments to the sub-program. P2 contains the address to jump to if the sub-program throws an IGNORE exception using the RAISE() function. P2 might be zero, if there is no possibility that an IGNORE exception will be raised. Register P3 contains the address of a memory cell in this (the parent) VM that is used to allocate the memory required by the sub-vdbe at runtime. P4 is a pointer to the VM containing the trigger program. If P5 is non-zero, then recursive program invocation is enabled."]
    Program,

    #[desc = "Invoke a user function (P4 is a pointer to an sqlite3_context object that contains a pointer to the function to be run) with arguments taken from register P2 and successors. The number of arguments is in the sqlite3_context object that P4 points to. The result of the function is stored in register P3. Register P3 must not be one of the function inputs. P1 is a 32-bit bitmask indicating whether or not each argument to the function was determined to be constant at compile time. If the first argument was constant then bit 0 of P1 is set. This is used to determine whether meta data associated with a user function argument using the sqlite3_set_auxdata() API may be safely retained until the next invocation of this opcode. This opcode works exactly like Function. The only difference is in its name. This opcode is used in places where the function must be purely non-deterministic. Some built-in date/time functions can be either deterministic of non-deterministic, depending on their arguments. When those function are used in a non-deterministic way, they will check to see if they were called using PureFunc instead of Function, and if they were, they throw an error. See also: AggStep, AggFinal, Function"]
    PureFunc,

    #[desc = "Read cookie number P3 from database P1 and write it into register P2. P3==1 is the schema version. P3==2 is the database format. P3==3 is the recommended pager cache size, and so forth. P1==0 is the main database file and P1==1 is the database file used to store temporary tables. There must be a read-lock on the database (either a transaction must be started or there must be an open cursor) before executing this instruction."]
    ReadCookie,

    #[desc = "Release registers from service. Any content that was in the the registers is unreliable after this opcode completes. The registers released will be the P2 registers starting at P1, except if bit ii of P3 set, then do not release register P1+ii. In other words, P3 is a mask of registers to preserve. Releasing a register clears the Mem.pScopyFrom pointer. That means that if the content of the released register was set using SCopy, a change to the value of the source register for the SCopy will no longer generate an assertion fault in sqlite3VdbeMemAboutToChange(). If P5 is set, then all released registers have their type set to MEM_Undefined so that any subsequent attempt to read the released register (before it is reinitialized) will generate an assertion fault. P5 ought to be set on every call to this opcode. However, there are places in the code generator will release registers before their are used, under the (valid) assumption that the registers will not be reallocated for some other purpose before they are used and hence are safe to release. This opcode is only available in testing and debugging builds. It is not generated for release builds. The purpose of this opcode is to help validate the generated bytecode. This opcode does not actually contribute to computing an answer."]
    ReleaseReg,

    #[desc = "Compute the remainder after integer register P2 is divided by register P1 and store the result in register P3. If the value in register P1 is zero the result is NULL. If either operand is NULL, the result is NULL."]
    Remainder,

    #[desc = "The ReopenIdx opcode works like OpenRead except that it first checks to see if the cursor on P1 is already open on the same b-tree and if it is this opcode becomes a no-op. In other words, if the cursor is already open, do not reopen it. The ReopenIdx opcode may only be used with P5==0 or P5==OPFLAG_SEEKEQ and with P4 being a P4_KEYINFO object. Furthermore, the P3 value must be the same as every other ReopenIdx or OpenRead for the same cursor number. Allowed P5 bits:
0x02 OPFLAG_SEEKEQ: This cursor will only be used for equality lookups (implemented as a pair of opcodes SeekGE/IdxGT of SeekLE/IdxLT) See also: OpenRead, OpenWrite"]
    ReopenIdx,

    #[desc = "The value of the change counter is copied to the database handle change counter (returned by subsequent calls to sqlite3_changes()). Then the VMs internal change counter resets to 0. This is used by trigger programs."]
    ResetCount,

    #[desc = "Delete all contents from the ephemeral table or sorter that is open on cursor P1. This opcode only works for cursors used for sorting and opened with OpenEphemeral or SorterOpen."]
    ResetSorter,

    #[desc = "The next use of the Rowid or Column or Next instruction for P1 will refer to the first entry in the database table or index. If the table or index is empty, jump immediately to P2. If the table or index is not empty, fall through to the following instruction. If P2 is zero, that is an assertion that the P1 table is never empty and hence the jump will never be taken. This opcode leaves the cursor configured to move in forward order, from the beginning toward the end. In other words, the cursor is configured to use Next, not Prev."]
    Rewind,

    #[desc = "P1 and P2 are both open cursors. Both must be opened on the same type of table - intkey or index. This opcode is used as part of copying the current row from P2 into P1. If the cursors are opened on intkey tables, register P3 contains the rowid to use with the new record in P1. If they are opened on index tables, P3 is not used. This opcode must be followed by either an Insert or InsertIdx opcode with the OPFLAG_PREFORMAT flag set to complete the insert operation."]
    RowCell,

    #[desc = "Write into register P2 the complete row content for the row at which cursor P1 is currently pointing. There is no interpretation of the data. It is just copied onto the P2 register exactly as it is found in the database file. If cursor P1 is an index, then the content is the key of the row. If cursor P2 is a table, then the content extracted is the data. If the P1 cursor must be pointing to a valid row (not a NULL row) of a real table, not a pseudo-table. If P3!=0 then this opcode is allowed to make an ephemeral pointer into the database page. That means that the content of the output register will be invalidated as soon as the cursor moves - including moves caused by other cursors that \"save\" the current cursors position in order that they can write to the same table. If P3==0 then a copy of the data is made into memory. P3!=0 is faster, but P3==0 is safer. If P3!=0 then the content of the P2 register is unsuitable for use in OP_Result and any OP_Result will invalidate the P2 register content. The P2 register content is invalidated by opcodes like Function or by any use of another cursor pointing to the same table."]
    RowData,

    #[desc = "Insert the integer value held by register P2 into a RowSet object held in register P1. An assertion fails if P2 is not an integer."]
    RowSetAdd,

    #[desc = "Extract the smallest value from the RowSet object in P1 and put that value into register P3. Or, if RowSet object P1 is initially empty, leave P3 unchanged and jump to instruction P2."]
    RowSetRead,

    #[desc = "Register P3 is assumed to hold a 64-bit integer value. If register P1 contains a RowSet object and that RowSet object contains the value held in P3, jump to register P2. Otherwise, insert the integer in P3 into the RowSet and continue on to the next opcode. The RowSet object is optimized for the case where sets of integers are inserted in distinct phases, which each set contains no duplicates. Each set is identified by a unique P4 value. The first set must have P4==0, the final set must have P4==-1, and for all other sets must have P4>0. This allows optimizations: (a) when P4==0 there is no need to test the RowSet object for P3, as it is guaranteed not to contain it, (b) when P4==-1 there is no need to insert the value, as it will never be tested for, and (c) when a value that is part of set X is inserted, there is no need to search to see if the same value was previously inserted as part of set X (only if it was previously inserted as part of some other set)."]
    RowSetTest,

    #[desc = "Open, release or rollback the savepoint named by parameter P4, depending on the value of P1. To open a new savepoint set P1==0 (SAVEPOINT_BEGIN). To release (commit) an existing savepoint set P1==1 (SAVEPOINT_RELEASE). To rollback an existing savepoint set P1==2 (SAVEPOINT_ROLLBACK)."]
    Savepoint,

    #[desc = "Make a shallow copy of register P1 into register P2. This instruction makes a shallow copy of the value. If the value is a string or blob, then the copy is only a pointer to the original and hence if the original changes so will the copy. Worse, if the original is deallocated, the copy becomes invalid. Thus the program must guarantee that the original will not change during the lifetime of the copy. Use Copy to make a complete copy."]
    SCopy,

    #[desc = "Position cursor P1 at the end of the btree for the purpose of appending a new entry onto the btree. It is assumed that the cursor is used only for appending and so if the cursor is valid, then the cursor must already be pointing at the end of the btree and so no changes are made to the cursor."]
    SeekEnd,

    #[desc = "Increase or decrease the seekHit value for cursor P1, if necessary, so that it is no less than P2 and no greater than P3. The seekHit integer represents the maximum of terms in an index for which there is known to be at least one match. If the seekHit value is smaller than the total number of equality terms in an index lookup, then the IfNoHope opcode might run to see if the IN loop can be abandoned early, thus saving work. This is part of the IN-early-out optimization. P1 must be a valid b-tree cursor."]
    SeekHit,

    #[desc = "If cursor P1 refers to an SQL table (B-Tree that uses integer keys), use the value in register P3 as a key. If cursor P1 refers to an SQL index, then P3 is the first in an array of P4 registers that are used as an unpacked index key. Reposition cursor P1 so that it points to the largest entry that is less than or equal to the key value. If there are no records less than or equal to the key and P2 is not zero, then jump to P2. This opcode leaves the cursor configured to move in reverse order, from the end toward the beginning. In other words, the cursor is configured to use Prev, not Next. If the cursor P1 was opened using the OPFLAG_SEEKEQ flag, then this opcode will either land on a record that exactly matches the key, or else it will cause a jump to P2. When the cursor is OPFLAG_SEEKEQ, this opcode must be followed by an IdxLE opcode with the same arguments. The IdxGE opcode will be skipped if this opcode succeeds, but the IdxGE opcode will be used on subsequent loop iterations. The OPFLAG_SEEKEQ flags is a hint to the btree layer to say that this is an equality search. See also: Found, NotFound, SeekGt, SeekGe, SeekLt"]
    SeekLE,

    #[desc = "If cursor P1 refers to an SQL table (B-Tree that uses integer keys), use the value in register P3 as a key. If cursor P1 refers to an SQL index, then P3 is the first in an array of P4 registers that are used as an unpacked index key. Reposition cursor P1 so that it points to the largest entry that is less than the key value. If there are no records less than the key and P2 is not zero, then jump to P2. This opcode leaves the cursor configured to move in reverse order, from the end toward the beginning. In other words, the cursor is configured to use Prev, not Next. See also: Found, NotFound, SeekGt, SeekGe, SeekLe"]
    SeekLT,

    #[desc = "This opcode is a prefix opcode to SeekGE. In other words, this opcode must be immediately followed by SeekGE. This constraint is checked by assert() statements. This opcode uses the P1 through P4 operands of the subsequent SeekGE. In the text that follows, the operands of the subsequent SeekGE opcode are denoted as SeekOP.P1 through SeekOP.P4. Only the P1, P2 and P5 operands of this opcode are also used, and are called This.P1, This.P2 and This.P5. This opcode helps to optimize IN operators on a multi-column index where the IN operator is on the later terms of the index by avoiding unnecessary seeks on the btree, substituting steps to the next row of the b-tree instead. A correct answer is obtained if this opcode is omitted or is a no-op. The SeekGE.P3 and SeekGE.P4 operands identify an unpacked key which is the desired entry that we want the cursor SeekGE.P1 to be pointing to. Call this SeekGE.P3/P4 row the \"target\". If the SeekGE.P1 cursor is not currently pointing to a valid row, then this opcode is a no-op and control passes through into the SeekGE. If the SeekGE.P1 cursor is pointing to a valid row, then that row might be the target row, or it might be near and slightly before the target row, or it might be after the target row. If the cursor is currently before the target row, then this opcode attempts to position the cursor on or after the target row by invoking sqlite3BtreeStep() on the cursor between 1 and This.P1 times. The This.P5 parameter is a flag that indicates what to do if the cursor ends up pointing at a valid row that is past the target row. If This.P5 is false (0) then a jump is made to SeekGE.P2. If This.P5 is true (non-zero) then a jump is made to This.P2. The P5==0 case occurs when there are no inequality constraints to the right of the IN constraint. The jump to SeekGE.P2 ends the loop. The P5!=0 case occurs when there are inequality constraints to the right of the IN operator. In that case, the This.P2 will point either directly to or to setup code prior to the IdxGT or IdxGE opcode that checks for loop terminate. Possible outcomes from this opcode:
If the cursor is initially not pointed to any valid row, description:  then fall through into the subsequent SeekGE opcode.
If the cursor is left pointing to a row that is before the target row, description:  even after making as many as This.P1 calls to sqlite3BtreeNext(), then also fall through into SeekGE.
If the cursor is left pointing at the target row, description:  either because it was at the target row to begin with or because one or more sqlite3BtreeNext() calls moved the cursor to the target row, then jump to This.P2..,
If the cursor started out before the target row and a call to to sqlite3BtreeNext() moved the cursor off the end of the index (indicating that the target row definitely does not exist in the btree) then jump to SeekGE.P2, description:  ending the loop.
If the cursor ends up on a valid row that is past the target row (indicating that the target row does not exist in the btree) then jump to SeekOP.P2 if This.P5==0 or to This.P2 if This.P5>0."]
    SeekScan,

    #[desc = "Find the next available sequence number for cursor P1. Write the sequence number into register P2. The sequence number on the cursor is incremented after this instruction."]
    Sequence,

    #[desc = "P1 is a sorter cursor. If the sequence counter is currently zero, jump to P2. Regardless of whether or not the jump is taken, increment the the sequence value."]
    SequenceTest,

    #[desc = "Write the integer value P3 into cookie number P2 of database P1. P2==1 is the schema version. P2==2 is the database format. P2==3 is the recommended pager cache size, and so forth. P1==0 is the main database file and P1==1 is the database file used to store temporary tables. A transaction must be started before executing this opcode. If P2 is the SCHEMA_VERSION cookie (cookie number 1) then the internal schema version is set to P3-P5. The \"PRAGMA schema_version=N\" statement has P5 set to 1, so that the internal schema version will be different from the database schema version, resulting in a schema reset."]
    SetCookie,

    #[desc = "Set the subtype value of register P2 to the integer from register P1. If P1 is NULL, clear the subtype from p2."]
    SetSubtype,

    #[desc = "Shift the integer value in register P2 to the left by the number of bits specified by the integer in register P1. Store the result in register P3. If either input is NULL, the result is NULL."]
    ShiftLeft,

    #[desc = "Shift the integer value in register P2 to the right by the number of bits specified by the integer in register P1. Store the result in register P3. If either input is NULL, the result is NULL."]
    ShiftRight,

    #[desc = "This opcode does exactly the same thing as Rewind except that it increments an undocumented global variable used for testing. Sorting is accomplished by writing records into a sorting index, then rewinding that index and playing it back from beginning to end. We use the Sort opcode instead of Rewind to do the rewinding so that the global variable will be incremented and regression tests can determine whether or not the optimizer is correctly optimizing out sorts."]
    Sort,

    #[desc = "P1 is a sorter cursor. This instruction compares a prefix of the record blob in register P3 against a prefix of the entry that the sorter cursor currently points to. Only the first P4 fields of r[P3] and the sorter record are compared. If either P3 or the sorter contains a NULL in one of their significant fields (not counting the P4 fields at the end which are ignored) then the comparison is assumed to be equal. Fall through to next instruction if the two records compare equal to each other. Jump to P2 if they are different."]
    SorterCompare,

    #[desc = "Run the SQL statement or statements specified in the P4 string. The P1 parameter is a bitmask of options: 0x0001 Disable Auth and Trace callbacks while the statements in P4 are running. 0x0002 Set db->nAnalysisLimit to P2 while the statements in P4 are running."]
    SqlExec,

    #[desc = "The string value P4 of length P1 (bytes) is stored in register P2. If P3 is not zero and the content of register P3 is equal to P5, then the datatype of the register P2 is converted to BLOB. The content is the same sequence of bytes, it is merely interpreted as a BLOB instead of a string, as if it had been CAST. In other words: if( P3!=0 and reg[P3]==P5 ) reg[P2] := CAST(reg[P2] as BLOB)"]
    String,

    #[desc = "Obtain a lock on a particular table. This instruction is only used when the shared-cache feature is enabled. P1 is the index of the database in sqlite3.aDb[] of the database on which the lock is acquired. A readlock is obtained if P3==0 or a write lock if P3==1. P2 contains the root-page of the table to lock. P4 contains a pointer to the name of the table being locked. This is only used to generate an error message if the lock cannot be obtained."]
    TableLock,

    #[desc = "Write P4 on the statement trace output if statement tracing is enabled. Operand P1 must be 0x7fffffff and P2 must positive."]
    Trace,

    #[desc = "Apply affinities to the range of P2 registers beginning with P1. Take the affinities from the Table object in P4. If any value cannot be coerced into the correct type, then raise an error. This opcode is similar to Affinity except that this opcode forces the register type to the Table column type. This is used to implement \"strict affinity\". GENERATED ALWAYS AS ... STATIC columns are only checked if P3 is zero. When P3 is non-zero, no type checking occurs for static generated columns. Virtual columns are computed at query time and so they are never checked. Preconditions:
P2 should be the number of non-virtual columns in the table of P4.
Table P4 should be a STRICT table. If any precondition is false, an assertion fault occurs."]
    TypeCheck,

    #[desc = "Vacuum the entire database P1. P1 is 0 for \"main\", and 2 or more for an attached database. The \"temp\" database may not be vacuumed. If P2 is not zero, then it is a register holding a string which is the file into which the result of vacuum should be written. When P2 is zero, the vacuum overwrites the original database."]
    Vacuum,

    #[desc = "Transfer the values of bound parameter P1 into register P2"]
    Variable,

    #[desc = "P4 may be a pointer to an sqlite3_vtab structure. If so, call the xBegin method for that table. Also, whether or not P4 is set, check that this is not being called from within a callback to a virtual table xSync() method. If it is, the error code will be set to SQLITE_LOCKED."]
    VBegin,

    #[desc = "P4 is a pointer to a Table object that is a virtual table in schema P1 that supports the xIntegrity() method. This opcode runs the xIntegrity() method for that virtual table, using P3 as the integer argument. If an error is reported back, the table name is prepended to the error message and that message is stored in P2. If no errors are seen, register P2 is set to NULL."]
    VCheck,

    #[desc = "Store in register P3 the value of the P2-th column of the current row of the virtual-table of cursor P1. If the VColumn opcode is being used to fetch the value of an unchanging column during an UPDATE operation, then the P5 value is OPFLAG_NOCHNG. This will cause the sqlite3_vtab_nochange() function to return true inside the xColumn method of the virtual table implementation. The P5 column might also contain other bits (OPFLAG_LENGTHARG or OPFLAG_TYPEOFARG) but those bits are unused by VColumn."]
    VColumn,

    #[desc = "P2 is a register that holds the name of a virtual table in database P1. Call the xCreate method for that table."]
    VCreate,

    #[desc = "P4 is the name of a virtual table in database P1. Call the xDestroy method of that table."]
    VDestroy,

    #[desc = "P1 is a cursor opened using VOpen. P2 is an address to jump to if the filtered result set is empty. P4 is either NULL or a string that was generated by the xBestIndex method of the module. The interpretation of the P4 string is left to the module implementation. This opcode invokes the xFilter method on the virtual table specified by P1. The integer query plan parameter to xFilter is stored in register P3. Register P3+1 stores the argc parameter to be passed to the xFilter method. Registers P3+2..P3+1+argc are the argc additional parameters which are passed to xFilter as argv. Register P3+2 becomes argv[0] when passed to xFilter. A jump is made to P2 if the result set after filtering would be empty."]
    VFilter,

    #[desc = "Set register P2 to be a pointer to a ValueList object for cursor P1 with cache register P3 and output register P3+1. This ValueList object can be used as the first argument to sqlite3_vtab_in_first() and sqlite3_vtab_in_next() to extract all of the values stored in the P1 cursor. Register P3 is used to hold the values returned by sqlite3_vtab_in_first() and sqlite3_vtab_in_next()."]
    VInitIn,

    #[desc = "Advance virtual table P1 to the next row in its result set and jump to instruction P2. Or, if the virtual table has reached the end of its result set, then fall through to the next instruction."]
    VNext,

    #[desc = "P4 is a pointer to a virtual table object, an sqlite3_vtab structure. P1 is a cursor number. This opcode opens a cursor to the virtual table and stores that cursor in P1."]
    VOpen,

    #[desc = "P4 is a pointer to a virtual table object, an sqlite3_vtab structure. This opcode invokes the corresponding xRename method. The value in register P1 is passed as the zName argument to the xRename method."]
    VRename,

    #[desc = "P4 is a pointer to a virtual table object, an sqlite3_vtab structure. This opcode invokes the corresponding xUpdate method. P2 values are contiguous memory cells starting at P3 to pass to the xUpdate invocation. The value in register (P3+P2-1) corresponds to the p2th element of the argv array passed to xUpdate. The xUpdate method will do a DELETE or an INSERT or both. The argv[0] element (which corresponds to memory cell P3) is the rowid of a row to delete. If argv[0] is NULL then no deletion occurs. The argv[1] element is the rowid of the new row. This can be NULL to have the virtual table select the new rowid for itself. The subsequent elements in the array are the values of columns in the new row. If P2==1 then no insert is performed. argv[0] is the rowid of a row to delete. P1 is a boolean flag. If it is set to true and the xUpdate call is successful, then the value returned by sqlite3_last_insert_rowid() is set to the value of the rowid for the row just inserted. P5 is the error actions (OE_Replace, OE_Fail, OE_Ignore, etc) to apply in the case of a constraint failure on an insert or update."]
    VUpdate,

    #[desc = "If both registers P1 and P3 are NOT NULL, then store a zero in register P2. If either registers P1 or P3 are NULL then put a NULL in register P2."]
    ZeroOrNull,
}

// Index of insn in list of insns
type InsnReference = usize;

pub enum StepResult<'a> {
    Done,
    IO,
    Row(Record<'a>),
    Interrupt,
}

/// If there is I/O, the instruction is restarted.
/// Evaluate a Result<CursorResult<T>>, if IO return Ok(StepResult::IO).
macro_rules! return_if_io {
    ($expr:expr) => {
        match $expr? {
            CursorResult::Ok(v) => v,
            CursorResult::IO => return Ok(StepResult::IO),
        }
    };
}

struct RegexCache {
    like: HashMap<String, Regex>,
    glob: HashMap<String, Regex>,
}
impl RegexCache {
    fn new() -> Self {
        RegexCache {
            like: HashMap::new(),
            glob: HashMap::new(),
        }
    }
}

/// The program state describes the environment in which the program executes.
pub struct ProgramState {
    pub pc: BranchOffset,
    cursors: RefCell<BTreeMap<CursorID, Box<dyn Cursor>>>,
    registers: Vec<OwnedValue>,
    last_compare: Option<std::cmp::Ordering>,
    deferred_seek: Option<(CursorID, CursorID)>,
    ended_coroutine: bool, // flag to notify yield coroutine finished
    regex_cache: RegexCache,
    interrupted: bool,
}

impl ProgramState {
    pub fn new(max_registers: usize) -> Self {
        let cursors = RefCell::new(BTreeMap::new());
        let mut registers = Vec::with_capacity(max_registers);
        registers.resize(max_registers, OwnedValue::Null);
        Self {
            pc: 0,
            cursors,
            registers,
            last_compare: None,
            deferred_seek: None,
            ended_coroutine: false,
            regex_cache: RegexCache::new(),
            interrupted: false,
        }
    }

    pub fn column_count(&self) -> usize {
        self.registers.len()
    }

    pub fn column(&self, i: usize) -> Option<String> {
        Some(format!("{:?}", self.registers[i]))
    }

    pub fn interrupt(&mut self) {
        self.interrupted = true;
    }

    pub fn is_interrupted(&self) -> bool {
        self.interrupted
    }
}

#[derive(Debug)]
pub struct Program {
    pub max_registers: usize,
    pub insns: Vec<Insn>,
    pub cursor_ref: Vec<(Option<String>, Option<Table>)>,
    pub database_header: Rc<RefCell<DatabaseHeader>>,
    pub comments: HashMap<BranchOffset, &'static str>,
    pub connection: Weak<Connection>,
    pub auto_commit: bool,
}

impl Program {
    pub fn explain(&self) {
        println!("addr  opcode             p1    p2    p3    p4             p5  comment");
        println!("----  -----------------  ----  ----  ----  -------------  --  -------");
        let mut indent_count: usize = 0;
        let indent = "  ";
        let mut prev_insn: Option<&Insn> = None;
        for (addr, insn) in self.insns.iter().enumerate() {
            indent_count = get_indent_count(indent_count, insn, prev_insn);
            print_insn(
                self,
                addr as InsnReference,
                insn,
                indent.repeat(indent_count),
            );
            prev_insn = Some(insn);
        }
    }

    pub fn step<'a>(
        &self,
        state: &'a mut ProgramState,
        pager: Rc<Pager>,
    ) -> Result<StepResult<'a>> {
        loop {
            if state.is_interrupted() {
                return Ok(StepResult::Interrupt);
            }
            let insn = &self.insns[state.pc as usize];
            trace_insn(self, state.pc as InsnReference, insn);
            let mut cursors = state.cursors.borrow_mut();
            match insn {
                Insn::Init { target_pc } => {
                    assert!(*target_pc >= 0);
                    state.pc = *target_pc;
                }
                Insn::Add { lhs, rhs, dest } => {
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let dest = *dest;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (OwnedValue::Integer(lhs), OwnedValue::Integer(rhs)) => {
                            state.registers[dest] = OwnedValue::Integer(lhs + rhs);
                        }
                        (OwnedValue::Float(lhs), OwnedValue::Float(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(lhs + rhs);
                        }
                        (OwnedValue::Float(f), OwnedValue::Integer(i))
                        | (OwnedValue::Integer(i), OwnedValue::Float(f)) => {
                            state.registers[dest] = OwnedValue::Float(*f + *i as f64);
                        }
                        (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        (OwnedValue::Agg(aggctx), other) | (other, OwnedValue::Agg(aggctx)) => {
                            match other {
                                OwnedValue::Null => {
                                    state.registers[dest] = OwnedValue::Null;
                                }
                                OwnedValue::Integer(i) => match aggctx.final_value() {
                                    OwnedValue::Float(acc) => {
                                        state.registers[dest] = OwnedValue::Float(acc + *i as f64);
                                    }
                                    OwnedValue::Integer(acc) => {
                                        state.registers[dest] = OwnedValue::Integer(acc + i);
                                    }
                                    _ => {
                                        todo!("{:?}", aggctx);
                                    }
                                },
                                OwnedValue::Float(f) => match aggctx.final_value() {
                                    OwnedValue::Float(acc) => {
                                        state.registers[dest] = OwnedValue::Float(acc + f);
                                    }
                                    OwnedValue::Integer(acc) => {
                                        state.registers[dest] = OwnedValue::Float(*acc as f64 + f);
                                    }
                                    _ => {
                                        todo!("{:?}", aggctx);
                                    }
                                },
                                OwnedValue::Agg(aggctx2) => {
                                    let acc = aggctx.final_value();
                                    let acc2 = aggctx2.final_value();
                                    match (acc, acc2) {
                                        (OwnedValue::Integer(acc), OwnedValue::Integer(acc2)) => {
                                            state.registers[dest] = OwnedValue::Integer(acc + acc2);
                                        }
                                        (OwnedValue::Float(acc), OwnedValue::Float(acc2)) => {
                                            state.registers[dest] = OwnedValue::Float(acc + acc2);
                                        }
                                        (OwnedValue::Integer(acc), OwnedValue::Float(acc2)) => {
                                            state.registers[dest] =
                                                OwnedValue::Float(*acc as f64 + acc2);
                                        }
                                        (OwnedValue::Float(acc), OwnedValue::Integer(acc2)) => {
                                            state.registers[dest] =
                                                OwnedValue::Float(acc + *acc2 as f64);
                                        }
                                        _ => {
                                            todo!("{:?} {:?}", acc, acc2);
                                        }
                                    }
                                }
                                rest => unimplemented!("{:?}", rest),
                            }
                        }
                        _ => {
                            todo!();
                        }
                    }
                    state.pc += 1;
                }
                Insn::Subtract { lhs, rhs, dest } => {
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let dest = *dest;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (OwnedValue::Integer(lhs), OwnedValue::Integer(rhs)) => {
                            state.registers[dest] = OwnedValue::Integer(lhs - rhs);
                        }
                        (OwnedValue::Float(lhs), OwnedValue::Float(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(lhs - rhs);
                        }
                        (OwnedValue::Float(lhs), OwnedValue::Integer(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(lhs - *rhs as f64);
                        }
                        (OwnedValue::Integer(lhs), OwnedValue::Float(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(*lhs as f64 - rhs);
                        }
                        (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        (OwnedValue::Agg(aggctx), rhs) => match rhs {
                            OwnedValue::Null => {
                                state.registers[dest] = OwnedValue::Null;
                            }
                            OwnedValue::Integer(i) => match aggctx.final_value() {
                                OwnedValue::Float(acc) => {
                                    state.registers[dest] = OwnedValue::Float(acc - *i as f64);
                                }
                                OwnedValue::Integer(acc) => {
                                    state.registers[dest] = OwnedValue::Integer(acc - i);
                                }
                                _ => {
                                    todo!("{:?}", aggctx);
                                }
                            },
                            OwnedValue::Float(f) => match aggctx.final_value() {
                                OwnedValue::Float(acc) => {
                                    state.registers[dest] = OwnedValue::Float(acc - f);
                                }
                                OwnedValue::Integer(acc) => {
                                    state.registers[dest] = OwnedValue::Float(*acc as f64 - f);
                                }
                                _ => {
                                    todo!("{:?}", aggctx);
                                }
                            },
                            OwnedValue::Agg(aggctx2) => {
                                let acc = aggctx.final_value();
                                let acc2 = aggctx2.final_value();
                                match (acc, acc2) {
                                    (OwnedValue::Integer(acc), OwnedValue::Integer(acc2)) => {
                                        state.registers[dest] = OwnedValue::Integer(acc - acc2);
                                    }
                                    (OwnedValue::Float(acc), OwnedValue::Float(acc2)) => {
                                        state.registers[dest] = OwnedValue::Float(acc - acc2);
                                    }
                                    (OwnedValue::Integer(acc), OwnedValue::Float(acc2)) => {
                                        state.registers[dest] =
                                            OwnedValue::Float(*acc as f64 - acc2);
                                    }
                                    (OwnedValue::Float(acc), OwnedValue::Integer(acc2)) => {
                                        state.registers[dest] =
                                            OwnedValue::Float(acc - *acc2 as f64);
                                    }
                                    _ => {
                                        todo!("{:?} {:?}", acc, acc2);
                                    }
                                }
                            }
                            rest => unimplemented!("{:?}", rest),
                        },
                        (lhs, OwnedValue::Agg(aggctx)) => match lhs {
                            OwnedValue::Null => {
                                state.registers[dest] = OwnedValue::Null;
                            }
                            OwnedValue::Integer(i) => match aggctx.final_value() {
                                OwnedValue::Float(acc) => {
                                    state.registers[dest] = OwnedValue::Float(*i as f64 - acc);
                                }
                                OwnedValue::Integer(acc) => {
                                    state.registers[dest] = OwnedValue::Integer(i - acc);
                                }
                                _ => {
                                    todo!("{:?}", aggctx);
                                }
                            },
                            OwnedValue::Float(f) => match aggctx.final_value() {
                                OwnedValue::Float(acc) => {
                                    state.registers[dest] = OwnedValue::Float(f - acc);
                                }
                                OwnedValue::Integer(acc) => {
                                    state.registers[dest] = OwnedValue::Float(f - *acc as f64);
                                }
                                _ => {
                                    todo!("{:?}", aggctx);
                                }
                            },
                            rest => unimplemented!("{:?}", rest),
                        },
                        others => {
                            todo!("{:?}", others);
                        }
                    }
                    state.pc += 1;
                }
                Insn::Multiply { lhs, rhs, dest } => {
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let dest = *dest;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (OwnedValue::Integer(lhs), OwnedValue::Integer(rhs)) => {
                            state.registers[dest] = OwnedValue::Integer(lhs * rhs);
                        }
                        (OwnedValue::Float(lhs), OwnedValue::Float(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(lhs * rhs);
                        }
                        (OwnedValue::Integer(i), OwnedValue::Float(f))
                        | (OwnedValue::Float(f), OwnedValue::Integer(i)) => {
                            state.registers[dest] = OwnedValue::Float(*i as f64 * { *f });
                        }
                        (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        (OwnedValue::Agg(aggctx), other) | (other, OwnedValue::Agg(aggctx)) => {
                            match other {
                                OwnedValue::Null => {
                                    state.registers[dest] = OwnedValue::Null;
                                }
                                OwnedValue::Integer(i) => match aggctx.final_value() {
                                    OwnedValue::Float(acc) => {
                                        state.registers[dest] = OwnedValue::Float(acc * *i as f64);
                                    }
                                    OwnedValue::Integer(acc) => {
                                        state.registers[dest] = OwnedValue::Integer(acc * i);
                                    }
                                    _ => {
                                        todo!("{:?}", aggctx);
                                    }
                                },
                                OwnedValue::Float(f) => match aggctx.final_value() {
                                    OwnedValue::Float(acc) => {
                                        state.registers[dest] = OwnedValue::Float(acc * f);
                                    }
                                    OwnedValue::Integer(acc) => {
                                        state.registers[dest] = OwnedValue::Float(*acc as f64 * f);
                                    }
                                    _ => {
                                        todo!("{:?}", aggctx);
                                    }
                                },
                                OwnedValue::Agg(aggctx2) => {
                                    let acc = aggctx.final_value();
                                    let acc2 = aggctx2.final_value();
                                    match (acc, acc2) {
                                        (OwnedValue::Integer(acc), OwnedValue::Integer(acc2)) => {
                                            state.registers[dest] = OwnedValue::Integer(acc * acc2);
                                        }
                                        (OwnedValue::Float(acc), OwnedValue::Float(acc2)) => {
                                            state.registers[dest] = OwnedValue::Float(acc * acc2);
                                        }
                                        (OwnedValue::Integer(acc), OwnedValue::Float(acc2)) => {
                                            state.registers[dest] =
                                                OwnedValue::Float(*acc as f64 * acc2);
                                        }
                                        (OwnedValue::Float(acc), OwnedValue::Integer(acc2)) => {
                                            state.registers[dest] =
                                                OwnedValue::Float(acc * *acc2 as f64);
                                        }
                                        _ => {
                                            todo!("{:?} {:?}", acc, acc2);
                                        }
                                    }
                                }
                                rest => unimplemented!("{:?}", rest),
                            }
                        }
                        others => {
                            todo!("{:?}", others);
                        }
                    }
                    state.pc += 1;
                }
                Insn::Divide { lhs, rhs, dest } => {
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let dest = *dest;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        // If the divisor is zero, the result is NULL.
                        (_, OwnedValue::Integer(0)) | (_, OwnedValue::Float(0.0)) => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        (OwnedValue::Integer(lhs), OwnedValue::Integer(rhs)) => {
                            state.registers[dest] = OwnedValue::Integer(lhs / rhs);
                        }
                        (OwnedValue::Float(lhs), OwnedValue::Float(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(lhs / rhs);
                        }
                        (OwnedValue::Float(lhs), OwnedValue::Integer(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(lhs / *rhs as f64);
                        }
                        (OwnedValue::Integer(lhs), OwnedValue::Float(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(*lhs as f64 / rhs);
                        }
                        (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        (OwnedValue::Agg(aggctx), rhs) => match rhs {
                            OwnedValue::Null => {
                                state.registers[dest] = OwnedValue::Null;
                            }
                            OwnedValue::Integer(i) => match aggctx.final_value() {
                                OwnedValue::Float(acc) => {
                                    state.registers[dest] = OwnedValue::Float(acc / *i as f64);
                                }
                                OwnedValue::Integer(acc) => {
                                    state.registers[dest] = OwnedValue::Integer(acc / i);
                                }
                                _ => {
                                    todo!("{:?}", aggctx);
                                }
                            },
                            OwnedValue::Float(f) => match aggctx.final_value() {
                                OwnedValue::Float(acc) => {
                                    state.registers[dest] = OwnedValue::Float(acc / f);
                                }
                                OwnedValue::Integer(acc) => {
                                    state.registers[dest] = OwnedValue::Float(*acc as f64 / f);
                                }
                                _ => {
                                    todo!("{:?}", aggctx);
                                }
                            },
                            OwnedValue::Agg(aggctx2) => {
                                let acc = aggctx.final_value();
                                let acc2 = aggctx2.final_value();
                                match (acc, acc2) {
                                    (OwnedValue::Integer(acc), OwnedValue::Integer(acc2)) => {
                                        state.registers[dest] = OwnedValue::Integer(acc / acc2);
                                    }
                                    (OwnedValue::Float(acc), OwnedValue::Float(acc2)) => {
                                        state.registers[dest] = OwnedValue::Float(acc / acc2);
                                    }
                                    (OwnedValue::Integer(acc), OwnedValue::Float(acc2)) => {
                                        state.registers[dest] =
                                            OwnedValue::Float(*acc as f64 / acc2);
                                    }
                                    (OwnedValue::Float(acc), OwnedValue::Integer(acc2)) => {
                                        state.registers[dest] =
                                            OwnedValue::Float(acc / *acc2 as f64);
                                    }
                                    _ => {
                                        todo!("{:?} {:?}", acc, acc2);
                                    }
                                }
                            }
                            rest => unimplemented!("{:?}", rest),
                        },
                        (lhs, OwnedValue::Agg(aggctx)) => match lhs {
                            OwnedValue::Null => {
                                state.registers[dest] = OwnedValue::Null;
                            }
                            OwnedValue::Integer(i) => match aggctx.final_value() {
                                OwnedValue::Float(acc) => {
                                    state.registers[dest] = OwnedValue::Float(*i as f64 / acc);
                                }
                                OwnedValue::Integer(acc) => {
                                    state.registers[dest] = OwnedValue::Integer(i / acc);
                                }
                                _ => {
                                    todo!("{:?}", aggctx);
                                }
                            },
                            OwnedValue::Float(f) => match aggctx.final_value() {
                                OwnedValue::Float(acc) => {
                                    state.registers[dest] = OwnedValue::Float(f / acc);
                                }
                                OwnedValue::Integer(acc) => {
                                    state.registers[dest] = OwnedValue::Float(f / *acc as f64);
                                }
                                _ => {
                                    todo!("{:?}", aggctx);
                                }
                            },
                            rest => unimplemented!("{:?}", rest),
                        },
                        others => {
                            todo!("{:?}", others);
                        }
                    }
                    state.pc += 1;
                }
                Insn::BitAnd { lhs, rhs, dest } => {
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let dest = *dest;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        // handle 0 and null cases
                        (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        (_, OwnedValue::Integer(0))
                        | (OwnedValue::Integer(0), _)
                        | (_, OwnedValue::Float(0.0))
                        | (OwnedValue::Float(0.0), _) => {
                            state.registers[dest] = OwnedValue::Integer(0);
                        }
                        (OwnedValue::Integer(lh), OwnedValue::Integer(rh)) => {
                            state.registers[dest] = OwnedValue::Integer(lh & rh);
                        }
                        (OwnedValue::Float(lh), OwnedValue::Float(rh)) => {
                            state.registers[dest] = OwnedValue::Integer(*lh as i64 & *rh as i64);
                        }
                        (OwnedValue::Float(lh), OwnedValue::Integer(rh)) => {
                            state.registers[dest] = OwnedValue::Integer(*lh as i64 & rh);
                        }
                        (OwnedValue::Integer(lh), OwnedValue::Float(rh)) => {
                            state.registers[dest] = OwnedValue::Integer(lh & *rh as i64);
                        }
                        (OwnedValue::Agg(aggctx), other) | (other, OwnedValue::Agg(aggctx)) => {
                            match other {
                                OwnedValue::Agg(aggctx2) => {
                                    match (aggctx.final_value(), aggctx2.final_value()) {
                                        (OwnedValue::Integer(lh), OwnedValue::Integer(rh)) => {
                                            state.registers[dest] = OwnedValue::Integer(lh & rh);
                                        }
                                        (OwnedValue::Float(lh), OwnedValue::Float(rh)) => {
                                            state.registers[dest] =
                                                OwnedValue::Integer(*lh as i64 & *rh as i64);
                                        }
                                        (OwnedValue::Integer(lh), OwnedValue::Float(rh)) => {
                                            state.registers[dest] =
                                                OwnedValue::Integer(lh & *rh as i64);
                                        }
                                        (OwnedValue::Float(lh), OwnedValue::Integer(rh)) => {
                                            state.registers[dest] =
                                                OwnedValue::Integer(*lh as i64 & rh);
                                        }
                                        _ => {
                                            unimplemented!(
                                                "{:?} {:?}",
                                                aggctx.final_value(),
                                                aggctx2.final_value()
                                            );
                                        }
                                    }
                                }
                                other => match (aggctx.final_value(), other) {
                                    (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                                        state.registers[dest] = OwnedValue::Null;
                                    }
                                    (_, OwnedValue::Integer(0))
                                    | (OwnedValue::Integer(0), _)
                                    | (_, OwnedValue::Float(0.0))
                                    | (OwnedValue::Float(0.0), _) => {
                                        state.registers[dest] = OwnedValue::Integer(0);
                                    }
                                    (OwnedValue::Integer(lh), OwnedValue::Integer(rh)) => {
                                        state.registers[dest] = OwnedValue::Integer(lh & rh);
                                    }
                                    (OwnedValue::Float(lh), OwnedValue::Integer(rh)) => {
                                        state.registers[dest] =
                                            OwnedValue::Integer(*lh as i64 & rh);
                                    }
                                    (OwnedValue::Integer(lh), OwnedValue::Float(rh)) => {
                                        state.registers[dest] =
                                            OwnedValue::Integer(lh & *rh as i64);
                                    }
                                    _ => {
                                        unimplemented!("{:?} {:?}", aggctx.final_value(), other);
                                    }
                                },
                            }
                        }
                        _ => {
                            unimplemented!("{:?} {:?}", state.registers[lhs], state.registers[rhs]);
                        }
                    }
                    state.pc += 1;
                }
                Insn::BitOr { lhs, rhs, dest } => {
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let dest = *dest;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        (OwnedValue::Integer(lh), OwnedValue::Integer(rh)) => {
                            state.registers[dest] = OwnedValue::Integer(lh | rh);
                        }
                        (OwnedValue::Float(lh), OwnedValue::Integer(rh)) => {
                            state.registers[dest] = OwnedValue::Integer(*lh as i64 | rh);
                        }
                        (OwnedValue::Integer(lh), OwnedValue::Float(rh)) => {
                            state.registers[dest] = OwnedValue::Integer(lh | *rh as i64);
                        }
                        (OwnedValue::Float(lh), OwnedValue::Float(rh)) => {
                            state.registers[dest] = OwnedValue::Integer(*lh as i64 | *rh as i64);
                        }
                        (OwnedValue::Agg(aggctx), other) | (other, OwnedValue::Agg(aggctx)) => {
                            match other {
                                OwnedValue::Agg(aggctx2) => {
                                    let final_lhs = aggctx.final_value();
                                    let final_rhs = aggctx2.final_value();
                                    match (final_lhs, final_rhs) {
                                        (OwnedValue::Integer(lh), OwnedValue::Integer(rh)) => {
                                            state.registers[dest] = OwnedValue::Integer(lh | rh);
                                        }
                                        (OwnedValue::Float(lh), OwnedValue::Float(rh)) => {
                                            state.registers[dest] =
                                                OwnedValue::Integer(*lh as i64 | *rh as i64);
                                        }
                                        (OwnedValue::Integer(lh), OwnedValue::Float(rh)) => {
                                            state.registers[dest] =
                                                OwnedValue::Integer(lh | *rh as i64);
                                        }
                                        (OwnedValue::Float(lh), OwnedValue::Integer(rh)) => {
                                            state.registers[dest] =
                                                OwnedValue::Integer(*lh as i64 | rh);
                                        }
                                        _ => {
                                            unimplemented!("{:?} {:?}", final_lhs, final_rhs);
                                        }
                                    }
                                }
                                other => match (aggctx.final_value(), other) {
                                    (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                                        state.registers[dest] = OwnedValue::Null;
                                    }
                                    (OwnedValue::Integer(lh), OwnedValue::Integer(rh)) => {
                                        state.registers[dest] = OwnedValue::Integer(lh | rh);
                                    }
                                    (OwnedValue::Float(lh), OwnedValue::Integer(rh)) => {
                                        state.registers[dest] =
                                            OwnedValue::Integer(*lh as i64 | rh);
                                    }
                                    (OwnedValue::Integer(lh), OwnedValue::Float(rh)) => {
                                        state.registers[dest] =
                                            OwnedValue::Integer(lh | *rh as i64);
                                    }
                                    _ => {
                                        unimplemented!("{:?} {:?}", aggctx.final_value(), other);
                                    }
                                },
                            }
                        }
                        _ => {
                            unimplemented!("{:?} {:?}", state.registers[lhs], state.registers[rhs]);
                        }
                    }
                    state.pc += 1;
                }
                Insn::BitNot { reg, dest } => {
                    let reg = *reg;
                    let dest = *dest;
                    match &state.registers[reg] {
                        OwnedValue::Integer(i) => state.registers[dest] = OwnedValue::Integer(!i),
                        OwnedValue::Float(f) => {
                            state.registers[dest] = OwnedValue::Integer(!{ *f as i64 })
                        }
                        OwnedValue::Null => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        OwnedValue::Agg(aggctx) => match aggctx.final_value() {
                            OwnedValue::Integer(i) => {
                                state.registers[dest] = OwnedValue::Integer(!i);
                            }
                            OwnedValue::Float(f) => {
                                state.registers[dest] = OwnedValue::Integer(!{ *f as i64 });
                            }
                            OwnedValue::Null => {
                                state.registers[dest] = OwnedValue::Null;
                            }
                            _ => unimplemented!("{:?}", aggctx),
                        },
                        _ => {
                            unimplemented!("{:?}", state.registers[reg]);
                        }
                    }
                    state.pc += 1;
                }
                Insn::Null { dest, dest_end } => {
                    if let Some(dest_end) = dest_end {
                        for i in *dest..=*dest_end {
                            state.registers[i] = OwnedValue::Null;
                        }
                    } else {
                        state.registers[*dest] = OwnedValue::Null;
                    }
                    state.pc += 1;
                }
                Insn::NullRow { cursor_id } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.set_null_flag(true);
                    state.pc += 1;
                }
                Insn::Compare {
                    start_reg_a,
                    start_reg_b,
                    count,
                } => {
                    let start_reg_a = *start_reg_a;
                    let start_reg_b = *start_reg_b;
                    let count = *count;

                    if start_reg_a + count > start_reg_b {
                        return Err(LimboError::InternalError(
                            "Compare registers overlap".to_string(),
                        ));
                    }

                    let mut cmp = None;
                    for i in 0..count {
                        let a = &state.registers[start_reg_a + i];
                        let b = &state.registers[start_reg_b + i];
                        cmp = Some(a.cmp(b));
                        if cmp != Some(std::cmp::Ordering::Equal) {
                            break;
                        }
                    }
                    state.last_compare = cmp;
                    state.pc += 1;
                }
                Insn::Jump {
                    target_pc_lt,
                    target_pc_eq,
                    target_pc_gt,
                } => {
                    let cmp = state.last_compare.take();
                    if cmp.is_none() {
                        return Err(LimboError::InternalError(
                            "Jump without compare".to_string(),
                        ));
                    }
                    let target_pc = match cmp.unwrap() {
                        std::cmp::Ordering::Less => *target_pc_lt,
                        std::cmp::Ordering::Equal => *target_pc_eq,
                        std::cmp::Ordering::Greater => *target_pc_gt,
                    };
                    assert!(target_pc >= 0);
                    state.pc = target_pc;
                }
                Insn::Move {
                    source_reg,
                    dest_reg,
                    count,
                } => {
                    let source_reg = *source_reg;
                    let dest_reg = *dest_reg;
                    let count = *count;
                    for i in 0..count {
                        state.registers[dest_reg + i] = std::mem::replace(
                            &mut state.registers[source_reg + i],
                            OwnedValue::Null,
                        );
                    }
                    state.pc += 1;
                }
                Insn::IfPos {
                    reg,
                    target_pc,
                    decrement_by,
                } => {
                    assert!(*target_pc >= 0);
                    let reg = *reg;
                    let target_pc = *target_pc;
                    match &state.registers[reg] {
                        OwnedValue::Integer(n) if *n > 0 => {
                            state.pc = target_pc;
                            state.registers[reg] = OwnedValue::Integer(*n - *decrement_by as i64);
                        }
                        OwnedValue::Integer(_) => {
                            state.pc += 1;
                        }
                        _ => {
                            return Err(LimboError::InternalError(
                                "IfPos: the value in the register is not an integer".into(),
                            ));
                        }
                    }
                }
                Insn::NotNull { reg, target_pc } => {
                    assert!(*target_pc >= 0);
                    let reg = *reg;
                    let target_pc = *target_pc;
                    match &state.registers[reg] {
                        OwnedValue::Null => {
                            state.pc += 1;
                        }
                        _ => {
                            state.pc = target_pc;
                        }
                    }
                }

                Insn::Eq {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc;
                        }
                        _ => {
                            if state.registers[lhs] == state.registers[rhs] {
                                state.pc = target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                    }
                }
                Insn::Ne {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc;
                        }
                        _ => {
                            if state.registers[lhs] != state.registers[rhs] {
                                state.pc = target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                    }
                }
                Insn::Lt {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc;
                        }
                        _ => {
                            if state.registers[lhs] < state.registers[rhs] {
                                state.pc = target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                    }
                }
                Insn::Le {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc;
                        }
                        _ => {
                            if state.registers[lhs] <= state.registers[rhs] {
                                state.pc = target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                    }
                }
                Insn::Gt {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc;
                        }
                        _ => {
                            if state.registers[lhs] > state.registers[rhs] {
                                state.pc = target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                    }
                }
                Insn::Ge {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc;
                        }
                        _ => {
                            if state.registers[lhs] >= state.registers[rhs] {
                                state.pc = target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                    }
                }
                Insn::If {
                    reg,
                    target_pc,
                    null_reg,
                } => {
                    assert!(*target_pc >= 0);
                    if exec_if(&state.registers[*reg], &state.registers[*null_reg], false) {
                        state.pc = *target_pc;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::IfNot {
                    reg,
                    target_pc,
                    null_reg,
                } => {
                    assert!(*target_pc >= 0);
                    if exec_if(&state.registers[*reg], &state.registers[*null_reg], true) {
                        state.pc = *target_pc;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::OpenReadAsync {
                    cursor_id,
                    root_page,
                } => {
                    let cursor = Box::new(BTreeCursor::new(
                        pager.clone(),
                        *root_page,
                        self.database_header.clone(),
                    ));
                    cursors.insert(*cursor_id, cursor);
                    state.pc += 1;
                }
                Insn::OpenReadAwait => {
                    state.pc += 1;
                }
                Insn::OpenPseudo {
                    cursor_id,
                    content_reg: _,
                    num_fields: _,
                } => {
                    let cursor = Box::new(PseudoCursor::new());
                    cursors.insert(*cursor_id, cursor);
                    state.pc += 1;
                }
                Insn::RewindAsync { cursor_id } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    return_if_io!(cursor.rewind());
                    state.pc += 1;
                }
                Insn::LastAsync { cursor_id } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    return_if_io!(cursor.last());
                    state.pc += 1;
                }
                Insn::LastAwait {
                    cursor_id,
                    pc_if_empty,
                } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.wait_for_completion()?;
                    if cursor.is_empty() {
                        state.pc = *pc_if_empty;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::RewindAwait {
                    cursor_id,
                    pc_if_empty,
                } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.wait_for_completion()?;
                    if cursor.is_empty() {
                        state.pc = *pc_if_empty;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::Column {
                    cursor_id,
                    column,
                    dest,
                } => {
                    if let Some((index_cursor_id, table_cursor_id)) = state.deferred_seek.take() {
                        let index_cursor = cursors.get_mut(&index_cursor_id).unwrap();
                        let rowid = index_cursor.rowid()?;
                        let table_cursor = cursors.get_mut(&table_cursor_id).unwrap();
                        match table_cursor.seek(SeekKey::TableRowId(rowid.unwrap()), SeekOp::EQ)? {
                            CursorResult::Ok(_) => {}
                            CursorResult::IO => {
                                state.deferred_seek = Some((index_cursor_id, table_cursor_id));
                                return Ok(StepResult::IO);
                            }
                        }
                    }

                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    if let Some(ref record) = *cursor.record()? {
                        let null_flag = cursor.get_null_flag();
                        state.registers[*dest] = if null_flag {
                            OwnedValue::Null
                        } else {
                            record.values[*column].clone()
                        };
                    } else {
                        state.registers[*dest] = OwnedValue::Null;
                    }
                    state.pc += 1;
                }
                Insn::MakeRecord {
                    start_reg,
                    count,
                    dest_reg,
                } => {
                    let record = make_owned_record(&state.registers, start_reg, count);
                    state.registers[*dest_reg] = OwnedValue::Record(record);
                    state.pc += 1;
                }
                Insn::ResultRow { start_reg, count } => {
                    let record = make_record(&state.registers, start_reg, count);
                    state.pc += 1;
                    return Ok(StepResult::Row(record));
                }
                Insn::NextAsync { cursor_id } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.set_null_flag(false);
                    return_if_io!(cursor.next());
                    state.pc += 1;
                }
                Insn::PrevAsync { cursor_id } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.set_null_flag(false);
                    return_if_io!(cursor.prev());
                    state.pc += 1;
                }
                Insn::PrevAwait {
                    cursor_id,
                    pc_if_next,
                } => {
                    assert!(*pc_if_next >= 0);
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.wait_for_completion()?;
                    if !cursor.is_empty() {
                        state.pc = *pc_if_next;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::NextAwait {
                    cursor_id,
                    pc_if_next,
                } => {
                    assert!(*pc_if_next >= 0);
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.wait_for_completion()?;
                    if !cursor.is_empty() {
                        state.pc = *pc_if_next;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::Halt {
                    err_code,
                    description,
                } => {
                    match *err_code {
                        0 => {}
                        SQLITE_CONSTRAINT_PRIMARYKEY => {
                            return Err(LimboError::Constraint(format!(
                                "UNIQUE constraint failed: {} (19)",
                                description
                            )));
                        }
                        _ => {
                            return Err(LimboError::Constraint(format!(
                                "undocumented halt error code {}",
                                description
                            )));
                        }
                    }
                    log::trace!("Halt auto_commit {}", self.auto_commit);
                    if self.auto_commit {
                        return match pager.end_tx() {
                            Ok(crate::storage::wal::CheckpointStatus::IO) => Ok(StepResult::IO),
                            Ok(crate::storage::wal::CheckpointStatus::Done) => Ok(StepResult::Done),
                            Err(e) => Err(e),
                        };
                    } else {
                        return Ok(StepResult::Done);
                    }
                }
                Insn::Transaction { write } => {
                    let connection = self.connection.upgrade().unwrap();
                    if let Some(db) = connection.db.upgrade() {
                        // TODO(pere): are backpointers good ?? this looks ugly af
                        // upgrade transaction if needed
                        let new_transaction_state =
                            match (db.transaction_state.borrow().clone(), write) {
                                (crate::TransactionState::Write, true) => TransactionState::Write,
                                (crate::TransactionState::Write, false) => TransactionState::Write,
                                (crate::TransactionState::Read, true) => TransactionState::Write,
                                (crate::TransactionState::Read, false) => TransactionState::Read,
                                (crate::TransactionState::None, true) => TransactionState::Read,
                                (crate::TransactionState::None, false) => TransactionState::Read,
                            };
                        // TODO(Pere):
                        //  1. lock wal
                        //  2. lock shared
                        //  3. lock write db if write
                        db.transaction_state.replace(new_transaction_state.clone());
                        if matches!(new_transaction_state, TransactionState::Write) {
                            pager.begin_read_tx()?;
                        } else {
                            pager.begin_write_tx()?;
                        }
                    }
                    state.pc += 1;
                }
                Insn::Goto { target_pc } => {
                    assert!(*target_pc >= 0);
                    state.pc = *target_pc;
                }
                Insn::Gosub {
                    target_pc,
                    return_reg,
                } => {
                    assert!(*target_pc >= 0);
                    state.registers[*return_reg] = OwnedValue::Integer(state.pc + 1);
                    state.pc = *target_pc;
                }
                Insn::Return { return_reg } => {
                    if let OwnedValue::Integer(pc) = state.registers[*return_reg] {
                        if pc < 0 {
                            return Err(LimboError::InternalError(
                                "Return register is negative".to_string(),
                            ));
                        }
                        state.pc = pc;
                    } else {
                        return Err(LimboError::InternalError(
                            "Return register is not an integer".to_string(),
                        ));
                    }
                }
                Insn::Integer { value, dest } => {
                    state.registers[*dest] = OwnedValue::Integer(*value);
                    state.pc += 1;
                }
                Insn::Real { value, dest } => {
                    state.registers[*dest] = OwnedValue::Float(*value);
                    state.pc += 1;
                }
                Insn::RealAffinity { register } => {
                    if let OwnedValue::Integer(i) = &state.registers[*register] {
                        state.registers[*register] = OwnedValue::Float(*i as f64);
                    };
                    state.pc += 1;
                }
                Insn::String8 { value, dest } => {
                    state.registers[*dest] = OwnedValue::build_text(Rc::new(value.into()));
                    state.pc += 1;
                }
                Insn::Blob { value, dest } => {
                    state.registers[*dest] = OwnedValue::Blob(Rc::new(value.clone()));
                    state.pc += 1;
                }
                Insn::RowId { cursor_id, dest } => {
                    if let Some((index_cursor_id, table_cursor_id)) = state.deferred_seek.take() {
                        let index_cursor = cursors.get_mut(&index_cursor_id).unwrap();
                        let rowid = index_cursor.rowid()?;
                        let table_cursor = cursors.get_mut(&table_cursor_id).unwrap();
                        match table_cursor.seek(SeekKey::TableRowId(rowid.unwrap()), SeekOp::EQ)? {
                            CursorResult::Ok(_) => {}
                            CursorResult::IO => {
                                state.deferred_seek = Some((index_cursor_id, table_cursor_id));
                                return Ok(StepResult::IO);
                            }
                        }
                    }

                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    if let Some(ref rowid) = cursor.rowid()? {
                        state.registers[*dest] = OwnedValue::Integer(*rowid as i64);
                    } else {
                        state.registers[*dest] = OwnedValue::Null;
                    }
                    state.pc += 1;
                }
                Insn::SeekRowid {
                    cursor_id,
                    src_reg,
                    target_pc,
                } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    let rowid = match &state.registers[*src_reg] {
                        OwnedValue::Integer(rowid) => *rowid as u64,
                        OwnedValue::Null => {
                            state.pc = *target_pc;
                            continue;
                        }
                        other => {
                            return Err(LimboError::InternalError(
                                format!("SeekRowid: the value in the register is not an integer or NULL: {}", other)
                            ));
                        }
                    };
                    let found = return_if_io!(cursor.seek(SeekKey::TableRowId(rowid), SeekOp::EQ));
                    if !found {
                        state.pc = *target_pc;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::DeferredSeek {
                    index_cursor_id,
                    table_cursor_id,
                } => {
                    state.deferred_seek = Some((*index_cursor_id, *table_cursor_id));
                    state.pc += 1;
                }
                Insn::SeekGE {
                    cursor_id,
                    start_reg,
                    num_regs,
                    target_pc,
                    is_index,
                } => {
                    if *is_index {
                        let cursor = cursors.get_mut(cursor_id).unwrap();
                        let record_from_regs: OwnedRecord =
                            make_owned_record(&state.registers, start_reg, num_regs);
                        let found = return_if_io!(
                            cursor.seek(SeekKey::IndexKey(&record_from_regs), SeekOp::GE)
                        );
                        if !found {
                            state.pc = *target_pc;
                        } else {
                            state.pc += 1;
                        }
                    } else {
                        let cursor = cursors.get_mut(cursor_id).unwrap();
                        let rowid = match &state.registers[*start_reg] {
                            OwnedValue::Null => {
                                // All integer values are greater than null so we just rewind the cursor
                                return_if_io!(cursor.rewind());
                                state.pc += 1;
                                continue;
                            }
                            OwnedValue::Integer(rowid) => *rowid as u64,
                            _ => {
                                return Err(LimboError::InternalError(
                                    "SeekGE: the value in the register is not an integer".into(),
                                ));
                            }
                        };
                        let found =
                            return_if_io!(cursor.seek(SeekKey::TableRowId(rowid), SeekOp::GE));
                        if !found {
                            state.pc = *target_pc;
                        } else {
                            state.pc += 1;
                        }
                    }
                }
                Insn::SeekGT {
                    cursor_id,
                    start_reg,
                    num_regs,
                    target_pc,
                    is_index,
                } => {
                    if *is_index {
                        let cursor = cursors.get_mut(cursor_id).unwrap();
                        let record_from_regs: OwnedRecord =
                            make_owned_record(&state.registers, start_reg, num_regs);
                        let found = return_if_io!(
                            cursor.seek(SeekKey::IndexKey(&record_from_regs), SeekOp::GT)
                        );
                        if !found {
                            state.pc = *target_pc;
                        } else {
                            state.pc += 1;
                        }
                    } else {
                        let cursor = cursors.get_mut(cursor_id).unwrap();
                        let rowid = match &state.registers[*start_reg] {
                            OwnedValue::Null => {
                                // All integer values are greater than null so we just rewind the cursor
                                return_if_io!(cursor.rewind());
                                state.pc += 1;
                                continue;
                            }
                            OwnedValue::Integer(rowid) => *rowid as u64,
                            _ => {
                                return Err(LimboError::InternalError(
                                    "SeekGT: the value in the register is not an integer".into(),
                                ));
                            }
                        };
                        let found =
                            return_if_io!(cursor.seek(SeekKey::TableRowId(rowid), SeekOp::GT));
                        if !found {
                            state.pc = *target_pc;
                        } else {
                            state.pc += 1;
                        }
                    }
                }
                Insn::IdxGE {
                    cursor_id,
                    start_reg,
                    num_regs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    let record_from_regs: OwnedRecord =
                        make_owned_record(&state.registers, start_reg, num_regs);
                    if let Some(ref idx_record) = *cursor.record()? {
                        // omit the rowid from the idx_record, which is the last value
                        if idx_record.values[..idx_record.values.len() - 1]
                            >= *record_from_regs.values
                        {
                            state.pc = *target_pc;
                        } else {
                            state.pc += 1;
                        }
                    } else {
                        state.pc = *target_pc;
                    }
                }
                Insn::IdxGT {
                    cursor_id,
                    start_reg,
                    num_regs,
                    target_pc,
                } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    let record_from_regs: OwnedRecord =
                        make_owned_record(&state.registers, start_reg, num_regs);
                    if let Some(ref idx_record) = *cursor.record()? {
                        // omit the rowid from the idx_record, which is the last value
                        if idx_record.values[..idx_record.values.len() - 1]
                            > *record_from_regs.values
                        {
                            state.pc = *target_pc;
                        } else {
                            state.pc += 1;
                        }
                    } else {
                        state.pc = *target_pc;
                    }
                }
                Insn::DecrJumpZero { reg, target_pc } => {
                    assert!(*target_pc >= 0);
                    match state.registers[*reg] {
                        OwnedValue::Integer(n) => {
                            let n = n - 1;
                            if n == 0 {
                                state.pc = *target_pc;
                            } else {
                                state.registers[*reg] = OwnedValue::Integer(n);
                                state.pc += 1;
                            }
                        }
                        _ => unreachable!("DecrJumpZero on non-integer register"),
                    }
                }
                Insn::AggStep {
                    acc_reg,
                    col,
                    delimiter,
                    func,
                } => {
                    if let OwnedValue::Null = &state.registers[*acc_reg] {
                        state.registers[*acc_reg] = match func {
                            AggFunc::Avg => OwnedValue::Agg(Box::new(AggContext::Avg(
                                OwnedValue::Float(0.0),
                                OwnedValue::Integer(0),
                            ))),
                            AggFunc::Sum => {
                                OwnedValue::Agg(Box::new(AggContext::Sum(OwnedValue::Null)))
                            }
                            AggFunc::Total => {
                                // The result of total() is always a floating point value.
                                // No overflow error is ever raised if any prior input was a floating point value.
                                // Total() never throws an integer overflow.
                                OwnedValue::Agg(Box::new(AggContext::Sum(OwnedValue::Float(0.0))))
                            }
                            AggFunc::Count => {
                                OwnedValue::Agg(Box::new(AggContext::Count(OwnedValue::Integer(0))))
                            }
                            AggFunc::Max => {
                                let col = state.registers[*col].clone();
                                match col {
                                    OwnedValue::Integer(_) => {
                                        OwnedValue::Agg(Box::new(AggContext::Max(None)))
                                    }
                                    OwnedValue::Float(_) => {
                                        OwnedValue::Agg(Box::new(AggContext::Max(None)))
                                    }
                                    OwnedValue::Text(_) => {
                                        OwnedValue::Agg(Box::new(AggContext::Max(None)))
                                    }
                                    _ => {
                                        unreachable!();
                                    }
                                }
                            }
                            AggFunc::Min => {
                                let col = state.registers[*col].clone();
                                match col {
                                    OwnedValue::Integer(_) => {
                                        OwnedValue::Agg(Box::new(AggContext::Min(None)))
                                    }
                                    OwnedValue::Float(_) => {
                                        OwnedValue::Agg(Box::new(AggContext::Min(None)))
                                    }
                                    OwnedValue::Text(_) => {
                                        OwnedValue::Agg(Box::new(AggContext::Min(None)))
                                    }
                                    _ => {
                                        unreachable!();
                                    }
                                }
                            }
                            AggFunc::GroupConcat | AggFunc::StringAgg => {
                                OwnedValue::Agg(Box::new(AggContext::GroupConcat(
                                    OwnedValue::build_text(Rc::new("".to_string())),
                                )))
                            }
                        };
                    }
                    match func {
                        AggFunc::Avg => {
                            let col = state.registers[*col].clone();
                            let OwnedValue::Agg(agg) = state.registers[*acc_reg].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::Avg(acc, count) = agg.borrow_mut() else {
                                unreachable!();
                            };
                            *acc += col;
                            *count += 1;
                        }
                        AggFunc::Sum | AggFunc::Total => {
                            let col = state.registers[*col].clone();
                            let OwnedValue::Agg(agg) = state.registers[*acc_reg].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::Sum(acc) = agg.borrow_mut() else {
                                unreachable!();
                            };
                            *acc += col;
                        }
                        AggFunc::Count => {
                            let OwnedValue::Agg(agg) = state.registers[*acc_reg].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::Count(count) = agg.borrow_mut() else {
                                unreachable!();
                            };
                            *count += 1;
                        }
                        AggFunc::Max => {
                            let col = state.registers[*col].clone();
                            let OwnedValue::Agg(agg) = state.registers[*acc_reg].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::Max(acc) = agg.borrow_mut() else {
                                unreachable!();
                            };

                            match (acc.as_mut(), col) {
                                (None, value) => {
                                    *acc = Some(value);
                                }
                                (
                                    Some(OwnedValue::Integer(ref mut current_max)),
                                    OwnedValue::Integer(value),
                                ) => {
                                    if value > *current_max {
                                        *current_max = value;
                                    }
                                }
                                (
                                    Some(OwnedValue::Float(ref mut current_max)),
                                    OwnedValue::Float(value),
                                ) => {
                                    if value > *current_max {
                                        *current_max = value;
                                    }
                                }
                                (
                                    Some(OwnedValue::Text(ref mut current_max)),
                                    OwnedValue::Text(value),
                                ) => {
                                    if value.value > current_max.value {
                                        *current_max = value;
                                    }
                                }
                                _ => {
                                    eprintln!("Unexpected types in max aggregation");
                                }
                            }
                        }
                        AggFunc::Min => {
                            let col = state.registers[*col].clone();
                            let OwnedValue::Agg(agg) = state.registers[*acc_reg].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::Min(acc) = agg.borrow_mut() else {
                                unreachable!();
                            };

                            match (acc.as_mut(), col) {
                                (None, value) => {
                                    *acc.borrow_mut() = Some(value);
                                }
                                (
                                    Some(OwnedValue::Integer(ref mut current_min)),
                                    OwnedValue::Integer(value),
                                ) => {
                                    if value < *current_min {
                                        *current_min = value;
                                    }
                                }
                                (
                                    Some(OwnedValue::Float(ref mut current_min)),
                                    OwnedValue::Float(value),
                                ) => {
                                    if value < *current_min {
                                        *current_min = value;
                                    }
                                }
                                (
                                    Some(OwnedValue::Text(ref mut current_min)),
                                    OwnedValue::Text(text),
                                ) => {
                                    if text.value < current_min.value {
                                        *current_min = text;
                                    }
                                }
                                _ => {
                                    eprintln!("Unexpected types in min aggregation");
                                }
                            }
                        }
                        AggFunc::GroupConcat | AggFunc::StringAgg => {
                            let col = state.registers[*col].clone();
                            let delimiter = state.registers[*delimiter].clone();
                            let OwnedValue::Agg(agg) = state.registers[*acc_reg].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                                unreachable!();
                            };
                            if acc.to_string().is_empty() {
                                *acc = col;
                            } else {
                                *acc += delimiter;
                                *acc += col;
                            }
                        }
                    };
                    state.pc += 1;
                }
                Insn::AggFinal { register, func } => {
                    match state.registers[*register].borrow_mut() {
                        OwnedValue::Agg(agg) => {
                            match func {
                                AggFunc::Avg => {
                                    let AggContext::Avg(acc, count) = agg.borrow_mut() else {
                                        unreachable!();
                                    };
                                    *acc /= count.clone();
                                }
                                AggFunc::Sum | AggFunc::Total => {}
                                AggFunc::Count => {}
                                AggFunc::Max => {}
                                AggFunc::Min => {}
                                AggFunc::GroupConcat | AggFunc::StringAgg => {}
                            };
                        }
                        OwnedValue::Null => {
                            // when the set is empty
                            match func {
                                AggFunc::Total => {
                                    state.registers[*register] = OwnedValue::Float(0.0);
                                }
                                AggFunc::Count => {
                                    state.registers[*register] = OwnedValue::Integer(0);
                                }
                                _ => {}
                            }
                        }
                        _ => {
                            unreachable!();
                        }
                    };
                    state.pc += 1;
                }
                Insn::SorterOpen {
                    cursor_id,
                    columns: _,
                    order,
                } => {
                    let order = order
                        .values
                        .iter()
                        .map(|v| match v {
                            OwnedValue::Integer(i) => *i == 0,
                            _ => unreachable!(),
                        })
                        .collect();
                    let cursor = Box::new(sorter::Sorter::new(order));
                    cursors.insert(*cursor_id, cursor);
                    state.pc += 1;
                }
                Insn::SorterData {
                    cursor_id,
                    dest_reg,
                    pseudo_cursor: sorter_cursor,
                } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    let record = match *cursor.record()? {
                        Some(ref record) => record.clone(),
                        None => {
                            state.pc += 1;
                            continue;
                        }
                    };
                    state.registers[*dest_reg] = OwnedValue::Record(record.clone());
                    let sorter_cursor = cursors.get_mut(sorter_cursor).unwrap();
                    sorter_cursor.insert(&OwnedValue::Integer(0), &record, false)?; // fix key later
                    state.pc += 1;
                }
                Insn::SorterInsert {
                    cursor_id,
                    record_reg,
                } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    let record = match &state.registers[*record_reg] {
                        OwnedValue::Record(record) => record,
                        _ => unreachable!("SorterInsert on non-record register"),
                    };
                    // TODO: set correct key
                    cursor.insert(&OwnedValue::Integer(0), record, false)?;
                    state.pc += 1;
                }
                Insn::SorterSort {
                    cursor_id,
                    pc_if_empty,
                } => {
                    if let Some(cursor) = cursors.get_mut(cursor_id) {
                        cursor.rewind()?;
                        state.pc += 1;
                    } else {
                        state.pc = *pc_if_empty;
                    }
                }
                Insn::SorterNext {
                    cursor_id,
                    pc_if_next,
                } => {
                    assert!(*pc_if_next >= 0);
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    return_if_io!(cursor.next());
                    if !cursor.is_empty() {
                        state.pc = *pc_if_next;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::Function {
                    constant_mask,
                    func,
                    start_reg,
                    dest,
                } => {
                    let arg_count = func.arg_count;
                    match &func.func {
                        #[cfg(feature = "json")]
                        crate::function::Func::Json(JsonFunc::Json) => {
                            let json_value = &state.registers[*start_reg];
                            let json_str = get_json(json_value);
                            match json_str {
                                Ok(json) => state.registers[*dest] = json,
                                Err(e) => return Err(e),
                            }
                        }
                        #[cfg(feature = "json")]
                        crate::function::Func::Json(JsonFunc::JsonArray) => {
                            let reg_values = state.registers[*start_reg..*start_reg + arg_count]
                                .iter()
                                .collect();

                            let json_array = json_array(reg_values);

                            match json_array {
                                Ok(json) => state.registers[*dest] = json,
                                Err(e) => return Err(e),
                            }
                        }
                        crate::function::Func::Scalar(scalar_func) => match scalar_func {
                            ScalarFunc::Cast => {
                                assert!(arg_count == 2);
                                assert!(*start_reg + 1 < state.registers.len());
                                let reg_value_argument = state.registers[*start_reg].clone();
                                let OwnedValue::Text(reg_value_type) =
                                    state.registers[*start_reg + 1].clone()
                                else {
                                    unreachable!("Cast with non-text type");
                                };
                                let result = exec_cast(&reg_value_argument, &reg_value_type.value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Char => {
                                let reg_values =
                                    state.registers[*start_reg..*start_reg + arg_count].to_vec();
                                state.registers[*dest] = exec_char(reg_values);
                            }
                            ScalarFunc::Coalesce => {}
                            ScalarFunc::Concat => {
                                let result = exec_concat(
                                    &state.registers[*start_reg..*start_reg + arg_count],
                                );
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::ConcatWs => {
                                let result = exec_concat_ws(
                                    &state.registers[*start_reg..*start_reg + arg_count],
                                );
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Glob => {
                                let pattern = &state.registers[*start_reg];
                                let text = &state.registers[*start_reg + 1];
                                let result = match (pattern, text) {
                                    (OwnedValue::Text(pattern), OwnedValue::Text(text)) => {
                                        let cache = if *constant_mask > 0 {
                                            Some(&mut state.regex_cache.glob)
                                        } else {
                                            None
                                        };
                                        OwnedValue::Integer(exec_glob(
                                            cache,
                                            &pattern.value,
                                            &text.value,
                                        )
                                            as i64)
                                    }
                                    _ => {
                                        unreachable!("Like on non-text registers");
                                    }
                                };
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::IfNull => {}
                            ScalarFunc::Iif => {}
                            ScalarFunc::Instr => {
                                let reg_value = &state.registers[*start_reg];
                                let pattern_value = &state.registers[*start_reg + 1];
                                let result = exec_instr(reg_value, pattern_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::LastInsertRowid => {
                                if let Some(conn) = self.connection.upgrade() {
                                    state.registers[*dest] =
                                        OwnedValue::Integer(conn.last_insert_rowid() as i64);
                                } else {
                                    state.registers[*dest] = OwnedValue::Null;
                                }
                            }
                            ScalarFunc::Like => {
                                let pattern = &state.registers[*start_reg];
                                let text = &state.registers[*start_reg + 1];
                                let result = match (pattern, text) {
                                    (OwnedValue::Text(pattern), OwnedValue::Text(text)) => {
                                        let cache = if *constant_mask > 0 {
                                            Some(&mut state.regex_cache.like)
                                        } else {
                                            None
                                        };
                                        OwnedValue::Integer(exec_like(
                                            cache,
                                            &pattern.value,
                                            &text.value,
                                        )
                                            as i64)
                                    }
                                    _ => {
                                        unreachable!("Like on non-text registers");
                                    }
                                };
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Abs
                            | ScalarFunc::Lower
                            | ScalarFunc::Upper
                            | ScalarFunc::Length
                            | ScalarFunc::OctetLength
                            | ScalarFunc::Typeof
                            | ScalarFunc::Unicode
                            | ScalarFunc::Quote
                            | ScalarFunc::RandomBlob
                            | ScalarFunc::Sign
                            | ScalarFunc::Soundex
                            | ScalarFunc::ZeroBlob => {
                                let reg_value = state.registers[*start_reg].borrow_mut();
                                let result = match scalar_func {
                                    ScalarFunc::Sign => exec_sign(reg_value),
                                    ScalarFunc::Abs => exec_abs(reg_value),
                                    ScalarFunc::Lower => exec_lower(reg_value),
                                    ScalarFunc::Upper => exec_upper(reg_value),
                                    ScalarFunc::Length => Some(exec_length(reg_value)),
                                    ScalarFunc::OctetLength => Some(exec_octet_length(reg_value)),
                                    ScalarFunc::Typeof => Some(exec_typeof(reg_value)),
                                    ScalarFunc::Unicode => Some(exec_unicode(reg_value)),
                                    ScalarFunc::Quote => Some(exec_quote(reg_value)),
                                    ScalarFunc::RandomBlob => Some(exec_randomblob(reg_value)),
                                    ScalarFunc::ZeroBlob => Some(exec_zeroblob(reg_value)),
                                    ScalarFunc::Soundex => Some(exec_soundex(reg_value)),
                                    _ => unreachable!(),
                                };
                                state.registers[*dest] = result.unwrap_or(OwnedValue::Null);
                            }
                            ScalarFunc::Hex => {
                                let reg_value = state.registers[*start_reg].borrow_mut();
                                let result = exec_hex(reg_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Unhex => {
                                let reg_value = state.registers[*start_reg].clone();
                                let ignored_chars = state.registers.get(*start_reg + 1);
                                let result = exec_unhex(&reg_value, ignored_chars);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Random => {
                                state.registers[*dest] = exec_random();
                            }
                            ScalarFunc::Trim => {
                                let reg_value = state.registers[*start_reg].clone();
                                let pattern_value = state.registers.get(*start_reg + 1).cloned();
                                let result = exec_trim(&reg_value, pattern_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::LTrim => {
                                let reg_value = state.registers[*start_reg].clone();
                                let pattern_value = state.registers.get(*start_reg + 1).cloned();
                                let result = exec_ltrim(&reg_value, pattern_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::RTrim => {
                                let reg_value = state.registers[*start_reg].clone();
                                let pattern_value = state.registers.get(*start_reg + 1).cloned();
                                let result = exec_rtrim(&reg_value, pattern_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Round => {
                                let reg_value = state.registers[*start_reg].clone();
                                assert!(arg_count == 1 || arg_count == 2);
                                let precision_value = if arg_count > 1 {
                                    Some(state.registers[*start_reg + 1].clone())
                                } else {
                                    None
                                };
                                let result = exec_round(&reg_value, precision_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Min => {
                                let reg_values = state.registers
                                    [*start_reg..*start_reg + arg_count]
                                    .iter()
                                    .collect();
                                state.registers[*dest] = exec_min(reg_values);
                            }
                            ScalarFunc::Max => {
                                let reg_values = state.registers
                                    [*start_reg..*start_reg + arg_count]
                                    .iter()
                                    .collect();
                                state.registers[*dest] = exec_max(reg_values);
                            }
                            ScalarFunc::Nullif => {
                                let first_value = &state.registers[*start_reg];
                                let second_value = &state.registers[*start_reg + 1];
                                state.registers[*dest] = exec_nullif(first_value, second_value);
                            }
                            ScalarFunc::Substr | ScalarFunc::Substring => {
                                let str_value = &state.registers[*start_reg];
                                let start_value = &state.registers[*start_reg + 1];
                                let length_value = &state.registers[*start_reg + 2];
                                let result = exec_substring(str_value, start_value, length_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Date => {
                                let result =
                                    exec_date(&state.registers[*start_reg..*start_reg + arg_count]);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Time => {
                                let result =
                                    exec_time(&state.registers[*start_reg..*start_reg + arg_count]);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::UnixEpoch => {
                                if *start_reg == 0 {
                                    let unixepoch: String = exec_unixepoch(
                                        &OwnedValue::build_text(Rc::new("now".to_string())),
                                    )?;
                                    state.registers[*dest] =
                                        OwnedValue::build_text(Rc::new(unixepoch));
                                } else {
                                    let datetime_value = &state.registers[*start_reg];
                                    let unixepoch = exec_unixepoch(datetime_value);
                                    match unixepoch {
                                        Ok(time) => {
                                            state.registers[*dest] =
                                                OwnedValue::build_text(Rc::new(time))
                                        }
                                        Err(e) => {
                                            return Err(LimboError::ParseError(format!(
                                                "Error encountered while parsing datetime value: {}",
                                                e
                                            )));
                                        }
                                    }
                                }
                            }
                            ScalarFunc::SqliteVersion => {
                                let version_integer: i64 =
                                    DATABASE_VERSION.get().unwrap().parse()?;
                                let version = execute_sqlite_version(version_integer);
                                state.registers[*dest] = OwnedValue::build_text(Rc::new(version));
                            }
                            ScalarFunc::Replace => {
                                assert!(arg_count == 3);
                                let source = &state.registers[*start_reg];
                                let pattern = &state.registers[*start_reg + 1];
                                let replacement = &state.registers[*start_reg + 2];
                                state.registers[*dest] = exec_replace(source, pattern, replacement);
                            }
                        },
                        crate::function::Func::Math(math_func) => match math_func.arity() {
                            MathFuncArity::Nullary => match math_func {
                                MathFunc::Pi => {
                                    state.registers[*dest] =
                                        OwnedValue::Float(std::f64::consts::PI);
                                }
                                _ => {
                                    unreachable!(
                                        "Unexpected mathematical Nullary function {:?}",
                                        math_func
                                    );
                                }
                            },

                            MathFuncArity::Unary => {
                                let reg_value = &state.registers[*start_reg];
                                let result = exec_math_unary(reg_value, math_func);
                                state.registers[*dest] = result;
                            }

                            MathFuncArity::Binary => {
                                let lhs = &state.registers[*start_reg];
                                let rhs = &state.registers[*start_reg + 1];
                                let result = exec_math_binary(lhs, rhs, math_func);
                                state.registers[*dest] = result;
                            }

                            MathFuncArity::UnaryOrBinary => match math_func {
                                MathFunc::Log => {
                                    let result = match arg_count {
                                        1 => {
                                            let arg = &state.registers[*start_reg];
                                            exec_math_log(arg, None)
                                        }
                                        2 => {
                                            let base = &state.registers[*start_reg];
                                            let arg = &state.registers[*start_reg + 1];
                                            exec_math_log(arg, Some(base))
                                        }
                                        _ => unreachable!(
                                            "{:?} function with unexpected number of arguments",
                                            math_func
                                        ),
                                    };
                                    state.registers[*dest] = result;
                                }
                                _ => unreachable!(
                                    "Unexpected mathematical UnaryOrBinary function {:?}",
                                    math_func
                                ),
                            },
                        },
                        crate::function::Func::Agg(_) => {
                            unreachable!("Aggregate functions should not be handled here")
                        }
                    }
                    state.pc += 1;
                }
                Insn::InitCoroutine {
                    yield_reg,
                    jump_on_definition,
                    start_offset,
                } => {
                    state.registers[*yield_reg] = OwnedValue::Integer(*start_offset);
                    state.pc = *jump_on_definition;
                }
                Insn::EndCoroutine { yield_reg } => {
                    if let OwnedValue::Integer(pc) = state.registers[*yield_reg] {
                        state.ended_coroutine = true;
                        state.pc = pc - 1; // yield jump is always next to yield. Here we substract 1 to go back to yield instruction
                    } else {
                        unreachable!();
                    }
                }
                Insn::Yield {
                    yield_reg,
                    end_offset,
                } => {
                    if let OwnedValue::Integer(pc) = state.registers[*yield_reg] {
                        if state.ended_coroutine {
                            state.pc = *end_offset;
                        } else {
                            // swap
                            (state.pc, state.registers[*yield_reg]) =
                                (pc, OwnedValue::Integer(state.pc + 1));
                        }
                    } else {
                        unreachable!();
                    }
                }
                Insn::InsertAsync {
                    cursor,
                    key_reg,
                    record_reg,
                    flag: _,
                } => {
                    let cursor = cursors.get_mut(cursor).unwrap();
                    let record = match &state.registers[*record_reg] {
                        OwnedValue::Record(r) => r,
                        _ => unreachable!("Not a record! Cannot insert a non record value."),
                    };
                    let key = &state.registers[*key_reg];
                    return_if_io!(cursor.insert(key, record, true));
                    state.pc += 1;
                }
                Insn::InsertAwait { cursor_id } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.wait_for_completion()?;
                    // Only update last_insert_rowid for regular table inserts, not schema modifications
                    if cursor.root_page() != 1 {
                        if let Some(rowid) = cursor.rowid()? {
                            if let Some(conn) = self.connection.upgrade() {
                                conn.update_last_rowid(rowid);
                            }
                        }
                    }
                    state.pc += 1;
                }
                Insn::NewRowid {
                    cursor, rowid_reg, ..
                } => {
                    let cursor = cursors.get_mut(cursor).unwrap();
                    // TODO: make io handle rng
                    let rowid = return_if_io!(get_new_rowid(cursor, thread_rng()));
                    state.registers[*rowid_reg] = OwnedValue::Integer(rowid);
                    state.pc += 1;
                }
                Insn::MustBeInt { reg } => {
                    match state.registers[*reg] {
                        OwnedValue::Integer(_) => {}
                        _ => {
                            crate::bail_parse_error!(
                                "MustBeInt: the value in the register is not an integer"
                            );
                        }
                    };
                    state.pc += 1;
                }
                Insn::SoftNull { reg } => {
                    state.registers[*reg] = OwnedValue::Null;
                    state.pc += 1;
                }
                Insn::NotExists {
                    cursor,
                    rowid_reg,
                    target_pc,
                } => {
                    let cursor = cursors.get_mut(cursor).unwrap();
                    let exists = return_if_io!(cursor.exists(&state.registers[*rowid_reg]));
                    if exists {
                        state.pc += 1;
                    } else {
                        state.pc = *target_pc;
                    }
                }
                // this cursor may be reused for next insert
                // Update: tablemoveto is used to travers on not exists, on insert depending on flags if nonseek it traverses again.
                // If not there might be some optimizations obviously.
                Insn::OpenWriteAsync {
                    cursor_id,
                    root_page,
                } => {
                    let cursor = Box::new(BTreeCursor::new(
                        pager.clone(),
                        *root_page,
                        self.database_header.clone(),
                    ));
                    cursors.insert(*cursor_id, cursor);
                    state.pc += 1;
                }
                Insn::OpenWriteAwait {} => {
                    state.pc += 1;
                }
                Insn::Copy {
                    src_reg,
                    dst_reg,
                    amount,
                } => {
                    for i in 0..=*amount {
                        state.registers[*dst_reg + i] = state.registers[*src_reg + i].clone();
                    }
                    state.pc += 1;
                }
                Insn::CreateBtree { db, root, flags: _ } => {
                    if *db > 0 {
                        // TODO: implement temp datbases
                        todo!("temp databases not implemented yet");
                    }
                    let mut cursor = Box::new(BTreeCursor::new(
                        pager.clone(),
                        0,
                        self.database_header.clone(),
                    ));

                    let root_page = cursor.btree_create(1);
                    state.registers[*root] = OwnedValue::Integer(root_page as i64);
                    state.pc += 1;
                }
                Insn::Close { cursor_id } => {
                    cursors.remove(cursor_id);
                    state.pc += 1;
                }
                Insn::IsNull { src, target_pc } => {
                    if matches!(state.registers[*src], OwnedValue::Null) {
                        state.pc = *target_pc;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::ParseSchema {
                    db: _,
                    where_clause,
                } => {
                    let conn = self.connection.upgrade();
                    let conn = conn.as_ref().unwrap();
                    let stmt = conn.prepare(format!(
                        "SELECT * FROM  sqlite_schema WHERE {}",
                        where_clause
                    ))?;
                    let rows = Rows { stmt };
                    let mut schema = RefCell::borrow_mut(&conn.schema);
                    // TODO: This function below is synchronous, make it not async
                    parse_schema_rows(Some(rows), &mut schema, conn.pager.io.clone())?;
                    state.pc += 1;
                }
                _ => {
                    todo!("{:?}", insn);
                }
            }
        }
    }
}

fn get_new_rowid<R: Rng>(cursor: &mut Box<dyn Cursor>, mut rng: R) -> Result<CursorResult<i64>> {
    match cursor.seek_to_last()? {
        CursorResult::Ok(()) => {}
        CursorResult::IO => return Ok(CursorResult::IO),
    }
    let mut rowid = cursor.rowid()?.unwrap_or(0) + 1;
    if rowid > i64::MAX.try_into().unwrap() {
        let distribution = Uniform::from(1..=i64::MAX);
        let max_attempts = 100;
        for count in 0..max_attempts {
            rowid = distribution.sample(&mut rng).try_into().unwrap();
            match cursor.seek(SeekKey::TableRowId(rowid), SeekOp::EQ)? {
                CursorResult::Ok(false) => break, // Found a non-existing rowid
                CursorResult::Ok(true) => {
                    if count == max_attempts - 1 {
                        return Err(LimboError::InternalError(
                            "Failed to generate a new rowid".to_string(),
                        ));
                    } else {
                        continue; // Try next random rowid
                    }
                }
                CursorResult::IO => return Ok(CursorResult::IO),
            }
        }
    }
    Ok(CursorResult::Ok(rowid.try_into().unwrap()))
}

fn make_record<'a>(registers: &'a [OwnedValue], start_reg: &usize, count: &usize) -> Record<'a> {
    let mut values = Vec::with_capacity(*count);
    for r in registers.iter().skip(*start_reg).take(*count) {
        values.push(crate::types::to_value(r))
    }
    Record::new(values)
}

fn make_owned_record(registers: &[OwnedValue], start_reg: &usize, count: &usize) -> OwnedRecord {
    let mut values = Vec::with_capacity(*count);
    for r in registers.iter().skip(*start_reg).take(*count) {
        values.push(r.clone())
    }
    OwnedRecord::new(values)
}

fn trace_insn(program: &Program, addr: InsnReference, insn: &Insn) {
    if !log::log_enabled!(log::Level::Trace) {
        return;
    }
    log::trace!(
        "{}",
        explain::insn_to_str(
            program,
            addr,
            insn,
            String::new(),
            program.comments.get(&(addr as BranchOffset)).copied()
        )
    );
}

fn print_insn(program: &Program, addr: InsnReference, insn: &Insn, indent: String) {
    let s = explain::insn_to_str(
        program,
        addr,
        insn,
        indent,
        program.comments.get(&(addr as BranchOffset)).copied(),
    );
    println!("{}", s);
}

fn get_indent_count(indent_count: usize, curr_insn: &Insn, prev_insn: Option<&Insn>) -> usize {
    let indent_count = if let Some(insn) = prev_insn {
        match insn {
            Insn::RewindAwait { .. }
            | Insn::LastAwait { .. }
            | Insn::SorterSort { .. }
            | Insn::SeekGE { .. }
            | Insn::SeekGT { .. } => indent_count + 1,
            _ => indent_count,
        }
    } else {
        indent_count
    };

    match curr_insn {
        Insn::NextAsync { .. } | Insn::SorterNext { .. } | Insn::PrevAsync { .. } => {
            indent_count - 1
        }
        _ => indent_count,
    }
}

fn exec_lower(reg: &OwnedValue) -> Option<OwnedValue> {
    match reg {
        OwnedValue::Text(t) => Some(OwnedValue::build_text(Rc::new(t.value.to_lowercase()))),
        t => Some(t.to_owned()),
    }
}

fn exec_length(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
            OwnedValue::Integer(reg.to_string().chars().count() as i64)
        }
        OwnedValue::Blob(blob) => OwnedValue::Integer(blob.len() as i64),
        OwnedValue::Agg(aggctx) => exec_length(aggctx.final_value()),
        _ => reg.to_owned(),
    }
}

fn exec_octet_length(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
            OwnedValue::Integer(reg.to_string().into_bytes().len() as i64)
        }
        OwnedValue::Blob(blob) => OwnedValue::Integer(blob.len() as i64),
        OwnedValue::Agg(aggctx) => exec_octet_length(aggctx.final_value()),
        _ => reg.to_owned(),
    }
}

fn exec_upper(reg: &OwnedValue) -> Option<OwnedValue> {
    match reg {
        OwnedValue::Text(t) => Some(OwnedValue::build_text(Rc::new(t.value.to_uppercase()))),
        t => Some(t.to_owned()),
    }
}

fn exec_concat(registers: &[OwnedValue]) -> OwnedValue {
    let mut result = String::new();
    for reg in registers {
        match reg {
            OwnedValue::Text(text) => result.push_str(&text.value),
            OwnedValue::Integer(i) => result.push_str(&i.to_string()),
            OwnedValue::Float(f) => result.push_str(&f.to_string()),
            OwnedValue::Agg(aggctx) => result.push_str(&aggctx.final_value().to_string()),
            OwnedValue::Null => continue,
            OwnedValue::Blob(_) => todo!("TODO concat blob"),
            OwnedValue::Record(_) => unreachable!(),
        }
    }
    OwnedValue::build_text(Rc::new(result))
}

fn exec_concat_ws(registers: &[OwnedValue]) -> OwnedValue {
    if registers.is_empty() {
        return OwnedValue::Null;
    }

    let separator = match &registers[0] {
        OwnedValue::Text(text) => text.value.clone(),
        OwnedValue::Integer(i) => Rc::new(i.to_string()),
        OwnedValue::Float(f) => Rc::new(f.to_string()),
        _ => return OwnedValue::Null,
    };

    let mut result = String::new();
    for (i, reg) in registers.iter().enumerate().skip(1) {
        if i > 1 {
            result.push_str(&separator);
        }
        match reg {
            OwnedValue::Text(text) => result.push_str(&text.value),
            OwnedValue::Integer(i) => result.push_str(&i.to_string()),
            OwnedValue::Float(f) => result.push_str(&f.to_string()),
            _ => continue,
        }
    }

    OwnedValue::build_text(Rc::new(result))
}

fn exec_sign(reg: &OwnedValue) -> Option<OwnedValue> {
    let num = match reg {
        OwnedValue::Integer(i) => *i as f64,
        OwnedValue::Float(f) => *f,
        OwnedValue::Text(s) => {
            if let Ok(i) = s.value.parse::<i64>() {
                i as f64
            } else if let Ok(f) = s.value.parse::<f64>() {
                f
            } else {
                return Some(OwnedValue::Null);
            }
        }
        OwnedValue::Blob(b) => match std::str::from_utf8(b) {
            Ok(s) => {
                if let Ok(i) = s.parse::<i64>() {
                    i as f64
                } else if let Ok(f) = s.parse::<f64>() {
                    f
                } else {
                    return Some(OwnedValue::Null);
                }
            }
            Err(_) => return Some(OwnedValue::Null),
        },
        _ => return Some(OwnedValue::Null),
    };

    let sign = if num > 0.0 {
        1
    } else if num < 0.0 {
        -1
    } else {
        0
    };

    Some(OwnedValue::Integer(sign))
}

/// Generates the Soundex code for a given word
pub fn exec_soundex(reg: &OwnedValue) -> OwnedValue {
    let s = match reg {
        OwnedValue::Null => return OwnedValue::build_text(Rc::new("?000".to_string())),
        OwnedValue::Text(s) => {
            // return ?000 if non ASCII alphabet character is found
            if !s.value.chars().all(|c| c.is_ascii_alphabetic()) {
                return OwnedValue::build_text(Rc::new("?000".to_string()));
            }
            s.clone()
        }
        _ => return OwnedValue::build_text(Rc::new("?000".to_string())), // For unsupported types, return NULL
    };

    // Remove numbers and spaces
    let word: String = s
        .value
        .chars()
        .filter(|c| !c.is_digit(10))
        .collect::<String>()
        .replace(" ", "");
    if word.is_empty() {
        return OwnedValue::build_text(Rc::new("0000".to_string()));
    }

    let soundex_code = |c| match c {
        'b' | 'f' | 'p' | 'v' => Some('1'),
        'c' | 'g' | 'j' | 'k' | 'q' | 's' | 'x' | 'z' => Some('2'),
        'd' | 't' => Some('3'),
        'l' => Some('4'),
        'm' | 'n' => Some('5'),
        'r' => Some('6'),
        _ => None,
    };

    // Convert the word to lowercase for consistent lookups
    let word = word.to_lowercase();
    let first_letter = word.chars().next().unwrap();

    // Remove all occurrences of 'h' and 'w' except the first letter
    let code: String = word
        .chars()
        .skip(1)
        .filter(|&ch| ch != 'h' && ch != 'w')
        .fold(first_letter.to_string(), |mut acc, ch| {
            acc.push(ch);
            acc
        });

    // Replace consonants with digits based on Soundex mapping
    let tmp: String = code
        .chars()
        .map(|ch| match soundex_code(ch) {
            Some(code) => code.to_string(),
            None => ch.to_string(),
        })
        .collect();

    // Remove adjacent same digits
    let tmp = tmp.chars().fold(String::new(), |mut acc, ch| {
        if acc.chars().last() != Some(ch) {
            acc.push(ch);
        }
        acc
    });

    // Remove all occurrences of a, e, i, o, u, y except the first letter
    let mut result = tmp
        .chars()
        .enumerate()
        .filter(|(i, ch)| *i == 0 || !matches!(ch, 'a' | 'e' | 'i' | 'o' | 'u' | 'y'))
        .map(|(_, ch)| ch)
        .collect::<String>();

    // If the first symbol is a digit, replace it with the saved first letter
    if let Some(first_digit) = result.chars().next() {
        if first_digit.is_digit(10) {
            result.replace_range(0..1, &first_letter.to_string());
        }
    }

    // Append zeros if the result contains less than 4 characters
    while result.len() < 4 {
        result.push('0');
    }

    // Retain the first 4 characters and convert to uppercase
    result.truncate(4);
    OwnedValue::build_text(Rc::new(result.to_uppercase()))
}

fn exec_abs(reg: &OwnedValue) -> Option<OwnedValue> {
    match reg {
        OwnedValue::Integer(x) => {
            if x < &0 {
                Some(OwnedValue::Integer(-x))
            } else {
                Some(OwnedValue::Integer(*x))
            }
        }
        OwnedValue::Float(x) => {
            if x < &0.0 {
                Some(OwnedValue::Float(-x))
            } else {
                Some(OwnedValue::Float(*x))
            }
        }
        OwnedValue::Null => Some(OwnedValue::Null),
        _ => Some(OwnedValue::Float(0.0)),
    }
}

fn exec_random() -> OwnedValue {
    let mut buf = [0u8; 8];
    getrandom::getrandom(&mut buf).unwrap();
    let random_number = i64::from_ne_bytes(buf);
    OwnedValue::Integer(random_number)
}

fn exec_randomblob(reg: &OwnedValue) -> OwnedValue {
    let length = match reg {
        OwnedValue::Integer(i) => *i,
        OwnedValue::Float(f) => *f as i64,
        OwnedValue::Text(t) => t.value.parse().unwrap_or(1),
        _ => 1,
    }
    .max(1) as usize;

    let mut blob: Vec<u8> = vec![0; length];
    getrandom::getrandom(&mut blob).expect("Failed to generate random blob");
    OwnedValue::Blob(Rc::new(blob))
}

fn exec_quote(value: &OwnedValue) -> OwnedValue {
    match value {
        OwnedValue::Null => OwnedValue::build_text(OwnedValue::Null.to_string().into()),
        OwnedValue::Integer(_) | OwnedValue::Float(_) => value.to_owned(),
        OwnedValue::Blob(_) => todo!(),
        OwnedValue::Text(s) => {
            let mut quoted = String::with_capacity(s.value.len() + 2);
            quoted.push('\'');
            for c in s.value.chars() {
                if c == '\0' {
                    break;
                } else {
                    quoted.push(c);
                }
            }
            quoted.push('\'');
            OwnedValue::build_text(Rc::new(quoted))
        }
        _ => OwnedValue::Null, // For unsupported types, return NULL
    }
}

fn exec_char(values: Vec<OwnedValue>) -> OwnedValue {
    let result: String = values
        .iter()
        .filter_map(|x| {
            if let OwnedValue::Integer(i) = x {
                Some(*i as u8 as char)
            } else {
                None
            }
        })
        .collect();
    OwnedValue::build_text(Rc::new(result))
}

fn construct_like_regex(pattern: &str) -> Regex {
    let mut regex_pattern = String::from("(?i)^");
    regex_pattern.push_str(&pattern.replace('%', ".*").replace('_', "."));
    regex_pattern.push('$');
    Regex::new(&regex_pattern).unwrap()
}

// Implements LIKE pattern matching. Caches the constructed regex if a cache is provided
fn exec_like(regex_cache: Option<&mut HashMap<String, Regex>>, pattern: &str, text: &str) -> bool {
    if let Some(cache) = regex_cache {
        match cache.get(pattern) {
            Some(re) => re.is_match(text),
            None => {
                let re = construct_like_regex(pattern);
                let res = re.is_match(text);
                cache.insert(pattern.to_string(), re);
                res
            }
        }
    } else {
        let re = construct_like_regex(pattern);
        re.is_match(text)
    }
}

fn construct_glob_regex(pattern: &str) -> Regex {
    let mut regex_pattern = String::from("^");
    regex_pattern.push_str(&pattern.replace('*', ".*").replace("?", "."));
    regex_pattern.push('$');
    Regex::new(&regex_pattern).unwrap()
}

// Implements GLOB pattern matching. Caches the constructed regex if a cache is provided
fn exec_glob(regex_cache: Option<&mut HashMap<String, Regex>>, pattern: &str, text: &str) -> bool {
    if let Some(cache) = regex_cache {
        match cache.get(pattern) {
            Some(re) => re.is_match(text),
            None => {
                let re = construct_glob_regex(pattern);
                let res = re.is_match(text);
                cache.insert(pattern.to_string(), re);
                res
            }
        }
    } else {
        let re = construct_glob_regex(pattern);
        re.is_match(text)
    }
}

fn exec_min(regs: Vec<&OwnedValue>) -> OwnedValue {
    regs.iter()
        .min()
        .map(|&v| v.to_owned())
        .unwrap_or(OwnedValue::Null)
}

fn exec_max(regs: Vec<&OwnedValue>) -> OwnedValue {
    regs.iter()
        .max()
        .map(|&v| v.to_owned())
        .unwrap_or(OwnedValue::Null)
}

fn exec_nullif(first_value: &OwnedValue, second_value: &OwnedValue) -> OwnedValue {
    if first_value != second_value {
        first_value.clone()
    } else {
        OwnedValue::Null
    }
}

fn exec_substring(
    str_value: &OwnedValue,
    start_value: &OwnedValue,
    length_value: &OwnedValue,
) -> OwnedValue {
    if let (OwnedValue::Text(str), OwnedValue::Integer(start), OwnedValue::Integer(length)) =
        (str_value, start_value, length_value)
    {
        let start = *start as usize;
        let str_len = str.value.len();

        if start > str_len {
            return OwnedValue::build_text(Rc::new("".to_string()));
        }

        let start_idx = start - 1;
        let end = if *length != -1 {
            start_idx + *length as usize
        } else {
            str_len
        };
        let substring = &str.value[start_idx..end.min(str_len)];

        OwnedValue::build_text(Rc::new(substring.to_string()))
    } else if let (OwnedValue::Text(str), OwnedValue::Integer(start)) = (str_value, start_value) {
        let start = *start as usize;
        let str_len = str.value.len();

        if start > str_len {
            return OwnedValue::build_text(Rc::new("".to_string()));
        }

        let start_idx = start - 1;
        let substring = &str.value[start_idx..str_len];

        OwnedValue::build_text(Rc::new(substring.to_string()))
    } else {
        OwnedValue::Null
    }
}

fn exec_instr(reg: &OwnedValue, pattern: &OwnedValue) -> OwnedValue {
    if reg == &OwnedValue::Null || pattern == &OwnedValue::Null {
        return OwnedValue::Null;
    }

    if let (OwnedValue::Blob(reg), OwnedValue::Blob(pattern)) = (reg, pattern) {
        let result = reg
            .windows(pattern.len())
            .position(|window| window == **pattern)
            .map_or(0, |i| i + 1);
        return OwnedValue::Integer(result as i64);
    }

    let reg_str;
    let reg = match reg {
        OwnedValue::Text(s) => s.value.as_str(),
        _ => {
            reg_str = reg.to_string();
            reg_str.as_str()
        }
    };

    let pattern_str;
    let pattern = match pattern {
        OwnedValue::Text(s) => s.value.as_str(),
        _ => {
            pattern_str = pattern.to_string();
            pattern_str.as_str()
        }
    };

    match reg.find(pattern) {
        Some(position) => OwnedValue::Integer(position as i64 + 1),
        None => OwnedValue::Integer(0),
    }
}

fn exec_typeof(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Null => OwnedValue::build_text(Rc::new("null".to_string())),
        OwnedValue::Integer(_) => OwnedValue::build_text(Rc::new("integer".to_string())),
        OwnedValue::Float(_) => OwnedValue::build_text(Rc::new("real".to_string())),
        OwnedValue::Text(_) => OwnedValue::build_text(Rc::new("text".to_string())),
        OwnedValue::Blob(_) => OwnedValue::build_text(Rc::new("blob".to_string())),
        OwnedValue::Agg(ctx) => exec_typeof(ctx.final_value()),
        OwnedValue::Record(_) => unimplemented!(),
    }
}

fn exec_hex(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Text(_)
        | OwnedValue::Integer(_)
        | OwnedValue::Float(_)
        | OwnedValue::Blob(_) => {
            let text = reg.to_string();
            OwnedValue::build_text(Rc::new(hex::encode_upper(text)))
        }
        _ => OwnedValue::Null,
    }
}

fn exec_unhex(reg: &OwnedValue, ignored_chars: Option<&OwnedValue>) -> OwnedValue {
    match reg {
        OwnedValue::Null => OwnedValue::Null,
        _ => match ignored_chars {
            None => match hex::decode(reg.to_string()) {
                Ok(bytes) => OwnedValue::Blob(Rc::new(bytes)),
                Err(_) => OwnedValue::Null,
            },
            Some(ignore) => match ignore {
                OwnedValue::Text(_) => {
                    let pat = ignore.to_string();
                    let trimmed = reg
                        .to_string()
                        .trim_start_matches(|x| pat.contains(x))
                        .trim_end_matches(|x| pat.contains(x))
                        .to_string();
                    match hex::decode(trimmed) {
                        Ok(bytes) => OwnedValue::Blob(Rc::new(bytes)),
                        Err(_) => OwnedValue::Null,
                    }
                }
                _ => OwnedValue::Null,
            },
        },
    }
}

fn exec_unicode(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Text(_)
        | OwnedValue::Integer(_)
        | OwnedValue::Float(_)
        | OwnedValue::Blob(_) => {
            let text = reg.to_string();
            if let Some(first_char) = text.chars().next() {
                OwnedValue::Integer(first_char as u32 as i64)
            } else {
                OwnedValue::Null
            }
        }
        _ => OwnedValue::Null,
    }
}

fn _to_float(reg: &OwnedValue) -> f64 {
    match reg {
        OwnedValue::Text(x) => x.value.parse().unwrap_or(0.0),
        OwnedValue::Integer(x) => *x as f64,
        OwnedValue::Float(x) => *x,
        _ => 0.0,
    }
}

fn exec_round(reg: &OwnedValue, precision: Option<OwnedValue>) -> OwnedValue {
    let precision = match precision {
        Some(OwnedValue::Text(x)) => x.value.parse().unwrap_or(0.0),
        Some(OwnedValue::Integer(x)) => x as f64,
        Some(OwnedValue::Float(x)) => x,
        Some(OwnedValue::Null) => return OwnedValue::Null,
        _ => 0.0,
    };

    let reg = match reg {
        OwnedValue::Agg(ctx) => _to_float(ctx.final_value()),
        _ => _to_float(reg),
    };

    let precision = if precision < 1.0 { 0.0 } else { precision };
    let multiplier = 10f64.powi(precision as i32);
    OwnedValue::Float(((reg * multiplier).round()) / multiplier)
}

// Implements TRIM pattern matching.
fn exec_trim(reg: &OwnedValue, pattern: Option<OwnedValue>) -> OwnedValue {
    match (reg, pattern) {
        (reg, Some(pattern)) => match reg {
            OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
                let pattern_chars: Vec<char> = pattern.to_string().chars().collect();
                OwnedValue::build_text(Rc::new(
                    reg.to_string().trim_matches(&pattern_chars[..]).to_string(),
                ))
            }
            _ => reg.to_owned(),
        },
        (OwnedValue::Text(t), None) => OwnedValue::build_text(Rc::new(t.value.trim().to_string())),
        (reg, _) => reg.to_owned(),
    }
}

// Implements LTRIM pattern matching.
fn exec_ltrim(reg: &OwnedValue, pattern: Option<OwnedValue>) -> OwnedValue {
    match (reg, pattern) {
        (reg, Some(pattern)) => match reg {
            OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
                let pattern_chars: Vec<char> = pattern.to_string().chars().collect();
                OwnedValue::build_text(Rc::new(
                    reg.to_string()
                        .trim_start_matches(&pattern_chars[..])
                        .to_string(),
                ))
            }
            _ => reg.to_owned(),
        },
        (OwnedValue::Text(t), None) => {
            OwnedValue::build_text(Rc::new(t.value.trim_start().to_string()))
        }
        (reg, _) => reg.to_owned(),
    }
}

// Implements RTRIM pattern matching.
fn exec_rtrim(reg: &OwnedValue, pattern: Option<OwnedValue>) -> OwnedValue {
    match (reg, pattern) {
        (reg, Some(pattern)) => match reg {
            OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
                let pattern_chars: Vec<char> = pattern.to_string().chars().collect();
                OwnedValue::build_text(Rc::new(
                    reg.to_string()
                        .trim_end_matches(&pattern_chars[..])
                        .to_string(),
                ))
            }
            _ => reg.to_owned(),
        },
        (OwnedValue::Text(t), None) => {
            OwnedValue::build_text(Rc::new(t.value.trim_end().to_string()))
        }
        (reg, _) => reg.to_owned(),
    }
}

fn exec_zeroblob(req: &OwnedValue) -> OwnedValue {
    let length: i64 = match req {
        OwnedValue::Integer(i) => *i,
        OwnedValue::Float(f) => *f as i64,
        OwnedValue::Text(s) => s.value.parse().unwrap_or(0),
        _ => 0,
    };
    OwnedValue::Blob(Rc::new(vec![0; length.max(0) as usize]))
}

// exec_if returns whether you should jump
fn exec_if(reg: &OwnedValue, null_reg: &OwnedValue, not: bool) -> bool {
    match reg {
        OwnedValue::Integer(0) | OwnedValue::Float(0.0) => not,
        OwnedValue::Integer(_) | OwnedValue::Float(_) => !not,
        OwnedValue::Null => match null_reg {
            OwnedValue::Integer(0) | OwnedValue::Float(0.0) => false,
            OwnedValue::Integer(_) | OwnedValue::Float(_) => true,
            _ => false,
        },
        _ => false,
    }
}

fn exec_cast(value: &OwnedValue, datatype: &str) -> OwnedValue {
    if matches!(value, OwnedValue::Null) {
        return OwnedValue::Null;
    }
    match affinity(datatype) {
        // NONE	Casting a value to a type-name with no affinity causes the value to be converted into a BLOB. Casting to a BLOB consists of first casting the value to TEXT in the encoding of the database connection, then interpreting the resulting byte sequence as a BLOB instead of as TEXT.
        // Historically called NONE, but it's the same as BLOB
        Affinity::Blob => {
            // Convert to TEXT first, then interpret as BLOB
            // TODO: handle encoding
            let text = value.to_string();
            OwnedValue::Blob(Rc::new(text.into_bytes()))
        }
        // TEXT To cast a BLOB value to TEXT, the sequence of bytes that make up the BLOB is interpreted as text encoded using the database encoding.
        // Casting an INTEGER or REAL value into TEXT renders the value as if via sqlite3_snprintf() except that the resulting TEXT uses the encoding of the database connection.
        Affinity::Text => {
            // Convert everything to text representation
            // TODO: handle encoding and whatever sqlite3_snprintf does
            OwnedValue::build_text(Rc::new(value.to_string()))
        }
        Affinity::Real => match value {
            OwnedValue::Blob(b) => {
                // Convert BLOB to TEXT first
                let text = String::from_utf8_lossy(b);
                cast_text_to_real(&text)
            }
            OwnedValue::Text(t) => cast_text_to_real(&t.value),
            OwnedValue::Integer(i) => OwnedValue::Float(*i as f64),
            OwnedValue::Float(f) => OwnedValue::Float(*f),
            _ => OwnedValue::Float(0.0),
        },
        Affinity::Integer => match value {
            OwnedValue::Blob(b) => {
                // Convert BLOB to TEXT first
                let text = String::from_utf8_lossy(b);
                cast_text_to_integer(&text)
            }
            OwnedValue::Text(t) => cast_text_to_integer(&t.value),
            OwnedValue::Integer(i) => OwnedValue::Integer(*i),
            // A cast of a REAL value into an INTEGER results in the integer between the REAL value and zero
            // that is closest to the REAL value. If a REAL is greater than the greatest possible signed integer (+9223372036854775807)
            // then the result is the greatest possible signed integer and if the REAL is less than the least possible signed integer (-9223372036854775808)
            // then the result is the least possible signed integer.
            OwnedValue::Float(f) => {
                let i = f.floor() as i128;
                if i > i64::MAX as i128 {
                    OwnedValue::Integer(i64::MAX)
                } else if i < i64::MIN as i128 {
                    OwnedValue::Integer(i64::MIN)
                } else {
                    OwnedValue::Integer(i as i64)
                }
            }
            _ => OwnedValue::Integer(0),
        },
        Affinity::Numeric => match value {
            OwnedValue::Blob(b) => {
                let text = String::from_utf8_lossy(b);
                cast_text_to_numeric(&text)
            }
            OwnedValue::Text(t) => cast_text_to_numeric(&t.value),
            OwnedValue::Integer(i) => OwnedValue::Integer(*i),
            OwnedValue::Float(f) => OwnedValue::Float(*f),
            _ => value.clone(), // TODO probably wrong
        },
    }
}

fn exec_replace(source: &OwnedValue, pattern: &OwnedValue, replacement: &OwnedValue) -> OwnedValue {
    // The replace(X,Y,Z) function returns a string formed by substituting string Z for every occurrence of
    // string Y in string X. The BINARY collating sequence is used for comparisons. If Y is an empty string
    // then return X unchanged. If Z is not initially a string, it is cast to a UTF-8 string prior to processing.

    // If any of the arguments is NULL, the result is NULL.
    if matches!(source, OwnedValue::Null)
        || matches!(pattern, OwnedValue::Null)
        || matches!(replacement, OwnedValue::Null)
    {
        return OwnedValue::Null;
    }

    let source = exec_cast(source, "TEXT");
    let pattern = exec_cast(pattern, "TEXT");
    let replacement = exec_cast(replacement, "TEXT");

    // If any of the casts failed, panic as text casting is not expected to fail.
    match (&source, &pattern, &replacement) {
        (OwnedValue::Text(source), OwnedValue::Text(pattern), OwnedValue::Text(replacement)) => {
            if pattern.value.is_empty() {
                return OwnedValue::build_text(source.value.clone());
            }

            let result = source
                .value
                .replace(pattern.value.as_str(), &replacement.value);
            OwnedValue::build_text(Rc::new(result))
        }
        _ => unreachable!("text cast should never fail"),
    }
}

enum Affinity {
    Integer,
    Text,
    Blob,
    Real,
    Numeric,
}

/// For tables not declared as STRICT, the affinity of a column is determined by the declared type of the column, according to the following rules in the order shown:
/// If the declared type contains the string "INT" then it is assigned INTEGER affinity.
/// If the declared type of the column contains any of the strings "CHAR", "CLOB", or "TEXT" then that column has TEXT affinity. Notice that the type VARCHAR contains the string "CHAR" and is thus assigned TEXT affinity.
/// If the declared type for a column contains the string "BLOB" or if no type is specified then the column has affinity BLOB.
/// If the declared type for a column contains any of the strings "REAL", "FLOA", or "DOUB" then the column has REAL affinity.
/// Otherwise, the affinity is NUMERIC.
/// Note that the order of the rules for determining column affinity is important. A column whose declared type is "CHARINT" will match both rules 1 and 2 but the first rule takes precedence and so the column affinity will be INTEGER.
fn affinity(datatype: &str) -> Affinity {
    // Note: callers of this function must ensure that the datatype is uppercase.
    // Rule 1: INT -> INTEGER affinity
    if datatype.contains("INT") {
        return Affinity::Integer;
    }

    // Rule 2: CHAR/CLOB/TEXT -> TEXT affinity
    if datatype.contains("CHAR") || datatype.contains("CLOB") || datatype.contains("TEXT") {
        return Affinity::Text;
    }

    // Rule 3: BLOB or empty -> BLOB affinity (historically called NONE)
    if datatype.contains("BLOB") || datatype.is_empty() {
        return Affinity::Blob;
    }

    // Rule 4: REAL/FLOA/DOUB -> REAL affinity
    if datatype.contains("REAL") || datatype.contains("FLOA") || datatype.contains("DOUB") {
        return Affinity::Real;
    }

    // Rule 5: Otherwise -> NUMERIC affinity
    Affinity::Numeric
}

/// When casting a TEXT value to INTEGER, the longest possible prefix of the value that can be interpreted as an integer number
/// is extracted from the TEXT value and the remainder ignored. Any leading spaces in the TEXT value when converting from TEXT to INTEGER are ignored.
/// If there is no prefix that can be interpreted as an integer number, the result of the conversion is 0.
/// If the prefix integer is greater than +9223372036854775807 then the result of the cast is exactly +9223372036854775807.
/// Similarly, if the prefix integer is less than -9223372036854775808 then the result of the cast is exactly -9223372036854775808.
/// When casting to INTEGER, if the text looks like a floating point value with an exponent, the exponent will be ignored
/// because it is no part of the integer prefix. For example, "CAST('123e+5' AS INTEGER)" results in 123, not in 12300000.
/// The CAST operator understands decimal integers only — conversion of hexadecimal integers stops at the "x" in the "0x" prefix of the hexadecimal integer string and thus result of the CAST is always zero.
fn cast_text_to_integer(text: &str) -> OwnedValue {
    let text = text.trim();
    if let Ok(i) = text.parse::<i64>() {
        return OwnedValue::Integer(i);
    }
    // Try to find longest valid prefix that parses as an integer
    // TODO: inefficient
    let mut end_index = text.len() - 1;
    while end_index > 0 {
        if let Ok(i) = text[..=end_index].parse::<i64>() {
            return OwnedValue::Integer(i);
        }
        end_index -= 1;
    }
    OwnedValue::Integer(0)
}

/// When casting a TEXT value to REAL, the longest possible prefix of the value that can be interpreted
/// as a real number is extracted from the TEXT value and the remainder ignored. Any leading spaces in
/// the TEXT value are ignored when converging from TEXT to REAL.
/// If there is no prefix that can be interpreted as a real number, the result of the conversion is 0.0.
fn cast_text_to_real(text: &str) -> OwnedValue {
    let trimmed = text.trim_start();
    if let Ok(num) = trimmed.parse::<f64>() {
        return OwnedValue::Float(num);
    }
    // Try to find longest valid prefix that parses as a float
    // TODO: inefficient
    let mut end_index = trimmed.len() - 1;
    while end_index > 0 {
        if let Ok(num) = trimmed[..=end_index].parse::<f64>() {
            return OwnedValue::Float(num);
        }
        end_index -= 1;
    }
    OwnedValue::Float(0.0)
}

/// NUMERIC Casting a TEXT or BLOB value into NUMERIC yields either an INTEGER or a REAL result.
/// If the input text looks like an integer (there is no decimal point nor exponent) and the value
/// is small enough to fit in a 64-bit signed integer, then the result will be INTEGER.
/// Input text that looks like floating point (there is a decimal point and/or an exponent)
/// and the text describes a value that can be losslessly converted back and forth between IEEE 754
/// 64-bit float and a 51-bit signed integer, then the result is INTEGER. (In the previous sentence,
/// a 51-bit integer is specified since that is one bit less than the length of the mantissa of an
/// IEEE 754 64-bit float and thus provides a 1-bit of margin for the text-to-float conversion operation.)
/// Any text input that describes a value outside the range of a 64-bit signed integer yields a REAL result.
/// Casting a REAL or INTEGER value to NUMERIC is a no-op, even if a real value could be losslessly converted to an integer.
fn cast_text_to_numeric(text: &str) -> OwnedValue {
    if !text.contains('.') && !text.contains('e') && !text.contains('E') {
        // Looks like an integer
        if let Ok(i) = text.parse::<i64>() {
            return OwnedValue::Integer(i);
        }
    }
    // Try as float
    if let Ok(f) = text.parse::<f64>() {
        // Check if can be losslessly converted to 51-bit integer
        let i = f as i64;
        if f == i as f64 && i.abs() < (1i64 << 51) {
            return OwnedValue::Integer(i);
        }
        return OwnedValue::Float(f);
    }
    OwnedValue::Integer(0)
}

fn execute_sqlite_version(version_integer: i64) -> String {
    let major = version_integer / 1_000_000;
    let minor = (version_integer % 1_000_000) / 1_000;
    let release = version_integer % 1_000;

    format!("{}.{}.{}", major, minor, release)
}

fn to_f64(reg: &OwnedValue) -> Option<f64> {
    match reg {
        OwnedValue::Integer(i) => Some(*i as f64),
        OwnedValue::Float(f) => Some(*f),
        OwnedValue::Text(t) => t.value.parse::<f64>().ok(),
        OwnedValue::Agg(ctx) => to_f64(ctx.final_value()),
        _ => None,
    }
}

fn exec_math_unary(reg: &OwnedValue, function: &MathFunc) -> OwnedValue {
    // In case of some functions and integer input, return the input as is
    if let OwnedValue::Integer(_) = reg {
        if matches! { function, MathFunc::Ceil | MathFunc::Ceiling | MathFunc::Floor | MathFunc::Trunc }
        {
            return reg.clone();
        }
    }

    let f = match to_f64(reg) {
        Some(f) => f,
        None => return OwnedValue::Null,
    };

    let result = match function {
        MathFunc::Acos => f.acos(),
        MathFunc::Acosh => f.acosh(),
        MathFunc::Asin => f.asin(),
        MathFunc::Asinh => f.asinh(),
        MathFunc::Atan => f.atan(),
        MathFunc::Atanh => f.atanh(),
        MathFunc::Ceil | MathFunc::Ceiling => f.ceil(),
        MathFunc::Cos => f.cos(),
        MathFunc::Cosh => f.cosh(),
        MathFunc::Degrees => f.to_degrees(),
        MathFunc::Exp => f.exp(),
        MathFunc::Floor => f.floor(),
        MathFunc::Ln => f.ln(),
        MathFunc::Log10 => f.log10(),
        MathFunc::Log2 => f.log2(),
        MathFunc::Radians => f.to_radians(),
        MathFunc::Sin => f.sin(),
        MathFunc::Sinh => f.sinh(),
        MathFunc::Sqrt => f.sqrt(),
        MathFunc::Tan => f.tan(),
        MathFunc::Tanh => f.tanh(),
        MathFunc::Trunc => f.trunc(),
        _ => unreachable!("Unexpected mathematical unary function {:?}", function),
    };

    if result.is_nan() {
        OwnedValue::Null
    } else {
        OwnedValue::Float(result)
    }
}

fn exec_math_binary(lhs: &OwnedValue, rhs: &OwnedValue, function: &MathFunc) -> OwnedValue {
    let lhs = match to_f64(lhs) {
        Some(f) => f,
        None => return OwnedValue::Null,
    };

    let rhs = match to_f64(rhs) {
        Some(f) => f,
        None => return OwnedValue::Null,
    };

    let result = match function {
        MathFunc::Atan2 => lhs.atan2(rhs),
        MathFunc::Mod => lhs % rhs,
        MathFunc::Pow | MathFunc::Power => lhs.powf(rhs),
        _ => unreachable!("Unexpected mathematical binary function {:?}", function),
    };

    if result.is_nan() {
        OwnedValue::Null
    } else {
        OwnedValue::Float(result)
    }
}

fn exec_math_log(arg: &OwnedValue, base: Option<&OwnedValue>) -> OwnedValue {
    let f = match to_f64(arg) {
        Some(f) => f,
        None => return OwnedValue::Null,
    };

    let base = match base {
        Some(base) => match to_f64(base) {
            Some(f) => f,
            None => return OwnedValue::Null,
        },
        None => 10.0,
    };

    if f <= 0.0 || base <= 0.0 || base == 1.0 {
        return OwnedValue::Null;
    }

    OwnedValue::Float(f.log(base))
}

#[cfg(test)]
mod tests {

    use crate::{
        types::{SeekKey, SeekOp},
        vdbe::exec_replace,
    };

    use super::{
        exec_abs, exec_char, exec_hex, exec_if, exec_instr, exec_length, exec_like, exec_lower,
        exec_ltrim, exec_max, exec_min, exec_nullif, exec_quote, exec_random, exec_randomblob,
        exec_round, exec_rtrim, exec_sign, exec_soundex, exec_substring, exec_trim, exec_typeof,
        exec_unhex, exec_unicode, exec_upper, exec_zeroblob, execute_sqlite_version, get_new_rowid,
        AggContext, Cursor, CursorResult, LimboError, OwnedRecord, OwnedValue, Result,
    };
    use mockall::{mock, predicate};
    use rand::{rngs::mock::StepRng, thread_rng};
    use std::{cell::Ref, collections::HashMap, rc::Rc};

    mock! {
        Cursor {
            fn seek_to_last(&mut self) -> Result<CursorResult<()>>;
            fn seek<'a>(&mut self, key: SeekKey<'a>, op: SeekOp) -> Result<CursorResult<bool>>;
            fn rowid(&self) -> Result<Option<u64>>;
            fn seek_rowid(&mut self, rowid: u64) -> Result<CursorResult<bool>>;
        }
    }

    impl Cursor for MockCursor {
        fn root_page(&self) -> usize {
            unreachable!()
        }

        fn seek_to_last(&mut self) -> Result<CursorResult<()>> {
            self.seek_to_last()
        }

        fn rowid(&self) -> Result<Option<u64>> {
            self.rowid()
        }

        fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> Result<CursorResult<bool>> {
            self.seek(key, op)
        }

        fn rewind(&mut self) -> Result<CursorResult<()>> {
            unimplemented!()
        }

        fn next(&mut self) -> Result<CursorResult<()>> {
            unimplemented!()
        }

        fn record(&self) -> Result<Ref<Option<OwnedRecord>>> {
            unimplemented!()
        }

        fn is_empty(&self) -> bool {
            unimplemented!()
        }

        fn set_null_flag(&mut self, _flag: bool) {
            unimplemented!()
        }

        fn get_null_flag(&self) -> bool {
            unimplemented!()
        }

        fn insert(
            &mut self,
            _key: &OwnedValue,
            _record: &OwnedRecord,
            _is_leaf: bool,
        ) -> Result<CursorResult<()>> {
            unimplemented!()
        }

        fn wait_for_completion(&mut self) -> Result<()> {
            unimplemented!()
        }

        fn exists(&mut self, _key: &OwnedValue) -> Result<CursorResult<bool>> {
            unimplemented!()
        }

        fn btree_create(&mut self, _flags: usize) -> u32 {
            unimplemented!()
        }

        fn last(&mut self) -> Result<CursorResult<()>> {
            todo!()
        }

        fn prev(&mut self) -> Result<CursorResult<()>> {
            todo!()
        }
    }

    #[test]
    fn test_get_new_rowid() -> Result<()> {
        // Test case 0: Empty table
        let mut mock = MockCursor::new();
        mock.expect_seek_to_last()
            .return_once(|| Ok(CursorResult::Ok(())));
        mock.expect_rowid().return_once(|| Ok(None));

        let result = get_new_rowid(&mut (Box::new(mock) as Box<dyn Cursor>), thread_rng())?;
        assert_eq!(
            result,
            CursorResult::Ok(1),
            "For an empty table, rowid should be 1"
        );

        // Test case 1: Normal case, rowid within i64::MAX
        let mut mock = MockCursor::new();
        mock.expect_seek_to_last()
            .return_once(|| Ok(CursorResult::Ok(())));
        mock.expect_rowid().return_once(|| Ok(Some(100)));

        let result = get_new_rowid(&mut (Box::new(mock) as Box<dyn Cursor>), thread_rng())?;
        assert_eq!(result, CursorResult::Ok(101));

        // Test case 2: Rowid exceeds i64::MAX, need to generate random rowid
        let mut mock = MockCursor::new();
        mock.expect_seek_to_last()
            .return_once(|| Ok(CursorResult::Ok(())));
        mock.expect_rowid()
            .return_once(|| Ok(Some(i64::MAX as u64)));
        mock.expect_seek()
            .with(predicate::always(), predicate::always())
            .returning(|rowid, _| {
                if rowid == SeekKey::TableRowId(50) {
                    Ok(CursorResult::Ok(false))
                } else {
                    Ok(CursorResult::Ok(true))
                }
            });

        // Mock the random number generation
        let new_rowid =
            get_new_rowid(&mut (Box::new(mock) as Box<dyn Cursor>), StepRng::new(1, 1))?;
        assert_eq!(new_rowid, CursorResult::Ok(50));

        // Test case 3: IO error
        let mut mock = MockCursor::new();
        mock.expect_seek_to_last()
            .return_once(|| Ok(CursorResult::Ok(())));
        mock.expect_rowid()
            .return_once(|| Ok(Some(i64::MAX as u64)));
        mock.expect_seek()
            .with(predicate::always(), predicate::always())
            .return_once(|_, _| Ok(CursorResult::IO));

        let result = get_new_rowid(&mut (Box::new(mock) as Box<dyn Cursor>), thread_rng());
        assert!(matches!(result, Ok(CursorResult::IO)));

        // Test case 4: Failure to generate new rowid
        let mut mock = MockCursor::new();
        mock.expect_seek_to_last()
            .return_once(|| Ok(CursorResult::Ok(())));
        mock.expect_rowid()
            .return_once(|| Ok(Some(i64::MAX as u64)));
        mock.expect_seek()
            .with(predicate::always(), predicate::always())
            .returning(|_, _| Ok(CursorResult::Ok(true)));

        // Mock the random number generation
        let result = get_new_rowid(&mut (Box::new(mock) as Box<dyn Cursor>), StepRng::new(1, 1));
        assert!(matches!(result, Err(LimboError::InternalError(_))));

        Ok(())
    }

    #[test]
    fn test_length() {
        let input_str = OwnedValue::build_text(Rc::new(String::from("bob")));
        let expected_len = OwnedValue::Integer(3);
        assert_eq!(exec_length(&input_str), expected_len);

        let input_integer = OwnedValue::Integer(123);
        let expected_len = OwnedValue::Integer(3);
        assert_eq!(exec_length(&input_integer), expected_len);

        let input_float = OwnedValue::Float(123.456);
        let expected_len = OwnedValue::Integer(7);
        assert_eq!(exec_length(&input_float), expected_len);

        let expected_blob = OwnedValue::Blob(Rc::new("example".as_bytes().to_vec()));
        let expected_len = OwnedValue::Integer(7);
        assert_eq!(exec_length(&expected_blob), expected_len);
    }

    #[test]
    fn test_quote() {
        let input = OwnedValue::build_text(Rc::new(String::from("abc\0edf")));
        let expected = OwnedValue::build_text(Rc::new(String::from("'abc'")));
        assert_eq!(exec_quote(&input), expected);

        let input = OwnedValue::Integer(123);
        let expected = OwnedValue::Integer(123);
        assert_eq!(exec_quote(&input), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("hello''world")));
        let expected = OwnedValue::build_text(Rc::new(String::from("'hello''world'")));
        assert_eq!(exec_quote(&input), expected);
    }

    #[test]
    fn test_typeof() {
        let input = OwnedValue::Null;
        let expected: OwnedValue = OwnedValue::build_text(Rc::new("null".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Integer(123);
        let expected: OwnedValue = OwnedValue::build_text(Rc::new("integer".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Float(123.456);
        let expected: OwnedValue = OwnedValue::build_text(Rc::new("real".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::build_text(Rc::new("hello".to_string()));
        let expected: OwnedValue = OwnedValue::build_text(Rc::new("text".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Blob(Rc::new("limbo".as_bytes().to_vec()));
        let expected: OwnedValue = OwnedValue::build_text(Rc::new("blob".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Agg(Box::new(AggContext::Sum(OwnedValue::Integer(123))));
        let expected = OwnedValue::build_text(Rc::new("integer".to_string()));
        assert_eq!(exec_typeof(&input), expected);
    }

    #[test]
    fn test_unicode() {
        assert_eq!(
            exec_unicode(&OwnedValue::build_text(Rc::new("a".to_string()))),
            OwnedValue::Integer(97)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::build_text(Rc::new("😊".to_string()))),
            OwnedValue::Integer(128522)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::build_text(Rc::new("".to_string()))),
            OwnedValue::Null
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Integer(23)),
            OwnedValue::Integer(50)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Integer(0)),
            OwnedValue::Integer(48)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Float(0.0)),
            OwnedValue::Integer(48)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Float(23.45)),
            OwnedValue::Integer(50)
        );
        assert_eq!(exec_unicode(&OwnedValue::Null), OwnedValue::Null);
        assert_eq!(
            exec_unicode(&OwnedValue::Blob(Rc::new("example".as_bytes().to_vec()))),
            OwnedValue::Integer(101)
        );
    }

    #[test]
    fn test_min_max() {
        let input_int_vec = vec![&OwnedValue::Integer(-1), &OwnedValue::Integer(10)];
        assert_eq!(exec_min(input_int_vec.clone()), OwnedValue::Integer(-1));
        assert_eq!(exec_max(input_int_vec.clone()), OwnedValue::Integer(10));

        let str1 = OwnedValue::build_text(Rc::new(String::from("A")));
        let str2 = OwnedValue::build_text(Rc::new(String::from("z")));
        let input_str_vec = vec![&str2, &str1];
        assert_eq!(
            exec_min(input_str_vec.clone()),
            OwnedValue::build_text(Rc::new(String::from("A")))
        );
        assert_eq!(
            exec_max(input_str_vec.clone()),
            OwnedValue::build_text(Rc::new(String::from("z")))
        );

        let input_null_vec = vec![&OwnedValue::Null, &OwnedValue::Null];
        assert_eq!(exec_min(input_null_vec.clone()), OwnedValue::Null);
        assert_eq!(exec_max(input_null_vec.clone()), OwnedValue::Null);

        let input_mixed_vec = vec![&OwnedValue::Integer(10), &str1];
        assert_eq!(exec_min(input_mixed_vec.clone()), OwnedValue::Integer(10));
        assert_eq!(
            exec_max(input_mixed_vec.clone()),
            OwnedValue::build_text(Rc::new(String::from("A")))
        );
    }

    #[test]
    fn test_trim() {
        let input_str = OwnedValue::build_text(Rc::new(String::from("     Bob and Alice     ")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("Bob and Alice")));
        assert_eq!(exec_trim(&input_str, None), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("     Bob and Alice     ")));
        let pattern_str = OwnedValue::build_text(Rc::new(String::from("Bob and")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("Alice")));
        assert_eq!(exec_trim(&input_str, Some(pattern_str)), expected_str);
    }

    #[test]
    fn test_ltrim() {
        let input_str = OwnedValue::build_text(Rc::new(String::from("     Bob and Alice     ")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("Bob and Alice     ")));
        assert_eq!(exec_ltrim(&input_str, None), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("     Bob and Alice     ")));
        let pattern_str = OwnedValue::build_text(Rc::new(String::from("Bob and")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("Alice     ")));
        assert_eq!(exec_ltrim(&input_str, Some(pattern_str)), expected_str);
    }

    #[test]
    fn test_rtrim() {
        let input_str = OwnedValue::build_text(Rc::new(String::from("     Bob and Alice     ")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("     Bob and Alice")));
        assert_eq!(exec_rtrim(&input_str, None), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("     Bob and Alice     ")));
        let pattern_str = OwnedValue::build_text(Rc::new(String::from("Bob and")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("     Bob and Alice")));
        assert_eq!(exec_rtrim(&input_str, Some(pattern_str)), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("     Bob and Alice     ")));
        let pattern_str = OwnedValue::build_text(Rc::new(String::from("and Alice")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("     Bob")));
        assert_eq!(exec_rtrim(&input_str, Some(pattern_str)), expected_str);
    }

    #[test]
    fn test_soundex() {
        let input_str = OwnedValue::build_text(Rc::new(String::from("Pfister")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("P236")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("husobee")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("H210")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("Tymczak")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("T522")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("Ashcraft")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("A261")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("Robert")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("R163")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("Rupert")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("R163")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("Rubin")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("R150")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("Kant")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("K530")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("Knuth")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("K530")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("x")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("X000")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("闪电五连鞭")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("?000")));
        assert_eq!(exec_soundex(&input_str), expected_str);
    }

    #[test]
    fn test_upper_case() {
        let input_str = OwnedValue::build_text(Rc::new(String::from("Limbo")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("LIMBO")));
        assert_eq!(exec_upper(&input_str).unwrap(), expected_str);

        let input_int = OwnedValue::Integer(10);
        assert_eq!(exec_upper(&input_int).unwrap(), input_int);
        assert_eq!(exec_upper(&OwnedValue::Null).unwrap(), OwnedValue::Null)
    }

    #[test]
    fn test_lower_case() {
        let input_str = OwnedValue::build_text(Rc::new(String::from("Limbo")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("limbo")));
        assert_eq!(exec_lower(&input_str).unwrap(), expected_str);

        let input_int = OwnedValue::Integer(10);
        assert_eq!(exec_lower(&input_int).unwrap(), input_int);
        assert_eq!(exec_lower(&OwnedValue::Null).unwrap(), OwnedValue::Null)
    }

    #[test]
    fn test_hex() {
        let input_str = OwnedValue::build_text(Rc::new("limbo".to_string()));
        let expected_val = OwnedValue::build_text(Rc::new(String::from("6C696D626F")));
        assert_eq!(exec_hex(&input_str), expected_val);

        let input_int = OwnedValue::Integer(100);
        let expected_val = OwnedValue::build_text(Rc::new(String::from("313030")));
        assert_eq!(exec_hex(&input_int), expected_val);

        let input_float = OwnedValue::Float(12.34);
        let expected_val = OwnedValue::build_text(Rc::new(String::from("31322E3334")));
        assert_eq!(exec_hex(&input_float), expected_val);
    }

    #[test]
    fn test_unhex() {
        let input = OwnedValue::build_text(Rc::new(String::from("6F")));
        let expected = OwnedValue::Blob(Rc::new(vec![0x6f]));
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("6f")));
        let expected = OwnedValue::Blob(Rc::new(vec![0x6f]));
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("611")));
        let expected = OwnedValue::Null;
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("")));
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("61x")));
        let expected = OwnedValue::Null;
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::Null;
        let expected = OwnedValue::Null;
        assert_eq!(exec_unhex(&input, None), expected);
    }

    #[test]
    fn test_abs() {
        let int_positive_reg = OwnedValue::Integer(10);
        let int_negative_reg = OwnedValue::Integer(-10);
        assert_eq!(exec_abs(&int_positive_reg).unwrap(), int_positive_reg);
        assert_eq!(exec_abs(&int_negative_reg).unwrap(), int_positive_reg);

        let float_positive_reg = OwnedValue::Integer(10);
        let float_negative_reg = OwnedValue::Integer(-10);
        assert_eq!(exec_abs(&float_positive_reg).unwrap(), float_positive_reg);
        assert_eq!(exec_abs(&float_negative_reg).unwrap(), float_positive_reg);

        assert_eq!(
            exec_abs(&OwnedValue::build_text(Rc::new(String::from("a")))).unwrap(),
            OwnedValue::Float(0.0)
        );
        assert_eq!(exec_abs(&OwnedValue::Null).unwrap(), OwnedValue::Null);
    }

    #[test]
    fn test_char() {
        assert_eq!(
            exec_char(vec![OwnedValue::Integer(108), OwnedValue::Integer(105)]),
            OwnedValue::build_text(Rc::new("li".to_string()))
        );
        assert_eq!(
            exec_char(vec![]),
            OwnedValue::build_text(Rc::new("".to_string()))
        );
        assert_eq!(
            exec_char(vec![OwnedValue::Null]),
            OwnedValue::build_text(Rc::new("".to_string()))
        );
        assert_eq!(
            exec_char(vec![OwnedValue::build_text(Rc::new("a".to_string()))]),
            OwnedValue::build_text(Rc::new("".to_string()))
        );
    }

    #[test]
    fn test_like_no_cache() {
        assert!(exec_like(None, "a%", "aaaa"));
        assert!(exec_like(None, "%a%a", "aaaa"));
        assert!(exec_like(None, "%a.a", "aaaa"));
        assert!(exec_like(None, "a.a%", "aaaa"));
        assert!(!exec_like(None, "%a.ab", "aaaa"));
    }

    #[test]
    fn test_like_with_cache() {
        let mut cache = HashMap::new();
        assert!(exec_like(Some(&mut cache), "a%", "aaaa"));
        assert!(exec_like(Some(&mut cache), "%a%a", "aaaa"));
        assert!(exec_like(Some(&mut cache), "%a.a", "aaaa"));
        assert!(exec_like(Some(&mut cache), "a.a%", "aaaa"));
        assert!(!exec_like(Some(&mut cache), "%a.ab", "aaaa"));

        // again after values have been cached
        assert!(exec_like(Some(&mut cache), "a%", "aaaa"));
        assert!(exec_like(Some(&mut cache), "%a%a", "aaaa"));
        assert!(exec_like(Some(&mut cache), "%a.a", "aaaa"));
        assert!(exec_like(Some(&mut cache), "a.a%", "aaaa"));
        assert!(!exec_like(Some(&mut cache), "%a.ab", "aaaa"));
    }

    #[test]
    fn test_random() {
        match exec_random() {
            OwnedValue::Integer(value) => {
                // Check that the value is within the range of i64
                assert!(
                    (i64::MIN..=i64::MAX).contains(&value),
                    "Random number out of range"
                );
            }
            _ => panic!("exec_random did not return an Integer variant"),
        }
    }

    #[test]
    fn test_exec_randomblob() {
        struct TestCase {
            input: OwnedValue,
            expected_len: usize,
        }

        let test_cases = vec![
            TestCase {
                input: OwnedValue::Integer(5),
                expected_len: 5,
            },
            TestCase {
                input: OwnedValue::Integer(0),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Integer(-1),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::build_text(Rc::new(String::from(""))),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::build_text(Rc::new(String::from("5"))),
                expected_len: 5,
            },
            TestCase {
                input: OwnedValue::build_text(Rc::new(String::from("0"))),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::build_text(Rc::new(String::from("-1"))),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Float(2.9),
                expected_len: 2,
            },
            TestCase {
                input: OwnedValue::Float(-3.14),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Null,
                expected_len: 1,
            },
        ];

        for test_case in &test_cases {
            let result = exec_randomblob(&test_case.input);
            match result {
                OwnedValue::Blob(blob) => {
                    assert_eq!(blob.len(), test_case.expected_len);
                }
                _ => panic!("exec_randomblob did not return a Blob variant"),
            }
        }
    }

    #[test]
    fn test_exec_round() {
        let input_val = OwnedValue::Float(123.456);
        let expected_val = OwnedValue::Float(123.0);
        assert_eq!(exec_round(&input_val, None), expected_val);

        let input_val = OwnedValue::Float(123.456);
        let precision_val = OwnedValue::Integer(2);
        let expected_val = OwnedValue::Float(123.46);
        assert_eq!(exec_round(&input_val, Some(precision_val)), expected_val);

        let input_val = OwnedValue::Float(123.456);
        let precision_val = OwnedValue::build_text(Rc::new(String::from("1")));
        let expected_val = OwnedValue::Float(123.5);
        assert_eq!(exec_round(&input_val, Some(precision_val)), expected_val);

        let input_val = OwnedValue::build_text(Rc::new(String::from("123.456")));
        let precision_val = OwnedValue::Integer(2);
        let expected_val = OwnedValue::Float(123.46);
        assert_eq!(exec_round(&input_val, Some(precision_val)), expected_val);

        let input_val = OwnedValue::Integer(123);
        let precision_val = OwnedValue::Integer(1);
        let expected_val = OwnedValue::Float(123.0);
        assert_eq!(exec_round(&input_val, Some(precision_val)), expected_val);

        let input_val = OwnedValue::Float(100.123);
        let expected_val = OwnedValue::Float(100.0);
        assert_eq!(exec_round(&input_val, None), expected_val);

        let input_val = OwnedValue::Float(100.123);
        let expected_val = OwnedValue::Null;
        assert_eq!(exec_round(&input_val, Some(OwnedValue::Null)), expected_val);
    }

    #[test]
    fn test_exec_if() {
        let reg = OwnedValue::Integer(0);
        let null_reg = OwnedValue::Integer(0);
        assert!(!exec_if(&reg, &null_reg, false));
        assert!(exec_if(&reg, &null_reg, true));

        let reg = OwnedValue::Integer(1);
        let null_reg = OwnedValue::Integer(0);
        assert!(exec_if(&reg, &null_reg, false));
        assert!(!exec_if(&reg, &null_reg, true));

        let reg = OwnedValue::Null;
        let null_reg = OwnedValue::Integer(0);
        assert!(!exec_if(&reg, &null_reg, false));
        assert!(!exec_if(&reg, &null_reg, true));

        let reg = OwnedValue::Null;
        let null_reg = OwnedValue::Integer(1);
        assert!(exec_if(&reg, &null_reg, false));
        assert!(exec_if(&reg, &null_reg, true));

        let reg = OwnedValue::Null;
        let null_reg = OwnedValue::Null;
        assert!(!exec_if(&reg, &null_reg, false));
        assert!(!exec_if(&reg, &null_reg, true));
    }

    #[test]
    fn test_nullif() {
        assert_eq!(
            exec_nullif(&OwnedValue::Integer(1), &OwnedValue::Integer(1)),
            OwnedValue::Null
        );
        assert_eq!(
            exec_nullif(&OwnedValue::Float(1.1), &OwnedValue::Float(1.1)),
            OwnedValue::Null
        );
        assert_eq!(
            exec_nullif(
                &OwnedValue::build_text(Rc::new("limbo".to_string())),
                &OwnedValue::build_text(Rc::new("limbo".to_string()))
            ),
            OwnedValue::Null
        );

        assert_eq!(
            exec_nullif(&OwnedValue::Integer(1), &OwnedValue::Integer(2)),
            OwnedValue::Integer(1)
        );
        assert_eq!(
            exec_nullif(&OwnedValue::Float(1.1), &OwnedValue::Float(1.2)),
            OwnedValue::Float(1.1)
        );
        assert_eq!(
            exec_nullif(
                &OwnedValue::build_text(Rc::new("limbo".to_string())),
                &OwnedValue::build_text(Rc::new("limb".to_string()))
            ),
            OwnedValue::build_text(Rc::new("limbo".to_string()))
        );
    }

    #[test]
    fn test_substring() {
        let str_value = OwnedValue::build_text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(1);
        let length_value = OwnedValue::Integer(3);
        let expected_val = OwnedValue::build_text(Rc::new(String::from("lim")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );

        let str_value = OwnedValue::build_text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(1);
        let length_value = OwnedValue::Integer(10);
        let expected_val = OwnedValue::build_text(Rc::new(String::from("limbo")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );

        let str_value = OwnedValue::build_text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(10);
        let length_value = OwnedValue::Integer(3);
        let expected_val = OwnedValue::build_text(Rc::new(String::from("")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );

        let str_value = OwnedValue::build_text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(3);
        let length_value = OwnedValue::Null;
        let expected_val = OwnedValue::build_text(Rc::new(String::from("mbo")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );

        let str_value = OwnedValue::build_text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(10);
        let length_value = OwnedValue::Null;
        let expected_val = OwnedValue::build_text(Rc::new(String::from("")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );
    }

    #[test]
    fn test_exec_instr() {
        let input = OwnedValue::build_text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::build_text(Rc::new(String::from("im")));
        let expected = OwnedValue::Integer(2);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::build_text(Rc::new(String::from("limbo")));
        let expected = OwnedValue::Integer(1);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::build_text(Rc::new(String::from("o")));
        let expected = OwnedValue::Integer(5);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("liiiiimbo")));
        let pattern = OwnedValue::build_text(Rc::new(String::from("ii")));
        let expected = OwnedValue::Integer(2);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::build_text(Rc::new(String::from("limboX")));
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::build_text(Rc::new(String::from("")));
        let expected = OwnedValue::Integer(1);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("")));
        let pattern = OwnedValue::build_text(Rc::new(String::from("limbo")));
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("")));
        let pattern = OwnedValue::build_text(Rc::new(String::from("")));
        let expected = OwnedValue::Integer(1);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Null;
        let pattern = OwnedValue::Null;
        let expected = OwnedValue::Null;
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::Null;
        let expected = OwnedValue::Null;
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Null;
        let pattern = OwnedValue::build_text(Rc::new(String::from("limbo")));
        let expected = OwnedValue::Null;
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Integer(123);
        let pattern = OwnedValue::Integer(2);
        let expected = OwnedValue::Integer(2);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Integer(123);
        let pattern = OwnedValue::Integer(5);
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Float(12.34);
        let pattern = OwnedValue::Float(2.3);
        let expected = OwnedValue::Integer(2);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Float(12.34);
        let pattern = OwnedValue::Float(5.6);
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Float(12.34);
        let pattern = OwnedValue::build_text(Rc::new(String::from(".")));
        let expected = OwnedValue::Integer(3);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Blob(Rc::new(vec![1, 2, 3, 4, 5]));
        let pattern = OwnedValue::Blob(Rc::new(vec![3, 4]));
        let expected = OwnedValue::Integer(3);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Blob(Rc::new(vec![1, 2, 3, 4, 5]));
        let pattern = OwnedValue::Blob(Rc::new(vec![3, 2]));
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Blob(Rc::new(vec![0x61, 0x62, 0x63, 0x64, 0x65]));
        let pattern = OwnedValue::build_text(Rc::new(String::from("cd")));
        let expected = OwnedValue::Integer(3);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("abcde")));
        let pattern = OwnedValue::Blob(Rc::new(vec![0x63, 0x64]));
        let expected = OwnedValue::Integer(3);
        assert_eq!(exec_instr(&input, &pattern), expected);
    }

    #[test]
    fn test_exec_sign() {
        let input = OwnedValue::Integer(42);
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Integer(-42);
        let expected = Some(OwnedValue::Integer(-1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Integer(0);
        let expected = Some(OwnedValue::Integer(0));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Float(0.0);
        let expected = Some(OwnedValue::Integer(0));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Float(0.1);
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Float(42.0);
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Float(-42.0);
        let expected = Some(OwnedValue::Integer(-1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::build_text(Rc::new("abc".to_string()));
        let expected = Some(OwnedValue::Null);
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::build_text(Rc::new("42".to_string()));
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::build_text(Rc::new("-42".to_string()));
        let expected = Some(OwnedValue::Integer(-1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::build_text(Rc::new("0".to_string()));
        let expected = Some(OwnedValue::Integer(0));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Blob(Rc::new(b"abc".to_vec()));
        let expected = Some(OwnedValue::Null);
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Blob(Rc::new(b"42".to_vec()));
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Blob(Rc::new(b"-42".to_vec()));
        let expected = Some(OwnedValue::Integer(-1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Blob(Rc::new(b"0".to_vec()));
        let expected = Some(OwnedValue::Integer(0));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Null;
        let expected = Some(OwnedValue::Null);
        assert_eq!(exec_sign(&input), expected);
    }

    #[test]
    fn test_exec_zeroblob() {
        let input = OwnedValue::Integer(0);
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Null;
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Integer(4);
        let expected = OwnedValue::Blob(Rc::new(vec![0; 4]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Integer(-1);
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::build_text(Rc::new("5".to_string()));
        let expected = OwnedValue::Blob(Rc::new(vec![0; 5]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::build_text(Rc::new("-5".to_string()));
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::build_text(Rc::new("text".to_string()));
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Float(2.6);
        let expected = OwnedValue::Blob(Rc::new(vec![0; 2]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Blob(Rc::new(vec![1]));
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);
    }

    #[test]
    fn test_execute_sqlite_version() {
        let version_integer = 3046001;
        let expected = "3.46.1";
        assert_eq!(execute_sqlite_version(version_integer), expected);
    }

    #[test]
    fn test_replace() {
        let input_str = OwnedValue::build_text(Rc::new(String::from("bob")));
        let pattern_str = OwnedValue::build_text(Rc::new(String::from("b")));
        let replace_str = OwnedValue::build_text(Rc::new(String::from("a")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("aoa")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text(Rc::new(String::from("bob")));
        let pattern_str = OwnedValue::build_text(Rc::new(String::from("b")));
        let replace_str = OwnedValue::build_text(Rc::new(String::from("")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("o")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text(Rc::new(String::from("bob")));
        let pattern_str = OwnedValue::build_text(Rc::new(String::from("b")));
        let replace_str = OwnedValue::build_text(Rc::new(String::from("abc")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("abcoabc")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text(Rc::new(String::from("bob")));
        let pattern_str = OwnedValue::build_text(Rc::new(String::from("a")));
        let replace_str = OwnedValue::build_text(Rc::new(String::from("b")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("bob")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text(Rc::new(String::from("bob")));
        let pattern_str = OwnedValue::build_text(Rc::new(String::from("")));
        let replace_str = OwnedValue::build_text(Rc::new(String::from("a")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("bob")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text(Rc::new(String::from("bob")));
        let pattern_str = OwnedValue::Null;
        let replace_str = OwnedValue::build_text(Rc::new(String::from("a")));
        let expected_str = OwnedValue::Null;
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text(Rc::new(String::from("bo5")));
        let pattern_str = OwnedValue::Integer(5);
        let replace_str = OwnedValue::build_text(Rc::new(String::from("a")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("boa")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text(Rc::new(String::from("bo5.0")));
        let pattern_str = OwnedValue::Float(5.0);
        let replace_str = OwnedValue::build_text(Rc::new(String::from("a")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("boa")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text(Rc::new(String::from("bo5")));
        let pattern_str = OwnedValue::Float(5.0);
        let replace_str = OwnedValue::build_text(Rc::new(String::from("a")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("bo5")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text(Rc::new(String::from("bo5.0")));
        let pattern_str = OwnedValue::Float(5.0);
        let replace_str = OwnedValue::Float(6.0);
        let expected_str = OwnedValue::build_text(Rc::new(String::from("bo6.0")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        // todo: change this test to use (0.1 + 0.2) instead of 0.3 when decimals are implemented.
        let input_str = OwnedValue::build_text(Rc::new(String::from("tes3")));
        let pattern_str = OwnedValue::Integer(3);
        let replace_str = OwnedValue::Agg(Box::new(AggContext::Sum(OwnedValue::Float(0.3))));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("tes0.3")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );
    }
}
