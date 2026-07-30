//! SSA control-flow IR for the composable compiler.
//!
//! A [`Function`] is a graph of [`Block`]s. Values are immutable and
//! symbolic ([`ValueId`]): nothing here knows about physical VDBE
//! registers. Control flow is explicit: every block ends in exactly one
//! [`Terminator`], and data flowing between blocks travels through block
//! parameters (the SSA-with-block-arguments form, as used by Cranelift and
//! MLIR) rather than through registers threaded by convention.
//!
//! The IR is built through [`FuncBuilder`], verified by
//! [`super::verify::verify`], and emitted to bytecode by
//! [`super::emit::emit_function`]. Frontends should not construct it
//! directly; they compose [`super::Compiler`] values instead.

use std::collections::HashMap;

use turso_parser::ast;

use crate::function::FuncCtx;
use crate::translate::collate::CollationSeq;
use crate::vdbe::affinity::Affinity;

/// A symbolic SSA value. Defined exactly once, either by an instruction or
/// as a block parameter; mapped to a physical register only at emission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ValueId(u32);

impl ValueId {
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// A basic block. `BlockId(0)` is always the entry block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlockId(u32);

impl BlockId {
    pub const ENTRY: BlockId = BlockId(0);

    pub fn index(self) -> usize {
        self.0 as usize
    }

    pub(super) fn from_index(index: usize) -> Self {
        BlockId(u32::try_from(index).expect("block count fits in u32"))
    }
}

/// `f64` with bitwise equality/hashing so real constants can be interned.
/// Distinct NaN payloads intern separately, and `0.0` != `-0.0`, which is
/// exactly what value-preserving constant dedup wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RealBits(u64);

impl RealBits {
    pub fn new(value: f64) -> Self {
        Self(value.to_bits())
    }

    pub fn value(self) -> f64 {
        f64::from_bits(self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Const {
    Null,
    Int(i64),
    Real(RealBits),
    Text(String),
    Blob(crate::ValueBlob),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOp {
    /// Boolean NOT with SQL three-valued semantics (`Insn::Not`).
    Not,
    /// Bitwise complement (`Insn::BitNot`).
    BitNot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    BitAnd,
    BitOr,
    ShiftLeft,
    ShiftRight,
    Concat,
    /// Logical AND in value position: three-valued at runtime
    /// (`Insn::And`).
    And,
    /// Logical OR in value position: three-valued at runtime
    /// (`Insn::Or`).
    Or,
}

/// Handle to a cast payload (an `Affinity`, which is not `Eq`/`Hash`, so
/// it lives in a side table like comparison payloads).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CastId(u32);

impl CastId {
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// Handle to a *leaf*: an AST expression the IR cannot decompose but can
/// treat as a value source — column reads, rowids. Emission delegates the
/// leaf back to eager translation, which keeps cursor/index/covering/
/// virtual-table resolution in one place while the IR owns the tree
/// around it.
///
/// Leaves are reads of mutable state (cursor position), so they are only
/// coherent within one IR island: a function must never span an effectful
/// boundary (cursor movement, slot write) as long as leaves dedup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LeafId(u32);

impl LeafId {
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// A SQL comparison operator with three-valued result semantics: the
/// value form produces 1, 0, or NULL (NULL when either operand is NULL).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    /// The comparison whose truth is exactly this one's falsity (SQL
    /// three-valued: both are NULL together). Used by emission to jump on
    /// the false side and fall through on the true side.
    pub fn negated(self) -> CmpOp {
        match self {
            CmpOp::Eq => CmpOp::Ne,
            CmpOp::Ne => CmpOp::Eq,
            CmpOp::Lt => CmpOp::Ge,
            CmpOp::Le => CmpOp::Gt,
            CmpOp::Gt => CmpOp::Le,
            CmpOp::Ge => CmpOp::Lt,
        }
    }
}

/// Handle to a comparison payload ([`CmpData`]). `Affinity` is not
/// `Eq`/`Hash`, so comparison payloads live in a side table like call
/// payloads do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CmpId(u32);

impl CmpId {
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// Payload of an [`Inst::Compare`]: affinity and collation are part of
/// the operation, captured at description time — never read from ambient
/// state during emission. `affinity: None` emits default comparison
/// flags with no affinity conversion at all (the eager CASE-base shape),
/// which is not the same as `Some(Affinity::Blob)`.
#[derive(Debug, Clone)]
pub struct CmpData {
    pub op: CmpOp,
    pub affinity: Option<Affinity>,
    pub collation: Option<CollationSeq>,
}

/// A symbolic external continuation: control flow that leaves the IR
/// island for a label owned by surrounding eager code. Like
/// [`Inst::External`] for registers, exits keep the IR free of physical
/// labels — each is bound to a `BranchOffset` only at emission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExitId(u32);

impl ExitId {
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// A symbolic cursor: an iteration resource declared by the IR and
/// bound to a physical VDBE cursor id only at emission (`emit` receives
/// one physical id per declared cursor). Like [`Inst::External`] for
/// registers and [`ExitId`] for labels, this keeps descriptions free of
/// physical resource numbering — a scan can be described before the
/// surrounding eager code has resolved its cursors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CursorId(u32);

impl CursorId {
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// Handle to a function-call payload ([`CallData`]). `FuncCtx` is neither
/// `Eq` nor `Hash`, so call payloads live in a side table and each call
/// site gets a fresh id — calls are never interned or deduplicated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CallId(u32);

impl CallId {
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// Payload of an [`Inst::Call`].
#[derive(Debug, Clone)]
pub struct CallData {
    pub func: FuncCtx,
    /// Whether the whole call expression is constant (deterministic
    /// function over constant arguments, per `Expr::is_constant`). Set by
    /// the frontend at description time; emission uses it to keep
    /// constant calls eligible for hoisting into the prologue.
    pub constant: bool,
    /// `Insn::Function`'s P1: a bitmask of constant arguments the
    /// runtime may cache across invocations (e.g. a LIKE pattern
    /// compiled once). Zero for most calls.
    pub constant_mask: i32,
}

/// A value-producing instruction. Effectful operations (cursor movement,
/// row production) will grow here as the migration proceeds; today the IR
/// covers pure scalar computation plus calls, external inputs, and opaque
/// leaves.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Inst {
    Const(Const),
    Unary {
        op: UnaryOp,
        operand: ValueId,
    },
    Binary {
        op: BinOp,
        lhs: ValueId,
        rhs: ValueId,
    },
    /// A scalar function call on the generic `Insn::Function` path. The
    /// callee requires its arguments in adjacent registers, so emission
    /// allocates a contiguous register pack per call site and steers or
    /// copies each argument into its slot.
    Call {
        call: CallId,
        args: Vec<ValueId>,
    },
    /// A three-valued comparison in value position (result 1/0/NULL).
    /// Emission expands it to the eager idiom: assume-true, conditional
    /// jump, `ZeroOrNull` — with labels invented by the backend.
    Compare {
        cmp: CmpId,
        lhs: ValueId,
        rhs: ValueId,
    },
    /// `IS NULL` (`negated: false`) / `IS NOT NULL` (`negated: true`) in
    /// value position: result is always 1 or 0, never NULL. Emission
    /// expands to the eager assume-true idiom over `IsNull`/`NotNull`.
    NullTest {
        operand: ValueId,
        negated: bool,
    },
    /// `CAST(operand AS type)`, plain affinity casts only (custom types
    /// stay eager). `Insn::Cast` mutates its register in place, so
    /// emission copies the operand into this value's fresh register
    /// first — sharing (interned constants) stays sound.
    Cast {
        cast: CastId,
        operand: ValueId,
    },
    /// `IS [NOT] TRUE/FALSE` truth test (`Insn::IsTrue`): `null_value`
    /// is the result for NULL operands, `invert` flips truthiness.
    Truth {
        operand: ValueId,
        null_value: bool,
        invert: bool,
    },
    /// A value that already lives in a physical register owned by code
    /// outside this function (the eager translation surrounding an IR
    /// island). Emission binds the value to that register directly; no
    /// instruction is generated. The register must remain valid for the
    /// whole emitted region.
    External {
        reg: usize,
    },
    /// Opaque leaf emitted by delegating its AST expression to the eager
    /// translation path. See [`LeafId`].
    Leaf(LeafId),
    /// Produce a result row from the given values (`Insn::ResultRow`),
    /// which must land in adjacent registers — emission allocates a
    /// contiguous pack per site, steering single-use argument
    /// definitions into their slots like call arguments. This is an
    /// effect: never constant, never interned, ordered by its position
    /// in the block. The instruction's own value is unit-like and must
    /// not be used.
    EmitRow {
        values: Vec<ValueId>,
    },
}

/// A control-flow edge: the destination block plus the values bound to its
/// block parameters when the edge is taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JumpTarget {
    pub block: BlockId,
    pub args: Vec<ValueId>,
}

impl JumpTarget {
    pub fn new(block: BlockId, args: Vec<ValueId>) -> Self {
        Self { block, args }
    }
}

/// Which way a scan walks its cursor. The IR's own vocabulary (not the
/// planner's `IterationDirection`) so the IR stays independent of plan
/// types; emission picks Rewind/Next or Last/Prev from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Backward,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Terminator {
    Jump(JumpTarget),
    /// Three-valued conditional branch on a SQL boolean: truthy values
    /// take `if_true`, falsy values take `if_false`, NULL takes `if_null`.
    /// The three-way split is real SQL semantics (it replaces the eager
    /// path's `ConditionMetadata` label triple), not an encoding detail.
    Branch {
        cond: ValueId,
        if_true: JumpTarget,
        if_false: JumpTarget,
        if_null: JumpTarget,
    },
    /// A comparison-driven three-way branch (condition position): the
    /// comparison's truth picks `if_true`/`if_false`, NULL results take
    /// `if_null` — which must coincide with one of the other two targets,
    /// because VDBE comparison jumps encode NULL routing as a
    /// `jump_if_null` flag, not a third destination. The verifier
    /// enforces this.
    CmpBranch {
        cmp: CmpId,
        lhs: ValueId,
        rhs: ValueId,
        if_true: JumpTarget,
        if_false: JumpTarget,
        if_null: JumpTarget,
    },
    /// A two-way branch on nullness: NULL values take `if_null`,
    /// everything else (including 0 and '') takes `if_not_null`. This is
    /// what truthiness `Branch` cannot express — the backbone of
    /// COALESCE/IFNULL and IS NULL conditions.
    NullBranch {
        value: ValueId,
        if_null: JumpTarget,
        if_not_null: JumpTarget,
    },
    /// Leave the function, yielding `value` as its result. A function may
    /// have multiple `Ret` sites; emission funnels them into one
    /// destination register.
    Ret {
        value: ValueId,
    },
    /// Leave the IR island for an external continuation. See [`ExitId`].
    Exit(ExitId),
    /// Position `cursor` (externally opened, bound to a physical
    /// cursor id at emission) at the first row in iteration order —
    /// the table start going forward (`Insn::Rewind`), the table end
    /// going backward (`Insn::Last`): `if_empty` when the table has no
    /// rows, `if_rows` otherwise.
    ScanStart {
        cursor: CursorId,
        direction: ScanDirection,
        if_empty: JumpTarget,
        if_rows: JumpTarget,
    },
    /// Advance `cursor` one row in iteration order (`Insn::Next`
    /// forward, `Insn::Prev` backward): `if_more` (the loop back-edge,
    /// which may carry loop values as block arguments) when another
    /// row exists, `if_done` otherwise.
    ScanAdvance {
        cursor: CursorId,
        direction: ScanDirection,
        if_more: JumpTarget,
        if_done: JumpTarget,
    },
    /// Decrement an external counter register in place (owned and
    /// initialized by surrounding eager code, referenced by its
    /// physical register like `Rewind` cursors): `if_zero` when the
    /// counter reaches zero, `if_more` otherwise
    /// (`Insn::DecrJumpZero`). The LIMIT countdown.
    DecrJumpZero {
        counter_reg: usize,
        if_zero: JumpTarget,
        if_more: JumpTarget,
    },
    /// Take `if_pos`, decrementing the external counter register in
    /// place, while the counter is still positive; `if_rest` once it
    /// has run out (`Insn::IfPos` with decrement 1). The OFFSET
    /// row-skip.
    IfPos {
        counter_reg: usize,
        if_pos: JumpTarget,
        if_rest: JumpTarget,
    },
}

impl Terminator {
    pub fn targets(&self) -> Vec<&JumpTarget> {
        match self {
            Terminator::Jump(target) => vec![target],
            Terminator::Branch {
                if_true,
                if_false,
                if_null,
                ..
            }
            | Terminator::CmpBranch {
                if_true,
                if_false,
                if_null,
                ..
            } => vec![if_true, if_false, if_null],
            Terminator::NullBranch {
                if_null,
                if_not_null,
                ..
            } => vec![if_null, if_not_null],
            Terminator::ScanStart {
                if_empty, if_rows, ..
            } => vec![if_empty, if_rows],
            Terminator::ScanAdvance {
                if_more, if_done, ..
            } => vec![if_more, if_done],
            Terminator::DecrJumpZero {
                if_zero, if_more, ..
            } => vec![if_zero, if_more],
            Terminator::IfPos {
                if_pos, if_rest, ..
            } => vec![if_pos, if_rest],
            Terminator::Ret { .. } | Terminator::Exit(_) => Vec::new(),
        }
    }
}

/// Where a value is defined. Used by the verifier for def-before-use
/// checks and by emission to know which instruction writes which value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefSite {
    Param { block: BlockId, index: usize },
    Inst { block: BlockId, index: usize },
}

#[derive(Debug, Default)]
pub struct Block {
    pub params: Vec<ValueId>,
    pub insts: Vec<(ValueId, Inst)>,
    pub terminator: Option<Terminator>,
}

#[derive(Debug)]
pub struct Function {
    pub blocks: Vec<Block>,
    /// Definition site of every value, indexed by [`ValueId`].
    defs: Vec<DefSite>,
    /// AST expressions backing [`Inst::Leaf`] instructions.
    leaves: Vec<ast::Expr>,
    /// Payloads backing [`Inst::Call`] instructions.
    calls: Vec<CallData>,
    /// Payloads backing [`Inst::Compare`] instructions.
    cmps: Vec<CmpData>,
    /// Affinities backing [`Inst::Cast`] instructions.
    casts: Vec<Affinity>,
    /// Number of declared external exits ([`ExitId`]s are dense).
    num_exits: usize,
    /// Number of declared symbolic cursors ([`CursorId`]s are dense).
    num_cursors: usize,
}

impl Function {
    pub fn block(&self, id: BlockId) -> &Block {
        &self.blocks[id.index()]
    }

    pub fn def_site(&self, value: ValueId) -> DefSite {
        self.defs[value.index()]
    }

    pub fn num_values(&self) -> usize {
        self.defs.len()
    }

    pub fn leaf_expr(&self, id: LeafId) -> &ast::Expr {
        &self.leaves[id.0 as usize]
    }

    pub fn call_data(&self, id: CallId) -> &CallData {
        &self.calls[id.0 as usize]
    }

    pub fn num_calls(&self) -> usize {
        self.calls.len()
    }

    pub fn cmp_data(&self, id: CmpId) -> &CmpData {
        &self.cmps[id.0 as usize]
    }

    pub fn cast_affinity(&self, id: CastId) -> Affinity {
        self.casts[id.index()]
    }

    pub fn num_exits(&self) -> usize {
        self.num_exits
    }

    pub fn num_cursors(&self) -> usize {
        self.num_cursors
    }
}

/// Builds a [`Function`] one block at a time. The builder has a *current*
/// block; instruction constructors append to it. Pure constants and
/// external inputs are placed in the entry block and interned, so they
/// dominate every use and identical constants share one value.
pub struct FuncBuilder {
    blocks: Vec<Block>,
    defs: Vec<DefSite>,
    current: BlockId,
    interned: HashMap<Inst, ValueId>,
    leaves: Vec<ast::Expr>,
    /// Placed leaf reads, for dominance-safe dedup in [`Self::leaf`].
    placed_leaves: Vec<(LeafId, ValueId)>,
    calls: Vec<CallData>,
    cmps: Vec<CmpData>,
    casts: Vec<Affinity>,
    num_exits: usize,
    num_cursors: usize,
}

impl FuncBuilder {
    pub fn new() -> Self {
        Self {
            blocks: vec![Block::default()],
            defs: Vec::new(),
            current: BlockId::ENTRY,
            interned: HashMap::new(),
            leaves: Vec::new(),
            placed_leaves: Vec::new(),
            calls: Vec::new(),
            cmps: Vec::new(),
            casts: Vec::new(),
            num_exits: 0,
            num_cursors: 0,
        }
    }

    pub fn current_block(&self) -> BlockId {
        self.current
    }

    /// Create a new, empty block. Does not change the current block.
    pub fn create_block(&mut self) -> BlockId {
        let id = BlockId(u32::try_from(self.blocks.len()).expect("block count fits in u32"));
        self.blocks.push(Block::default());
        id
    }

    /// Append a parameter to `block`, returning the value it binds. Must
    /// be called before any jump to `block` is created (the verifier
    /// enforces argument arity on every edge).
    pub fn add_block_param(&mut self, block: BlockId) -> ValueId {
        let index = self.blocks[block.index()].params.len();
        let value = self.new_value(DefSite::Param { block, index });
        self.blocks[block.index()].params.push(value);
        value
    }

    /// Make `block` the current block. It must not be terminated yet.
    pub fn switch_to(&mut self, block: BlockId) {
        assert!(
            self.blocks[block.index()].terminator.is_none(),
            "switch_to: block {block:?} is already terminated"
        );
        self.current = block;
    }

    pub fn null(&mut self) -> ValueId {
        self.intern_in_entry(Inst::Const(Const::Null))
    }

    pub fn int(&mut self, value: i64) -> ValueId {
        self.intern_in_entry(Inst::Const(Const::Int(value)))
    }

    pub fn real(&mut self, value: f64) -> ValueId {
        self.intern_in_entry(Inst::Const(Const::Real(RealBits::new(value))))
    }

    pub fn text(&mut self, value: impl Into<String>) -> ValueId {
        self.intern_in_entry(Inst::Const(Const::Text(value.into())))
    }

    pub fn blob(&mut self, value: crate::ValueBlob) -> ValueId {
        self.intern_in_entry(Inst::Const(Const::Blob(value)))
    }

    /// Import a value that already lives in physical register `reg`
    /// outside this function. See [`Inst::External`].
    pub fn external(&mut self, reg: usize) -> ValueId {
        self.intern_in_entry(Inst::External { reg })
    }

    /// An opaque leaf backed by `expr`, emitted by delegation to the
    /// eager translation path. Structurally equal leaves dedup (linear
    /// scan — leaves per expression are few), so repeated reads of the
    /// same column share a value — but only when the earlier read surely
    /// dominates this one: it was placed in the entry block or in the
    /// current block. Reads placed in other blocks may sit on sibling
    /// branches (e.g. the two AND-continuations of an OR), where reuse
    /// would be a dominance violation; those re-read the leaf instead,
    /// exactly like eager translation re-reads a column per terminal.
    pub fn leaf(&mut self, expr: &ast::Expr) -> ValueId {
        for &(leaf, value) in &self.placed_leaves {
            if self.leaves[leaf.index()] == *expr {
                let DefSite::Inst { block, .. } = self.defs[value.index()] else {
                    unreachable!("leaves are defined by instructions");
                };
                if block == BlockId::ENTRY || block == self.current {
                    return value;
                }
            }
        }
        let id = match self.leaves.iter().position(|e| e == expr) {
            Some(index) => LeafId(u32::try_from(index).expect("leaf table bounded by values")),
            None => {
                let id = LeafId(u32::try_from(self.leaves.len()).expect("leaf count fits in u32"));
                self.leaves.push(expr.clone());
                id
            }
        };
        let value = self.push_inst(Inst::Leaf(id));
        self.placed_leaves.push((id, value));
        value
    }

    /// A leaf that must never share a value with other reads. Anonymous
    /// `?` parameters register a fresh index per occurrence, so two
    /// structurally equal `Expr::Variable`s are different parameters —
    /// and eager translation never dedups variables either.
    pub fn leaf_unique(&mut self, expr: &ast::Expr) -> ValueId {
        let id = LeafId(u32::try_from(self.leaves.len()).expect("leaf count fits in u32"));
        self.leaves.push(expr.clone());
        // Deliberately not recorded in placed_leaves: not a dedup
        // candidate.
        self.push_inst(Inst::Leaf(id))
    }

    pub fn unary(&mut self, op: UnaryOp, operand: ValueId) -> ValueId {
        self.push_inst(Inst::Unary { op, operand })
    }

    /// `IS NULL` / `IS NOT NULL` in value position (result 1/0).
    pub fn null_test(&mut self, operand: ValueId, negated: bool) -> ValueId {
        self.push_inst(Inst::NullTest { operand, negated })
    }

    /// A plain affinity cast.
    pub fn cast_value(&mut self, operand: ValueId, affinity: Affinity) -> ValueId {
        let id = CastId(u32::try_from(self.casts.len()).expect("cast count fits in u32"));
        self.casts.push(affinity);
        self.push_inst(Inst::Cast { cast: id, operand })
    }

    /// An `IS [NOT] TRUE/FALSE` truth test.
    pub fn truth(&mut self, operand: ValueId, null_value: bool, invert: bool) -> ValueId {
        self.push_inst(Inst::Truth {
            operand,
            null_value,
            invert,
        })
    }

    /// Terminate the current block with a nullness branch.
    pub fn null_branch(&mut self, value: ValueId, if_null: JumpTarget, if_not_null: JumpTarget) {
        self.terminate(Terminator::NullBranch {
            value,
            if_null,
            if_not_null,
        });
    }

    /// Terminate the current block by positioning a declared cursor at
    /// the first row in `direction`'s iteration order.
    pub fn scan_start(
        &mut self,
        cursor: CursorId,
        direction: ScanDirection,
        if_empty: JumpTarget,
        if_rows: JumpTarget,
    ) {
        self.terminate(Terminator::ScanStart {
            cursor,
            direction,
            if_empty,
            if_rows,
        });
    }

    /// Terminate the current block by advancing a declared cursor one
    /// row in `direction`'s iteration order.
    pub fn scan_advance(
        &mut self,
        cursor: CursorId,
        direction: ScanDirection,
        if_more: JumpTarget,
        if_done: JumpTarget,
    ) {
        self.terminate(Terminator::ScanAdvance {
            cursor,
            direction,
            if_more,
            if_done,
        });
    }

    /// Forward [`Self::scan_start`]: rewind a declared cursor.
    pub fn rewind(&mut self, cursor: CursorId, if_empty: JumpTarget, if_rows: JumpTarget) {
        self.scan_start(cursor, ScanDirection::Forward, if_empty, if_rows);
    }

    /// Forward [`Self::scan_advance`]: step a declared cursor to its
    /// next row.
    pub fn next_row(&mut self, cursor: CursorId, if_more: JumpTarget, if_done: JumpTarget) {
        self.scan_advance(cursor, ScanDirection::Forward, if_more, if_done);
    }

    /// Terminate the current block by decrementing an external counter
    /// register, leaving for `if_zero` when it reaches zero.
    pub fn decr_jump_zero(&mut self, counter_reg: usize, if_zero: JumpTarget, if_more: JumpTarget) {
        self.terminate(Terminator::DecrJumpZero {
            counter_reg,
            if_zero,
            if_more,
        });
    }

    /// Terminate the current block by testing an external counter
    /// register: `if_pos` (decrementing it) while positive, `if_rest`
    /// once it has run out.
    pub fn if_pos(&mut self, counter_reg: usize, if_pos: JumpTarget, if_rest: JumpTarget) {
        self.terminate(Terminator::IfPos {
            counter_reg,
            if_pos,
            if_rest,
        });
    }

    /// Produce a result row from `values`. The returned value is
    /// unit-like bookkeeping and must not be used.
    pub fn emit_row(&mut self, values: Vec<ValueId>) {
        let _ = self.push_inst(Inst::EmitRow { values });
    }

    pub fn binary(&mut self, op: BinOp, lhs: ValueId, rhs: ValueId) -> ValueId {
        self.push_inst(Inst::Binary { op, lhs, rhs })
    }

    /// A scalar function call. `constant` marks calls that are
    /// deterministic over constant arguments (hoistable); calls are never
    /// deduplicated, so two identical calls run twice. Argument order is
    /// pack-slot order, which may differ from evaluation order (the
    /// frontend controls evaluation by when it runs each operand).
    pub fn call(
        &mut self,
        func: FuncCtx,
        constant: bool,
        constant_mask: i32,
        args: Vec<ValueId>,
    ) -> ValueId {
        let id = CallId(u32::try_from(self.calls.len()).expect("call count fits in u32"));
        self.calls.push(CallData {
            func,
            constant,
            constant_mask,
        });
        self.push_inst(Inst::Call { call: id, args })
    }

    /// A three-valued comparison in value position. Affinity and
    /// collation are payloads of the operation, fixed at description
    /// time.
    pub fn compare(
        &mut self,
        op: CmpOp,
        affinity: Option<Affinity>,
        collation: Option<CollationSeq>,
        lhs: ValueId,
        rhs: ValueId,
    ) -> ValueId {
        let id = CmpId(u32::try_from(self.cmps.len()).expect("cmp count fits in u32"));
        self.cmps.push(CmpData {
            op,
            affinity,
            collation,
        });
        self.push_inst(Inst::Compare { cmp: id, lhs, rhs })
    }

    /// Declare an external continuation, bound to a physical label at
    /// emission time (`emit` receives one label per declared exit).
    pub fn declare_exit(&mut self) -> ExitId {
        let id = ExitId(u32::try_from(self.num_exits).expect("exit count fits in u32"));
        self.num_exits += 1;
        id
    }

    /// Declare a symbolic cursor, bound to a physical cursor id at
    /// emission time (`emit` receives one id per declared cursor).
    pub fn declare_cursor(&mut self) -> CursorId {
        let id = CursorId(u32::try_from(self.num_cursors).expect("cursor count fits in u32"));
        self.num_cursors += 1;
        id
    }

    /// A block that immediately leaves the island for `exit`. Jumping to
    /// it is how in-island control flow reaches external continuations;
    /// emission bypasses the block entirely, jumping straight to the
    /// bound label.
    pub fn exit_block(&mut self, exit: ExitId) -> BlockId {
        let block = self.create_block();
        self.blocks[block.index()].terminator = Some(Terminator::Exit(exit));
        block
    }

    /// Terminate the current block by leaving the island for `exit`.
    pub fn exit(&mut self, exit: ExitId) {
        self.terminate(Terminator::Exit(exit));
    }

    /// Terminate the current block with a comparison-driven three-way
    /// branch. `if_null` must equal `if_true` or `if_false` (VDBE
    /// comparison jumps route NULL via a flag, not a third target).
    #[allow(clippy::too_many_arguments)] // op + payloads + operands + three targets
    pub fn cmp_branch(
        &mut self,
        op: CmpOp,
        affinity: Option<Affinity>,
        collation: Option<CollationSeq>,
        lhs: ValueId,
        rhs: ValueId,
        if_true: JumpTarget,
        if_false: JumpTarget,
        if_null: JumpTarget,
    ) {
        let id = CmpId(u32::try_from(self.cmps.len()).expect("cmp count fits in u32"));
        self.cmps.push(CmpData {
            op,
            affinity,
            collation,
        });
        self.terminate(Terminator::CmpBranch {
            cmp: id,
            lhs,
            rhs,
            if_true,
            if_false,
            if_null,
        });
    }

    pub fn jump(&mut self, block: BlockId, args: Vec<ValueId>) {
        self.terminate(Terminator::Jump(JumpTarget::new(block, args)));
    }

    pub fn branch(
        &mut self,
        cond: ValueId,
        if_true: JumpTarget,
        if_false: JumpTarget,
        if_null: JumpTarget,
    ) {
        self.terminate(Terminator::Branch {
            cond,
            if_true,
            if_false,
            if_null,
        });
    }

    pub fn ret(&mut self, value: ValueId) {
        self.terminate(Terminator::Ret { value });
    }

    pub fn finish(self) -> Function {
        Function {
            blocks: self.blocks,
            defs: self.defs,
            leaves: self.leaves,
            calls: self.calls,
            cmps: self.cmps,
            casts: self.casts,
            num_exits: self.num_exits,
            num_cursors: self.num_cursors,
        }
    }

    fn terminate(&mut self, terminator: Terminator) {
        let block = &mut self.blocks[self.current.index()];
        assert!(
            block.terminator.is_none(),
            "block {:?} terminated twice",
            self.current
        );
        block.terminator = Some(terminator);
    }

    fn new_value(&mut self, def: DefSite) -> ValueId {
        let id = ValueId(u32::try_from(self.defs.len()).expect("value count fits in u32"));
        self.defs.push(def);
        id
    }

    /// Constants and external inputs go to the entry block, interned:
    /// they are pure, so hoisting them to the entry preserves semantics,
    /// makes them dominate every possible use, and dedups identical
    /// definitions (CSE-by-construction for leaves).
    fn intern_in_entry(&mut self, inst: Inst) -> ValueId {
        if let Some(&value) = self.interned.get(&inst) {
            return value;
        }
        let index = self.blocks[BlockId::ENTRY.index()].insts.len();
        let value = self.new_value(DefSite::Inst {
            block: BlockId::ENTRY,
            index,
        });
        self.blocks[BlockId::ENTRY.index()]
            .insts
            .push((value, inst.clone()));
        self.interned.insert(inst, value);
        value
    }

    fn push_inst(&mut self, inst: Inst) -> ValueId {
        let block = self.current;
        let index = self.blocks[block.index()].insts.len();
        let value = self.new_value(DefSite::Inst { block, index });
        self.blocks[block.index()].insts.push((value, inst));
        value
    }
}

impl Default for FuncBuilder {
    fn default() -> Self {
        Self::new()
    }
}
