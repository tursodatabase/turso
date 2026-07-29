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
}

/// A value-producing instruction. Effectful operations (cursor movement,
/// row production) will grow here as the migration proceeds; today the IR
/// covers pure scalar computation plus external inputs.
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
    /// A value that already lives in a physical register owned by code
    /// outside this function (the eager translation surrounding an IR
    /// island). Emission binds the value to that register directly; no
    /// instruction is generated. The register must remain valid for the
    /// whole emitted region.
    External {
        reg: usize,
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
    /// Leave the function, yielding `value` as its result. A function may
    /// have multiple `Ret` sites; emission funnels them into one
    /// destination register.
    Ret {
        value: ValueId,
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
            } => vec![if_true, if_false, if_null],
            Terminator::Ret { .. } => Vec::new(),
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
}

impl FuncBuilder {
    pub fn new() -> Self {
        Self {
            blocks: vec![Block::default()],
            defs: Vec::new(),
            current: BlockId::ENTRY,
            interned: HashMap::new(),
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

    pub fn unary(&mut self, op: UnaryOp, operand: ValueId) -> ValueId {
        self.push_inst(Inst::Unary { op, operand })
    }

    pub fn binary(&mut self, op: BinOp, lhs: ValueId, rhs: ValueId) -> ValueId {
        self.push_inst(Inst::Binary { op, lhs, rhs })
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
