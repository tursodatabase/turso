use std::collections::HashMap;

use crate::ValueBlob;

/// Handle to a node in an [`ExprArena`]. This is what translation returns
/// and what composes: building `a + b` is building `a`, building `b`, and
/// interning a binary node over the two handles. Nothing is emitted until
/// a [`super::Lowerer`] materializes the graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ValId(u32);

impl ValId {
    pub(crate) fn index(self) -> usize {
        self.0 as usize
    }
}

/// An explicitly declared mutable register cell.
///
/// VDBE registers are mutable and several constructs depend on in-place
/// mutation (aggregate accumulators, coroutine yield slots, in-place
/// custom-type encoding). Those must never be modeled as interned values;
/// a slot's register binding is provided by the frontend via
/// [`super::Lowerer::bind_slot`], and slot reads are only coherent within
/// a single lowering region.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlotId(u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOp {
    /// Logical NOT with SQL three-valued semantics (`Insn::Not`).
    Not,
    /// Bitwise NOT (`Insn::BitNot`).
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

/// `f64` with bitwise equality/hashing so real constants can be interned.
/// Bitwise identity is the right notion here: two literals intern to the
/// same node iff they produce the identical runtime value.
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
pub enum Node {
    ConstNull,
    ConstInt(i64),
    ConstReal(RealBits),
    ConstText(String),
    ConstBlob(ValueBlob),
    Unary(UnaryOp, ValId),
    Binary(BinOp, ValId, ValId),
    /// Read of a mutable slot. Interned like any node (two reads of the
    /// same slot within a region are the same value by the region purity
    /// rule), but never constant and never hoistable.
    Slot(SlotId),
}

/// Interning arena for expression nodes.
///
/// Structurally identical nodes share a [`ValId`], which gives common
/// subexpression elimination by construction: a lowerer computes each
/// distinct node at most once per region.
#[derive(Debug, Default)]
pub struct ExprArena {
    nodes: Vec<Node>,
    interned: HashMap<Node, ValId>,
    next_slot: u32,
}

impl ExprArena {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn node(&self, id: ValId) -> &Node {
        &self.nodes[id.index()]
    }

    fn intern(&mut self, node: Node) -> ValId {
        if let Some(&id) = self.interned.get(&node) {
            return id;
        }
        let id = ValId(
            u32::try_from(self.nodes.len()).expect("expression arena exceeded u32::MAX nodes"),
        );
        self.nodes.push(node.clone());
        self.interned.insert(node, id);
        id
    }

    pub fn null(&mut self) -> ValId {
        self.intern(Node::ConstNull)
    }

    pub fn int(&mut self, value: i64) -> ValId {
        self.intern(Node::ConstInt(value))
    }

    pub fn real(&mut self, value: f64) -> ValId {
        self.intern(Node::ConstReal(RealBits::new(value)))
    }

    pub fn text(&mut self, value: impl Into<String>) -> ValId {
        self.intern(Node::ConstText(value.into()))
    }

    pub fn blob(&mut self, value: ValueBlob) -> ValId {
        self.intern(Node::ConstBlob(value))
    }

    pub fn unary(&mut self, op: UnaryOp, operand: ValId) -> ValId {
        self.intern(Node::Unary(op, operand))
    }

    pub fn binary(&mut self, op: BinOp, lhs: ValId, rhs: ValId) -> ValId {
        self.intern(Node::Binary(op, lhs, rhs))
    }

    /// Declare a new mutable slot. The frontend owns binding it to a
    /// register at lowering time via [`super::Lowerer::bind_slot`].
    pub fn declare_slot(&mut self) -> SlotId {
        let slot = SlotId(self.next_slot);
        self.next_slot += 1;
        slot
    }

    /// A value that reads the current contents of `slot`.
    pub fn slot(&mut self, slot: SlotId) -> ValId {
        self.intern(Node::Slot(slot))
    }

    /// Whether the value is compile-time constant, i.e. its transitive
    /// inputs contain no slot reads (and, as node kinds grow, no column
    /// reads or non-deterministic functions). Constant values are eligible
    /// for hoisting into the program's constant prologue.
    pub fn is_constant(&self, id: ValId) -> bool {
        let mut stack = vec![id];
        while let Some(id) = stack.pop() {
            match self.node(id) {
                Node::ConstNull
                | Node::ConstInt(_)
                | Node::ConstReal(_)
                | Node::ConstText(_)
                | Node::ConstBlob(_) => {}
                Node::Slot(_) => return false,
                Node::Unary(_, operand) => stack.push(*operand),
                Node::Binary(_, lhs, rhs) => {
                    stack.push(*lhs);
                    stack.push(*rhs);
                }
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interning_shares_identical_nodes() {
        let mut arena = ExprArena::new();
        let a = arena.int(42);
        let b = arena.int(42);
        assert_eq!(a, b);

        let t1 = arena.text("hello");
        let t2 = arena.text("hello");
        assert_eq!(t1, t2);
        assert_ne!(a, t1);

        let sum1 = arena.binary(BinOp::Add, a, t1);
        let sum2 = arena.binary(BinOp::Add, b, t2);
        assert_eq!(sum1, sum2);

        // Operand order matters for interning.
        let flipped = arena.binary(BinOp::Add, t1, a);
        assert_ne!(sum1, flipped);
    }

    #[test]
    fn real_constants_intern_bitwise() {
        let mut arena = ExprArena::new();
        assert_eq!(arena.real(1.5), arena.real(1.5));
        assert_ne!(arena.real(0.0), arena.real(-0.0));
    }

    #[test]
    fn constness_is_transitive() {
        let mut arena = ExprArena::new();
        let one = arena.int(1);
        let two = arena.int(2);
        let sum = arena.binary(BinOp::Add, one, two);
        assert!(arena.is_constant(sum));

        let slot = arena.declare_slot();
        let slot_read = arena.slot(slot);
        assert!(!arena.is_constant(slot_read));
        let mixed = arena.binary(BinOp::Multiply, sum, slot_read);
        assert!(!arena.is_constant(mixed));
    }

    #[test]
    fn slots_are_distinct_cells() {
        let mut arena = ExprArena::new();
        let s1 = arena.declare_slot();
        let s2 = arena.declare_slot();
        assert_ne!(arena.slot(s1), arena.slot(s2));
        // Two reads of the same slot are the same value within a region.
        assert_eq!(arena.slot(s1), arena.slot(s1));
    }

    #[test]
    fn constness_walk_handles_deep_trees() {
        let mut arena = ExprArena::new();
        let one = arena.int(1);
        let mut acc = one;
        for i in 0..100_000i64 {
            let leaf = arena.int(i);
            acc = arena.binary(BinOp::Add, acc, leaf);
        }
        assert!(arena.is_constant(acc));
    }
}
