//! Deferred, typed building blocks for VDBE compilation.
//!
//! Compiler combinators describe work without mutating [`ProgramBuilder`]. The
//! completed description is first interpreted into symbolic SSA IR and only
//! then lowered into physical VDBE registers, labels, and instructions.

use std::{fmt, marker::PhantomData};

use smallvec::{smallvec, SmallVec};

use crate::{
    numeric::Numeric,
    types::Value,
    vdbe::{builder::ProgramBuilder, insn::Insn},
    LimboError, Result,
};

/// A deferred compilation step with one typed output.
pub(crate) trait Compile: Sized {
    type Output;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output>;

    fn map<F, Output>(self, map: F) -> Map<Self, F, Output>
    where
        F: FnOnce(Self::Output) -> Output,
    {
        Map {
            compiler: self,
            map,
            output: PhantomData,
        }
    }

    fn then<Other>(self, other: Other) -> Then<Self, Other>
    where
        Other: Compile,
    {
        Then {
            first: self,
            second: other,
        }
    }

    fn and_then<F, Next>(self, next: F) -> AndThen<Self, F, Next>
    where
        F: FnOnce(Self::Output) -> Next,
        Next: Compile,
    {
        AndThen {
            compiler: self,
            next,
            output: PhantomData,
        }
    }

    fn boxed(self) -> BoxedCompile<Self::Output>
    where
        Self: 'static,
    {
        BoxedCompile {
            compiler: Box::new(|builder| self.compile(builder)),
        }
    }

    /// Select one of two compilers using SQL truthiness.
    ///
    /// A false or NULL condition selects `if_false`; every other value selects
    /// `if_true`. Only the selected compiler is evaluated at runtime.
    fn branch<IfTrue, IfFalse>(
        self,
        if_true: IfTrue,
        if_false: IfFalse,
    ) -> Branch<Self, IfTrue, IfFalse>
    where
        Self: Compile<Output = ValueId>,
        IfTrue: Compile<Output = ValueId>,
        IfFalse: Compile<Output = ValueId>,
    {
        Branch {
            condition: self,
            if_true,
            if_false,
        }
    }
}

type BoxedCompilerFn<Output> = Box<dyn FnOnce(&mut IrBuilder) -> Result<Output>>;

pub(crate) struct BoxedCompile<Output> {
    compiler: BoxedCompilerFn<Output>,
}

impl<Output> Compile for BoxedCompile<Output> {
    type Output = Output;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        (self.compiler)(builder)
    }
}

pub(crate) struct Map<Compiler, F, Output> {
    compiler: Compiler,
    map: F,
    output: PhantomData<fn() -> Output>,
}

impl<Compiler, F, Output> Compile for Map<Compiler, F, Output>
where
    Compiler: Compile,
    F: FnOnce(Compiler::Output) -> Output,
{
    type Output = Output;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        self.compiler.compile(builder).map(self.map)
    }
}

pub(crate) struct Then<First, Second> {
    first: First,
    second: Second,
}

impl<First, Second> Compile for Then<First, Second>
where
    First: Compile,
    Second: Compile,
{
    type Output = (First::Output, Second::Output);

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let first = self.first.compile(builder)?;
        let second = self.second.compile(builder)?;
        Ok((first, second))
    }
}

pub(crate) struct AndThen<Compiler, F, Next> {
    compiler: Compiler,
    next: F,
    output: PhantomData<fn() -> Next>,
}

impl<Compiler, F, Next> Compile for AndThen<Compiler, F, Next>
where
    Compiler: Compile,
    F: FnOnce(Compiler::Output) -> Next,
    Next: Compile,
{
    type Output = Next::Output;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let output = self.compiler.compile(builder)?;
        (self.next)(output).compile(builder)
    }
}

pub(crate) struct Branch<Condition, IfTrue, IfFalse> {
    condition: Condition,
    if_true: IfTrue,
    if_false: IfFalse,
}

impl<Condition, IfTrue, IfFalse> Compile for Branch<Condition, IfTrue, IfFalse>
where
    Condition: Compile<Output = ValueId>,
    IfTrue: Compile<Output = ValueId>,
    IfFalse: Compile<Output = ValueId>,
{
    type Output = ValueId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let condition = self.condition.compile(builder)?;
        let if_true_block = builder.create_block()?;
        let if_false_block = builder.create_block()?;
        let merge_block = builder.create_block()?;
        let output = builder.add_block_parameter(merge_block)?;

        builder.terminate(Terminator::Branch {
            condition,
            if_true: if_true_block,
            if_false: if_false_block,
        })?;

        builder.switch_to(if_true_block)?;
        let if_true = self.if_true.compile(builder)?;
        builder.terminate(Terminator::Jump {
            target: merge_block,
            arguments: smallvec![if_true],
        })?;

        builder.switch_to(if_false_block)?;
        let if_false = self.if_false.compile(builder)?;
        builder.terminate(Terminator::Jump {
            target: merge_block,
            arguments: smallvec![if_false],
        })?;

        builder.switch_to(merge_block)?;
        Ok(output)
    }
}

/// The symbolic result of one SSA operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ValueId(u32);

impl ValueId {
    fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BlockId(u32);

impl BlockId {
    fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug)]
enum ScalarOp {
    Constant(Value),
    Add { lhs: ValueId, rhs: ValueId },
}

impl ScalarOp {
    fn operands(&self) -> impl Iterator<Item = ValueId> + '_ {
        let operands = match self {
            Self::Constant(_) => [None, None],
            Self::Add { lhs, rhs } => [Some(*lhs), Some(*rhs)],
        };
        operands.into_iter().flatten()
    }
}

#[derive(Debug)]
struct Instruction {
    result: ValueId,
    op: ScalarOp,
}

#[derive(Debug)]
enum Terminator {
    Jump {
        target: BlockId,
        arguments: SmallVec<[ValueId; 2]>,
    },
    Branch {
        condition: ValueId,
        if_true: BlockId,
        if_false: BlockId,
    },
    Return(ValueId),
}

impl Terminator {
    fn successors(&self) -> impl Iterator<Item = BlockId> {
        let successors = match self {
            Self::Jump { target, .. } => [Some(*target), None],
            Self::Branch {
                if_true, if_false, ..
            } => [Some(*if_true), Some(*if_false)],
            Self::Return(_) => [None, None],
        };
        successors.into_iter().flatten()
    }

    fn operands(&self) -> impl Iterator<Item = ValueId> + '_ {
        let (first, rest) = match self {
            Self::Jump { arguments, .. } => (None, arguments.as_slice()),
            Self::Branch { condition, .. } | Self::Return(condition) => (Some(*condition), &[][..]),
        };
        first.into_iter().chain(rest.iter().copied())
    }
}

#[derive(Debug)]
struct BasicBlock {
    id: BlockId,
    parameters: SmallVec<[ValueId; 2]>,
    instructions: SmallVec<[Instruction; 8]>,
    terminator: Terminator,
}

struct BlockUnderConstruction {
    id: BlockId,
    parameters: SmallVec<[ValueId; 2]>,
    instructions: SmallVec<[Instruction; 8]>,
    terminator: Option<Terminator>,
}

/// Builds an SSA control-flow region without allocating VDBE resources.
pub(crate) struct IrBuilder {
    blocks: SmallVec<[BlockUnderConstruction; 4]>,
    current: BlockId,
    next_value: u32,
}

impl IrBuilder {
    fn new() -> Self {
        Self {
            blocks: smallvec![BlockUnderConstruction {
                id: BlockId(0),
                parameters: SmallVec::new(),
                instructions: SmallVec::new(),
                terminator: None,
            }],
            current: BlockId(0),
            next_value: 0,
        }
    }

    fn allocate_value(&mut self) -> Result<ValueId> {
        let value = ValueId(self.next_value);
        self.next_value = self.next_value.checked_add(1).ok_or_else(|| {
            LimboError::InternalError("compiler IR value identifier overflow".to_owned())
        })?;
        Ok(value)
    }

    fn push(&mut self, op: ScalarOp) -> Result<ValueId> {
        let result = self.allocate_value()?;
        self.blocks[self.current.index()]
            .instructions
            .push(Instruction { result, op });
        Ok(result)
    }

    fn create_block(&mut self) -> Result<BlockId> {
        let id = u32::try_from(self.blocks.len()).map_err(|_| {
            LimboError::InternalError("compiler IR block identifier overflow".to_owned())
        })?;
        let id = BlockId(id);
        self.blocks.push(BlockUnderConstruction {
            id,
            parameters: SmallVec::new(),
            instructions: SmallVec::new(),
            terminator: None,
        });
        Ok(id)
    }

    fn add_block_parameter(&mut self, block: BlockId) -> Result<ValueId> {
        let parameter = self.allocate_value()?;
        let Some(block) = self.blocks.get_mut(block.index()) else {
            return Err(LimboError::InternalError(
                "cannot add a parameter to an unknown compiler IR block".to_owned(),
            ));
        };
        if !block.instructions.is_empty() || block.terminator.is_some() {
            return Err(LimboError::InternalError(
                "compiler IR block parameters must be declared before its body".to_owned(),
            ));
        }
        block.parameters.push(parameter);
        Ok(parameter)
    }

    fn switch_to(&mut self, block: BlockId) -> Result<()> {
        if self.blocks[self.current.index()].terminator.is_none() {
            return Err(LimboError::InternalError(format!(
                "cannot leave unterminated compiler IR block {:?}",
                self.current
            )));
        }
        if block.index() >= self.blocks.len() {
            return Err(LimboError::InternalError(format!(
                "cannot switch to unknown compiler IR block {block:?}"
            )));
        }
        if self.blocks[block.index()].terminator.is_some() {
            return Err(LimboError::InternalError(format!(
                "cannot reopen terminated compiler IR block {block:?}"
            )));
        }
        self.current = block;
        Ok(())
    }

    fn terminate(&mut self, terminator: Terminator) -> Result<()> {
        let block = &mut self.blocks[self.current.index()];
        if block.terminator.replace(terminator).is_some() {
            return Err(LimboError::InternalError(format!(
                "compiler IR block {:?} has multiple terminators",
                self.current
            )));
        }
        Ok(())
    }

    fn finish(mut self, output: ValueId) -> Result<IrProgram> {
        self.terminate(Terminator::Return(output))?;
        let blocks = self
            .blocks
            .into_iter()
            .map(|block| {
                let terminator = block.terminator.ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "compiler IR block {:?} has no terminator",
                        block.id
                    ))
                })?;
                Ok(BasicBlock {
                    id: block.id,
                    parameters: block.parameters,
                    instructions: block.instructions,
                    terminator,
                })
            })
            .collect::<Result<_>>()?;
        let program = IrProgram {
            blocks,
            value_count: self.next_value,
        };
        program.verify()?;
        Ok(program)
    }
}

#[derive(Clone, Copy)]
struct Definition {
    block: BlockId,
    instruction: Option<usize>,
}

/// A verified SSA control-flow region.
#[derive(Debug)]
pub(crate) struct IrProgram {
    blocks: SmallVec<[BasicBlock; 4]>,
    value_count: u32,
}

impl IrProgram {
    fn verify(&self) -> Result<()> {
        if self.blocks.is_empty() {
            return Err(LimboError::InternalError(
                "compiler IR must contain an entry block".to_owned(),
            ));
        }

        let block_count = self.blocks.len();
        let mut definitions = vec![None; self.value_count as usize];
        let mut predecessors = vec![Vec::new(); block_count];
        let mut return_count = 0;

        for (block_index, block) in self.blocks.iter().enumerate() {
            if block.id.index() != block_index {
                return Err(LimboError::InternalError(format!(
                    "compiler IR block {block_index} has non-canonical id {:?}",
                    block.id
                )));
            }
            for parameter in &block.parameters {
                Self::record_definition(
                    &mut definitions,
                    *parameter,
                    Definition {
                        block: block.id,
                        instruction: None,
                    },
                )?;
            }
            for (instruction_index, instruction) in block.instructions.iter().enumerate() {
                Self::record_definition(
                    &mut definitions,
                    instruction.result,
                    Definition {
                        block: block.id,
                        instruction: Some(instruction_index),
                    },
                )?;
            }
            for successor in block.terminator.successors() {
                let Some(target) = self.blocks.get(successor.index()) else {
                    return Err(LimboError::InternalError(format!(
                        "compiler IR block {:?} targets unknown block {successor:?}",
                        block.id
                    )));
                };
                predecessors[target.id.index()].push(block.id);
            }
            match &block.terminator {
                Terminator::Jump { target, arguments } => {
                    let parameter_count = self.blocks[target.index()].parameters.len();
                    if arguments.len() != parameter_count {
                        return Err(LimboError::InternalError(format!(
                            "compiler IR edge {:?} -> {target:?} supplies {} arguments for {parameter_count} parameters",
                            block.id,
                            arguments.len()
                        )));
                    }
                }
                Terminator::Branch {
                    if_true, if_false, ..
                } => {
                    for target in [if_true, if_false] {
                        if !self.blocks[target.index()].parameters.is_empty() {
                            return Err(LimboError::InternalError(format!(
                                "compiler IR branch edge {:?} -> {target:?} cannot pass block arguments",
                                block.id
                            )));
                        }
                    }
                }
                Terminator::Return(_) => return_count += 1,
            }
        }

        if let Some(missing) = definitions.iter().position(Option::is_none) {
            return Err(LimboError::InternalError(format!(
                "compiler IR value %{missing} has no definition"
            )));
        }
        if return_count != 1 {
            return Err(LimboError::InternalError(format!(
                "compiler IR must have exactly one return, found {return_count}"
            )));
        }

        let reachable = self.reachable_blocks();
        if let Some(unreachable) = reachable.iter().position(|reachable| !reachable) {
            return Err(LimboError::InternalError(format!(
                "compiler IR block {unreachable} is unreachable"
            )));
        }
        let dominators = Self::compute_dominators(&predecessors);

        for block in &self.blocks {
            for (instruction_index, instruction) in block.instructions.iter().enumerate() {
                for operand in instruction.op.operands() {
                    Self::verify_use(
                        &definitions,
                        &dominators,
                        operand,
                        block.id,
                        instruction_index,
                    )?;
                }
            }
            for operand in block.terminator.operands() {
                Self::verify_use(
                    &definitions,
                    &dominators,
                    operand,
                    block.id,
                    block.instructions.len(),
                )?;
            }
        }
        Ok(())
    }

    fn record_definition(
        definitions: &mut [Option<Definition>],
        value: ValueId,
        definition: Definition,
    ) -> Result<()> {
        let Some(slot) = definitions.get_mut(value.index()) else {
            return Err(LimboError::InternalError(format!(
                "compiler IR defines out-of-range value {value:?}"
            )));
        };
        if slot.replace(definition).is_some() {
            return Err(LimboError::InternalError(format!(
                "compiler IR value {value:?} has multiple definitions"
            )));
        }
        Ok(())
    }

    fn reachable_blocks(&self) -> Vec<bool> {
        let mut reachable = vec![false; self.blocks.len()];
        let mut stack = vec![BlockId(0)];
        while let Some(block) = stack.pop() {
            if reachable[block.index()] {
                continue;
            }
            reachable[block.index()] = true;
            stack.extend(self.blocks[block.index()].terminator.successors());
        }
        reachable
    }

    fn compute_dominators(predecessors: &[Vec<BlockId>]) -> Vec<Vec<bool>> {
        let block_count = predecessors.len();
        let mut dominators = vec![vec![true; block_count]; block_count];
        dominators[0].fill(false);
        dominators[0][0] = true;

        loop {
            let mut changed = false;
            for block in 1..block_count {
                let mut next = vec![true; block_count];
                for predecessor in &predecessors[block] {
                    for (candidate, dominates) in next.iter_mut().enumerate() {
                        *dominates &= dominators[predecessor.index()][candidate];
                    }
                }
                next[block] = true;
                if next != dominators[block] {
                    dominators[block] = next;
                    changed = true;
                }
            }
            if !changed {
                return dominators;
            }
        }
    }

    fn verify_use(
        definitions: &[Option<Definition>],
        dominators: &[Vec<bool>],
        value: ValueId,
        use_block: BlockId,
        use_instruction: usize,
    ) -> Result<()> {
        let Some(Some(definition)) = definitions.get(value.index()) else {
            return Err(LimboError::InternalError(format!(
                "compiler IR uses undefined value {value:?}"
            )));
        };
        let valid = if definition.block == use_block {
            definition
                .instruction
                .is_none_or(|definition| definition < use_instruction)
        } else {
            dominators[use_block.index()][definition.block.index()]
        };
        if !valid {
            return Err(LimboError::InternalError(format!(
                "compiler IR value {value:?} does not dominate its use in {use_block:?}"
            )));
        }
        Ok(())
    }

    fn output(&self) -> ValueId {
        self.blocks
            .iter()
            .find_map(|block| match block.terminator {
                Terminator::Return(value) => Some(value),
                _ => None,
            })
            .expect("verified compiler IR has exactly one return")
    }

    /// Assign physical registers and labels, then append equivalent VDBE instructions.
    pub(crate) fn lower_into(
        self,
        program: &mut ProgramBuilder,
        target_register: usize,
    ) -> Result<()> {
        self.verify()?;
        let output = self.output();
        let registers = (0..self.value_count)
            .map(|value| {
                if ValueId(value) == output {
                    target_register
                } else {
                    program.alloc_register()
                }
            })
            .collect::<SmallVec<[usize; 8]>>();
        let labels = self
            .blocks
            .iter()
            .map(|_| program.allocate_label())
            .collect::<SmallVec<[_; 4]>>();
        let mut targeted = vec![false; self.blocks.len()];
        for block in &self.blocks {
            for successor in block.terminator.successors() {
                targeted[successor.index()] = true;
            }
        }

        if targeted[0] && program.offset().as_offset_int() == 0 {
            program.emit_insn(Insn::Noop {});
        }
        let return_block = self
            .blocks
            .iter()
            .position(|block| matches!(block.terminator, Terminator::Return(_)))
            .expect("verified compiler IR has exactly one return");
        let continuation =
            (return_block + 1 != self.blocks.len()).then(|| program.allocate_label());

        for block in &self.blocks {
            if targeted[block.id.index()] {
                program.preassign_label_to_next_insn(labels[block.id.index()]);
            }
            for instruction in &block.instructions {
                let destination = registers[instruction.result.index()];
                match &instruction.op {
                    ScalarOp::Constant(Value::Null) => program.emit_insn(Insn::Null {
                        dest: destination,
                        dest_end: None,
                    }),
                    ScalarOp::Constant(Value::Numeric(Numeric::Integer(value))) => {
                        program.emit_insn(Insn::Integer {
                            value: *value,
                            dest: destination,
                        });
                    }
                    ScalarOp::Constant(Value::Numeric(Numeric::Float(value))) => {
                        program.emit_insn(Insn::Real {
                            value: (*value).into(),
                            dest: destination,
                        });
                    }
                    ScalarOp::Constant(Value::Text(value)) => program.emit_insn(Insn::String8 {
                        value: value.to_string(),
                        dest: destination,
                    }),
                    ScalarOp::Constant(Value::Blob(value)) => program.emit_insn(Insn::Blob {
                        value: value.clone(),
                        dest: destination,
                    }),
                    ScalarOp::Add { lhs, rhs } => program.emit_insn(Insn::Add {
                        lhs: registers[lhs.index()],
                        rhs: registers[rhs.index()],
                        dest: destination,
                    }),
                }
            }
            match &block.terminator {
                Terminator::Jump { target, arguments } => {
                    for (argument, parameter) in arguments
                        .iter()
                        .zip(&self.blocks[target.index()].parameters)
                    {
                        let source = registers[argument.index()];
                        let destination = registers[parameter.index()];
                        if source != destination {
                            program.emit_insn(Insn::Copy {
                                src_reg: source,
                                dst_reg: destination,
                                extra_amount: 0,
                            });
                        }
                    }
                    program.emit_insn(Insn::Goto {
                        target_pc: labels[target.index()],
                    });
                }
                Terminator::Branch {
                    condition,
                    if_true,
                    if_false,
                } => {
                    program.emit_insn(Insn::IfNot {
                        reg: registers[condition.index()],
                        target_pc: labels[if_false.index()],
                        jump_if_null: true,
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: labels[if_true.index()],
                    });
                }
                Terminator::Return(_) => {
                    if let Some(continuation) = continuation {
                        program.emit_insn(Insn::Goto {
                            target_pc: continuation,
                        });
                    }
                }
            }
        }
        if let Some(continuation) = continuation {
            program.preassign_label_to_next_insn(continuation);
        }
        Ok(())
    }
}

impl fmt::Display for IrProgram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (block_index, block) in self.blocks.iter().enumerate() {
            write!(f, "block{}", block.id.0)?;
            if !block.parameters.is_empty() {
                write!(f, "(")?;
                for (index, parameter) in block.parameters.iter().enumerate() {
                    if index != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "%{}", parameter.0)?;
                }
                write!(f, ")")?;
            }
            writeln!(f, ":")?;
            for instruction in &block.instructions {
                write!(f, "  %{} = ", instruction.result.0)?;
                match &instruction.op {
                    ScalarOp::Constant(value) => writeln!(f, "constant {value:?}")?,
                    ScalarOp::Add { lhs, rhs } => {
                        writeln!(f, "add %{}, %{}", lhs.0, rhs.0)?;
                    }
                }
            }
            write!(f, "  ")?;
            match &block.terminator {
                Terminator::Jump { target, arguments } => {
                    write!(f, "jump block{}(", target.0)?;
                    for (index, argument) in arguments.iter().enumerate() {
                        if index != 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "%{}", argument.0)?;
                    }
                    writeln!(f, ")")?;
                }
                Terminator::Branch {
                    condition,
                    if_true,
                    if_false,
                } => writeln!(
                    f,
                    "branch %{}, block{}, block{}",
                    condition.0, if_true.0, if_false.0
                )?,
                Terminator::Return(value) => writeln!(f, "return %{}", value.0)?,
            }
            if block_index + 1 != self.blocks.len() {
                writeln!(f)?;
            }
        }
        Ok(())
    }
}

pub(crate) struct Constant(Value);

pub(crate) fn constant(value: Value) -> Constant {
    Constant(value)
}

impl Compile for Constant {
    type Output = ValueId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        builder.push(ScalarOp::Constant(self.0))
    }
}

pub(crate) struct Add {
    lhs: ValueId,
    rhs: ValueId,
}

pub(crate) fn add(lhs: ValueId, rhs: ValueId) -> Add {
    Add { lhs, rhs }
}

impl Compile for Add {
    type Output = ValueId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        builder.push(ScalarOp::Add {
            lhs: self.lhs,
            rhs: self.rhs,
        })
    }
}

pub(crate) fn compile_scalar<Compiler>(compiler: Compiler) -> Result<IrProgram>
where
    Compiler: Compile<Output = ValueId>,
{
    let mut builder = IrBuilder::new();
    let output = compiler.compile(&mut builder)?;
    builder.finish(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vdbe::builder::{ProgramBuilderOpts, QueryMode};
    use crate::{io::MemoryIO, sync::Arc, Database, SqliteDialect};

    #[test]
    fn combinators_build_symbolic_ssa_before_lowering() {
        let compiler = constant(Value::from_i64(40))
            .then(constant(Value::from_i64(2)))
            .and_then(|(lhs, rhs)| add(lhs, rhs));

        let ir = compile_scalar(compiler).unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "block0:\n",
                "  %0 = constant Numeric(Integer(40))\n",
                "  %1 = constant Numeric(Integer(2))\n",
                "  %2 = add %0, %1\n",
                "  return %2\n",
            )
        );
    }

    #[test]
    fn branch_builds_a_diamond_with_a_block_parameter() {
        let compiler = constant(Value::from_i64(1))
            .branch(constant(Value::from_i64(10)), constant(Value::from_i64(20)));

        let ir = compile_scalar(compiler).unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "block0:\n",
                "  %0 = constant Numeric(Integer(1))\n",
                "  branch %0, block1, block2\n",
                "\n",
                "block1:\n",
                "  %2 = constant Numeric(Integer(10))\n",
                "  jump block3(%2)\n",
                "\n",
                "block2:\n",
                "  %3 = constant Numeric(Integer(20))\n",
                "  jump block3(%3)\n",
                "\n",
                "block3(%1):\n",
                "  return %1\n",
            )
        );
    }

    #[test]
    fn nested_branches_lower_when_the_return_block_is_not_last() {
        let compiler = constant(Value::from_i64(1)).branch(
            constant(Value::from_i64(0))
                .branch(constant(Value::from_i64(10)), constant(Value::from_i64(20))),
            constant(Value::from_i64(30)),
        );
        let ir = compile_scalar(compiler).unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 3, 0));

        ir.lower_into(&mut program, 7).unwrap();
        program.resolve_labels().unwrap();

        let goto_count = program
            .insns
            .iter()
            .filter(|(insn, _)| matches!(insn, Insn::Goto { .. }))
            .count();
        assert_eq!(goto_count, 7);
    }

    #[test]
    fn map_transforms_compiler_output_without_emitting_an_operation() {
        let compiler = constant(Value::from_i64(1)).map(|value| value);

        let ir = compile_scalar(compiler).unwrap();

        assert_eq!(ir.value_count, 1);
        assert_eq!(ir.output(), ValueId(0));
    }

    #[test]
    fn boxed_compilers_preserve_composition() {
        let lhs = constant(Value::from_i64(40)).boxed();
        let rhs = constant(Value::from_i64(2)).boxed();
        let compiler = lhs.then(rhs).and_then(|(lhs, rhs)| add(lhs, rhs)).boxed();

        let ir = compile_scalar(compiler).unwrap();

        assert_eq!(ir.output(), ValueId(2));
    }

    #[test]
    fn verifier_rejects_use_without_dominance() {
        let ir = IrProgram {
            blocks: smallvec![
                BasicBlock {
                    id: BlockId(0),
                    parameters: SmallVec::new(),
                    instructions: smallvec![Instruction {
                        result: ValueId(0),
                        op: ScalarOp::Constant(Value::from_i64(1)),
                    }],
                    terminator: Terminator::Branch {
                        condition: ValueId(0),
                        if_true: BlockId(1),
                        if_false: BlockId(2),
                    },
                },
                BasicBlock {
                    id: BlockId(1),
                    parameters: SmallVec::new(),
                    instructions: smallvec![Instruction {
                        result: ValueId(1),
                        op: ScalarOp::Constant(Value::from_i64(2)),
                    }],
                    terminator: Terminator::Jump {
                        target: BlockId(3),
                        arguments: SmallVec::new(),
                    },
                },
                BasicBlock {
                    id: BlockId(2),
                    parameters: SmallVec::new(),
                    instructions: SmallVec::new(),
                    terminator: Terminator::Jump {
                        target: BlockId(3),
                        arguments: SmallVec::new(),
                    },
                },
                BasicBlock {
                    id: BlockId(3),
                    parameters: SmallVec::new(),
                    instructions: SmallVec::new(),
                    terminator: Terminator::Return(ValueId(1)),
                },
            ],
            value_count: 2,
        };

        let error = ir.verify().unwrap_err();
        assert!(error.to_string().contains("does not dominate"));
    }

    #[test]
    fn verifier_rejects_wrong_block_argument_count() {
        let mut builder = IrBuilder::new();
        let target = builder.create_block().unwrap();
        builder.add_block_parameter(target).unwrap();
        builder
            .terminate(Terminator::Jump {
                target,
                arguments: SmallVec::new(),
            })
            .unwrap();
        builder.switch_to(target).unwrap();
        let value = builder.push(ScalarOp::Constant(Value::Null)).unwrap();

        let error = builder.finish(value).unwrap_err();
        assert!(error
            .to_string()
            .contains("supplies 0 arguments for 1 parameters"));
    }

    #[test]
    fn lowering_assigns_registers_after_composition() {
        let compiler = constant(Value::from_i64(40))
            .then(constant(Value::from_i64(2)))
            .and_then(|(lhs, rhs)| add(lhs, rhs));
        let ir = compile_scalar(compiler).unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 3, 0));

        ir.lower_into(&mut program, 7).unwrap();

        assert!(matches!(
            &program.insns[..],
            [
                (Insn::Integer { value: 40, dest: 1 }, _),
                (Insn::Integer { value: 2, dest: 2 }, _),
                (
                    Insn::Add {
                        lhs: 1,
                        rhs: 2,
                        dest: 7
                    },
                    _
                ),
            ]
        ));
    }

    #[test]
    fn numeric_literal_addition_runs_through_the_vdbe() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();

        let rows = connection
            .prepare("SELECT 40 + 2, (40 + 1) + (1 + 0)")
            .unwrap()
            .run_collect_rows()
            .unwrap();

        assert_eq!(rows, vec![vec![Value::from_i64(42), Value::from_i64(42)]]);
    }

    #[test]
    fn literal_case_runs_control_flow_ir_through_the_vdbe() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();

        let rows = connection
            .prepare(
                "SELECT CASE WHEN 1 THEN 10 ELSE 20 END, \
                        CASE WHEN 0 THEN 10 ELSE 20 END, \
                        CASE WHEN NULL THEN 10 ELSE 20 END, \
                        CASE WHEN TRUE THEN 10 END, \
                        CASE WHEN 1 + 0 THEN 10 + 1 ELSE 20 + 2 END",
            )
            .unwrap()
            .run_collect_rows()
            .unwrap();

        assert_eq!(
            rows,
            vec![vec![
                Value::from_i64(10),
                Value::from_i64(20),
                Value::from_i64(20),
                Value::from_i64(10),
                Value::from_i64(11),
            ]]
        );
    }
}
