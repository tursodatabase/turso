//! Deferred, typed building blocks for VDBE compilation.
//!
//! Compiler combinators describe work without mutating [`ProgramBuilder`]. The
//! completed description is first interpreted into symbolic SSA IR and only
//! then lowered into physical VDBE registers, labels, and instructions.

use std::{fmt, marker::PhantomData};

use smallvec::{smallvec, SmallVec};

use crate::{
    numeric::Numeric,
    schema::BTreeTable,
    sync::Arc,
    translate::collate::CollationSeq,
    types::Value,
    vdbe::{
        affinity::Affinity,
        builder::{CursorType, ProgramBuilder},
        insn::{CmpInsFlags, Insn},
        PageIdx,
    },
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

    /// Repeat `body` while `condition` is truthy, carrying one SSA value.
    #[cfg_attr(not(test), allow(dead_code))]
    fn loop_while<ConditionFn, Condition, BodyFn, Body>(
        self,
        condition: ConditionFn,
        body: BodyFn,
    ) -> LoopWhile<Self, ConditionFn, Condition, BodyFn, Body>
    where
        Self: Compile<Output = ValueId>,
        ConditionFn: FnOnce(ValueId) -> Condition,
        Condition: Compile<Output = ValueId>,
        BodyFn: FnOnce(ValueId) -> Body,
        Body: Compile<Output = ValueId>,
    {
        LoopWhile {
            initial: self,
            condition,
            body,
            compilers: PhantomData,
        }
    }

    /// Fold the rows of an already-open symbolic cursor into one SSA value.
    #[cfg_attr(not(test), allow(dead_code))]
    fn fold_cursor<BodyFn, Body>(
        self,
        cursor: CursorId,
        body: BodyFn,
    ) -> CursorFold<Self, BodyFn, Body>
    where
        Self: Compile<Output = ValueId>,
        BodyFn: FnOnce(ValueId) -> Body,
        Body: Compile<Output = ValueId>,
    {
        CursorFold {
            initial: self,
            cursor,
            body,
            compiler: PhantomData,
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

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct LoopWhile<Initial, ConditionFn, Condition, BodyFn, Body> {
    initial: Initial,
    condition: ConditionFn,
    body: BodyFn,
    compilers: PhantomData<fn() -> (Condition, Body)>,
}

impl<Initial, ConditionFn, Condition, BodyFn, Body> Compile
    for LoopWhile<Initial, ConditionFn, Condition, BodyFn, Body>
where
    Initial: Compile<Output = ValueId>,
    ConditionFn: FnOnce(ValueId) -> Condition,
    Condition: Compile<Output = ValueId>,
    BodyFn: FnOnce(ValueId) -> Body,
    Body: Compile<Output = ValueId>,
{
    type Output = ValueId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let initial = self.initial.compile(builder)?;
        let header = builder.create_block()?;
        let body = builder.create_block()?;
        let exit = builder.create_block()?;
        let carried = builder.add_block_parameter(header)?;

        builder.terminate(Terminator::Jump {
            target: header,
            arguments: smallvec![initial],
        })?;

        builder.switch_to(header)?;
        let condition = (self.condition)(carried).compile(builder)?;
        builder.terminate(Terminator::Branch {
            condition,
            if_true: body,
            if_false: exit,
        })?;

        builder.switch_to(body)?;
        let next = (self.body)(carried).compile(builder)?;
        builder.terminate(Terminator::Jump {
            target: header,
            arguments: smallvec![next],
        })?;

        builder.switch_to(exit)?;
        Ok(carried)
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct CursorFold<Initial, BodyFn, Body> {
    initial: Initial,
    cursor: CursorId,
    body: BodyFn,
    compiler: PhantomData<fn() -> Body>,
}

/// Applies one deferred compiler to every row visited by a symbolic cursor.
///
/// Unlike [`CursorFold`], row production has no loop-carried value. The row and
/// exit blocks therefore have no SSA parameters and each cursor edge carries no
/// arguments.
pub(crate) struct ForEachRow<BodyFn, Body> {
    cursor: CursorId,
    body: BodyFn,
    compiler: PhantomData<fn() -> Body>,
}

/// Executes an effectful compiler only when an SSA condition is truthy.
pub(crate) struct When<Body> {
    condition: ValueId,
    body: Body,
}

pub(crate) const fn when<Body>(condition: ValueId, body: Body) -> When<Body>
where
    Body: Compile<Output = ()>,
{
    When { condition, body }
}

impl<Body> Compile for When<Body>
where
    Body: Compile<Output = ()>,
{
    type Output = ();

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let body = builder.create_block()?;
        let continuation = builder.create_block()?;
        builder.terminate(Terminator::Branch {
            condition: self.condition,
            if_true: body,
            if_false: continuation,
        })?;

        builder.switch_to(body)?;
        self.body.compile(builder)?;
        builder.terminate(Terminator::Jump {
            target: continuation,
            arguments: SmallVec::new(),
        })?;

        builder.switch_to(continuation)
    }
}

impl<BodyFn, Body> Compile for ForEachRow<BodyFn, Body>
where
    BodyFn: FnOnce(Row) -> Body,
    Body: Compile<Output = ()>,
{
    type Output = ();

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let row = builder.create_block()?;
        let exit = builder.create_block()?;

        builder.terminate(Terminator::CursorRewind {
            cursor: self.cursor,
            if_non_empty: row,
            if_empty: exit,
            arguments: SmallVec::new(),
        })?;

        builder.switch_to(row)?;
        (self.body)(Row {
            cursor: self.cursor,
        })
        .compile(builder)?;
        builder.terminate(Terminator::CursorNext {
            cursor: self.cursor,
            if_next: row,
            if_done: exit,
            arguments: SmallVec::new(),
        })?;

        builder.switch_to(exit)
    }
}

impl<Initial, BodyFn, Body> Compile for CursorFold<Initial, BodyFn, Body>
where
    Initial: Compile<Output = ValueId>,
    BodyFn: FnOnce(ValueId) -> Body,
    Body: Compile<Output = ValueId>,
{
    type Output = ValueId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let initial = self.initial.compile(builder)?;
        let row = builder.create_block()?;
        let exit = builder.create_block()?;
        let state = builder.add_block_parameter(row)?;
        let result = builder.add_block_parameter(exit)?;

        builder.terminate(Terminator::CursorRewind {
            cursor: self.cursor,
            if_non_empty: row,
            if_empty: exit,
            arguments: smallvec![initial],
        })?;

        builder.switch_to(row)?;
        let next = (self.body)(state).compile(builder)?;
        builder.terminate(Terminator::CursorNext {
            cursor: self.cursor,
            if_next: row,
            if_done: exit,
            arguments: smallvec![next],
        })?;

        builder.switch_to(exit)?;
        Ok(result)
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

/// A comparison operation after the SQL frontend has resolved its semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ComparisonOp {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

/// A SQL three-valued logical operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LogicalOp {
    And,
    Or,
}

/// SQLite comparison metadata resolved before symbolic IR construction.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct ResolvedComparison {
    op: ComparisonOp,
    affinity: Affinity,
    collation: Option<CollationSeq>,
}

pub(crate) const fn resolved_comparison(
    op: ComparisonOp,
    affinity: Affinity,
    collation: Option<CollationSeq>,
) -> ResolvedComparison {
    ResolvedComparison {
        op,
        affinity,
        collation,
    }
}

/// An ordered set of SSA values that must occupy consecutive VDBE registers.
#[derive(Debug)]
pub(crate) struct ValuePack(SmallVec<[ValueId; 4]>);

impl ValuePack {
    fn values(&self) -> &[ValueId] {
        &self.0
    }
}

/// Physical resources allocated while lowering one compiler IR region.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct LoweredRegion {
    result_row_packs: SmallVec<[(usize, usize); 1]>,
}

impl LoweredRegion {
    pub(crate) fn single_result_row_pack(&self) -> Result<(usize, usize)> {
        match self.result_row_packs.as_slice() {
            [pack] => Ok(*pack),
            packs => Err(LimboError::InternalError(format!(
                "compiler IR SELECT expected one result-row pack, lowered {}",
                packs.len()
            ))),
        }
    }
}

/// A symbolic value supplied when an IR region is lowered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct InputId(u32);

impl InputId {
    pub(crate) const fn new(index: u32) -> Self {
        Self(index)
    }

    fn index(self) -> usize {
        self.0 as usize
    }
}

/// A cursor supplied by the surrounding compiler at the lowering boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CursorInputId(u32);

impl CursorInputId {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) const fn new(index: u32) -> Self {
        Self(index)
    }

    fn index(self) -> usize {
        self.0 as usize
    }
}

/// A symbolic cursor resource used by IR operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CursorId(u32);

impl CursorId {
    fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug)]
enum CursorResource {
    External(CursorInputId),
    Owned(CursorType),
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
    Input(InputId),
    Constant(Value),
    Add {
        lhs: ValueId,
        rhs: ValueId,
    },
    Logical {
        op: LogicalOp,
        lhs: ValueId,
        rhs: ValueId,
    },
    Column {
        cursor: CursorId,
        column: usize,
    },
}

#[derive(Debug)]
enum EffectOp {
    OpenRead {
        cursor: CursorId,
        root_page: PageIdx,
        db: usize,
        schema_cookie: u32,
    },
    ResultRow {
        pack: ValuePack,
    },
}

impl ScalarOp {
    fn operands(&self) -> impl Iterator<Item = ValueId> + '_ {
        let operands = match self {
            Self::Input(_) | Self::Constant(_) | Self::Column { .. } => [None, None],
            Self::Add { lhs, rhs } | Self::Logical { lhs, rhs, .. } => [Some(*lhs), Some(*rhs)],
        };
        operands.into_iter().flatten()
    }

    fn cursor(&self) -> Option<CursorId> {
        match self {
            Self::Column { cursor, .. } => Some(*cursor),
            Self::Input(_) | Self::Constant(_) | Self::Add { .. } | Self::Logical { .. } => None,
        }
    }
}

#[derive(Debug)]
enum Instruction {
    Value { result: ValueId, op: ScalarOp },
    Effect(EffectOp),
}

impl Instruction {
    fn operands(&self) -> impl Iterator<Item = ValueId> + '_ {
        let (scalar, values) = match self {
            Self::Value { op, .. } => (Some(op.operands()), &[][..]),
            Self::Effect(EffectOp::OpenRead { .. }) => (None, &[][..]),
            Self::Effect(EffectOp::ResultRow { pack }) => (None, pack.values()),
        };
        scalar.into_iter().flatten().chain(values.iter().copied())
    }

    fn cursor_use(&self) -> Option<CursorId> {
        match self {
            Self::Value { op, .. } => op.cursor(),
            Self::Effect(EffectOp::OpenRead { .. } | EffectOp::ResultRow { .. }) => None,
        }
    }

    fn cursor_definition(&self) -> Option<CursorId> {
        match self {
            Self::Effect(EffectOp::OpenRead { cursor, .. }) => Some(*cursor),
            Self::Value { .. } | Self::Effect(EffectOp::ResultRow { .. }) => None,
        }
    }
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
    Compare {
        lhs: ValueId,
        rhs: ValueId,
        comparison: ResolvedComparison,
        if_true: BlockId,
        if_false: BlockId,
        if_null: BlockId,
    },
    CursorRewind {
        cursor: CursorId,
        if_non_empty: BlockId,
        if_empty: BlockId,
        arguments: SmallVec<[ValueId; 2]>,
    },
    CursorNext {
        cursor: CursorId,
        if_next: BlockId,
        if_done: BlockId,
        arguments: SmallVec<[ValueId; 2]>,
    },
    Return(ValueId),
}

impl Terminator {
    fn successors(&self) -> impl Iterator<Item = BlockId> + '_ {
        self.edges().map(|(target, _)| target)
    }

    fn edges(&self) -> impl Iterator<Item = (BlockId, &[ValueId])> {
        let edges = match self {
            Self::Jump { target, arguments } => [Some((*target, arguments.as_slice())), None, None],
            Self::Branch {
                if_true, if_false, ..
            } => [Some((*if_true, &[][..])), Some((*if_false, &[][..])), None],
            Self::Compare {
                if_true,
                if_false,
                if_null,
                ..
            } => [
                Some((*if_true, &[][..])),
                Some((*if_false, &[][..])),
                Some((*if_null, &[][..])),
            ],
            Self::CursorRewind {
                if_non_empty,
                if_empty,
                arguments,
                ..
            } => [
                Some((*if_non_empty, arguments.as_slice())),
                Some((*if_empty, arguments.as_slice())),
                None,
            ],
            Self::CursorNext {
                if_next,
                if_done,
                arguments,
                ..
            } => [
                Some((*if_next, arguments.as_slice())),
                Some((*if_done, arguments.as_slice())),
                None,
            ],
            Self::Return(_) => [None, None, None],
        };
        edges.into_iter().flatten()
    }

    fn operands(&self) -> impl Iterator<Item = ValueId> + '_ {
        let (first, second, rest) = match self {
            Self::Jump { arguments, .. } => (None, None, arguments.as_slice()),
            Self::Branch { condition, .. } | Self::Return(condition) => {
                (Some(*condition), None, &[][..])
            }
            Self::Compare { lhs, rhs, .. } => (Some(*lhs), Some(*rhs), &[][..]),
            Self::CursorRewind { arguments, .. } | Self::CursorNext { arguments, .. } => {
                (None, None, arguments.as_slice())
            }
        };
        first.into_iter().chain(second).chain(rest.iter().copied())
    }

    fn cursor(&self) -> Option<CursorId> {
        match self {
            Self::CursorRewind { cursor, .. } | Self::CursorNext { cursor, .. } => Some(*cursor),
            Self::Jump { .. } | Self::Branch { .. } | Self::Compare { .. } | Self::Return(_) => {
                None
            }
        }
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
    input_count: u32,
    cursor_input_count: u32,
    cursor_resources: SmallVec<[CursorResource; 2]>,
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
            input_count: 0,
            cursor_input_count: 0,
            cursor_resources: SmallVec::new(),
        }
    }

    fn allocate_cursor(&mut self, resource: CursorResource) -> Result<CursorId> {
        let id = u32::try_from(self.cursor_resources.len()).map_err(|_| {
            LimboError::InternalError("compiler IR cursor identifier overflow".to_owned())
        })?;
        self.cursor_resources.push(resource);
        Ok(CursorId(id))
    }

    fn external_cursor(&mut self, input: CursorInputId) -> Result<CursorId> {
        self.cursor_input_count = self
            .cursor_input_count
            .max(input.0.checked_add(1).ok_or_else(|| {
                LimboError::InternalError("compiler IR cursor input identifier overflow".to_owned())
            })?);
        self.allocate_cursor(CursorResource::External(input))
    }

    fn ensure_cursor_declared(&self, cursor: CursorId) -> Result<()> {
        if cursor.index() >= self.cursor_resources.len() {
            return Err(LimboError::InternalError(format!(
                "compiler IR references undeclared cursor {cursor:?}"
            )));
        }
        Ok(())
    }

    fn allocate_value(&mut self) -> Result<ValueId> {
        let value = ValueId(self.next_value);
        self.next_value = self.next_value.checked_add(1).ok_or_else(|| {
            LimboError::InternalError("compiler IR value identifier overflow".to_owned())
        })?;
        Ok(value)
    }

    fn push(&mut self, op: ScalarOp) -> Result<ValueId> {
        if let ScalarOp::Input(input) = &op {
            self.input_count = self.input_count.max(input.0.checked_add(1).ok_or_else(|| {
                LimboError::InternalError("compiler IR input identifier overflow".to_owned())
            })?);
        }
        if let Some(cursor) = op.cursor() {
            self.ensure_cursor_declared(cursor)?;
        }
        let result = self.allocate_value()?;
        self.blocks[self.current.index()]
            .instructions
            .push(Instruction::Value { result, op });
        Ok(result)
    }

    fn push_effect(&mut self, op: EffectOp) -> Result<()> {
        let cursor = match &op {
            EffectOp::OpenRead { cursor, .. } => Some(*cursor),
            EffectOp::ResultRow { .. } => None,
        };
        if let Some(cursor) = cursor {
            self.ensure_cursor_declared(cursor)?;
        }
        self.blocks[self.current.index()]
            .instructions
            .push(Instruction::Effect(op));
        Ok(())
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
        if let Some(cursor) = terminator.cursor() {
            self.ensure_cursor_declared(cursor)?;
        }
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
            input_count: self.input_count,
            cursor_input_count: self.cursor_input_count,
            cursor_resources: self.cursor_resources,
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
    input_count: u32,
    cursor_input_count: u32,
    cursor_resources: SmallVec<[CursorResource; 2]>,
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
        let mut inputs = vec![false; self.input_count as usize];
        let mut cursor_inputs = vec![false; self.cursor_input_count as usize];
        let mut cursor_definitions = vec![None; self.cursor_resources.len()];
        let mut cursor_uses = vec![false; self.cursor_resources.len()];
        let mut predecessors = vec![Vec::new(); block_count];
        let mut return_count = 0;

        for (index, resource) in self.cursor_resources.iter().enumerate() {
            if let CursorResource::External(input) = resource {
                let Some(used) = cursor_inputs.get_mut(input.index()) else {
                    return Err(LimboError::InternalError(format!(
                        "compiler IR references out-of-range cursor input {input:?}"
                    )));
                };
                *used = true;
                cursor_definitions[index] = Some(Definition {
                    block: BlockId(0),
                    instruction: None,
                });
            }
        }

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
                if matches!(
                    instruction,
                    Instruction::Effect(EffectOp::ResultRow { pack }) if pack.values().is_empty()
                ) {
                    return Err(LimboError::InternalError(
                        "compiler IR result row must contain at least one value".to_owned(),
                    ));
                }
                if let Instruction::Value { result, op } = instruction {
                    if let ScalarOp::Input(input) = op {
                        let Some(used) = inputs.get_mut(input.index()) else {
                            return Err(LimboError::InternalError(format!(
                                "compiler IR references out-of-range input {input:?}"
                            )));
                        };
                        *used = true;
                    }
                    Self::record_definition(
                        &mut definitions,
                        *result,
                        Definition {
                            block: block.id,
                            instruction: Some(instruction_index),
                        },
                    )?;
                }
                if let Some(cursor) = instruction.cursor_use() {
                    let Some(used) = cursor_uses.get_mut(cursor.index()) else {
                        return Err(LimboError::InternalError(format!(
                            "compiler IR references out-of-range cursor {cursor:?}"
                        )));
                    };
                    *used = true;
                }
                if let Some(cursor) = instruction.cursor_definition() {
                    let Some(resource) = self.cursor_resources.get(cursor.index()) else {
                        return Err(LimboError::InternalError(format!(
                            "compiler IR defines out-of-range cursor {cursor:?}"
                        )));
                    };
                    if !matches!(resource, CursorResource::Owned(_)) {
                        return Err(LimboError::InternalError(format!(
                            "compiler IR cannot open external cursor {cursor:?}"
                        )));
                    }
                    Self::record_cursor_definition(
                        &mut cursor_definitions,
                        cursor,
                        Definition {
                            block: block.id,
                            instruction: Some(instruction_index),
                        },
                    )?;
                }
            }
            for (successor, arguments) in block.terminator.edges() {
                let Some(target) = self.blocks.get(successor.index()) else {
                    return Err(LimboError::InternalError(format!(
                        "compiler IR block {:?} targets unknown block {successor:?}",
                        block.id
                    )));
                };
                let parameter_count = target.parameters.len();
                if arguments.len() != parameter_count {
                    return Err(LimboError::InternalError(format!(
                        "compiler IR edge {:?} -> {successor:?} supplies {} arguments for {parameter_count} parameters",
                        block.id,
                        arguments.len()
                    )));
                }
                predecessors[target.id.index()].push(block.id);
            }
            if let Some(cursor) = block.terminator.cursor() {
                let Some(used) = cursor_uses.get_mut(cursor.index()) else {
                    return Err(LimboError::InternalError(format!(
                        "compiler IR references out-of-range cursor {cursor:?}"
                    )));
                };
                *used = true;
            }
            if matches!(block.terminator, Terminator::Return(_)) {
                return_count += 1;
            }
        }

        if let Some(missing) = definitions.iter().position(Option::is_none) {
            return Err(LimboError::InternalError(format!(
                "compiler IR value %{missing} has no definition"
            )));
        }
        if let Some(missing) = inputs.iter().position(|used| !used) {
            return Err(LimboError::InternalError(format!(
                "compiler IR input @{missing} is not referenced"
            )));
        }
        if let Some(missing) = cursor_inputs.iter().position(|used| !used) {
            return Err(LimboError::InternalError(format!(
                "compiler IR cursor input &{missing} is not referenced"
            )));
        }
        if let Some(missing) = cursor_definitions.iter().position(Option::is_none) {
            return Err(LimboError::InternalError(format!(
                "compiler IR cursor ${missing} is not opened"
            )));
        }
        if let Some(missing) = cursor_uses.iter().position(|used| !used) {
            return Err(LimboError::InternalError(format!(
                "compiler IR cursor ${missing} is not used"
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
                for operand in instruction.operands() {
                    Self::verify_use(
                        &definitions,
                        &dominators,
                        operand,
                        block.id,
                        instruction_index,
                    )?;
                }
                if let Some(cursor) = instruction.cursor_use() {
                    Self::verify_cursor_use(
                        &cursor_definitions,
                        &dominators,
                        cursor,
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
            if let Some(cursor) = block.terminator.cursor() {
                Self::verify_cursor_use(
                    &cursor_definitions,
                    &dominators,
                    cursor,
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

    fn record_cursor_definition(
        definitions: &mut [Option<Definition>],
        cursor: CursorId,
        definition: Definition,
    ) -> Result<()> {
        let Some(slot) = definitions.get_mut(cursor.index()) else {
            return Err(LimboError::InternalError(format!(
                "compiler IR defines out-of-range cursor {cursor:?}"
            )));
        };
        if slot.replace(definition).is_some() {
            return Err(LimboError::InternalError(format!(
                "compiler IR cursor {cursor:?} has multiple definitions"
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

    fn verify_cursor_use(
        definitions: &[Option<Definition>],
        dominators: &[Vec<bool>],
        cursor: CursorId,
        use_block: BlockId,
        use_instruction: usize,
    ) -> Result<()> {
        let Some(Some(definition)) = definitions.get(cursor.index()) else {
            return Err(LimboError::InternalError(format!(
                "compiler IR uses unopened cursor {cursor:?}"
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
                "compiler IR cursor {cursor:?} is not open on every path to {use_block:?}"
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

    fn emit_edge_copies(
        &self,
        program: &mut ProgramBuilder,
        registers: &[usize],
        target: BlockId,
        arguments: &[ValueId],
    ) {
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
    }

    /// Assign physical registers and labels, then append equivalent VDBE instructions.
    pub(crate) fn lower_into(
        self,
        program: &mut ProgramBuilder,
        target_register: usize,
    ) -> Result<LoweredRegion> {
        self.lower_into_with_resources(program, target_register, &[], &[])
    }

    /// Bind symbolic inputs to existing registers and lower the region.
    pub(crate) fn lower_into_with_inputs(
        self,
        program: &mut ProgramBuilder,
        target_register: usize,
        input_registers: &[usize],
    ) -> Result<LoweredRegion> {
        self.lower_into_with_resources(program, target_register, input_registers, &[])
    }

    /// Bind symbolic values and cursors, then lower the region.
    pub(crate) fn lower_into_with_resources(
        self,
        program: &mut ProgramBuilder,
        target_register: usize,
        input_registers: &[usize],
        cursor_ids: &[usize],
    ) -> Result<LoweredRegion> {
        self.verify()?;
        if input_registers.len() != self.input_count as usize {
            return Err(LimboError::InternalError(format!(
                "compiler IR expects {} inputs, received {}",
                self.input_count,
                input_registers.len()
            )));
        }
        if cursor_ids.len() != self.cursor_input_count as usize {
            return Err(LimboError::InternalError(format!(
                "compiler IR expects {} cursors, received {}",
                self.cursor_input_count,
                cursor_ids.len()
            )));
        }
        if let Some(cursor) = cursor_ids
            .iter()
            .copied()
            .find(|cursor| *cursor >= program.cursor_ref.len())
        {
            return Err(LimboError::InternalError(format!(
                "compiler IR physical cursor {cursor} is not allocated"
            )));
        }
        let physical_cursors = self
            .cursor_resources
            .iter()
            .map(|resource| match resource {
                CursorResource::External(input) => cursor_ids[input.index()],
                CursorResource::Owned(cursor_type) => program.alloc_cursor_id(cursor_type.clone()),
            })
            .collect::<SmallVec<[usize; 2]>>();
        let output = self.output();
        let mut input_values = vec![None; self.value_count as usize];
        for block in &self.blocks {
            for instruction in &block.instructions {
                if let Instruction::Value {
                    result,
                    op: ScalarOp::Input(input),
                } = instruction
                {
                    input_values[result.index()] = Some(*input);
                }
            }
        }
        let registers = (0..self.value_count)
            .map(|value| {
                if ValueId(value) == output {
                    target_register
                } else if let Some(input) = input_values[value as usize] {
                    input_registers[input.index()]
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
        let mut result_row_packs = SmallVec::new();

        for block in &self.blocks {
            if targeted[block.id.index()] {
                program.preassign_label_to_next_insn(labels[block.id.index()]);
            }
            for instruction in &block.instructions {
                match instruction {
                    Instruction::Value { result, op } => {
                        let destination = registers[result.index()];
                        match op {
                            ScalarOp::Input(input) => {
                                let source = input_registers[input.index()];
                                if source != destination {
                                    program.emit_insn(Insn::Copy {
                                        src_reg: source,
                                        dst_reg: destination,
                                        extra_amount: 0,
                                    });
                                }
                            }
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
                            ScalarOp::Constant(Value::Text(value)) => {
                                program.emit_insn(Insn::String8 {
                                    value: value.to_string(),
                                    dest: destination,
                                });
                            }
                            ScalarOp::Constant(Value::Blob(value)) => {
                                program.emit_insn(Insn::Blob {
                                    value: value.clone(),
                                    dest: destination,
                                });
                            }
                            ScalarOp::Add { lhs, rhs } => program.emit_insn(Insn::Add {
                                lhs: registers[lhs.index()],
                                rhs: registers[rhs.index()],
                                dest: destination,
                            }),
                            ScalarOp::Logical { op, lhs, rhs } => {
                                let lhs = registers[lhs.index()];
                                let rhs = registers[rhs.index()];
                                program.emit_insn(match op {
                                    LogicalOp::And => Insn::And {
                                        lhs,
                                        rhs,
                                        dest: destination,
                                    },
                                    LogicalOp::Or => Insn::Or {
                                        lhs,
                                        rhs,
                                        dest: destination,
                                    },
                                });
                            }
                            ScalarOp::Column { cursor, column } => {
                                program.emit_column_or_rowid(
                                    physical_cursors[cursor.index()],
                                    *column,
                                    destination,
                                );
                            }
                        }
                    }
                    Instruction::Effect(EffectOp::OpenRead {
                        cursor,
                        root_page,
                        db,
                        schema_cookie,
                    }) => {
                        program.begin_read_on_database(*db, *schema_cookie)?;
                        program.emit_insn(Insn::OpenRead {
                            cursor_id: physical_cursors[cursor.index()],
                            root_page: *root_page,
                            db: *db,
                        });
                    }
                    Instruction::Effect(EffectOp::ResultRow { pack }) => {
                        let start = program.alloc_registers(pack.values().len());
                        result_row_packs.push((start, pack.values().len()));
                        for (index, value) in pack.values().iter().enumerate() {
                            let source = registers[value.index()];
                            let destination = start + index;
                            if source != destination {
                                program.emit_insn(Insn::Copy {
                                    src_reg: source,
                                    dst_reg: destination,
                                    extra_amount: 0,
                                });
                            }
                        }
                        program.emit_insn(Insn::ResultRow {
                            start_reg: start,
                            count: pack.values().len(),
                        });
                    }
                }
            }
            match &block.terminator {
                Terminator::Jump { target, arguments } => {
                    self.emit_edge_copies(program, &registers, *target, arguments);
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
                Terminator::Compare {
                    lhs,
                    rhs,
                    comparison,
                    if_true,
                    if_false,
                    if_null,
                } => {
                    // VDBE comparison affinity may rewrite both operands. Keep
                    // the registers backing immutable SSA values unchanged.
                    let lhs_source = registers[lhs.index()];
                    let rhs_source = registers[rhs.index()];
                    let lhs = program.alloc_register();
                    let rhs = program.alloc_register();
                    program.emit_insn(Insn::Copy {
                        src_reg: lhs_source,
                        dst_reg: lhs,
                        extra_amount: 0,
                    });
                    program.emit_insn(Insn::Copy {
                        src_reg: rhs_source,
                        dst_reg: rhs,
                        extra_amount: 0,
                    });
                    program.emit_insn(Insn::IsNull {
                        reg: lhs,
                        target_pc: labels[if_null.index()],
                    });
                    program.emit_insn(Insn::IsNull {
                        reg: rhs,
                        target_pc: labels[if_null.index()],
                    });
                    let flags = CmpInsFlags::default().with_affinity(comparison.affinity);
                    let target_pc = labels[if_true.index()];
                    let collation = comparison.collation;
                    let instruction = match comparison.op {
                        ComparisonOp::Equal => Insn::Eq {
                            lhs,
                            rhs,
                            target_pc,
                            flags,
                            collation,
                        },
                        ComparisonOp::NotEqual => Insn::Ne {
                            lhs,
                            rhs,
                            target_pc,
                            flags,
                            collation,
                        },
                        ComparisonOp::Less => Insn::Lt {
                            lhs,
                            rhs,
                            target_pc,
                            flags,
                            collation,
                        },
                        ComparisonOp::LessEqual => Insn::Le {
                            lhs,
                            rhs,
                            target_pc,
                            flags,
                            collation,
                        },
                        ComparisonOp::Greater => Insn::Gt {
                            lhs,
                            rhs,
                            target_pc,
                            flags,
                            collation,
                        },
                        ComparisonOp::GreaterEqual => Insn::Ge {
                            lhs,
                            rhs,
                            target_pc,
                            flags,
                            collation,
                        },
                    };
                    program.emit_insn(instruction);
                    program.emit_insn(Insn::Goto {
                        target_pc: labels[if_false.index()],
                    });
                }
                Terminator::CursorRewind {
                    cursor,
                    if_non_empty,
                    if_empty,
                    arguments,
                } => {
                    for target in [if_non_empty, if_empty] {
                        self.emit_edge_copies(program, &registers, *target, arguments);
                    }
                    program.emit_insn(Insn::Rewind {
                        cursor_id: physical_cursors[cursor.index()],
                        pc_if_empty: labels[if_empty.index()],
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: labels[if_non_empty.index()],
                    });
                }
                Terminator::CursorNext {
                    cursor,
                    if_next,
                    if_done,
                    arguments,
                } => {
                    for target in [if_next, if_done] {
                        self.emit_edge_copies(program, &registers, *target, arguments);
                    }
                    program.emit_insn(Insn::Next {
                        cursor_id: physical_cursors[cursor.index()],
                        pc_if_next: labels[if_next.index()],
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: labels[if_done.index()],
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
        Ok(LoweredRegion { result_row_packs })
    }
}

impl fmt::Display for IrProgram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, resource) in self.cursor_resources.iter().enumerate() {
            match resource {
                CursorResource::External(input) => {
                    writeln!(f, "cursor ${index} = input &{}", input.0)?;
                }
                CursorResource::Owned(CursorType::BTreeTable(table)) => writeln!(
                    f,
                    "cursor ${index} = btree_table {:?} root {}",
                    table.name, table.root_page
                )?,
                CursorResource::Owned(cursor_type) => {
                    writeln!(f, "cursor ${index} = {cursor_type:?}")?;
                }
            }
        }
        if !self.cursor_resources.is_empty() {
            writeln!(f)?;
        }
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
                match instruction {
                    Instruction::Value { result, op } => {
                        write!(f, "  %{} = ", result.0)?;
                        match op {
                            ScalarOp::Input(input) => writeln!(f, "input @{}", input.0)?,
                            ScalarOp::Constant(value) => writeln!(f, "constant {value:?}")?,
                            ScalarOp::Add { lhs, rhs } => {
                                writeln!(f, "add %{}, %{}", lhs.0, rhs.0)?;
                            }
                            ScalarOp::Logical { op, lhs, rhs } => {
                                writeln!(
                                    f,
                                    "{} %{}, %{}",
                                    match op {
                                        LogicalOp::And => "and",
                                        LogicalOp::Or => "or",
                                    },
                                    lhs.0,
                                    rhs.0
                                )?;
                            }
                            ScalarOp::Column { cursor, column } => {
                                writeln!(f, "column ${}[{column}]", cursor.0)?;
                            }
                        }
                    }
                    Instruction::Effect(EffectOp::OpenRead {
                        cursor,
                        root_page,
                        db,
                        schema_cookie,
                    }) => writeln!(
                        f,
                        "  open_read ${} root {root_page} db {db} schema {schema_cookie}",
                        cursor.0
                    )?,
                    Instruction::Effect(EffectOp::ResultRow { pack }) => {
                        write!(f, "  result_row [")?;
                        Self::fmt_arguments(f, pack.values())?;
                        writeln!(f, "]")?;
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
                Terminator::Compare {
                    lhs,
                    rhs,
                    comparison,
                    if_true,
                    if_false,
                    if_null,
                } => writeln!(
                    f,
                    "compare {:?} %{}, %{} affinity {:?} collation {:?}, block{}, block{}, block{}",
                    comparison.op,
                    lhs.0,
                    rhs.0,
                    comparison.affinity,
                    comparison.collation,
                    if_true.0,
                    if_false.0,
                    if_null.0,
                )?,
                Terminator::CursorRewind {
                    cursor,
                    if_non_empty,
                    if_empty,
                    arguments,
                } => {
                    write!(f, "rewind ${}, block{}(", cursor.0, if_non_empty.0)?;
                    Self::fmt_arguments(f, arguments)?;
                    write!(f, "), block{}(", if_empty.0)?;
                    Self::fmt_arguments(f, arguments)?;
                    writeln!(f, ")")?;
                }
                Terminator::CursorNext {
                    cursor,
                    if_next,
                    if_done,
                    arguments,
                } => {
                    write!(f, "next ${}, block{}(", cursor.0, if_next.0)?;
                    Self::fmt_arguments(f, arguments)?;
                    write!(f, "), block{}(", if_done.0)?;
                    Self::fmt_arguments(f, arguments)?;
                    writeln!(f, ")")?;
                }
                Terminator::Return(value) => writeln!(f, "return %{}", value.0)?,
            }
            if block_index + 1 != self.blocks.len() {
                writeln!(f)?;
            }
        }
        Ok(())
    }
}

impl IrProgram {
    fn fmt_arguments(f: &mut fmt::Formatter<'_>, arguments: &[ValueId]) -> fmt::Result {
        for (index, argument) in arguments.iter().enumerate() {
            if index != 0 {
                write!(f, ", ")?;
            }
            write!(f, "%{}", argument.0)?;
        }
        Ok(())
    }
}

pub(crate) struct Constant(Value);

pub(crate) struct Input(InputId);

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct CursorInput(CursorInputId);

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const fn cursor_input(id: CursorInputId) -> CursorInput {
    CursorInput(id)
}

impl Compile for CursorInput {
    type Output = CursorId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        builder.external_cursor(self.0)
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct OpenReadTable {
    table: Arc<BTreeTable>,
    db: usize,
    schema_cookie: u32,
}

/// The base symbolic stream of rows backed by an opened cursor.
#[derive(Clone, Copy)]
pub(crate) struct CursorRows {
    cursor: CursorId,
}

/// A compile-time row-program algebra analogous to [`Iterator`].
///
/// Stream operators compose compiler descriptions. They do not inspect rows or
/// advance cursors while the Rust expression is being constructed.
pub(crate) trait RowStream: Sized {
    fn for_each<BodyFn, Body>(self, body: BodyFn) -> impl Compile<Output = ()>
    where
        BodyFn: FnOnce(Row) -> Body,
        Body: Compile<Output = ()>;

    fn filter<PredicateFn, Predicate>(
        self,
        predicate: PredicateFn,
    ) -> FilterRows<Self, PredicateFn, Predicate>
    where
        PredicateFn: FnOnce(Row) -> Predicate,
        Predicate: Compile<Output = ValueId>,
    {
        FilterRows {
            source: self,
            predicate,
            compiler: PhantomData,
        }
    }
}

impl RowStream for CursorRows {
    fn for_each<BodyFn, Body>(self, body: BodyFn) -> impl Compile<Output = ()>
    where
        BodyFn: FnOnce(Row) -> Body,
        Body: Compile<Output = ()>,
    {
        ForEachRow {
            cursor: self.cursor,
            body,
            compiler: PhantomData,
        }
    }
}

/// A row stream that admits only rows whose predicate is truthy.
pub(crate) struct FilterRows<Source, PredicateFn, Predicate> {
    source: Source,
    predicate: PredicateFn,
    compiler: PhantomData<fn() -> Predicate>,
}

impl<Source, PredicateFn, Predicate> RowStream for FilterRows<Source, PredicateFn, Predicate>
where
    Source: RowStream,
    PredicateFn: FnOnce(Row) -> Predicate,
    Predicate: Compile<Output = ValueId>,
{
    fn for_each<BodyFn, Body>(self, body: BodyFn) -> impl Compile<Output = ()>
    where
        BodyFn: FnOnce(Row) -> Body,
        Body: Compile<Output = ()>,
    {
        let Self {
            source, predicate, ..
        } = self;
        source.for_each(move |row| {
            predicate(row).and_then(move |condition| when(condition, body(row)))
        })
    }
}

/// One row yielded by a [`RowStream`].
#[derive(Clone, Copy)]
pub(crate) struct Row {
    cursor: CursorId,
}

impl Row {
    pub(crate) const fn column(self, column_index: usize) -> Column {
        column(self.cursor, column_index)
    }
}

/// Opens a table when compiled and returns its symbolic row stream.
pub(crate) struct ScanTable(OpenReadTable);

pub(crate) fn scan_table(table: Arc<BTreeTable>, db: usize, schema_cookie: u32) -> ScanTable {
    ScanTable(open_read_table(table, db, schema_cookie))
}

impl Compile for ScanTable {
    type Output = CursorRows;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        self.0.compile(builder).map(|cursor| CursorRows { cursor })
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn open_read_table(
    table: Arc<BTreeTable>,
    db: usize,
    schema_cookie: u32,
) -> OpenReadTable {
    OpenReadTable {
        table,
        db,
        schema_cookie,
    }
}

impl Compile for OpenReadTable {
    type Output = CursorId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let root_page = self.table.root_page;
        let cursor =
            builder.allocate_cursor(CursorResource::Owned(CursorType::BTreeTable(self.table)))?;
        builder.push_effect(EffectOp::OpenRead {
            cursor,
            root_page,
            db: self.db,
            schema_cookie: self.schema_cookie,
        })?;
        Ok(cursor)
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct Pure<Output>(Output);

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const fn pure<Output>(output: Output) -> Pure<Output> {
    Pure(output)
}

impl<Output> Compile for Pure<Output> {
    type Output = Output;

    fn compile(self, _builder: &mut IrBuilder) -> Result<Self::Output> {
        Ok(self.0)
    }
}

pub(crate) const fn input(id: InputId) -> Input {
    Input(id)
}

impl Compile for Input {
    type Output = ValueId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        builder.push(ScalarOp::Input(self.0))
    }
}

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

pub(crate) struct Logical {
    op: LogicalOp,
    lhs: ValueId,
    rhs: ValueId,
}

pub(crate) struct Compare {
    lhs: ValueId,
    rhs: ValueId,
    comparison: ResolvedComparison,
}

pub(crate) const fn compare(lhs: ValueId, rhs: ValueId, comparison: ResolvedComparison) -> Compare {
    Compare {
        lhs,
        rhs,
        comparison,
    }
}

impl Compile for Compare {
    type Output = ValueId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let if_true = builder.create_block()?;
        let if_false = builder.create_block()?;
        let if_null = builder.create_block()?;
        let merge = builder.create_block()?;
        let result = builder.add_block_parameter(merge)?;

        builder.terminate(Terminator::Compare {
            lhs: self.lhs,
            rhs: self.rhs,
            comparison: self.comparison,
            if_true,
            if_false,
            if_null,
        })?;

        for (block, value) in [
            (if_true, Value::from_i64(1)),
            (if_false, Value::from_i64(0)),
            (if_null, Value::Null),
        ] {
            builder.switch_to(block)?;
            let value = builder.push(ScalarOp::Constant(value))?;
            builder.terminate(Terminator::Jump {
                target: merge,
                arguments: smallvec![value],
            })?;
        }

        builder.switch_to(merge)?;
        Ok(result)
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct Column {
    cursor: CursorId,
    column: usize,
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ResultRow {
    pack: ValuePack,
}

/// Compiles an ordered set of independently composed values into one pack.
pub(crate) struct PackValues {
    values: SmallVec<[BoxedCompile<ValueId>; 4]>,
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn result_row<const N: usize>(values: [ValueId; N]) -> ResultRow {
    ResultRow {
        pack: ValuePack(values.into_iter().collect()),
    }
}

pub(crate) fn result_row_pack(pack: ValuePack) -> ResultRow {
    ResultRow { pack }
}

pub(crate) fn pack_values(values: SmallVec<[BoxedCompile<ValueId>; 4]>) -> PackValues {
    PackValues { values }
}

impl Compile for PackValues {
    type Output = ValuePack;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        if self.values.is_empty() {
            return Err(LimboError::InternalError(
                "compiler IR value pack must contain at least one value".to_owned(),
            ));
        }
        let mut values = SmallVec::with_capacity(self.values.len());
        for compiler in self.values {
            values.push(compiler.compile(builder)?);
        }
        Ok(ValuePack(values))
    }
}

impl Compile for ResultRow {
    type Output = ();

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        if self.pack.values().is_empty() {
            return Err(LimboError::InternalError(
                "compiler IR result row must contain at least one value".to_owned(),
            ));
        }
        builder.push_effect(EffectOp::ResultRow { pack: self.pack })
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const fn column(cursor: CursorId, column: usize) -> Column {
    Column { cursor, column }
}

impl Compile for Column {
    type Output = ValueId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        builder.push(ScalarOp::Column {
            cursor: self.cursor,
            column: self.column,
        })
    }
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

pub(crate) const fn logical(op: LogicalOp, lhs: ValueId, rhs: ValueId) -> Logical {
    Logical { op, lhs, rhs }
}

impl Compile for Logical {
    type Output = ValueId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        builder.push(ScalarOp::Logical {
            op: self.op,
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

/// Builds and verifies an IR program whose observable result is its effects.
pub(crate) fn compile_effect<Compiler>(compiler: Compiler) -> Result<IrProgram>
where
    Compiler: Compile<Output = ()>,
{
    let mut builder = IrBuilder::new();
    compiler.compile(&mut builder)?;
    let completion = builder.push(ScalarOp::Constant(Value::Null))?;
    builder.finish(completion)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vdbe::builder::{CursorType, ProgramBuilderOpts, QueryMode};
    use crate::{io::MemoryIO, schema::BTreeTable, sync::Arc, Database, SqliteDialect, Statement};

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
    fn logical_operators_remain_symbolic_until_lowering() {
        let compiler = constant(Value::Null)
            .then(constant(Value::from_i64(1)))
            .and_then(|(lhs, rhs)| logical(LogicalOp::Or, lhs, rhs));

        let ir = compile_scalar(compiler).unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "block0:\n",
                "  %0 = constant Null\n",
                "  %1 = constant Numeric(Integer(1))\n",
                "  %2 = or %0, %1\n",
                "  return %2\n",
            )
        );
    }

    #[test]
    fn comparisons_expose_sql_three_valued_control_flow() {
        let compiler = constant(Value::from_text("10"))
            .then(constant(Value::from_i64(2)))
            .and_then(|(lhs, rhs)| {
                compare(
                    lhs,
                    rhs,
                    resolved_comparison(
                        ComparisonOp::Greater,
                        Affinity::Numeric,
                        Some(CollationSeq::Binary),
                    ),
                )
            });

        let ir = compile_scalar(compiler).unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "block0:\n",
                "  %0 = constant Text(Text { value: \"10\", subtype: Text })\n",
                "  %1 = constant Numeric(Integer(2))\n",
                "  compare Greater %0, %1 affinity Numeric collation Some(Binary), block1, block2, block3\n",
                "\n",
                "block1:\n",
                "  %3 = constant Numeric(Integer(1))\n",
                "  jump block4(%3)\n",
                "\n",
                "block2:\n",
                "  %4 = constant Numeric(Integer(0))\n",
                "  jump block4(%4)\n",
                "\n",
                "block3:\n",
                "  %5 = constant Null\n",
                "  jump block4(%5)\n",
                "\n",
                "block4(%2):\n",
                "  return %2\n",
            )
        );
    }

    #[test]
    fn comparison_lowering_preserves_immutable_ssa_operands() {
        let compiler = constant(Value::from_text("10"))
            .then(constant(Value::from_i64(2)))
            .and_then(|(lhs, rhs)| {
                compare(
                    lhs,
                    rhs,
                    resolved_comparison(ComparisonOp::Greater, Affinity::Numeric, None),
                )
                .and_then(move |result| result_row([lhs, result]).map(move |()| result))
            });
        let ir = compile_scalar(compiler).unwrap();
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        let mut builder =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 8, 2));
        let result = builder.alloc_register();
        ir.lower_into(&mut builder, result).unwrap();
        builder.emit_insn(Insn::Halt {
            err_code: 0,
            description: String::new(),
            on_error: None,
            description_reg: None,
        });
        let program = builder
            .build(connection.clone(), false, "comparison compiler test")
            .unwrap();
        let mut statement = Statement::new(program, connection.get_pager(), QueryMode::Normal, 0);

        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![vec![Value::from_text("10"), Value::from_i64(1)]]
        );
    }

    #[test]
    fn result_rows_keep_values_symbolic_until_pack_lowering() {
        let compiler = constant(Value::from_i64(1))
            .then(constant(Value::from_i64(2)))
            .and_then(|(first, second)| result_row([first, second]).map(move |()| second));

        let ir = compile_scalar(compiler).unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "block0:\n",
                "  %0 = constant Numeric(Integer(1))\n",
                "  %1 = constant Numeric(Integer(2))\n",
                "  result_row [%0, %1]\n",
                "  return %1\n",
            )
        );
    }

    #[test]
    fn composed_values_are_collected_into_one_symbolic_pack() {
        let values = smallvec![
            constant(Value::from_i64(40)).boxed(),
            constant(Value::from_i64(1))
                .then(constant(Value::from_i64(1)))
                .and_then(|(lhs, rhs)| add(lhs, rhs))
                .boxed(),
        ];
        let ir = compile_effect(pack_values(values).and_then(result_row_pack)).unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "block0:\n",
                "  %0 = constant Numeric(Integer(40))\n",
                "  %1 = constant Numeric(Integer(1))\n",
                "  %2 = constant Numeric(Integer(1))\n",
                "  %3 = add %1, %2\n",
                "  result_row [%0, %3]\n",
                "  %4 = constant Null\n",
                "  return %4\n",
            )
        );
    }

    #[test]
    fn row_stream_for_each_builds_a_cursor_loop_without_loop_state() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE streamed(a)", 2).unwrap());
        let compiler = scan_table(table, 0, 0).and_then(|rows| {
            rows.for_each(|row| {
                pack_values(smallvec![row.column(0).boxed()]).and_then(result_row_pack)
            })
        });

        let ir = compile_effect(compiler).unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "cursor $0 = btree_table \"streamed\" root 2\n",
                "\n",
                "block0:\n",
                "  open_read $0 root 2 db 0 schema 0\n",
                "  rewind $0, block1(), block2()\n",
                "\n",
                "block1:\n",
                "  %0 = column $0[0]\n",
                "  result_row [%0]\n",
                "  next $0, block1(), block2()\n",
                "\n",
                "block2:\n",
                "  %1 = constant Null\n",
                "  return %1\n",
            )
        );
    }

    #[test]
    fn row_stream_filters_wrap_the_consumer_in_source_order() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE filtered(a,b,c)", 2).unwrap());
        let compiler = scan_table(table, 0, 0).and_then(|rows| {
            rows.filter(|row| row.column(0))
                .filter(|row| row.column(1))
                .for_each(|row| {
                    pack_values(smallvec![row.column(2).boxed()]).and_then(result_row_pack)
                })
        });

        let ir = compile_effect(compiler).unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "cursor $0 = btree_table \"filtered\" root 2\n",
                "\n",
                "block0:\n",
                "  open_read $0 root 2 db 0 schema 0\n",
                "  rewind $0, block1(), block2()\n",
                "\n",
                "block1:\n",
                "  %0 = column $0[0]\n",
                "  branch %0, block3, block4\n",
                "\n",
                "block2:\n",
                "  %3 = constant Null\n",
                "  return %3\n",
                "\n",
                "block3:\n",
                "  %1 = column $0[1]\n",
                "  branch %1, block5, block6\n",
                "\n",
                "block4:\n",
                "  next $0, block1(), block2()\n",
                "\n",
                "block5:\n",
                "  %2 = column $0[2]\n",
                "  result_row [%2]\n",
                "  jump block6()\n",
                "\n",
                "block6:\n",
                "  jump block4()\n",
            )
        );
    }

    #[test]
    fn lowered_result_row_uses_a_contiguous_register_pack() {
        let compiler = constant(Value::from_i64(1))
            .then(constant(Value::from_i64(2)))
            .and_then(|(first, second)| result_row([first, second]).map(move |()| second));
        let ir = compile_scalar(compiler).unwrap();
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        let mut builder =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 8, 2));
        let result = builder.alloc_register();
        let lowered = ir.lower_into(&mut builder, result).unwrap();
        assert_eq!(lowered.single_result_row_pack().unwrap().1, 2);
        builder.emit_insn(Insn::Halt {
            err_code: 0,
            description: String::new(),
            on_error: None,
            description_reg: None,
        });
        let program = builder
            .build(connection.clone(), false, "result row compiler test")
            .unwrap();
        let mut statement = Statement::new(program, connection.get_pager(), QueryMode::Normal, 0);

        let rows = statement.run_collect_rows().unwrap();

        assert_eq!(rows, vec![vec![Value::from_i64(1), Value::from_i64(2)]]);
    }

    #[test]
    fn result_rows_reject_empty_value_packs() {
        let compiler =
            constant(Value::from_i64(1)).and_then(|value| result_row([]).map(move |()| value));

        let error = compile_scalar(compiler).unwrap_err();

        assert!(error
            .to_string()
            .contains("result row must contain at least one value"));
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
    fn loop_carries_a_value_through_a_header_parameter() {
        let compiler = constant(Value::from_i64(3)).loop_while(pure, |state| {
            constant(Value::from_i64(-1)).and_then(move |step| add(state, step))
        });

        let ir = compile_scalar(compiler).unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "block0:\n",
                "  %0 = constant Numeric(Integer(3))\n",
                "  jump block1(%0)\n",
                "\n",
                "block1(%1):\n",
                "  branch %1, block2, block3\n",
                "\n",
                "block2:\n",
                "  %2 = constant Numeric(Integer(-1))\n",
                "  %3 = add %1, %2\n",
                "  jump block1(%3)\n",
                "\n",
                "block3:\n",
                "  return %1\n",
            )
        );
    }

    #[test]
    fn loop_lowering_emits_a_resolved_backedge() {
        let compiler = constant(Value::from_i64(3)).loop_while(pure, |state| {
            constant(Value::from_i64(-1)).and_then(move |step| add(state, step))
        });
        let ir = compile_scalar(compiler).unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 3, 0));

        ir.lower_into(&mut program, 7).unwrap();
        program.resolve_labels().unwrap();

        assert!(program.insns.iter().enumerate().any(|(pc, (insn, _))| {
            matches!(
                insn,
                Insn::Goto { target_pc } if target_pc.as_offset_int() < u32::try_from(pc).unwrap()
            )
        }));
        assert!(program.insns.iter().all(|(insn, _)| match insn {
            Insn::Goto { target_pc } | Insn::IfNot { target_pc, .. } => target_pc.is_offset(),
            _ => true,
        }));
    }

    #[test]
    fn lowered_loop_runs_until_its_carried_value_is_false() {
        let compiler = constant(Value::from_i64(3)).loop_while(pure, |state| {
            constant(Value::from_i64(-1)).and_then(move |step| add(state, step))
        });
        let ir = compile_scalar(compiler).unwrap();
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        let mut builder =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 12, 4));
        let result = builder.alloc_register();
        ir.lower_into(&mut builder, result).unwrap();
        builder.emit_insn(Insn::ResultRow {
            start_reg: result,
            count: 1,
        });
        builder.emit_insn(Insn::Halt {
            err_code: 0,
            description: String::new(),
            on_error: None,
            description_reg: None,
        });
        let program = builder
            .build(connection.clone(), false, "loop compiler test")
            .unwrap();
        let mut statement = Statement::new(program, connection.get_pager(), QueryMode::Normal, 0);

        let rows = statement.run_collect_rows().unwrap();

        assert_eq!(rows, vec![vec![Value::from_i64(0)]]);
    }

    #[test]
    fn cursor_fold_builds_effectful_control_flow_with_ssa_state() {
        let compiler = cursor_input(CursorInputId::new(0)).and_then(|cursor| {
            constant(Value::from_i64(0)).fold_cursor(cursor, move |sum| {
                column(cursor, 2).and_then(move |value| add(sum, value))
            })
        });

        let ir = compile_scalar(compiler).unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "cursor $0 = input &0\n",
                "\n",
                "block0:\n",
                "  %0 = constant Numeric(Integer(0))\n",
                "  rewind $0, block1(%0), block2(%0)\n",
                "\n",
                "block1(%1):\n",
                "  %3 = column $0[2]\n",
                "  %4 = add %1, %3\n",
                "  next $0, block1(%4), block2(%4)\n",
                "\n",
                "block2(%2):\n",
                "  return %2\n",
            )
        );
    }

    #[test]
    fn cursor_fold_binds_the_physical_cursor_only_during_lowering() {
        let compiler = cursor_input(CursorInputId::new(0)).and_then(|cursor| {
            constant(Value::from_i64(0)).fold_cursor(cursor, move |sum| {
                column(cursor, 2).and_then(move |value| add(sum, value))
            })
        });
        let ir = compile_scalar(compiler).unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 3, 0));
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE t(a, b, c)", 2).unwrap());
        program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
        let bound_cursor = program.alloc_cursor_id(CursorType::BTreeTable(table));

        ir.lower_into_with_resources(&mut program, 7, &[], &[bound_cursor])
            .unwrap();
        program.resolve_labels().unwrap();

        assert!(program.insns.iter().all(|(insn, _)| match insn {
            Insn::Rewind {
                cursor_id,
                pc_if_empty,
            } => *cursor_id == bound_cursor && pc_if_empty.is_offset(),
            Insn::Column { cursor_id, .. } => *cursor_id == bound_cursor,
            Insn::Next {
                cursor_id,
                pc_if_next,
            } => *cursor_id == bound_cursor && pc_if_next.is_offset(),
            Insn::Goto { target_pc } => target_pc.is_offset(),
            _ => true,
        }));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Rewind { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Column { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Next { .. })));
    }

    #[test]
    fn owned_cursor_is_allocated_and_opened_only_during_lowering() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE owned(a)", 2).unwrap());
        let compiler = open_read_table(table, 0, 0).and_then(|cursor| {
            constant(Value::from_i64(0)).fold_cursor(cursor, move |state| {
                column(cursor, 0).and_then(move |value| add(state, value))
            })
        });
        let ir = compile_scalar(compiler).unwrap();

        assert!(ir.to_string().starts_with(concat!(
            "cursor $0 = btree_table \"owned\" root 2\n",
            "\n",
            "block0:\n",
            "  open_read $0 root 2 db 0 schema 0\n",
            "  %0 = constant Numeric(Integer(0))\n",
        )));

        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 3, 0));
        ir.lower_into(&mut program, 7).unwrap();
        program.resolve_labels().unwrap();

        assert_eq!(program.cursor_ref.len(), 1);
        assert!(matches!(
            &program.cursor_ref[0].1,
            CursorType::BTreeTable(_)
        ));
        let open = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::OpenRead { cursor_id: 0, .. }))
            .unwrap();
        let rewind = program
            .insns
            .iter()
            .position(|(insn, _)| matches!(insn, Insn::Rewind { cursor_id: 0, .. }))
            .unwrap();
        assert!(open < rewind);
    }

    #[test]
    fn cursor_fold_rejects_missing_cursor_bindings() {
        let ir = compile_scalar(cursor_input(CursorInputId::new(0)).and_then(|cursor| {
            constant(Value::from_i64(0))
                .fold_cursor(cursor, move |state| column(cursor, 0).map(move |_| state))
        }))
        .unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 1, 0));

        let error = ir.lower_into(&mut program, 1).unwrap_err();

        assert!(error.to_string().contains("expects 1 cursors, received 0"));
    }

    #[test]
    fn cursor_fold_rejects_an_unallocated_physical_cursor() {
        let ir = compile_scalar(cursor_input(CursorInputId::new(0)).and_then(|cursor| {
            constant(Value::from_i64(0))
                .fold_cursor(cursor, move |state| column(cursor, 0).map(move |_| state))
        }))
        .unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 1, 0));

        let error = ir
            .lower_into_with_resources(&mut program, 1, &[], &[0])
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("physical cursor 0 is not allocated"));
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
    fn symbolic_inputs_are_bound_only_during_lowering() {
        let compiler = input(InputId::new(0))
            .then(constant(Value::from_i64(2)))
            .and_then(|(lhs, rhs)| add(lhs, rhs));
        let ir = compile_scalar(compiler).unwrap();
        assert!(ir.to_string().contains("%0 = input @0"));
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 3, 0));

        ir.lower_into_with_inputs(&mut program, 7, &[4]).unwrap();

        assert!(matches!(
            &program.insns[..],
            [
                (Insn::Integer { value: 2, dest: 1 }, _),
                (
                    Insn::Add {
                        lhs: 4,
                        rhs: 1,
                        dest: 7,
                    },
                    _
                ),
            ]
        ));
    }

    #[test]
    fn lowering_rejects_missing_input_bindings() {
        let ir = compile_scalar(input(InputId::new(0))).unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 1, 0));

        let error = ir.lower_into(&mut program, 1).unwrap_err();

        assert!(error.to_string().contains("expects 1 inputs, received 0"));
    }

    #[test]
    fn verifier_rejects_use_without_dominance() {
        let ir = IrProgram {
            blocks: smallvec![
                BasicBlock {
                    id: BlockId(0),
                    parameters: SmallVec::new(),
                    instructions: smallvec![Instruction::Value {
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
                    instructions: smallvec![Instruction::Value {
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
            input_count: 0,
            cursor_input_count: 0,
            cursor_resources: SmallVec::new(),
        };

        let error = ir.verify().unwrap_err();
        assert!(error.to_string().contains("does not dominate"));
    }

    #[test]
    fn verifier_checks_values_inside_result_row_packs() {
        let ir = IrProgram {
            blocks: smallvec![
                BasicBlock {
                    id: BlockId(0),
                    parameters: SmallVec::new(),
                    instructions: smallvec![Instruction::Value {
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
                    instructions: smallvec![Instruction::Value {
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
                    instructions: smallvec![Instruction::Effect(EffectOp::ResultRow {
                        pack: ValuePack(smallvec![ValueId(1)]),
                    })],
                    terminator: Terminator::Return(ValueId(0)),
                },
            ],
            value_count: 2,
            input_count: 0,
            cursor_input_count: 0,
            cursor_resources: SmallVec::new(),
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
    fn verifier_rejects_wrong_cursor_edge_argument_count() {
        let mut builder = IrBuilder::new();
        let initial = builder.push(ScalarOp::Constant(Value::Null)).unwrap();
        let cursor = builder.external_cursor(CursorInputId::new(0)).unwrap();
        let row = builder.create_block().unwrap();
        let exit = builder.create_block().unwrap();
        builder.add_block_parameter(row).unwrap();
        builder
            .terminate(Terminator::CursorRewind {
                cursor,
                if_non_empty: row,
                if_empty: exit,
                arguments: smallvec![initial],
            })
            .unwrap();
        builder.switch_to(row).unwrap();
        builder
            .terminate(Terminator::Jump {
                target: exit,
                arguments: SmallVec::new(),
            })
            .unwrap();
        builder.switch_to(exit).unwrap();
        let value = builder.push(ScalarOp::Constant(Value::Null)).unwrap();

        let error = builder.finish(value).unwrap_err();

        assert!(error
            .to_string()
            .contains("edge BlockId(0) -> BlockId(2) supplies 1 arguments for 0 parameters"));
    }

    #[test]
    fn verifier_rejects_a_loop_condition_defined_only_in_the_body() {
        let ir = IrProgram {
            blocks: smallvec![
                BasicBlock {
                    id: BlockId(0),
                    parameters: SmallVec::new(),
                    instructions: smallvec![Instruction::Value {
                        result: ValueId(0),
                        op: ScalarOp::Constant(Value::from_i64(1)),
                    }],
                    terminator: Terminator::Jump {
                        target: BlockId(1),
                        arguments: smallvec![ValueId(0)],
                    },
                },
                BasicBlock {
                    id: BlockId(1),
                    parameters: smallvec![ValueId(1)],
                    instructions: SmallVec::new(),
                    terminator: Terminator::Branch {
                        condition: ValueId(2),
                        if_true: BlockId(2),
                        if_false: BlockId(3),
                    },
                },
                BasicBlock {
                    id: BlockId(2),
                    parameters: SmallVec::new(),
                    instructions: smallvec![Instruction::Value {
                        result: ValueId(2),
                        op: ScalarOp::Constant(Value::from_i64(0)),
                    }],
                    terminator: Terminator::Jump {
                        target: BlockId(1),
                        arguments: smallvec![ValueId(2)],
                    },
                },
                BasicBlock {
                    id: BlockId(3),
                    parameters: SmallVec::new(),
                    instructions: SmallVec::new(),
                    terminator: Terminator::Return(ValueId(1)),
                },
            ],
            value_count: 3,
            input_count: 0,
            cursor_input_count: 0,
            cursor_resources: SmallVec::new(),
        };

        let error = ir.verify().unwrap_err();

        assert!(error.to_string().contains("does not dominate"));
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
    fn verifier_rejects_a_cursor_opened_on_only_one_path() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE t(a)", 2).unwrap());
        let mut builder = IrBuilder::new();
        let cursor = builder
            .allocate_cursor(CursorResource::Owned(CursorType::BTreeTable(table)))
            .unwrap();
        let condition = builder
            .push(ScalarOp::Constant(Value::from_i64(1)))
            .unwrap();
        let opened = builder.create_block().unwrap();
        let unopened = builder.create_block().unwrap();
        let merge = builder.create_block().unwrap();
        builder
            .terminate(Terminator::Branch {
                condition,
                if_true: opened,
                if_false: unopened,
            })
            .unwrap();
        builder.switch_to(opened).unwrap();
        builder
            .push_effect(EffectOp::OpenRead {
                cursor,
                root_page: 2,
                db: 0,
                schema_cookie: 0,
            })
            .unwrap();
        builder
            .terminate(Terminator::Jump {
                target: merge,
                arguments: SmallVec::new(),
            })
            .unwrap();
        builder.switch_to(unopened).unwrap();
        builder
            .terminate(Terminator::Jump {
                target: merge,
                arguments: SmallVec::new(),
            })
            .unwrap();
        builder.switch_to(merge).unwrap();
        let value = builder
            .push(ScalarOp::Column { cursor, column: 0 })
            .unwrap();

        let error = builder.finish(value).unwrap_err();

        assert!(error
            .to_string()
            .contains("cursor CursorId(0) is not open on every path to BlockId(3)"));
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

    #[test]
    fn case_with_external_condition_runs_through_control_flow_ir() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection.execute("CREATE TABLE flags(value)").unwrap();
        connection
            .execute("INSERT INTO flags VALUES (1), (0), (NULL)")
            .unwrap();

        let rows = connection
            .prepare(
                "SELECT value, CASE WHEN value THEN 10 + 1 ELSE 20 + 2 END \
                 FROM flags ORDER BY rowid",
            )
            .unwrap()
            .run_collect_rows()
            .unwrap();

        assert_eq!(
            rows,
            vec![
                vec![Value::from_i64(1), Value::from_i64(11)],
                vec![Value::from_i64(0), Value::from_i64(22)],
                vec![Value::Null, Value::from_i64(22)],
            ]
        );
    }

    #[test]
    fn column_addition_uses_symbolic_inputs() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection.execute("CREATE TABLE numbers(a, b)").unwrap();
        connection
            .execute("INSERT INTO numbers VALUES (1, 2), (NULL, 3), (-4, 5)")
            .unwrap();

        let rows = connection
            .prepare("SELECT a + b, a + a, (a + 1) + (b + 2) FROM numbers ORDER BY rowid")
            .unwrap()
            .run_collect_rows()
            .unwrap();

        assert_eq!(
            rows,
            vec![
                vec![Value::from_i64(3), Value::from_i64(2), Value::from_i64(6),],
                vec![Value::Null, Value::Null, Value::Null],
                vec![Value::from_i64(1), Value::from_i64(-8), Value::from_i64(4),],
            ]
        );
    }

    #[test]
    fn parameter_addition_uses_symbolic_inputs() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        let mut statement = connection.prepare("SELECT ?1 + 2, (?1 + ?2) + 1").unwrap();
        statement
            .bind_at(1.try_into().unwrap(), Value::from_i64(40))
            .unwrap();
        statement
            .bind_at(2.try_into().unwrap(), Value::from_i64(1))
            .unwrap();

        let rows = statement.run_collect_rows().unwrap();

        assert_eq!(rows, vec![vec![Value::from_i64(42), Value::from_i64(42)]]);
    }
}
