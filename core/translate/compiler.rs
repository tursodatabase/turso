//! Deferred, typed building blocks for VDBE compilation.
//!
//! Compiler combinators describe work without mutating [`ProgramBuilder`]. The
//! completed description is first interpreted into symbolic SSA IR and only
//! then lowered into physical VDBE registers, labels, and instructions.

use std::{fmt, marker::PhantomData};

use rustc_hash::FxHashSet as HashSet;
use smallvec::{smallvec, SmallVec};
use turso_parser::ast::{NullsOrder, SortOrder, Variable};

use crate::{
    numeric::Numeric,
    schema::{BTreeTable, Index, PseudoCursorType},
    sync::Arc,
    translate::collate::CollationSeq,
    types::{SeekOp, Value},
    vdbe::{
        affinity::Affinity,
        builder::{CursorType, ProgramBuilder},
        insn::{to_u16, CmpInsFlags, HashDistinctData, IdxInsertFlags, Insn, SortComparatorType},
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
        IfTrue: Compile,
        IfTrue::Output: BranchOutput,
        IfFalse: Compile<Output = IfTrue::Output>,
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

pub(crate) struct BindCursorInput<Compiler> {
    input: CursorInputId,
    cursor: CursorId,
    compiler: Compiler,
}

pub(crate) struct BindInput<Compiler> {
    input: InputId,
    value: ValueId,
    compiler: Compiler,
}

pub(crate) fn bind_input<Compiler>(
    input: InputId,
    value: ValueId,
    compiler: Compiler,
) -> BindInput<Compiler> {
    BindInput {
        input,
        value,
        compiler,
    }
}

impl<Compiler> Compile for BindInput<Compiler>
where
    Compiler: Compile,
{
    type Output = Compiler::Output;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let previous = builder.bind_input(self.input, self.value);
        let result = self.compiler.compile(builder);
        builder.restore_input(self.input, previous);
        result
    }
}

pub(crate) fn bind_cursor_input<Compiler>(
    input: CursorInputId,
    cursor: CursorId,
    compiler: Compiler,
) -> BindCursorInput<Compiler> {
    BindCursorInput {
        input,
        cursor,
        compiler,
    }
}

impl<Compiler> Compile for BindCursorInput<Compiler>
where
    Compiler: Compile,
{
    type Output = Compiler::Output;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let previous = builder.bind_cursor_input(self.input, self.cursor)?;
        let result = self.compiler.compile(builder);
        builder.restore_cursor_input(self.input, previous);
        result
    }
}

/// Initializes one symbolic cursor the first time control reaches this region.
///
/// The cursor identity is allocated while the compiler description is built,
/// but its open and population effects remain in the guarded runtime region.
/// Re-entry skips those effects and continues with the already-open cursor.
pub(crate) struct InitializeCursorOnce<Compiler> {
    compiler: Compiler,
}

pub(crate) const fn initialize_cursor_once<Compiler>(
    compiler: Compiler,
) -> InitializeCursorOnce<Compiler>
where
    Compiler: Compile<Output = CursorId>,
{
    InitializeCursorOnce { compiler }
}

impl<Compiler> Compile for InitializeCursorOnce<Compiler>
where
    Compiler: Compile<Output = CursorId>,
{
    type Output = CursorId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let initialize = builder.create_block()?;
        let ready = builder.create_block()?;
        builder.terminate(Terminator::Once { initialize, ready })?;

        builder.switch_to(initialize)?;
        let cursor = self.compiler.compile(builder)?;
        builder.terminate(Terminator::Jump {
            target: ready,
            arguments: SmallVec::new(),
        })?;

        builder.switch_to(ready)?;
        Ok(cursor)
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
    IfTrue: Compile,
    IfTrue::Output: BranchOutput,
    IfFalse: Compile<Output = IfTrue::Output>,
{
    type Output = IfTrue::Output;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let condition = self.condition.compile(builder)?;
        let if_true_block = builder.create_block()?;
        let if_false_block = builder.create_block()?;
        let merge_block = builder.create_block()?;

        builder.terminate(Terminator::Branch {
            condition,
            if_true: if_true_block,
            if_false: if_false_block,
        })?;

        builder.switch_to(if_true_block)?;
        let if_true = self.if_true.compile(builder)?.into_branch_values();
        let mut output = SmallVec::with_capacity(if_true.len());
        for _ in 0..if_true.len() {
            output.push(builder.add_block_parameter(merge_block)?);
        }
        builder.terminate(Terminator::Jump {
            target: merge_block,
            arguments: if_true,
        })?;

        builder.switch_to(if_false_block)?;
        let if_false = self.if_false.compile(builder)?.into_branch_values();
        if if_false.len() != output.len() {
            return Err(LimboError::InternalError(format!(
                "compiler branch changed output arity from {} to {}",
                output.len(),
                if_false.len()
            )));
        }
        builder.terminate(Terminator::Jump {
            target: merge_block,
            arguments: if_false,
        })?;

        builder.switch_to(merge_block)?;
        IfTrue::Output::from_branch_values(output)
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
    row_cursor: CursorId,
    deferred_seek: Option<DeferredSeekCursors>,
    source: CursorRowSource,
    body: BodyFn,
    compiler: PhantomData<fn() -> Body>,
}

/// Folds a symbolic state pack over rows until the consumer asks to stop.
pub(crate) struct TryFoldRows<Initial, BodyFn, Body> {
    initial: Initial,
    cursor: CursorId,
    row_cursor: CursorId,
    deferred_seek: Option<DeferredSeekCursors>,
    source: CursorRowSource,
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
        let check = self
            .source
            .has_end_bound()
            .then(|| builder.create_block())
            .transpose()?;

        match self.source.clone() {
            CursorRowSource::Scan(direction) => builder.terminate(Terminator::CursorStart {
                cursor: self.cursor,
                direction,
                if_non_empty: row,
                if_empty: exit,
                arguments: SmallVec::new(),
            })?,
            CursorRowSource::Rowid(rowid) => builder.terminate(Terminator::CursorSeekRowid {
                cursor: self.cursor,
                rowid,
                if_found: row,
                if_not_found: exit,
                arguments: SmallVec::new(),
            })?,
            CursorRowSource::TableRange(range) => match range.start {
                Some(start) => builder.terminate(Terminator::TableSeek {
                    cursor: self.cursor,
                    rowid: start.rowid,
                    op: start.op,
                    if_found: check.unwrap_or(row),
                    if_empty: exit,
                    arguments: SmallVec::new(),
                })?,
                None => builder.terminate(Terminator::CursorStart {
                    cursor: self.cursor,
                    direction: range.direction,
                    if_non_empty: check.unwrap_or(row),
                    if_empty: exit,
                    arguments: SmallVec::new(),
                })?,
            },
            CursorRowSource::IndexRange(range) => match range.start {
                Some(start) => builder.terminate(Terminator::IndexSeek {
                    cursor: self.cursor,
                    key: start.key,
                    op: start.op,
                    if_found: check.unwrap_or(row),
                    if_empty: exit,
                    arguments: SmallVec::new(),
                })?,
                None => builder.terminate(Terminator::CursorStart {
                    cursor: self.cursor,
                    direction: range.direction,
                    if_non_empty: check.unwrap_or(row),
                    if_empty: exit,
                    arguments: SmallVec::new(),
                })?,
            },
        }

        if let CursorRowSource::TableRange(range) = self.source.clone() {
            if let Some(end) = range.end {
                builder.switch_to(check.expect("bounded table range must have a check block"))?;
                builder.terminate(Terminator::TableBound {
                    cursor: self.cursor,
                    rowid: end.rowid,
                    op: end.op,
                    affinity: range.affinity,
                    if_before_end: row,
                    if_at_end: exit,
                    arguments: SmallVec::new(),
                })?;
            }
        }

        if let CursorRowSource::IndexRange(range) = self.source.clone() {
            if let Some(end) = range.end {
                builder.switch_to(check.expect("bounded index range must have a check block"))?;
                builder.terminate(Terminator::IndexBound {
                    cursor: self.cursor,
                    key: end.key,
                    op: end.op,
                    if_before_end: row,
                    if_at_end: exit,
                    arguments: SmallVec::new(),
                })?;
            }
        }

        builder.switch_to(row)?;
        if let Some(seek) = self.deferred_seek {
            builder.push_effect(EffectOp::DeferredSeek {
                index: seek.index,
                table: seek.table,
            })?;
        }
        (self.body)(Row {
            cursor: self.row_cursor,
        })
        .compile(builder)?;
        match self.source {
            CursorRowSource::Scan(direction) => builder.terminate(Terminator::CursorAdvance {
                cursor: self.cursor,
                direction,
                if_next: row,
                if_done: exit,
                arguments: SmallVec::new(),
            })?,
            CursorRowSource::Rowid(_) => builder.terminate(Terminator::Jump {
                target: exit,
                arguments: SmallVec::new(),
            })?,
            CursorRowSource::TableRange(range) => builder.terminate(Terminator::CursorAdvance {
                cursor: self.cursor,
                direction: range.direction,
                if_next: check.unwrap_or(row),
                if_done: exit,
                arguments: SmallVec::new(),
            })?,
            CursorRowSource::IndexRange(range) => builder.terminate(Terminator::CursorAdvance {
                cursor: self.cursor,
                direction: range.direction,
                if_next: check.unwrap_or(row),
                if_done: exit,
                arguments: SmallVec::new(),
            })?,
        }

        builder.switch_to(exit)
    }
}

impl<Initial, BodyFn, Body> Compile for TryFoldRows<Initial, BodyFn, Body>
where
    Initial: Compile<Output = LoopState>,
    BodyFn: FnOnce(Row, LoopState) -> Body,
    Body: Compile<Output = LoopStep>,
{
    type Output = LoopState;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let initial = self.initial.compile(builder)?;
        let row = builder.create_block()?;
        let advance = builder.create_block()?;
        let stop = builder.create_block()?;
        let exit = builder.create_block()?;
        let check = self
            .source
            .has_end_bound()
            .then(|| builder.create_block())
            .transpose()?;
        let mut row_state = SmallVec::with_capacity(initial.len());
        let mut result_state = SmallVec::with_capacity(initial.len());
        let mut check_state = SmallVec::with_capacity(initial.len());
        for _ in 0..initial.len() {
            row_state.push(builder.add_block_parameter(row)?);
            result_state.push(builder.add_block_parameter(exit)?);
            if let Some(check) = check {
                check_state.push(builder.add_block_parameter(check)?);
            }
        }

        match self.source.clone() {
            CursorRowSource::Scan(direction) => builder.terminate(Terminator::CursorStart {
                cursor: self.cursor,
                direction,
                if_non_empty: row,
                if_empty: exit,
                arguments: initial.values,
            })?,
            CursorRowSource::Rowid(rowid) => builder.terminate(Terminator::CursorSeekRowid {
                cursor: self.cursor,
                rowid,
                if_found: row,
                if_not_found: exit,
                arguments: initial.values,
            })?,
            CursorRowSource::TableRange(range) => match range.start {
                Some(start) => builder.terminate(Terminator::TableSeek {
                    cursor: self.cursor,
                    rowid: start.rowid,
                    op: start.op,
                    if_found: check.unwrap_or(row),
                    if_empty: exit,
                    arguments: initial.values,
                })?,
                None => builder.terminate(Terminator::CursorStart {
                    cursor: self.cursor,
                    direction: range.direction,
                    if_non_empty: check.unwrap_or(row),
                    if_empty: exit,
                    arguments: initial.values,
                })?,
            },
            CursorRowSource::IndexRange(range) => match range.start {
                Some(start) => builder.terminate(Terminator::IndexSeek {
                    cursor: self.cursor,
                    key: start.key,
                    op: start.op,
                    if_found: check.unwrap_or(row),
                    if_empty: exit,
                    arguments: initial.values,
                })?,
                None => builder.terminate(Terminator::CursorStart {
                    cursor: self.cursor,
                    direction: range.direction,
                    if_non_empty: check.unwrap_or(row),
                    if_empty: exit,
                    arguments: initial.values,
                })?,
            },
        }

        if let CursorRowSource::TableRange(range) = self.source.clone() {
            if let Some(end) = range.end {
                builder.switch_to(check.expect("bounded table range must have a check block"))?;
                builder.terminate(Terminator::TableBound {
                    cursor: self.cursor,
                    rowid: end.rowid,
                    op: end.op,
                    affinity: range.affinity,
                    if_before_end: row,
                    if_at_end: exit,
                    arguments: check_state.clone(),
                })?;
            }
        }

        if let CursorRowSource::IndexRange(range) = self.source.clone() {
            if let Some(end) = range.end {
                builder.switch_to(check.expect("bounded index range must have a check block"))?;
                builder.terminate(Terminator::IndexBound {
                    cursor: self.cursor,
                    key: end.key,
                    op: end.op,
                    if_before_end: row,
                    if_at_end: exit,
                    arguments: check_state,
                })?;
            }
        }

        builder.switch_to(row)?;
        if let Some(seek) = self.deferred_seek {
            builder.push_effect(EffectOp::DeferredSeek {
                index: seek.index,
                table: seek.table,
            })?;
        }
        let step = (self.body)(
            Row {
                cursor: self.row_cursor,
            },
            LoopState { values: row_state },
        )
        .compile(builder)?;
        if step.state.len() != result_state.len() {
            return Err(LimboError::InternalError(format!(
                "row stream loop body changed state arity from {} to {}",
                result_state.len(),
                step.state.len()
            )));
        }
        builder.terminate(Terminator::Branch {
            condition: step.should_continue,
            if_true: advance,
            if_false: stop,
        })?;

        builder.switch_to(advance)?;
        match self.source {
            CursorRowSource::Scan(direction) => builder.terminate(Terminator::CursorAdvance {
                cursor: self.cursor,
                direction,
                if_next: row,
                if_done: exit,
                arguments: step.state.values.clone(),
            })?,
            CursorRowSource::Rowid(_) => builder.terminate(Terminator::Jump {
                target: exit,
                arguments: step.state.values.clone(),
            })?,
            CursorRowSource::TableRange(range) => builder.terminate(Terminator::CursorAdvance {
                cursor: self.cursor,
                direction: range.direction,
                if_next: check.unwrap_or(row),
                if_done: exit,
                arguments: step.state.values.clone(),
            })?,
            CursorRowSource::IndexRange(range) => builder.terminate(Terminator::CursorAdvance {
                cursor: self.cursor,
                direction: range.direction,
                if_next: check.unwrap_or(row),
                if_done: exit,
                arguments: step.state.values.clone(),
            })?,
        }

        builder.switch_to(stop)?;
        builder.terminate(Terminator::Jump {
            target: exit,
            arguments: step.state.values,
        })?;

        builder.switch_to(exit)?;
        Ok(LoopState {
            values: result_state,
        })
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

        builder.terminate(Terminator::CursorStart {
            cursor: self.cursor,
            direction: ScanDirection::Forward,
            if_non_empty: row,
            if_empty: exit,
            arguments: smallvec![initial],
        })?;

        builder.switch_to(row)?;
        let next = (self.body)(state).compile(builder)?;
        builder.terminate(Terminator::CursorAdvance {
            cursor: self.cursor,
            direction: ScanDirection::Forward,
            if_next: row,
            if_done: exit,
            arguments: smallvec![next],
        })?;

        builder.switch_to(exit)?;
        Ok(result)
    }
}

/// The symbolic result of one SSA operation.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) struct ValueId(u32);

impl ValueId {
    fn index(self) -> usize {
        self.0 as usize
    }
}

/// An output whose SSA values can be joined through block parameters.
pub(crate) trait BranchOutput: Sized {
    fn into_branch_values(self) -> SmallVec<[ValueId; 2]>;

    fn from_branch_values(values: SmallVec<[ValueId; 2]>) -> Result<Self>;
}

impl BranchOutput for ValueId {
    fn into_branch_values(self) -> SmallVec<[ValueId; 2]> {
        smallvec![self]
    }

    fn from_branch_values(mut values: SmallVec<[ValueId; 2]>) -> Result<Self> {
        let value = values.pop().ok_or_else(|| {
            LimboError::InternalError("scalar compiler branch produced no value".to_owned())
        })?;
        if !values.is_empty() {
            return Err(LimboError::InternalError(format!(
                "scalar compiler branch produced {} values",
                values.len() + 1
            )));
        }
        Ok(value)
    }
}

/// SSA values carried together across a row-stream loop backedge.
#[derive(Clone, Debug)]
pub(crate) struct LoopState {
    values: SmallVec<[ValueId; 2]>,
}

impl LoopState {
    pub(crate) fn empty() -> Self {
        Self {
            values: SmallVec::new(),
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn push(&mut self, value: ValueId) {
        self.values.push(value);
    }

    fn pop(&mut self) -> Option<ValueId> {
        self.values.pop()
    }

    fn single(value: ValueId) -> Self {
        Self {
            values: smallvec![value],
        }
    }

    fn replace_single(&mut self, value: ValueId) {
        assert_eq!(self.values.len(), 1);
        self.values[0] = value;
    }

    fn into_single(mut self) -> ValueId {
        let value = self
            .values
            .pop()
            .expect("single-value loop state must contain one value");
        assert!(self.values.is_empty());
        value
    }
}

/// The next loop-carried state and whether the producer should advance.
pub(crate) struct LoopStep {
    state: LoopState,
    should_continue: ValueId,
}

impl BranchOutput for LoopState {
    fn into_branch_values(self) -> SmallVec<[ValueId; 2]> {
        self.values
    }

    fn from_branch_values(values: SmallVec<[ValueId; 2]>) -> Result<Self> {
        Ok(Self { values })
    }
}

impl BranchOutput for LoopStep {
    fn into_branch_values(self) -> SmallVec<[ValueId; 2]> {
        let mut values = self.state.values;
        values.push(self.should_continue);
        values
    }

    fn from_branch_values(mut values: SmallVec<[ValueId; 2]>) -> Result<Self> {
        let should_continue = values.pop().ok_or_else(|| {
            LimboError::InternalError("row-stream branch produced no continuation value".to_owned())
        })?;
        Ok(Self {
            state: LoopState { values },
            should_continue,
        })
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
#[derive(Clone, Debug)]
pub(crate) struct ValuePack(SmallVec<[ValueId; 4]>);

impl ValuePack {
    fn values(&self) -> &[ValueId] {
        &self.0
    }
}

/// A symbolic index key together with the comparison affinities resolved by
/// the SQL frontend. Its values remain independent SSA values until lowering
/// materializes the contiguous register pack required by VDBE seek opcodes.
#[derive(Clone, Debug)]
struct IndexKey {
    pack: ValuePack,
    affinities: SmallVec<[Affinity; 4]>,
    null_policies: SmallVec<[IndexNullPolicy; 4]>,
}

/// Whether a NULL endpoint value makes the range empty or participates in the
/// B-tree comparison as a planner-injected sentinel.
#[derive(Clone, Copy, Debug)]
enum IndexNullPolicy {
    AbortRange,
    Compare,
}

impl IndexKey {
    fn new(
        values: SmallVec<[ValueId; 4]>,
        affinities: SmallVec<[Affinity; 4]>,
        null_policies: SmallVec<[IndexNullPolicy; 4]>,
    ) -> Result<Self> {
        if values.is_empty()
            || values.len() != affinities.len()
            || values.len() != null_policies.len()
        {
            return Err(LimboError::InternalError(format!(
                "compiler IR index key has {} values, {} affinities, and {} NULL policies",
                values.len(),
                affinities.len(),
                null_policies.len()
            )));
        }
        Ok(Self {
            pack: ValuePack(values),
            affinities,
            null_policies,
        })
    }

    fn values(&self) -> &[ValueId] {
        self.pack.values()
    }
}

/// Fully resolved ordering semantics for one symbolic sorter key.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct SortKey {
    pub(crate) order: SortOrder,
    pub(crate) collation: Option<CollationSeq>,
    pub(crate) nulls: Option<NullsOrder>,
    pub(crate) comparator: Option<SortComparatorType>,
}

impl SortKey {
    pub(crate) const fn new(
        order: SortOrder,
        collation: Option<CollationSeq>,
        nulls: Option<NullsOrder>,
        comparator: Option<SortComparatorType>,
    ) -> Self {
        Self {
            order,
            collation,
            nulls,
            comparator,
        }
    }
}

/// Physical resources allocated while lowering one compiler IR region.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct LoweredRegion {
    result_row_packs: SmallVec<[(usize, usize); 1]>,
}

#[derive(Clone, Copy)]
struct PhysicalSorter {
    cursor: usize,
    pseudo_cursor: usize,
    data_register: usize,
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

    pub(crate) fn expect_no_result_rows(&self) -> Result<()> {
        if self.result_row_packs.is_empty() {
            Ok(())
        } else {
            Err(LimboError::InternalError(format!(
                "compiler IR destination expected no result rows, lowered {}",
                self.result_row_packs.len()
            )))
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

/// Logical traversal order for a symbolic cursor row stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScanDirection {
    Forward,
    Reverse,
}

/// A symbolic sorter resource, distinct from cursors that can be scanned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SorterId(u32);

impl SorterId {
    fn index(self) -> usize {
        self.0 as usize
    }
}

/// A symbolic set used to admit only the first occurrence of a value pack.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DistinctSetId(u32);

impl DistinctSetId {
    fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug)]
enum CursorResource {
    External(CursorInputId),
    Owned(CursorType),
}

#[derive(Debug)]
struct SorterResource {
    keys: SmallVec<[SortKey; 4]>,
    record_width: usize,
    affinities: Option<SmallVec<[Affinity; 4]>>,
}

#[derive(Debug)]
struct DistinctSetResource {
    collations: SmallVec<[CollationSeq; 4]>,
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
    Parameter(Variable),
    Constant(Value),
    Add {
        lhs: ValueId,
        rhs: ValueId,
    },
    MustBeInt {
        value: ValueId,
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
    RowId {
        cursor: CursorId,
    },
    IndexRowId {
        cursor: CursorId,
    },
    SorterColumn {
        sorter: SorterId,
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
    OpenEphemeralIndex {
        cursor: CursorId,
    },
    DeferredSeek {
        index: CursorId,
        table: CursorId,
    },
    ResultRow {
        pack: ValuePack,
    },
    IndexInsert {
        cursor: CursorId,
        pack: ValuePack,
        index_name: String,
        affinity: Option<String>,
    },
    OpenSorter {
        sorter: SorterId,
    },
    SorterInsert {
        sorter: SorterId,
        pack: ValuePack,
    },
    SorterData {
        sorter: SorterId,
    },
    OpenDistinctSet {
        set: DistinctSetId,
    },
}

impl ScalarOp {
    fn operands(&self) -> impl Iterator<Item = ValueId> + '_ {
        let operands = match self {
            Self::Input(_)
            | Self::Parameter(_)
            | Self::Constant(_)
            | Self::Column { .. }
            | Self::RowId { .. }
            | Self::IndexRowId { .. }
            | Self::SorterColumn { .. } => [None, None],
            Self::MustBeInt { value } => [Some(*value), None],
            Self::Add { lhs, rhs } | Self::Logical { lhs, rhs, .. } => [Some(*lhs), Some(*rhs)],
        };
        operands.into_iter().flatten()
    }

    fn cursor(&self) -> Option<CursorId> {
        match self {
            Self::Column { cursor, .. } | Self::RowId { cursor } | Self::IndexRowId { cursor } => {
                Some(*cursor)
            }
            Self::Input(_)
            | Self::Parameter(_)
            | Self::Constant(_)
            | Self::Add { .. }
            | Self::MustBeInt { .. }
            | Self::Logical { .. }
            | Self::SorterColumn { .. } => None,
        }
    }

    fn sorter(&self) -> Option<SorterId> {
        match self {
            Self::SorterColumn { sorter, .. } => Some(*sorter),
            Self::Input(_)
            | Self::Parameter(_)
            | Self::Constant(_)
            | Self::Add { .. }
            | Self::MustBeInt { .. }
            | Self::Logical { .. }
            | Self::Column { .. }
            | Self::RowId { .. }
            | Self::IndexRowId { .. } => None,
        }
    }

    /// Whether removing an unused result preserves observable SQL behavior.
    fn can_eliminate_if_unused(&self) -> bool {
        match self {
            Self::Input(_)
            | Self::Parameter(_)
            | Self::Constant(_)
            | Self::Add { .. }
            | Self::Logical { .. } => true,
            // Integer coercion can raise a datatype error. Column reads remain
            // ordered until storage-read and corruption behavior is modeled as
            // an explicit effect in the IR.
            Self::MustBeInt { .. }
            | Self::Column { .. }
            | Self::RowId { .. }
            | Self::IndexRowId { .. }
            | Self::SorterColumn { .. } => false,
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
            Self::Effect(
                EffectOp::OpenRead { .. }
                | EffectOp::OpenEphemeralIndex { .. }
                | EffectOp::DeferredSeek { .. }
                | EffectOp::OpenSorter { .. }
                | EffectOp::SorterData { .. }
                | EffectOp::OpenDistinctSet { .. },
            ) => (None, &[][..]),
            Self::Effect(
                EffectOp::ResultRow { pack }
                | EffectOp::IndexInsert { pack, .. }
                | EffectOp::SorterInsert { pack, .. },
            ) => (None, pack.values()),
        };
        scalar.into_iter().flatten().chain(values.iter().copied())
    }

    fn cursor_uses(&self) -> smallvec::IntoIter<[CursorId; 2]> {
        let cursors = match self {
            Self::Value { op, .. } => op.cursor().into_iter().collect(),
            Self::Effect(EffectOp::DeferredSeek { index, table }) => smallvec![*index, *table],
            Self::Effect(EffectOp::IndexInsert { cursor, .. }) => smallvec![*cursor],
            Self::Effect(
                EffectOp::OpenRead { .. }
                | EffectOp::OpenEphemeralIndex { .. }
                | EffectOp::ResultRow { .. }
                | EffectOp::OpenSorter { .. }
                | EffectOp::SorterInsert { .. }
                | EffectOp::SorterData { .. }
                | EffectOp::OpenDistinctSet { .. },
            ) => SmallVec::new(),
        };
        cursors.into_iter()
    }

    fn cursor_definition(&self) -> Option<CursorId> {
        match self {
            Self::Effect(
                EffectOp::OpenRead { cursor, .. } | EffectOp::OpenEphemeralIndex { cursor },
            ) => Some(*cursor),
            Self::Value { .. }
            | Self::Effect(
                EffectOp::DeferredSeek { .. }
                | EffectOp::ResultRow { .. }
                | EffectOp::IndexInsert { .. }
                | EffectOp::OpenSorter { .. }
                | EffectOp::SorterInsert { .. }
                | EffectOp::SorterData { .. }
                | EffectOp::OpenDistinctSet { .. },
            ) => None,
        }
    }

    fn sorter_use(&self) -> Option<SorterId> {
        match self {
            Self::Value { op, .. } => op.sorter(),
            Self::Effect(
                EffectOp::SorterInsert { sorter, .. } | EffectOp::SorterData { sorter },
            ) => Some(*sorter),
            Self::Effect(
                EffectOp::OpenRead { .. }
                | EffectOp::OpenEphemeralIndex { .. }
                | EffectOp::DeferredSeek { .. }
                | EffectOp::ResultRow { .. }
                | EffectOp::IndexInsert { .. }
                | EffectOp::OpenSorter { .. }
                | EffectOp::OpenDistinctSet { .. },
            ) => None,
        }
    }

    fn sorter_definition(&self) -> Option<SorterId> {
        match self {
            Self::Effect(EffectOp::OpenSorter { sorter }) => Some(*sorter),
            Self::Value { .. }
            | Self::Effect(
                EffectOp::OpenRead { .. }
                | EffectOp::OpenEphemeralIndex { .. }
                | EffectOp::DeferredSeek { .. }
                | EffectOp::ResultRow { .. }
                | EffectOp::IndexInsert { .. }
                | EffectOp::SorterInsert { .. }
                | EffectOp::SorterData { .. }
                | EffectOp::OpenDistinctSet { .. },
            ) => None,
        }
    }

    fn distinct_set_definition(&self) -> Option<DistinctSetId> {
        match self {
            Self::Effect(EffectOp::OpenDistinctSet { set }) => Some(*set),
            Self::Value { .. }
            | Self::Effect(
                EffectOp::OpenRead { .. }
                | EffectOp::OpenEphemeralIndex { .. }
                | EffectOp::DeferredSeek { .. }
                | EffectOp::ResultRow { .. }
                | EffectOp::IndexInsert { .. }
                | EffectOp::OpenSorter { .. }
                | EffectOp::SorterInsert { .. }
                | EffectOp::SorterData { .. },
            ) => None,
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
    /// Run `initialize` on first entry and jump directly to `ready` whenever
    /// the same VDBE program execution reaches this terminator again.
    Once {
        initialize: BlockId,
        ready: BlockId,
    },
    Compare {
        lhs: ValueId,
        rhs: ValueId,
        comparison: ResolvedComparison,
        if_true: BlockId,
        if_false: BlockId,
        if_null: BlockId,
    },
    CursorStart {
        cursor: CursorId,
        direction: ScanDirection,
        if_non_empty: BlockId,
        if_empty: BlockId,
        arguments: SmallVec<[ValueId; 2]>,
    },
    CursorSeekRowid {
        cursor: CursorId,
        rowid: ValueId,
        if_found: BlockId,
        if_not_found: BlockId,
        arguments: SmallVec<[ValueId; 2]>,
    },
    TableSeek {
        cursor: CursorId,
        rowid: ValueId,
        op: SeekOp,
        if_found: BlockId,
        if_empty: BlockId,
        arguments: SmallVec<[ValueId; 2]>,
    },
    TableBound {
        cursor: CursorId,
        rowid: ValueId,
        op: SeekOp,
        affinity: Affinity,
        if_before_end: BlockId,
        if_at_end: BlockId,
        arguments: SmallVec<[ValueId; 2]>,
    },
    IndexSeek {
        cursor: CursorId,
        key: IndexKey,
        op: SeekOp,
        if_found: BlockId,
        if_empty: BlockId,
        arguments: SmallVec<[ValueId; 2]>,
    },
    IndexBound {
        cursor: CursorId,
        key: IndexKey,
        op: SeekOp,
        if_before_end: BlockId,
        if_at_end: BlockId,
        arguments: SmallVec<[ValueId; 2]>,
    },
    CursorAdvance {
        cursor: CursorId,
        direction: ScanDirection,
        if_next: BlockId,
        if_done: BlockId,
        arguments: SmallVec<[ValueId; 2]>,
    },
    SorterSort {
        sorter: SorterId,
        if_non_empty: BlockId,
        if_empty: BlockId,
        arguments: SmallVec<[ValueId; 2]>,
    },
    SorterNext {
        sorter: SorterId,
        if_next: BlockId,
        if_done: BlockId,
        arguments: SmallVec<[ValueId; 2]>,
    },
    DistinctCheck {
        set: DistinctSetId,
        pack: ValuePack,
        if_unique: BlockId,
        if_duplicate: BlockId,
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
            Self::Once { initialize, ready } => {
                [Some((*initialize, &[][..])), Some((*ready, &[][..])), None]
            }
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
            Self::CursorStart {
                if_non_empty,
                if_empty,
                arguments,
                ..
            } => [
                Some((*if_non_empty, arguments.as_slice())),
                Some((*if_empty, arguments.as_slice())),
                None,
            ],
            Self::CursorSeekRowid {
                if_found,
                if_not_found,
                arguments,
                ..
            } => [
                Some((*if_found, arguments.as_slice())),
                Some((*if_not_found, arguments.as_slice())),
                None,
            ],
            Self::TableSeek {
                if_found,
                if_empty,
                arguments,
                ..
            } => [
                Some((*if_found, arguments.as_slice())),
                Some((*if_empty, arguments.as_slice())),
                None,
            ],
            Self::TableBound {
                if_before_end,
                if_at_end,
                arguments,
                ..
            } => [
                Some((*if_before_end, arguments.as_slice())),
                Some((*if_at_end, arguments.as_slice())),
                None,
            ],
            Self::IndexSeek {
                if_found,
                if_empty,
                arguments,
                ..
            } => [
                Some((*if_found, arguments.as_slice())),
                Some((*if_empty, arguments.as_slice())),
                None,
            ],
            Self::IndexBound {
                if_before_end,
                if_at_end,
                arguments,
                ..
            } => [
                Some((*if_before_end, arguments.as_slice())),
                Some((*if_at_end, arguments.as_slice())),
                None,
            ],
            Self::CursorAdvance {
                if_next,
                if_done,
                arguments,
                ..
            }
            | Self::SorterNext {
                if_next,
                if_done,
                arguments,
                ..
            } => [
                Some((*if_next, arguments.as_slice())),
                Some((*if_done, arguments.as_slice())),
                None,
            ],
            Self::SorterSort {
                if_non_empty,
                if_empty,
                arguments,
                ..
            } => [
                Some((*if_non_empty, arguments.as_slice())),
                Some((*if_empty, arguments.as_slice())),
                None,
            ],
            Self::DistinctCheck {
                if_unique,
                if_duplicate,
                ..
            } => [
                Some((*if_unique, &[][..])),
                Some((*if_duplicate, &[][..])),
                None,
            ],
            Self::Return(_) => [None, None, None],
        };
        edges.into_iter().flatten()
    }

    fn operands(&self) -> smallvec::IntoIter<[ValueId; 8]> {
        let operands = match self {
            Self::Jump { arguments, .. } => arguments.iter().copied().collect(),
            Self::Branch { condition, .. } | Self::Return(condition) => smallvec![*condition],
            Self::Once { .. } => SmallVec::new(),
            Self::Compare { lhs, rhs, .. } => smallvec![*lhs, *rhs],
            Self::CursorStart { arguments, .. }
            | Self::CursorAdvance { arguments, .. }
            | Self::SorterSort { arguments, .. }
            | Self::SorterNext { arguments, .. } => arguments.iter().copied().collect(),
            Self::CursorSeekRowid {
                rowid, arguments, ..
            }
            | Self::TableSeek {
                rowid, arguments, ..
            }
            | Self::TableBound {
                rowid, arguments, ..
            } => std::iter::once(*rowid)
                .chain(arguments.iter().copied())
                .collect(),
            Self::IndexSeek { key, arguments, .. } | Self::IndexBound { key, arguments, .. } => key
                .values()
                .iter()
                .copied()
                .chain(arguments.iter().copied())
                .collect(),
            Self::DistinctCheck { pack, .. } => pack.values().iter().copied().collect(),
        };
        operands.into_iter()
    }

    fn control_operands(&self) -> smallvec::IntoIter<[ValueId; 4]> {
        let operands = match self {
            Self::Branch { condition, .. } | Self::Return(condition) => smallvec![*condition],
            Self::Compare { lhs, rhs, .. } => smallvec![*lhs, *rhs],
            Self::CursorSeekRowid { rowid, .. }
            | Self::TableSeek { rowid, .. }
            | Self::TableBound { rowid, .. } => smallvec![*rowid],
            Self::IndexSeek { key, .. } | Self::IndexBound { key, .. } => {
                key.values().iter().copied().collect()
            }
            Self::DistinctCheck { pack, .. } => pack.values().iter().copied().collect(),
            Self::Jump { .. }
            | Self::Once { .. }
            | Self::CursorStart { .. }
            | Self::CursorAdvance { .. }
            | Self::SorterSort { .. }
            | Self::SorterNext { .. } => SmallVec::new(),
        };
        operands.into_iter()
    }

    fn cursor(&self) -> Option<CursorId> {
        match self {
            Self::CursorStart { cursor, .. }
            | Self::CursorSeekRowid { cursor, .. }
            | Self::TableSeek { cursor, .. }
            | Self::TableBound { cursor, .. }
            | Self::IndexSeek { cursor, .. }
            | Self::IndexBound { cursor, .. }
            | Self::CursorAdvance { cursor, .. } => Some(*cursor),
            Self::Jump { .. }
            | Self::Branch { .. }
            | Self::Once { .. }
            | Self::Compare { .. }
            | Self::Return(_) => None,
            Self::SorterSort { .. } | Self::SorterNext { .. } | Self::DistinctCheck { .. } => None,
        }
    }

    fn sorter(&self) -> Option<SorterId> {
        match self {
            Self::SorterSort { sorter, .. } | Self::SorterNext { sorter, .. } => Some(*sorter),
            Self::Jump { .. }
            | Self::Branch { .. }
            | Self::Once { .. }
            | Self::Compare { .. }
            | Self::CursorStart { .. }
            | Self::CursorSeekRowid { .. }
            | Self::TableSeek { .. }
            | Self::TableBound { .. }
            | Self::IndexSeek { .. }
            | Self::IndexBound { .. }
            | Self::CursorAdvance { .. }
            | Self::DistinctCheck { .. }
            | Self::Return(_) => None,
        }
    }

    fn distinct_set(&self) -> Option<DistinctSetId> {
        match self {
            Self::DistinctCheck { set, .. } => Some(*set),
            Self::Jump { .. }
            | Self::Branch { .. }
            | Self::Once { .. }
            | Self::Compare { .. }
            | Self::CursorStart { .. }
            | Self::CursorSeekRowid { .. }
            | Self::TableSeek { .. }
            | Self::TableBound { .. }
            | Self::IndexSeek { .. }
            | Self::IndexBound { .. }
            | Self::CursorAdvance { .. }
            | Self::SorterSort { .. }
            | Self::SorterNext { .. }
            | Self::Return(_) => None,
        }
    }

    fn remap_blocks(&mut self, remap: &[Option<BlockId>]) -> Result<()> {
        let remap_target = |target: &mut BlockId| -> Result<()> {
            *target = remap
                .get(target.index())
                .copied()
                .flatten()
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "reachable compiler IR block targets removed block {target:?}"
                    ))
                })?;
            Ok(())
        };
        match self {
            Self::Jump { target, .. } => remap_target(target),
            Self::Branch {
                if_true, if_false, ..
            } => {
                remap_target(if_true)?;
                remap_target(if_false)
            }
            Self::Once { initialize, ready } => {
                remap_target(initialize)?;
                remap_target(ready)
            }
            Self::Compare {
                if_true,
                if_false,
                if_null,
                ..
            } => {
                remap_target(if_true)?;
                remap_target(if_false)?;
                remap_target(if_null)
            }
            Self::CursorStart {
                if_non_empty,
                if_empty,
                ..
            } => {
                remap_target(if_non_empty)?;
                remap_target(if_empty)
            }
            Self::CursorSeekRowid {
                if_found,
                if_not_found,
                ..
            } => {
                remap_target(if_found)?;
                remap_target(if_not_found)
            }
            Self::TableSeek {
                if_found, if_empty, ..
            } => {
                remap_target(if_found)?;
                remap_target(if_empty)
            }
            Self::TableBound {
                if_before_end,
                if_at_end,
                ..
            } => {
                remap_target(if_before_end)?;
                remap_target(if_at_end)
            }
            Self::IndexSeek {
                if_found, if_empty, ..
            } => {
                remap_target(if_found)?;
                remap_target(if_empty)
            }
            Self::IndexBound {
                if_before_end,
                if_at_end,
                ..
            } => {
                remap_target(if_before_end)?;
                remap_target(if_at_end)
            }
            Self::CursorAdvance {
                if_next, if_done, ..
            } => {
                remap_target(if_next)?;
                remap_target(if_done)
            }
            Self::SorterSort {
                if_non_empty,
                if_empty,
                ..
            } => {
                remap_target(if_non_empty)?;
                remap_target(if_empty)
            }
            Self::SorterNext {
                if_next, if_done, ..
            } => {
                remap_target(if_next)?;
                remap_target(if_done)
            }
            Self::DistinctCheck {
                if_unique,
                if_duplicate,
                ..
            } => {
                remap_target(if_unique)?;
                remap_target(if_duplicate)
            }
            Self::Return(_) => Ok(()),
        }
    }

    fn prune_block_arguments(&mut self, parameter_live: &[Vec<bool>]) -> bool {
        fn retain_live_arguments(arguments: &mut SmallVec<[ValueId; 2]>, live: &[bool]) -> bool {
            assert_eq!(
                arguments.len(),
                live.len(),
                "verified compiler IR edge must match its target parameters"
            );
            let previous_len = arguments.len();
            let mut index = 0;
            arguments.retain(|_| {
                let retain = live[index];
                index += 1;
                retain
            });
            arguments.len() != previous_len
        }

        match self {
            Self::Jump { target, arguments } => {
                retain_live_arguments(arguments, &parameter_live[target.index()])
            }
            Self::CursorStart {
                if_non_empty,
                if_empty,
                arguments,
                ..
            } => {
                assert_eq!(
                    parameter_live[if_non_empty.index()],
                    parameter_live[if_empty.index()],
                    "shared cursor edge targets must retain the same parameter positions"
                );
                retain_live_arguments(arguments, &parameter_live[if_non_empty.index()])
            }
            Self::CursorSeekRowid {
                if_found,
                if_not_found,
                arguments,
                ..
            } => {
                assert_eq!(
                    parameter_live[if_found.index()],
                    parameter_live[if_not_found.index()],
                    "shared cursor edge targets must retain the same parameter positions"
                );
                retain_live_arguments(arguments, &parameter_live[if_found.index()])
            }
            Self::TableSeek {
                if_found,
                if_empty,
                arguments,
                ..
            } => {
                assert_eq!(
                    parameter_live[if_found.index()],
                    parameter_live[if_empty.index()],
                    "shared table-seek edge targets must retain the same parameter positions"
                );
                retain_live_arguments(arguments, &parameter_live[if_found.index()])
            }
            Self::TableBound {
                if_before_end,
                if_at_end,
                arguments,
                ..
            } => {
                assert_eq!(
                    parameter_live[if_before_end.index()],
                    parameter_live[if_at_end.index()],
                    "shared table-bound edge targets must retain the same parameter positions"
                );
                retain_live_arguments(arguments, &parameter_live[if_before_end.index()])
            }
            Self::IndexSeek {
                if_found,
                if_empty,
                arguments,
                ..
            } => {
                assert_eq!(
                    parameter_live[if_found.index()],
                    parameter_live[if_empty.index()],
                    "shared index-seek edge targets must retain the same parameter positions"
                );
                retain_live_arguments(arguments, &parameter_live[if_found.index()])
            }
            Self::IndexBound {
                if_before_end,
                if_at_end,
                arguments,
                ..
            } => {
                assert_eq!(
                    parameter_live[if_before_end.index()],
                    parameter_live[if_at_end.index()],
                    "shared index-bound edge targets must retain the same parameter positions"
                );
                retain_live_arguments(arguments, &parameter_live[if_before_end.index()])
            }
            Self::CursorAdvance {
                if_next,
                if_done,
                arguments,
                ..
            } => {
                assert_eq!(
                    parameter_live[if_next.index()],
                    parameter_live[if_done.index()],
                    "shared cursor edge targets must retain the same parameter positions"
                );
                retain_live_arguments(arguments, &parameter_live[if_next.index()])
            }
            Self::SorterSort {
                if_non_empty,
                if_empty,
                arguments,
                ..
            } => {
                assert_eq!(
                    parameter_live[if_non_empty.index()],
                    parameter_live[if_empty.index()],
                    "shared sorter edge targets must retain the same parameter positions"
                );
                retain_live_arguments(arguments, &parameter_live[if_non_empty.index()])
            }
            Self::SorterNext {
                if_next,
                if_done,
                arguments,
                ..
            } => {
                assert_eq!(
                    parameter_live[if_next.index()],
                    parameter_live[if_done.index()],
                    "shared sorter edge targets must retain the same parameter positions"
                );
                retain_live_arguments(arguments, &parameter_live[if_next.index()])
            }
            Self::Branch { .. }
            | Self::Once { .. }
            | Self::Compare { .. }
            | Self::DistinctCheck { .. }
            | Self::Return(_) => false,
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
    input_bindings: SmallVec<[Option<ValueId>; 2]>,
    cursor_input_count: u32,
    cursor_input_bindings: SmallVec<[Option<CursorId>; 2]>,
    cursor_resources: SmallVec<[CursorResource; 2]>,
    sorter_resources: SmallVec<[SorterResource; 1]>,
    distinct_set_resources: SmallVec<[DistinctSetResource; 1]>,
    parameter_declarations: SmallVec<[Variable; 2]>,
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
            input_bindings: SmallVec::new(),
            cursor_input_count: 0,
            cursor_input_bindings: SmallVec::new(),
            cursor_resources: SmallVec::new(),
            sorter_resources: SmallVec::new(),
            distinct_set_resources: SmallVec::new(),
            parameter_declarations: SmallVec::new(),
        }
    }

    fn allocate_cursor(&mut self, resource: CursorResource) -> Result<CursorId> {
        let id = u32::try_from(self.cursor_resources.len()).map_err(|_| {
            LimboError::InternalError("compiler IR cursor identifier overflow".to_owned())
        })?;
        self.cursor_resources.push(resource);
        Ok(CursorId(id))
    }

    fn external_input(&mut self, input: InputId) -> Result<ValueId> {
        if let Some(value) = self.input_bindings.get(input.index()).copied().flatten() {
            return Ok(value);
        }
        self.push(ScalarOp::Input(input))
    }

    fn bind_input(&mut self, input: InputId, value: ValueId) -> Option<ValueId> {
        if self.input_bindings.len() <= input.index() {
            self.input_bindings.resize(input.index() + 1, None);
        }
        self.input_bindings[input.index()].replace(value)
    }

    fn restore_input(&mut self, input: InputId, previous: Option<ValueId>) {
        self.input_bindings[input.index()] = previous;
    }

    fn external_cursor(&mut self, input: CursorInputId) -> Result<CursorId> {
        if let Some(cursor) = self
            .cursor_input_bindings
            .get(input.index())
            .copied()
            .flatten()
        {
            return Ok(cursor);
        }
        self.cursor_input_count = self
            .cursor_input_count
            .max(input.0.checked_add(1).ok_or_else(|| {
                LimboError::InternalError("compiler IR cursor input identifier overflow".to_owned())
            })?);
        self.allocate_cursor(CursorResource::External(input))
    }

    fn bind_cursor_input(
        &mut self,
        input: CursorInputId,
        cursor: CursorId,
    ) -> Result<Option<CursorId>> {
        self.ensure_cursor_declared(cursor)?;
        if self.cursor_input_bindings.len() <= input.index() {
            self.cursor_input_bindings.resize(input.index() + 1, None);
        }
        Ok(self.cursor_input_bindings[input.index()].replace(cursor))
    }

    fn restore_cursor_input(&mut self, input: CursorInputId, previous: Option<CursorId>) {
        self.cursor_input_bindings[input.index()] = previous;
    }

    fn ensure_cursor_declared(&self, cursor: CursorId) -> Result<()> {
        if cursor.index() >= self.cursor_resources.len() {
            return Err(LimboError::InternalError(format!(
                "compiler IR references undeclared cursor {cursor:?}"
            )));
        }
        Ok(())
    }

    fn allocate_sorter(
        &mut self,
        keys: SmallVec<[SortKey; 4]>,
        record_width: usize,
    ) -> Result<SorterId> {
        self.allocate_sorter_with_affinities(keys, record_width, None)
    }

    fn allocate_sorter_with_affinities(
        &mut self,
        keys: SmallVec<[SortKey; 4]>,
        record_width: usize,
        affinities: Option<SmallVec<[Affinity; 4]>>,
    ) -> Result<SorterId> {
        if keys.is_empty() {
            return Err(LimboError::InternalError(
                "compiler IR sorter must have at least one key".to_owned(),
            ));
        }
        if record_width < keys.len() {
            return Err(LimboError::InternalError(format!(
                "compiler IR sorter has {} keys but record width {record_width}",
                keys.len()
            )));
        }
        if affinities
            .as_ref()
            .is_some_and(|affinities| affinities.len() != record_width)
        {
            return Err(LimboError::InternalError(format!(
                "compiler IR sorter record width {record_width} has {} affinities",
                affinities.as_ref().map_or(0, SmallVec::len)
            )));
        }
        let id = u32::try_from(self.sorter_resources.len()).map_err(|_| {
            LimboError::InternalError("compiler IR sorter identifier overflow".to_owned())
        })?;
        self.sorter_resources.push(SorterResource {
            keys,
            record_width,
            affinities,
        });
        Ok(SorterId(id))
    }

    fn ensure_sorter_declared(&self, sorter: SorterId) -> Result<()> {
        if sorter.index() >= self.sorter_resources.len() {
            return Err(LimboError::InternalError(format!(
                "compiler IR references undeclared sorter {sorter:?}"
            )));
        }
        Ok(())
    }

    fn allocate_distinct_set(
        &mut self,
        collations: SmallVec<[CollationSeq; 4]>,
    ) -> Result<DistinctSetId> {
        if collations.is_empty() {
            return Err(LimboError::InternalError(
                "compiler IR distinct set must have at least one key".to_owned(),
            ));
        }
        let id = u32::try_from(self.distinct_set_resources.len()).map_err(|_| {
            LimboError::InternalError("compiler IR distinct-set identifier overflow".to_owned())
        })?;
        self.distinct_set_resources
            .push(DistinctSetResource { collations });
        Ok(DistinctSetId(id))
    }

    fn ensure_distinct_set_declared(&self, set: DistinctSetId) -> Result<()> {
        if set.index() >= self.distinct_set_resources.len() {
            return Err(LimboError::InternalError(format!(
                "compiler IR references undeclared distinct set {set:?}"
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
        if let Some(sorter) = op.sorter() {
            self.ensure_sorter_declared(sorter)?;
        }
        if let ScalarOp::Parameter(variable) = &op {
            self.parameter_declarations.push(variable.clone());
        }
        let result = self.allocate_value()?;
        self.blocks[self.current.index()]
            .instructions
            .push(Instruction::Value { result, op });
        Ok(result)
    }

    fn push_effect(&mut self, op: EffectOp) -> Result<()> {
        let cursors: SmallVec<[CursorId; 2]> = match &op {
            EffectOp::OpenRead { cursor, .. } | EffectOp::OpenEphemeralIndex { cursor } => {
                smallvec![*cursor]
            }
            EffectOp::DeferredSeek { index, table } => smallvec![*index, *table],
            EffectOp::IndexInsert { cursor, .. } => smallvec![*cursor],
            EffectOp::ResultRow { .. }
            | EffectOp::OpenSorter { .. }
            | EffectOp::SorterInsert { .. }
            | EffectOp::SorterData { .. }
            | EffectOp::OpenDistinctSet { .. } => SmallVec::new(),
        };
        for cursor in cursors {
            self.ensure_cursor_declared(cursor)?;
        }
        let sorter = match &op {
            EffectOp::OpenSorter { sorter }
            | EffectOp::SorterInsert { sorter, .. }
            | EffectOp::SorterData { sorter } => Some(*sorter),
            EffectOp::OpenRead { .. }
            | EffectOp::OpenEphemeralIndex { .. }
            | EffectOp::DeferredSeek { .. }
            | EffectOp::ResultRow { .. }
            | EffectOp::IndexInsert { .. }
            | EffectOp::OpenDistinctSet { .. } => None,
        };
        if let Some(sorter) = sorter {
            self.ensure_sorter_declared(sorter)?;
        }
        if let EffectOp::OpenDistinctSet { set } = &op {
            self.ensure_distinct_set_declared(*set)?;
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
        if let Some(sorter) = terminator.sorter() {
            self.ensure_sorter_declared(sorter)?;
        }
        if let Some(set) = terminator.distinct_set() {
            self.ensure_distinct_set_declared(set)?;
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
            sorter_resources: self.sorter_resources,
            distinct_set_resources: self.distinct_set_resources,
            parameter_declarations: self.parameter_declarations,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SorterPhase {
    Unopened,
    Filling,
    Reading,
}

impl SorterPhase {
    const fn mask(self) -> u8 {
        1 << self as u8
    }
}

/// A verified SSA control-flow region.
#[derive(Debug)]
pub(crate) struct IrProgram {
    blocks: SmallVec<[BasicBlock; 4]>,
    value_count: u32,
    input_count: u32,
    cursor_input_count: u32,
    cursor_resources: SmallVec<[CursorResource; 2]>,
    sorter_resources: SmallVec<[SorterResource; 1]>,
    distinct_set_resources: SmallVec<[DistinctSetResource; 1]>,
    parameter_declarations: SmallVec<[Variable; 2]>,
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
        let mut cursor_definitions = vec![None; self.cursor_resources.len()];
        let mut sorter_definitions = vec![None; self.sorter_resources.len()];
        let mut distinct_set_definitions = vec![None; self.distinct_set_resources.len()];
        let mut predecessors = vec![Vec::new(); block_count];
        let mut cursor_predecessors = vec![Vec::new(); block_count];
        let mut return_count = 0;

        for (index, resource) in self.cursor_resources.iter().enumerate() {
            if let CursorResource::External(input) = resource {
                if input.index() >= self.cursor_input_count as usize {
                    return Err(LimboError::InternalError(format!(
                        "compiler IR references out-of-range cursor input {input:?}"
                    )));
                }
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
                    Instruction::Effect(
                        EffectOp::ResultRow { pack } | EffectOp::IndexInsert { pack, .. }
                    ) if pack.values().is_empty()
                ) {
                    return Err(LimboError::InternalError(
                        "compiler IR row effect must contain at least one value".to_owned(),
                    ));
                }
                if let Instruction::Effect(EffectOp::SorterInsert { sorter, pack }) = instruction {
                    let resource = self.sorter_resources.get(sorter.index()).ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "compiler IR inserts into out-of-range sorter {sorter:?}"
                        ))
                    })?;
                    if pack.values().len() != resource.record_width {
                        return Err(LimboError::InternalError(format!(
                            "compiler IR sorter {:?} expects record width {}, received {}",
                            sorter,
                            resource.record_width,
                            pack.values().len()
                        )));
                    }
                }
                if let Instruction::Effect(EffectOp::OpenDistinctSet { set }) = instruction {
                    if set.index() >= self.distinct_set_resources.len() {
                        return Err(LimboError::InternalError(format!(
                            "compiler IR defines out-of-range distinct set {set:?}"
                        )));
                    }
                }
                if let Instruction::Value { result, op } = instruction {
                    if let ScalarOp::Input(input) = op {
                        if input.index() >= self.input_count as usize {
                            return Err(LimboError::InternalError(format!(
                                "compiler IR references out-of-range input {input:?}"
                            )));
                        }
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
                for cursor in instruction.cursor_uses() {
                    if cursor.index() >= self.cursor_resources.len() {
                        return Err(LimboError::InternalError(format!(
                            "compiler IR references out-of-range cursor {cursor:?}"
                        )));
                    }
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
                if let Some(sorter) = instruction.sorter_use() {
                    if sorter.index() >= self.sorter_resources.len() {
                        return Err(LimboError::InternalError(format!(
                            "compiler IR references out-of-range sorter {sorter:?}"
                        )));
                    }
                }
                if let Some(sorter) = instruction.sorter_definition() {
                    if sorter.index() >= self.sorter_resources.len() {
                        return Err(LimboError::InternalError(format!(
                            "compiler IR defines out-of-range sorter {sorter:?}"
                        )));
                    }
                    Self::record_sorter_definition(
                        &mut sorter_definitions,
                        sorter,
                        Definition {
                            block: block.id,
                            instruction: Some(instruction_index),
                        },
                    )?;
                }
                if let Some(set) = instruction.distinct_set_definition() {
                    Self::record_distinct_set_definition(
                        &mut distinct_set_definitions,
                        set,
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
                // A Once re-entry edge is stateful: it can execute only after
                // the initialization edge completed during this statement
                // execution. Cursor opens in the initialization region
                // therefore dominate uses after `ready`, while ordinary SSA
                // values continue to use the complete CFG above.
                if !matches!(
                    block.terminator,
                    Terminator::Once { ready, .. } if ready == successor
                ) {
                    cursor_predecessors[target.id.index()].push(block.id);
                }
            }
            if let Some(cursor) = block.terminator.cursor() {
                let Some(resource) = self.cursor_resources.get(cursor.index()) else {
                    return Err(LimboError::InternalError(format!(
                        "compiler IR references out-of-range cursor {cursor:?}"
                    )));
                };
                if matches!(
                    block.terminator,
                    Terminator::IndexSeek { .. } | Terminator::IndexBound { .. }
                ) && matches!(resource, CursorResource::Owned(cursor) if !matches!(cursor, CursorType::BTreeIndex(_)))
                {
                    return Err(LimboError::InternalError(format!(
                        "compiler IR index seek requires a B-tree index cursor, found {cursor:?}"
                    )));
                }
                if matches!(
                    block.terminator,
                    Terminator::CursorSeekRowid { .. }
                        | Terminator::TableSeek { .. }
                        | Terminator::TableBound { .. }
                ) && matches!(resource, CursorResource::Owned(cursor) if !matches!(cursor, CursorType::BTreeTable(_)))
                {
                    return Err(LimboError::InternalError(format!(
                        "compiler IR rowid control requires a B-tree table cursor, found {cursor:?}"
                    )));
                }
            }
            if let Some(sorter) = block.terminator.sorter() {
                if sorter.index() >= self.sorter_resources.len() {
                    return Err(LimboError::InternalError(format!(
                        "compiler IR references out-of-range sorter {sorter:?}"
                    )));
                }
            }
            if let Some(set) = block.terminator.distinct_set() {
                let Some(resource) = self.distinct_set_resources.get(set.index()) else {
                    return Err(LimboError::InternalError(format!(
                        "compiler IR references out-of-range distinct set {set:?}"
                    )));
                };
                let Terminator::DistinctCheck { pack, .. } = &block.terminator else {
                    unreachable!("only distinct-check terminators use distinct sets");
                };
                if pack.values().len() != resource.collations.len() {
                    return Err(LimboError::InternalError(format!(
                        "compiler IR distinct set {:?} expects key width {}, received {}",
                        set,
                        resource.collations.len(),
                        pack.values().len()
                    )));
                }
            }
            if matches!(block.terminator, Terminator::Return(_)) {
                return_count += 1;
            }
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
        let cursor_dominators = Self::compute_dominators(&cursor_predecessors);
        self.verify_sorter_phases()?;

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
                for cursor in instruction.cursor_uses() {
                    Self::verify_cursor_use(
                        &cursor_definitions,
                        &cursor_dominators,
                        cursor,
                        block.id,
                        instruction_index,
                    )?;
                }
                if let Some(sorter) = instruction.sorter_use() {
                    Self::verify_sorter_use(
                        &sorter_definitions,
                        &dominators,
                        sorter,
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
                    &cursor_dominators,
                    cursor,
                    block.id,
                    block.instructions.len(),
                )?;
            }
            if let Some(sorter) = block.terminator.sorter() {
                Self::verify_sorter_use(
                    &sorter_definitions,
                    &dominators,
                    sorter,
                    block.id,
                    block.instructions.len(),
                )?;
            }
            if let Some(set) = block.terminator.distinct_set() {
                Self::verify_distinct_set_use(
                    &distinct_set_definitions,
                    &dominators,
                    set,
                    block.id,
                    block.instructions.len(),
                )?;
            }
        }
        Ok(())
    }

    fn verify_sorter_phases(&self) -> Result<()> {
        if self.sorter_resources.is_empty() {
            return Ok(());
        }
        let initial = vec![SorterPhase::Unopened.mask(); self.sorter_resources.len()];
        let mut incoming = vec![None; self.blocks.len()];
        incoming[0] = Some(initial);
        let mut worklist = vec![BlockId(0)];

        while let Some(block_id) = worklist.pop() {
            let block = &self.blocks[block_id.index()];
            let mut phases = incoming[block_id.index()]
                .clone()
                .expect("sorter phase worklist block has incoming state");
            for instruction in &block.instructions {
                match instruction {
                    Instruction::Effect(EffectOp::OpenSorter { sorter }) => {
                        Self::require_sorter_phase(
                            &phases,
                            *sorter,
                            SorterPhase::Unopened,
                            "open",
                        )?;
                        phases[sorter.index()] = SorterPhase::Filling.mask();
                    }
                    Instruction::Effect(EffectOp::SorterInsert { sorter, .. }) => {
                        Self::require_sorter_phase(
                            &phases,
                            *sorter,
                            SorterPhase::Filling,
                            "insert into",
                        )?;
                    }
                    Instruction::Effect(EffectOp::SorterData { sorter })
                    | Instruction::Value {
                        op: ScalarOp::SorterColumn { sorter, .. },
                        ..
                    } => {
                        Self::require_sorter_phase(&phases, *sorter, SorterPhase::Reading, "read")?;
                    }
                    Instruction::Value { .. }
                    | Instruction::Effect(
                        EffectOp::OpenRead { .. }
                        | EffectOp::OpenEphemeralIndex { .. }
                        | EffectOp::DeferredSeek { .. }
                        | EffectOp::ResultRow { .. }
                        | EffectOp::IndexInsert { .. }
                        | EffectOp::OpenDistinctSet { .. },
                    ) => {}
                }
            }

            match &block.terminator {
                Terminator::SorterSort { sorter, .. } => {
                    Self::require_sorter_phase(&phases, *sorter, SorterPhase::Filling, "sort")?;
                    phases[sorter.index()] = SorterPhase::Reading.mask();
                }
                Terminator::SorterNext { sorter, .. } => {
                    Self::require_sorter_phase(&phases, *sorter, SorterPhase::Reading, "advance")?;
                }
                Terminator::Jump { .. }
                | Terminator::Branch { .. }
                | Terminator::Once { .. }
                | Terminator::Compare { .. }
                | Terminator::CursorStart { .. }
                | Terminator::CursorSeekRowid { .. }
                | Terminator::TableSeek { .. }
                | Terminator::TableBound { .. }
                | Terminator::IndexSeek { .. }
                | Terminator::IndexBound { .. }
                | Terminator::CursorAdvance { .. }
                | Terminator::DistinctCheck { .. }
                | Terminator::Return(_) => {}
            }

            for successor in block.terminator.successors() {
                match &incoming[successor.index()] {
                    None => {
                        incoming[successor.index()] = Some(phases.clone());
                        worklist.push(successor);
                    }
                    Some(existing) => {
                        let mut merged = existing.clone();
                        for (merged, incoming) in merged.iter_mut().zip(&phases) {
                            *merged |= incoming;
                        }
                        if &merged != existing {
                            incoming[successor.index()] = Some(merged);
                            worklist.push(successor);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn require_sorter_phase(
        phases: &[u8],
        sorter: SorterId,
        expected: SorterPhase,
        operation: &str,
    ) -> Result<()> {
        let actual = phases[sorter.index()];
        if actual != expected.mask() {
            let mut names = SmallVec::<[&str; 3]>::new();
            for phase in [
                SorterPhase::Unopened,
                SorterPhase::Filling,
                SorterPhase::Reading,
            ] {
                if actual & phase.mask() != 0 {
                    names.push(match phase {
                        SorterPhase::Unopened => "Unopened",
                        SorterPhase::Filling => "Filling",
                        SorterPhase::Reading => "Reading",
                    });
                }
            }
            return Err(LimboError::InternalError(format!(
                "compiler IR cannot {operation} sorter {sorter:?} in phase {}; expected {expected:?}",
                names.join("|")
            )));
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

    fn record_sorter_definition(
        definitions: &mut [Option<Definition>],
        sorter: SorterId,
        definition: Definition,
    ) -> Result<()> {
        let Some(slot) = definitions.get_mut(sorter.index()) else {
            return Err(LimboError::InternalError(format!(
                "compiler IR defines out-of-range sorter {sorter:?}"
            )));
        };
        if slot.replace(definition).is_some() {
            return Err(LimboError::InternalError(format!(
                "compiler IR sorter {sorter:?} has multiple definitions"
            )));
        }
        Ok(())
    }

    fn record_distinct_set_definition(
        definitions: &mut [Option<Definition>],
        set: DistinctSetId,
        definition: Definition,
    ) -> Result<()> {
        let Some(slot) = definitions.get_mut(set.index()) else {
            return Err(LimboError::InternalError(format!(
                "compiler IR defines out-of-range distinct set {set:?}"
            )));
        };
        if slot.replace(definition).is_some() {
            return Err(LimboError::InternalError(format!(
                "compiler IR distinct set {set:?} has multiple definitions"
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

    /// Optimize a verified symbolic program before assigning physical resources.
    fn optimize(mut self) -> Result<Self> {
        self.verify()?;
        if self.fold_constant_branches() {
            self.remove_unreachable_blocks()?;
        }
        self.eliminate_dead_values();
        self.verify()?;
        Ok(self)
    }

    /// Replace branches on direct constants with an unconditional edge.
    ///
    /// Deliberately do not evaluate other scalar operations here: doing so
    /// requires preserving their coercion and error behavior exactly.
    fn fold_constant_branches(&mut self) -> bool {
        let mut constant_truth = vec![None; self.value_count as usize];
        for block in &self.blocks {
            for instruction in &block.instructions {
                if let Instruction::Value {
                    result,
                    op: ScalarOp::Constant(value),
                } = instruction
                {
                    constant_truth[result.index()] =
                        Some(Numeric::from_value(value).is_some_and(|numeric| numeric.to_bool()));
                }
            }
        }

        let replacements = self
            .blocks
            .iter()
            .map(|block| match &block.terminator {
                Terminator::Branch {
                    condition,
                    if_true,
                    if_false,
                } => constant_truth[condition.index()].map(|condition| {
                    if condition {
                        *if_true
                    } else {
                        *if_false
                    }
                }),
                _ => None,
            })
            .collect::<Vec<_>>();
        if replacements.iter().all(Option::is_none) {
            return false;
        }

        // A scalar region promises one eventual result. If folding every known
        // branch would make that return unreachable (for example, a constant
        // true loop), retain the original control flow rather than turning a
        // valid region into IR that cannot be lowered.
        let return_block = self
            .blocks
            .iter()
            .position(|block| matches!(block.terminator, Terminator::Return(_)))
            .expect("verified compiler IR has exactly one return");
        let mut reachable = vec![false; self.blocks.len()];
        let mut stack = vec![BlockId(0)];
        while let Some(block) = stack.pop() {
            if reachable[block.index()] {
                continue;
            }
            reachable[block.index()] = true;
            if let Some(target) = replacements[block.index()] {
                stack.push(target);
            } else {
                stack.extend(self.blocks[block.index()].terminator.successors());
            }
        }
        if !reachable[return_block] {
            return false;
        }

        for (block, replacement) in self.blocks.iter_mut().zip(replacements) {
            if let Some(target) = replacement {
                block.terminator = Terminator::Jump {
                    target,
                    arguments: SmallVec::new(),
                };
            }
        }
        true
    }

    /// Remove blocks no longer reachable from the entry block and canonicalize
    /// the remaining block identifiers. Value and resource identifiers remain
    /// stable arena indices, so optimization is allowed to leave unused slots.
    fn remove_unreachable_blocks(&mut self) -> Result<()> {
        let reachable = self.reachable_blocks();
        if reachable.iter().all(|reachable| *reachable) {
            return Ok(());
        }

        let mut remap = vec![None; self.blocks.len()];
        let mut next_block = 0;
        for block in &self.blocks {
            if reachable[block.id.index()] {
                remap[block.id.index()] = Some(BlockId(next_block));
                next_block += 1;
            }
        }

        self.blocks.retain(|block| reachable[block.id.index()]);
        for block in &mut self.blocks {
            block.id = remap[block.id.index()].expect("retained block has a remapped id");
            block.terminator.remap_blocks(&remap)?;
        }
        Ok(())
    }

    /// Remove unused value instructions and SSA join positions that cannot
    /// affect SQL-visible behavior.
    fn eliminate_dead_values(&mut self) -> bool {
        let mut dependencies = vec![SmallVec::<[ValueId; 2]>::new(); self.value_count as usize];
        let mut roots = Vec::new();

        for block in &self.blocks {
            roots.extend(block.terminator.control_operands());
            for instruction in &block.instructions {
                match instruction {
                    Instruction::Value { result, op } => {
                        dependencies[result.index()].extend(op.operands());
                        if !op.can_eliminate_if_unused() {
                            roots.push(*result);
                        }
                    }
                    Instruction::Effect(_) => roots.extend(instruction.operands()),
                }
            }
            for (target, arguments) in block.terminator.edges() {
                for (parameter, argument) in
                    self.blocks[target.index()].parameters.iter().zip(arguments)
                {
                    dependencies[parameter.index()].push(*argument);
                }
            }

            let shared_targets = match &block.terminator {
                Terminator::CursorStart {
                    if_non_empty,
                    if_empty,
                    ..
                } => Some((*if_non_empty, *if_empty)),
                Terminator::CursorSeekRowid {
                    if_found,
                    if_not_found,
                    ..
                } => Some((*if_found, *if_not_found)),
                Terminator::TableSeek {
                    if_found, if_empty, ..
                } => Some((*if_found, *if_empty)),
                Terminator::TableBound {
                    if_before_end,
                    if_at_end,
                    ..
                } => Some((*if_before_end, *if_at_end)),
                Terminator::IndexSeek {
                    if_found, if_empty, ..
                } => Some((*if_found, *if_empty)),
                Terminator::IndexBound {
                    if_before_end,
                    if_at_end,
                    ..
                } => Some((*if_before_end, *if_at_end)),
                Terminator::CursorAdvance {
                    if_next, if_done, ..
                } => Some((*if_next, *if_done)),
                Terminator::SorterSort {
                    if_non_empty,
                    if_empty,
                    ..
                } => Some((*if_non_empty, *if_empty)),
                Terminator::SorterNext {
                    if_next, if_done, ..
                } => Some((*if_next, *if_done)),
                _ => None,
            };
            if let Some((first, second)) = shared_targets {
                for (first, second) in self.blocks[first.index()]
                    .parameters
                    .iter()
                    .zip(&self.blocks[second.index()].parameters)
                {
                    dependencies[first.index()].push(*second);
                    dependencies[second.index()].push(*first);
                }
            }
        }

        let mut live = vec![false; self.value_count as usize];
        let mut worklist = Vec::new();
        for root in roots {
            if !live[root.index()] {
                live[root.index()] = true;
                worklist.push(root);
            }
        }
        while let Some(value) = worklist.pop() {
            for dependency in &dependencies[value.index()] {
                if !live[dependency.index()] {
                    live[dependency.index()] = true;
                    worklist.push(*dependency);
                }
            }
        }

        let parameter_live = self
            .blocks
            .iter()
            .map(|block| {
                block
                    .parameters
                    .iter()
                    .map(|parameter| live[parameter.index()])
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        let mut changed = false;
        for block in &mut self.blocks {
            changed |= block.terminator.prune_block_arguments(&parameter_live);
        }
        for block in &mut self.blocks {
            let previous_parameter_len = block.parameters.len();
            block.parameters.retain(|parameter| live[parameter.index()]);
            changed |= block.parameters.len() != previous_parameter_len;

            let previous_len = block.instructions.len();
            block.instructions.retain(|instruction| match instruction {
                Instruction::Value { result, .. } => live[result.index()],
                Instruction::Effect(_) => true,
            });
            changed |= block.instructions.len() != previous_len;
        }
        changed
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

    fn verify_sorter_use(
        definitions: &[Option<Definition>],
        dominators: &[Vec<bool>],
        sorter: SorterId,
        use_block: BlockId,
        use_instruction: usize,
    ) -> Result<()> {
        let Some(Some(definition)) = definitions.get(sorter.index()) else {
            return Err(LimboError::InternalError(format!(
                "compiler IR uses unopened sorter {sorter:?}"
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
                "compiler IR sorter {sorter:?} is not open on every path to {use_block:?}"
            )));
        }
        Ok(())
    }

    fn verify_distinct_set_use(
        definitions: &[Option<Definition>],
        dominators: &[Vec<bool>],
        set: DistinctSetId,
        use_block: BlockId,
        use_instruction: usize,
    ) -> Result<()> {
        let Some(Some(definition)) = definitions.get(set.index()) else {
            return Err(LimboError::InternalError(format!(
                "compiler IR uses unopened distinct set {set:?}"
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
                "compiler IR distinct set {set:?} is not open on every path to {use_block:?}"
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

    fn defined_values(&self) -> Vec<bool> {
        let mut defined = vec![false; self.value_count as usize];
        for block in &self.blocks {
            for parameter in &block.parameters {
                defined[parameter.index()] = true;
            }
            for instruction in &block.instructions {
                if let Instruction::Value { result, .. } = instruction {
                    defined[result.index()] = true;
                }
            }
        }
        defined
    }

    /// Compute the values live after each block terminator. Block parameters
    /// are definitions at the successor entry; their incoming edge arguments
    /// are uses in the predecessor block.
    fn live_out_values(&self) -> Vec<HashSet<ValueId>> {
        let mut block_uses = Vec::with_capacity(self.blocks.len());
        let mut block_definitions = Vec::with_capacity(self.blocks.len());
        for block in &self.blocks {
            let mut uses = HashSet::default();
            let mut definitions = HashSet::default();
            definitions.extend(block.parameters.iter().copied());
            for instruction in &block.instructions {
                for operand in instruction.operands() {
                    if !definitions.contains(&operand) {
                        uses.insert(operand);
                    }
                }
                if let Instruction::Value { result, .. } = instruction {
                    definitions.insert(*result);
                }
            }
            for operand in block.terminator.operands() {
                if !definitions.contains(&operand) {
                    uses.insert(operand);
                }
            }
            block_uses.push(uses);
            block_definitions.push(definitions);
        }

        let mut live_in = vec![HashSet::default(); self.blocks.len()];
        let mut live_out = vec![HashSet::default(); self.blocks.len()];
        loop {
            let mut changed = false;
            for block_index in (0..self.blocks.len()).rev() {
                let mut next_out = HashSet::default();
                for successor in self.blocks[block_index].terminator.successors() {
                    next_out.extend(live_in[successor.index()].iter().copied());
                }
                let mut next_in = block_uses[block_index].clone();
                next_in.extend(
                    next_out
                        .iter()
                        .filter(|value| !block_definitions[block_index].contains(value))
                        .copied(),
                );
                if next_out != live_out[block_index] {
                    live_out[block_index] = next_out;
                    changed = true;
                }
                if next_in != live_in[block_index] {
                    live_in[block_index] = next_in;
                    changed = true;
                }
            }
            if !changed {
                return live_out;
            }
        }
    }

    fn add_interference(interference: &mut [HashSet<ValueId>], first: ValueId, second: ValueId) {
        if first == second {
            return;
        }
        interference[first.index()].insert(second);
        interference[second.index()].insert(first);
    }

    /// Build a conservative SSA interference graph. Results also interfere
    /// with their operands so lowering never relies on a VDBE instruction
    /// accepting an aliased input and destination register.
    fn value_interference(&self) -> Vec<HashSet<ValueId>> {
        let live_out = self.live_out_values();
        let mut interference = vec![HashSet::default(); self.value_count as usize];

        for block in &self.blocks {
            let mut live = live_out[block.id.index()].clone();
            live.extend(block.terminator.operands());
            for instruction in block.instructions.iter().rev() {
                match instruction {
                    Instruction::Value { result, op } => {
                        let operands = op.operands().collect::<SmallVec<[_; 2]>>();
                        for other in live.iter().chain(operands.iter()) {
                            Self::add_interference(&mut interference, *result, *other);
                        }
                        live.remove(result);
                        live.extend(operands);
                    }
                    Instruction::Effect(_) => live.extend(instruction.operands()),
                }
            }

            // Every parameter receives a value at the block entry, including
            // signature positions retained for shared cursor edges. Keep all
            // destinations distinct and away from values live through entry.
            for (index, parameter) in block.parameters.iter().enumerate() {
                for other in &block.parameters[index + 1..] {
                    Self::add_interference(&mut interference, *parameter, *other);
                }
                for other in &live {
                    Self::add_interference(&mut interference, *parameter, *other);
                }
            }

            // Lowering materializes edge arguments before executing a cursor
            // control operation. Its key or rowid operands must therefore
            // remain intact while every successor parameter is assigned.
            let control_operands = block
                .terminator
                .control_operands()
                .collect::<SmallVec<[_; 4]>>();
            for successor in block.terminator.successors() {
                for parameter in &self.blocks[successor.index()].parameters {
                    for operand in &control_operands {
                        Self::add_interference(&mut interference, *parameter, *operand);
                    }
                }
            }

            // Cursor terminators materialize both successor argument lists
            // before branching. Those two parameter packs therefore coexist
            // physically even though only one successor executes.
            let shared_targets = match &block.terminator {
                Terminator::CursorStart {
                    if_non_empty,
                    if_empty,
                    ..
                } => Some((*if_non_empty, *if_empty)),
                Terminator::CursorSeekRowid {
                    if_found,
                    if_not_found,
                    ..
                } => Some((*if_found, *if_not_found)),
                Terminator::TableSeek {
                    if_found, if_empty, ..
                } => Some((*if_found, *if_empty)),
                Terminator::TableBound {
                    if_before_end,
                    if_at_end,
                    ..
                } => Some((*if_before_end, *if_at_end)),
                Terminator::IndexSeek {
                    if_found, if_empty, ..
                } => Some((*if_found, *if_empty)),
                Terminator::IndexBound {
                    if_before_end,
                    if_at_end,
                    ..
                } => Some((*if_before_end, *if_at_end)),
                Terminator::CursorAdvance {
                    if_next, if_done, ..
                } => Some((*if_next, *if_done)),
                Terminator::SorterSort {
                    if_non_empty,
                    if_empty,
                    ..
                } => Some((*if_non_empty, *if_empty)),
                Terminator::SorterNext {
                    if_next, if_done, ..
                } => Some((*if_next, *if_done)),
                _ => None,
            };
            if let Some((first, second)) = shared_targets {
                for first in &self.blocks[first.index()].parameters {
                    for second in &self.blocks[second.index()].parameters {
                        Self::add_interference(&mut interference, *first, *second);
                    }
                }
            }
        }
        interference
    }

    fn allocate_value_registers(
        &self,
        program: &mut ProgramBuilder,
        target_register: usize,
        input_registers: &[usize],
    ) -> SmallVec<[Option<usize>; 8]> {
        let output = self.output();
        let defined_values = self.defined_values();
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

        let interference = self.value_interference();
        let mut colors = vec![None; self.value_count as usize];
        let mut color_count = 0;
        for value in 0..self.value_count {
            let value = ValueId(value);
            if !defined_values[value.index()]
                || value == output
                || input_values[value.index()].is_some()
            {
                continue;
            }
            let mut unavailable = vec![false; color_count];
            for neighbor in &interference[value.index()] {
                if let Some(color) = colors[neighbor.index()] {
                    unavailable[color] = true;
                }
            }
            let color = unavailable
                .iter()
                .position(|unavailable| !unavailable)
                .unwrap_or_else(|| {
                    color_count += 1;
                    color_count - 1
                });
            colors[value.index()] = Some(color);
        }
        let physical_colors = (0..color_count)
            .map(|_| program.alloc_register())
            .collect::<SmallVec<[_; 8]>>();

        (0..self.value_count)
            .map(|value| {
                let value = ValueId(value);
                if !defined_values[value.index()] {
                    None
                } else if value == output {
                    Some(target_register)
                } else if let Some(input) = input_values[value.index()] {
                    Some(input_registers[input.index()])
                } else {
                    Some(physical_colors[colors[value.index()].expect("allocatable SSA value")])
                }
            })
            .collect()
    }

    fn required_cursors(&self) -> Vec<bool> {
        let mut required = vec![false; self.cursor_resources.len()];
        for block in &self.blocks {
            for instruction in &block.instructions {
                for cursor in instruction
                    .cursor_uses()
                    .chain(instruction.cursor_definition())
                {
                    required[cursor.index()] = true;
                }
            }
            if let Some(cursor) = block.terminator.cursor() {
                required[cursor.index()] = true;
            }
        }
        required
    }

    fn required_sorters(&self) -> Vec<bool> {
        let mut required = vec![false; self.sorter_resources.len()];
        for block in &self.blocks {
            for instruction in &block.instructions {
                for sorter in [instruction.sorter_use(), instruction.sorter_definition()]
                    .into_iter()
                    .flatten()
                {
                    required[sorter.index()] = true;
                }
            }
            if let Some(sorter) = block.terminator.sorter() {
                required[sorter.index()] = true;
            }
        }
        required
    }

    fn required_distinct_sets(&self) -> Vec<bool> {
        let mut required = vec![false; self.distinct_set_resources.len()];
        for block in &self.blocks {
            for instruction in &block.instructions {
                if let Some(set) = instruction.distinct_set_definition() {
                    required[set.index()] = true;
                }
            }
            if let Some(set) = block.terminator.distinct_set() {
                required[set.index()] = true;
            }
        }
        required
    }

    fn register_for(registers: &[Option<usize>], value: ValueId) -> usize {
        registers[value.index()].expect("verified live SSA value has a physical register")
    }

    fn cursor_for(cursors: &[Option<usize>], cursor: CursorId) -> usize {
        cursors[cursor.index()].expect("verified live cursor has a physical cursor")
    }

    fn sorter_for(sorters: &[Option<PhysicalSorter>], sorter: SorterId) -> PhysicalSorter {
        sorters[sorter.index()].expect("verified live sorter has physical resources")
    }

    fn distinct_set_for(sets: &[Option<usize>], set: DistinctSetId) -> usize {
        sets[set.index()].expect("verified live distinct set has a physical hash table")
    }

    fn collect_edge_copies(
        &self,
        registers: &[Option<usize>],
        target: BlockId,
        arguments: &[ValueId],
        copies: &mut SmallVec<[(usize, usize); 8]>,
    ) {
        for (argument, parameter) in arguments
            .iter()
            .zip(&self.blocks[target.index()].parameters)
        {
            let source = Self::register_for(registers, *argument);
            let destination = Self::register_for(registers, *parameter);
            if source == destination {
                continue;
            }
            if let Some((existing_source, _)) = copies
                .iter()
                .find(|(_, existing_destination)| *existing_destination == destination)
            {
                assert_eq!(
                    *existing_source, source,
                    "one physical edge destination cannot receive different SSA values"
                );
            } else {
                copies.push((source, destination));
            }
        }
    }

    fn emit_parallel_copies(
        program: &mut ProgramBuilder,
        mut copies: SmallVec<[(usize, usize); 8]>,
        temporary: &mut Option<usize>,
    ) {
        while !copies.is_empty() {
            if let Some(index) = copies
                .iter()
                .position(|(_, destination)| copies.iter().all(|(source, _)| source != destination))
            {
                let (source, destination) = copies.remove(index);
                program.emit_insn(Insn::Copy {
                    src_reg: source,
                    dst_reg: destination,
                    extra_amount: 0,
                });
                continue;
            }

            // Every remaining destination is still needed as a source, so the
            // moves contain a cycle. Preserve one source and rewrite all of
            // its pending uses to break that cycle.
            let source = copies[0].0;
            let temporary = *temporary.get_or_insert_with(|| program.alloc_register());
            program.emit_insn(Insn::Copy {
                src_reg: source,
                dst_reg: temporary,
                extra_amount: 0,
            });
            for (pending_source, _) in &mut copies {
                if *pending_source == source {
                    *pending_source = temporary;
                }
            }
        }
    }

    fn emit_edge_copies(
        &self,
        program: &mut ProgramBuilder,
        registers: &[Option<usize>],
        target: BlockId,
        arguments: &[ValueId],
        temporary: &mut Option<usize>,
    ) {
        let mut copies = SmallVec::new();
        self.collect_edge_copies(registers, target, arguments, &mut copies);
        Self::emit_parallel_copies(program, copies, temporary);
    }

    fn materialize_index_key(
        program: &mut ProgramBuilder,
        registers: &[Option<usize>],
        key: &IndexKey,
        if_null: crate::vdbe::BranchOffset,
    ) -> usize {
        let start = program.alloc_registers(key.values().len());
        for (index, (value, null_policy)) in key.values().iter().zip(&key.null_policies).enumerate()
        {
            let source = Self::register_for(registers, *value);
            let destination = start + index;
            if source != destination {
                program.emit_insn(Insn::Copy {
                    src_reg: source,
                    dst_reg: destination,
                    extra_amount: 0,
                });
            }
            if matches!(null_policy, IndexNullPolicy::AbortRange) {
                program.emit_insn(Insn::IsNull {
                    reg: destination,
                    target_pc: if_null,
                });
            }
        }
        let affinities = key
            .affinities
            .iter()
            .map(|affinity| affinity.aff_mask())
            .collect::<String>();
        if affinities
            .chars()
            .any(|affinity| affinity != crate::vdbe::affinity::SQLITE_AFF_NONE)
        {
            program.emit_insn(Insn::Affinity {
                start_reg: start,
                count: std::num::NonZeroUsize::new(key.values().len())
                    .expect("verified index key is not empty"),
                affinities,
            });
        }
        start
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
        mut self,
        program: &mut ProgramBuilder,
        target_register: usize,
        input_registers: &[usize],
        cursor_ids: &[usize],
    ) -> Result<LoweredRegion> {
        self = self.optimize()?;
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
        for variable in &self.parameter_declarations {
            program.register_variable(variable);
        }
        let required_cursors = self.required_cursors();
        let physical_cursors = self
            .cursor_resources
            .iter()
            .enumerate()
            .map(
                |(index, resource)| match (required_cursors[index], resource) {
                    (false, _) => None,
                    (true, CursorResource::External(input)) => Some(cursor_ids[input.index()]),
                    (true, CursorResource::Owned(cursor_type)) => {
                        Some(program.alloc_cursor_id(cursor_type.clone()))
                    }
                },
            )
            .collect::<SmallVec<[Option<usize>; 2]>>();
        let required_sorters = self.required_sorters();
        let physical_sorters = self
            .sorter_resources
            .iter()
            .enumerate()
            .map(|(index, resource)| {
                required_sorters[index].then(|| PhysicalSorter {
                    cursor: program.alloc_cursor_id(CursorType::Sorter),
                    pseudo_cursor: program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
                        column_count: resource.record_width,
                    })),
                    data_register: program.alloc_register(),
                })
            })
            .collect::<SmallVec<[Option<PhysicalSorter>; 1]>>();
        let required_distinct_sets = self.required_distinct_sets();
        let physical_distinct_sets = self
            .distinct_set_resources
            .iter()
            .enumerate()
            .map(|(index, _)| required_distinct_sets[index].then(|| program.alloc_hash_table_id()))
            .collect::<SmallVec<[Option<usize>; 1]>>();
        let registers = self.allocate_value_registers(program, target_register, input_registers);
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
        let mut edge_copy_temporary = None;

        for block in &self.blocks {
            if targeted[block.id.index()] {
                program.preassign_label_to_next_insn(labels[block.id.index()]);
            }
            for instruction in &block.instructions {
                match instruction {
                    Instruction::Value { result, op } => {
                        let destination = Self::register_for(&registers, *result);
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
                            ScalarOp::Parameter(variable) => {
                                let index = program.register_variable(variable);
                                program.emit_insn(Insn::Variable {
                                    index,
                                    dest: destination,
                                });
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
                                lhs: Self::register_for(&registers, *lhs),
                                rhs: Self::register_for(&registers, *rhs),
                                dest: destination,
                            }),
                            ScalarOp::MustBeInt { value } => {
                                let source = Self::register_for(&registers, *value);
                                if source != destination {
                                    program.emit_insn(Insn::Copy {
                                        src_reg: source,
                                        dst_reg: destination,
                                        extra_amount: 0,
                                    });
                                }
                                program.emit_insn(Insn::MustBeInt {
                                    reg: destination,
                                    target_pc: None,
                                });
                            }
                            ScalarOp::Logical { op, lhs, rhs } => {
                                let lhs = Self::register_for(&registers, *lhs);
                                let rhs = Self::register_for(&registers, *rhs);
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
                                    Self::cursor_for(&physical_cursors, *cursor),
                                    *column,
                                    destination,
                                );
                            }
                            ScalarOp::RowId { cursor } => program.emit_insn(Insn::RowId {
                                cursor_id: Self::cursor_for(&physical_cursors, *cursor),
                                dest: destination,
                            }),
                            ScalarOp::IndexRowId { cursor } => {
                                program.emit_insn(Insn::IdxRowId {
                                    cursor_id: Self::cursor_for(&physical_cursors, *cursor),
                                    dest: destination,
                                });
                            }
                            ScalarOp::SorterColumn { sorter, column } => {
                                let sorter = Self::sorter_for(&physical_sorters, *sorter);
                                program.emit_column_or_rowid(
                                    sorter.pseudo_cursor,
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
                            cursor_id: Self::cursor_for(&physical_cursors, *cursor),
                            root_page: *root_page,
                            db: *db,
                        });
                    }
                    Instruction::Effect(EffectOp::OpenEphemeralIndex { cursor }) => {
                        program.emit_insn(Insn::OpenEphemeral {
                            cursor_id: Self::cursor_for(&physical_cursors, *cursor),
                            is_table: false,
                        });
                    }
                    Instruction::Effect(EffectOp::DeferredSeek { index, table }) => {
                        program.emit_insn(Insn::DeferredSeek {
                            index_cursor_id: Self::cursor_for(&physical_cursors, *index),
                            table_cursor_id: Self::cursor_for(&physical_cursors, *table),
                        });
                    }
                    Instruction::Effect(EffectOp::ResultRow { pack }) => {
                        let start = program.alloc_registers(pack.values().len());
                        result_row_packs.push((start, pack.values().len()));
                        for (index, value) in pack.values().iter().enumerate() {
                            let source = Self::register_for(&registers, *value);
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
                    Instruction::Effect(EffectOp::IndexInsert {
                        cursor,
                        pack,
                        index_name,
                        affinity,
                    }) => {
                        let start = program.alloc_registers(pack.values().len());
                        for (index, value) in pack.values().iter().enumerate() {
                            let source = Self::register_for(&registers, *value);
                            let destination = start + index;
                            if source != destination {
                                program.emit_insn(Insn::Copy {
                                    src_reg: source,
                                    dst_reg: destination,
                                    extra_amount: 0,
                                });
                            }
                        }
                        let record = program.alloc_register();
                        program.emit_insn(Insn::MakeRecord {
                            start_reg: to_u16(start),
                            count: to_u16(pack.values().len()),
                            dest_reg: to_u16(record),
                            index_name: Some(index_name.clone()),
                            affinity_str: affinity.clone(),
                        });
                        program.emit_insn(Insn::IdxInsert {
                            cursor_id: Self::cursor_for(&physical_cursors, *cursor),
                            record_reg: record,
                            unpacked_start: None,
                            unpacked_count: None,
                            flags: IdxInsertFlags::new().no_op_duplicate(),
                        });
                    }
                    Instruction::Effect(EffectOp::OpenSorter { sorter }) => {
                        let physical = Self::sorter_for(&physical_sorters, *sorter);
                        let resource = &self.sorter_resources[sorter.index()];
                        program.emit_insn(Insn::SorterOpen {
                            cursor_id: physical.cursor,
                            columns: resource.keys.len(),
                            order_collations_nulls: resource
                                .keys
                                .iter()
                                .map(|key| (key.order, key.collation, key.nulls))
                                .collect(),
                            comparators: resource.keys.iter().map(|key| key.comparator).collect(),
                        });
                        program.emit_insn(Insn::OpenPseudo {
                            cursor_id: physical.pseudo_cursor,
                            content_reg: physical.data_register,
                            num_fields: resource.record_width,
                        });
                    }
                    Instruction::Effect(EffectOp::SorterInsert { sorter, pack }) => {
                        let physical = Self::sorter_for(&physical_sorters, *sorter);
                        let resource = &self.sorter_resources[sorter.index()];
                        let start = program.alloc_registers(pack.values().len());
                        for (index, value) in pack.values().iter().enumerate() {
                            let source = Self::register_for(&registers, *value);
                            let destination = start + index;
                            if source != destination {
                                program.emit_insn(Insn::Copy {
                                    src_reg: source,
                                    dst_reg: destination,
                                    extra_amount: 0,
                                });
                            }
                        }
                        let record = program.alloc_register();
                        program.emit_insn(Insn::MakeRecord {
                            start_reg: to_u16(start),
                            count: to_u16(pack.values().len()),
                            dest_reg: to_u16(record),
                            index_name: None,
                            affinity_str: resource.affinities.as_ref().map(|affinities| {
                                affinities
                                    .iter()
                                    .map(Affinity::aff_mask)
                                    .collect::<String>()
                            }),
                        });
                        program.emit_insn(Insn::SorterInsert {
                            cursor_id: physical.cursor,
                            record_reg: record,
                        });
                    }
                    Instruction::Effect(EffectOp::SorterData { sorter }) => {
                        let physical = Self::sorter_for(&physical_sorters, *sorter);
                        program.emit_insn(Insn::SorterData {
                            cursor_id: physical.cursor,
                            dest_reg: physical.data_register,
                            pseudo_cursor: physical.pseudo_cursor,
                        });
                    }
                    Instruction::Effect(EffectOp::OpenDistinctSet { set }) => {
                        program.emit_insn(Insn::HashClear {
                            hash_table_id: Self::distinct_set_for(&physical_distinct_sets, *set),
                        });
                    }
                }
            }
            match &block.terminator {
                Terminator::Jump { target, arguments } => {
                    self.emit_edge_copies(
                        program,
                        &registers,
                        *target,
                        arguments,
                        &mut edge_copy_temporary,
                    );
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
                        reg: Self::register_for(&registers, *condition),
                        target_pc: labels[if_false.index()],
                        jump_if_null: true,
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: labels[if_true.index()],
                    });
                }
                Terminator::Once { initialize, ready } => {
                    program.emit_insn(Insn::Once {
                        target_pc_when_reentered: labels[ready.index()],
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: labels[initialize.index()],
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
                    let lhs_source = Self::register_for(&registers, *lhs);
                    let rhs_source = Self::register_for(&registers, *rhs);
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
                Terminator::CursorStart {
                    cursor,
                    direction,
                    if_non_empty,
                    if_empty,
                    arguments,
                } => {
                    let mut copies = SmallVec::new();
                    for target in [if_non_empty, if_empty] {
                        self.collect_edge_copies(&registers, *target, arguments, &mut copies);
                    }
                    Self::emit_parallel_copies(program, copies, &mut edge_copy_temporary);
                    let cursor_id = Self::cursor_for(&physical_cursors, *cursor);
                    program.emit_insn(match direction {
                        ScanDirection::Forward => Insn::Rewind {
                            cursor_id,
                            pc_if_empty: labels[if_empty.index()],
                        },
                        ScanDirection::Reverse => Insn::Last {
                            cursor_id,
                            pc_if_empty: labels[if_empty.index()],
                        },
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: labels[if_non_empty.index()],
                    });
                }
                Terminator::CursorSeekRowid {
                    cursor,
                    rowid,
                    if_found,
                    if_not_found,
                    arguments,
                } => {
                    let mut copies = SmallVec::new();
                    for target in [if_found, if_not_found] {
                        self.collect_edge_copies(&registers, *target, arguments, &mut copies);
                    }
                    Self::emit_parallel_copies(program, copies, &mut edge_copy_temporary);
                    program.emit_insn(Insn::SeekRowid {
                        cursor_id: Self::cursor_for(&physical_cursors, *cursor),
                        src_reg: Self::register_for(&registers, *rowid),
                        target_pc: labels[if_not_found.index()],
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: labels[if_found.index()],
                    });
                }
                Terminator::TableSeek {
                    cursor,
                    rowid,
                    op,
                    if_found,
                    if_empty,
                    arguments,
                } => {
                    let mut copies = SmallVec::new();
                    for target in [if_found, if_empty] {
                        self.collect_edge_copies(&registers, *target, arguments, &mut copies);
                    }
                    Self::emit_parallel_copies(program, copies, &mut edge_copy_temporary);
                    // A seek may apply numeric affinity to its key. Keep the
                    // register backing the immutable SSA value unchanged.
                    let start_reg = program.alloc_register();
                    program.emit_insn(Insn::Copy {
                        src_reg: Self::register_for(&registers, *rowid),
                        dst_reg: start_reg,
                        extra_amount: 0,
                    });
                    program.emit_insn(Insn::IsNull {
                        reg: start_reg,
                        target_pc: labels[if_empty.index()],
                    });
                    let cursor_id = Self::cursor_for(&physical_cursors, *cursor);
                    program.emit_insn(match op {
                        SeekOp::GE { eq_only } => Insn::SeekGE {
                            is_index: false,
                            cursor_id,
                            start_reg,
                            num_regs: 1,
                            target_pc: labels[if_empty.index()],
                            eq_only: *eq_only,
                        },
                        SeekOp::GT => Insn::SeekGT {
                            is_index: false,
                            cursor_id,
                            start_reg,
                            num_regs: 1,
                            target_pc: labels[if_empty.index()],
                        },
                        SeekOp::LE { eq_only } => Insn::SeekLE {
                            is_index: false,
                            cursor_id,
                            start_reg,
                            num_regs: 1,
                            target_pc: labels[if_empty.index()],
                            eq_only: *eq_only,
                        },
                        SeekOp::LT => Insn::SeekLT {
                            is_index: false,
                            cursor_id,
                            start_reg,
                            num_regs: 1,
                            target_pc: labels[if_empty.index()],
                        },
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: labels[if_found.index()],
                    });
                }
                Terminator::TableBound {
                    cursor,
                    rowid,
                    op,
                    affinity,
                    if_before_end,
                    if_at_end,
                    arguments,
                } => {
                    let mut copies = SmallVec::new();
                    for target in [if_before_end, if_at_end] {
                        self.collect_edge_copies(&registers, *target, arguments, &mut copies);
                    }
                    Self::emit_parallel_copies(program, copies, &mut edge_copy_temporary);
                    // VDBE comparisons may coerce both operands. Materialize
                    // fresh registers so lowering preserves SSA immutability.
                    let bound = program.alloc_register();
                    program.emit_insn(Insn::Copy {
                        src_reg: Self::register_for(&registers, *rowid),
                        dst_reg: bound,
                        extra_amount: 0,
                    });
                    program.emit_insn(Insn::IsNull {
                        reg: bound,
                        target_pc: labels[if_at_end.index()],
                    });
                    let current = program.alloc_register();
                    program.emit_insn(Insn::RowId {
                        cursor_id: Self::cursor_for(&physical_cursors, *cursor),
                        dest: current,
                    });
                    let flags = CmpInsFlags::default()
                        .jump_if_null()
                        .with_affinity(*affinity);
                    let target_pc = labels[if_at_end.index()];
                    program.emit_insn(match op {
                        SeekOp::GE { .. } => Insn::Ge {
                            lhs: current,
                            rhs: bound,
                            target_pc,
                            flags,
                            collation: None,
                        },
                        SeekOp::GT => Insn::Gt {
                            lhs: current,
                            rhs: bound,
                            target_pc,
                            flags,
                            collation: None,
                        },
                        SeekOp::LE { .. } => Insn::Le {
                            lhs: current,
                            rhs: bound,
                            target_pc,
                            flags,
                            collation: None,
                        },
                        SeekOp::LT => Insn::Lt {
                            lhs: current,
                            rhs: bound,
                            target_pc,
                            flags,
                            collation: None,
                        },
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: labels[if_before_end.index()],
                    });
                }
                Terminator::IndexSeek {
                    cursor,
                    key,
                    op,
                    if_found,
                    if_empty,
                    arguments,
                } => {
                    let mut copies = SmallVec::new();
                    for target in [if_found, if_empty] {
                        self.collect_edge_copies(&registers, *target, arguments, &mut copies);
                    }
                    Self::emit_parallel_copies(program, copies, &mut edge_copy_temporary);
                    let start_reg = Self::materialize_index_key(
                        program,
                        &registers,
                        key,
                        labels[if_empty.index()],
                    );
                    let cursor_id = Self::cursor_for(&physical_cursors, *cursor);
                    program.emit_insn(match op {
                        SeekOp::GE { eq_only } => Insn::SeekGE {
                            is_index: true,
                            cursor_id,
                            start_reg,
                            num_regs: key.values().len(),
                            target_pc: labels[if_empty.index()],
                            eq_only: *eq_only,
                        },
                        SeekOp::GT => Insn::SeekGT {
                            is_index: true,
                            cursor_id,
                            start_reg,
                            num_regs: key.values().len(),
                            target_pc: labels[if_empty.index()],
                        },
                        SeekOp::LE { eq_only } => Insn::SeekLE {
                            is_index: true,
                            cursor_id,
                            start_reg,
                            num_regs: key.values().len(),
                            target_pc: labels[if_empty.index()],
                            eq_only: *eq_only,
                        },
                        SeekOp::LT => Insn::SeekLT {
                            is_index: true,
                            cursor_id,
                            start_reg,
                            num_regs: key.values().len(),
                            target_pc: labels[if_empty.index()],
                        },
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: labels[if_found.index()],
                    });
                }
                Terminator::IndexBound {
                    cursor,
                    key,
                    op,
                    if_before_end,
                    if_at_end,
                    arguments,
                } => {
                    let mut copies = SmallVec::new();
                    for target in [if_before_end, if_at_end] {
                        self.collect_edge_copies(&registers, *target, arguments, &mut copies);
                    }
                    Self::emit_parallel_copies(program, copies, &mut edge_copy_temporary);
                    let start_reg = Self::materialize_index_key(
                        program,
                        &registers,
                        key,
                        labels[if_at_end.index()],
                    );
                    let cursor_id = Self::cursor_for(&physical_cursors, *cursor);
                    program.emit_insn(match op {
                        SeekOp::GE { .. } => Insn::IdxGE {
                            cursor_id,
                            start_reg,
                            num_regs: key.values().len(),
                            target_pc: labels[if_at_end.index()],
                        },
                        SeekOp::GT => Insn::IdxGT {
                            cursor_id,
                            start_reg,
                            num_regs: key.values().len(),
                            target_pc: labels[if_at_end.index()],
                        },
                        SeekOp::LE { .. } => Insn::IdxLE {
                            cursor_id,
                            start_reg,
                            num_regs: key.values().len(),
                            target_pc: labels[if_at_end.index()],
                        },
                        SeekOp::LT => Insn::IdxLT {
                            cursor_id,
                            start_reg,
                            num_regs: key.values().len(),
                            target_pc: labels[if_at_end.index()],
                        },
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: labels[if_before_end.index()],
                    });
                }
                Terminator::CursorAdvance {
                    cursor,
                    direction,
                    if_next,
                    if_done,
                    arguments,
                } => {
                    let mut copies = SmallVec::new();
                    for target in [if_next, if_done] {
                        self.collect_edge_copies(&registers, *target, arguments, &mut copies);
                    }
                    Self::emit_parallel_copies(program, copies, &mut edge_copy_temporary);
                    let cursor_id = Self::cursor_for(&physical_cursors, *cursor);
                    program.emit_insn(match direction {
                        ScanDirection::Forward => Insn::Next {
                            cursor_id,
                            pc_if_next: labels[if_next.index()],
                        },
                        ScanDirection::Reverse => Insn::Prev {
                            cursor_id,
                            pc_if_prev: labels[if_next.index()],
                        },
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: labels[if_done.index()],
                    });
                }
                Terminator::SorterSort {
                    sorter,
                    if_non_empty,
                    if_empty,
                    arguments,
                } => {
                    let mut copies = SmallVec::new();
                    for target in [if_non_empty, if_empty] {
                        self.collect_edge_copies(&registers, *target, arguments, &mut copies);
                    }
                    Self::emit_parallel_copies(program, copies, &mut edge_copy_temporary);
                    program.emit_insn(Insn::SorterSort {
                        cursor_id: Self::sorter_for(&physical_sorters, *sorter).cursor,
                        pc_if_empty: labels[if_empty.index()],
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: labels[if_non_empty.index()],
                    });
                }
                Terminator::SorterNext {
                    sorter,
                    if_next,
                    if_done,
                    arguments,
                } => {
                    let mut copies = SmallVec::new();
                    for target in [if_next, if_done] {
                        self.collect_edge_copies(&registers, *target, arguments, &mut copies);
                    }
                    Self::emit_parallel_copies(program, copies, &mut edge_copy_temporary);
                    program.emit_insn(Insn::SorterNext {
                        cursor_id: Self::sorter_for(&physical_sorters, *sorter).cursor,
                        pc_if_next: labels[if_next.index()],
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: labels[if_done.index()],
                    });
                }
                Terminator::DistinctCheck {
                    set,
                    pack,
                    if_unique,
                    if_duplicate,
                } => {
                    let start = program.alloc_registers(pack.values().len());
                    for (index, value) in pack.values().iter().enumerate() {
                        let source = Self::register_for(&registers, *value);
                        let destination = start + index;
                        if source != destination {
                            program.emit_insn(Insn::Copy {
                                src_reg: source,
                                dst_reg: destination,
                                extra_amount: 0,
                            });
                        }
                    }
                    let resource = &self.distinct_set_resources[set.index()];
                    program.emit_insn(Insn::HashDistinct {
                        data: Box::new(HashDistinctData {
                            hash_table_id: Self::distinct_set_for(&physical_distinct_sets, *set),
                            key_start_reg: start,
                            num_keys: pack.values().len(),
                            collations: resource.collations.iter().copied().collect(),
                            target_pc: labels[if_duplicate.index()],
                        }),
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: labels[if_unique.index()],
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
                CursorResource::Owned(CursorType::BTreeIndex(index_resource)) => writeln!(
                    f,
                    "cursor ${index} = btree_index {:?} root {}",
                    index_resource.name, index_resource.root_page
                )?,
                CursorResource::Owned(cursor_type) => {
                    writeln!(f, "cursor ${index} = {cursor_type:?}")?;
                }
            }
        }
        for (index, resource) in self.sorter_resources.iter().enumerate() {
            write!(
                f,
                "sorter #{index} keys {} width {} [",
                resource.keys.len(),
                resource.record_width
            )?;
            for (key_index, key) in resource.keys.iter().enumerate() {
                if key_index != 0 {
                    write!(f, ", ")?;
                }
                write!(
                    f,
                    "{:?} {:?} {:?} {:?}",
                    key.order, key.collation, key.nulls, key.comparator
                )?;
            }
            write!(f, "]")?;
            if let Some(affinities) = &resource.affinities {
                write!(f, " affinity {affinities:?}")?;
            }
            writeln!(f)?;
        }
        for (index, resource) in self.distinct_set_resources.iter().enumerate() {
            write!(
                f,
                "distinct_set &{index} width {} [",
                resource.collations.len()
            )?;
            for (collation_index, collation) in resource.collations.iter().enumerate() {
                if collation_index != 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{collation:?}")?;
            }
            writeln!(f, "]")?;
        }
        if !self.cursor_resources.is_empty()
            || !self.sorter_resources.is_empty()
            || !self.distinct_set_resources.is_empty()
        {
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
                            ScalarOp::Parameter(variable) => {
                                if let Some(name) = variable.name.as_deref() {
                                    writeln!(f, "parameter {name} @{}", variable.index)?;
                                } else {
                                    writeln!(f, "parameter ?{}", variable.index)?;
                                }
                            }
                            ScalarOp::Constant(value) => writeln!(f, "constant {value:?}")?,
                            ScalarOp::Add { lhs, rhs } => {
                                writeln!(f, "add %{}, %{}", lhs.0, rhs.0)?;
                            }
                            ScalarOp::MustBeInt { value } => {
                                writeln!(f, "must_be_int %{}", value.0)?;
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
                            ScalarOp::RowId { cursor } => {
                                writeln!(f, "rowid ${}", cursor.0)?;
                            }
                            ScalarOp::IndexRowId { cursor } => {
                                writeln!(f, "index_rowid ${}", cursor.0)?;
                            }
                            ScalarOp::SorterColumn { sorter, column } => {
                                writeln!(f, "sorter_column #{}[{column}]", sorter.0)?;
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
                    Instruction::Effect(EffectOp::OpenEphemeralIndex { cursor }) => {
                        writeln!(f, "  open_ephemeral_index ${}", cursor.0)?;
                    }
                    Instruction::Effect(EffectOp::DeferredSeek { index, table }) => {
                        writeln!(f, "  deferred_seek ${} -> ${}", index.0, table.0)?;
                    }
                    Instruction::Effect(EffectOp::ResultRow { pack }) => {
                        write!(f, "  result_row [")?;
                        Self::fmt_arguments(f, pack.values())?;
                        writeln!(f, "]")?;
                    }
                    Instruction::Effect(EffectOp::IndexInsert { cursor, pack, .. }) => {
                        write!(f, "  index_insert ${} [", cursor.0)?;
                        Self::fmt_arguments(f, pack.values())?;
                        writeln!(f, "]")?;
                    }
                    Instruction::Effect(EffectOp::OpenSorter { sorter }) => {
                        writeln!(f, "  open_sorter #{}", sorter.0)?;
                    }
                    Instruction::Effect(EffectOp::SorterInsert { sorter, pack }) => {
                        write!(f, "  sorter_insert #{} [", sorter.0)?;
                        Self::fmt_arguments(f, pack.values())?;
                        writeln!(f, "]")?;
                    }
                    Instruction::Effect(EffectOp::SorterData { sorter }) => {
                        writeln!(f, "  sorter_data #{}", sorter.0)?;
                    }
                    Instruction::Effect(EffectOp::OpenDistinctSet { set }) => {
                        writeln!(f, "  open_distinct_set &{}", set.0)?;
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
                Terminator::Once { initialize, ready } => {
                    writeln!(f, "once block{}, block{}", initialize.0, ready.0)?
                }
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
                Terminator::CursorStart {
                    cursor,
                    direction,
                    if_non_empty,
                    if_empty,
                    arguments,
                } => {
                    write!(
                        f,
                        "cursor_start {direction:?} ${}, block{}(",
                        cursor.0, if_non_empty.0
                    )?;
                    Self::fmt_arguments(f, arguments)?;
                    write!(f, "), block{}(", if_empty.0)?;
                    Self::fmt_arguments(f, arguments)?;
                    writeln!(f, ")")?;
                }
                Terminator::CursorSeekRowid {
                    cursor,
                    rowid,
                    if_found,
                    if_not_found,
                    arguments,
                } => {
                    write!(
                        f,
                        "cursor_seek_rowid ${}, %{}, block{}(",
                        cursor.0, rowid.0, if_found.0
                    )?;
                    Self::fmt_arguments(f, arguments)?;
                    write!(f, "), block{}(", if_not_found.0)?;
                    Self::fmt_arguments(f, arguments)?;
                    writeln!(f, ")")?;
                }
                Terminator::TableSeek {
                    cursor,
                    rowid,
                    op,
                    if_found,
                    if_empty,
                    arguments,
                } => {
                    write!(
                        f,
                        "table_seek {op:?} ${}, %{}, block{}(",
                        cursor.0, rowid.0, if_found.0
                    )?;
                    Self::fmt_arguments(f, arguments)?;
                    write!(f, "), block{}(", if_empty.0)?;
                    Self::fmt_arguments(f, arguments)?;
                    writeln!(f, ")")?;
                }
                Terminator::TableBound {
                    cursor,
                    rowid,
                    op,
                    affinity,
                    if_before_end,
                    if_at_end,
                    arguments,
                } => {
                    write!(
                        f,
                        "table_bound {op:?} affinity {affinity:?} ${}, %{}, block{}(",
                        cursor.0, rowid.0, if_before_end.0
                    )?;
                    Self::fmt_arguments(f, arguments)?;
                    write!(f, "), block{}(", if_at_end.0)?;
                    Self::fmt_arguments(f, arguments)?;
                    writeln!(f, ")")?;
                }
                Terminator::IndexSeek {
                    cursor,
                    key,
                    op,
                    if_found,
                    if_empty,
                    arguments,
                } => {
                    write!(f, "index_seek {op:?} ${} ", cursor.0)?;
                    Self::fmt_index_key(f, key)?;
                    write!(f, ", block{}(", if_found.0)?;
                    Self::fmt_arguments(f, arguments)?;
                    write!(f, "), block{}(", if_empty.0)?;
                    Self::fmt_arguments(f, arguments)?;
                    writeln!(f, ")")?;
                }
                Terminator::IndexBound {
                    cursor,
                    key,
                    op,
                    if_before_end,
                    if_at_end,
                    arguments,
                } => {
                    write!(f, "index_bound {op:?} ${} ", cursor.0)?;
                    Self::fmt_index_key(f, key)?;
                    write!(f, ", block{}(", if_before_end.0)?;
                    Self::fmt_arguments(f, arguments)?;
                    write!(f, "), block{}(", if_at_end.0)?;
                    Self::fmt_arguments(f, arguments)?;
                    writeln!(f, ")")?;
                }
                Terminator::CursorAdvance {
                    cursor,
                    direction,
                    if_next,
                    if_done,
                    arguments,
                } => {
                    write!(
                        f,
                        "cursor_advance {direction:?} ${}, block{}(",
                        cursor.0, if_next.0
                    )?;
                    Self::fmt_arguments(f, arguments)?;
                    write!(f, "), block{}(", if_done.0)?;
                    Self::fmt_arguments(f, arguments)?;
                    writeln!(f, ")")?;
                }
                Terminator::SorterSort {
                    sorter,
                    if_non_empty,
                    if_empty,
                    arguments,
                } => {
                    write!(f, "sort #{}, block{}(", sorter.0, if_non_empty.0)?;
                    Self::fmt_arguments(f, arguments)?;
                    write!(f, "), block{}(", if_empty.0)?;
                    Self::fmt_arguments(f, arguments)?;
                    writeln!(f, ")")?;
                }
                Terminator::SorterNext {
                    sorter,
                    if_next,
                    if_done,
                    arguments,
                } => {
                    write!(f, "sorter_next #{}, block{}(", sorter.0, if_next.0)?;
                    Self::fmt_arguments(f, arguments)?;
                    write!(f, "), block{}(", if_done.0)?;
                    Self::fmt_arguments(f, arguments)?;
                    writeln!(f, ")")?;
                }
                Terminator::DistinctCheck {
                    set,
                    pack,
                    if_unique,
                    if_duplicate,
                } => {
                    write!(f, "distinct_check &{} [", set.0)?;
                    Self::fmt_arguments(f, pack.values())?;
                    writeln!(f, "], block{}, block{}", if_unique.0, if_duplicate.0)?;
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
    fn fmt_index_key(f: &mut fmt::Formatter<'_>, key: &IndexKey) -> fmt::Result {
        write!(f, "[")?;
        Self::fmt_arguments(f, key.values())?;
        write!(f, "] affinity [")?;
        for (index, affinity) in key.affinities.iter().enumerate() {
            if index != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{affinity:?}")?;
        }
        write!(f, "] null [")?;
        for (index, policy) in key.null_policies.iter().enumerate() {
            if index != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{policy:?}")?;
        }
        write!(f, "]")
    }

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
pub(crate) struct OpenReadCursor {
    resource: CursorType,
    root_page: PageIdx,
    db: usize,
    schema_cookie: u32,
}

pub(crate) struct OpenEphemeralIndex {
    index: Arc<Index>,
}

pub(crate) struct DeclareEphemeralIndex {
    index: Arc<Index>,
}

/// A symbolic ephemeral-index cursor that has storage identity but no runtime
/// open effect yet. Keeping the raw cursor private prevents consumers from
/// reading or writing the resource before initialization is described.
#[derive(Clone, Copy)]
pub(crate) struct UnopenedEphemeralIndex {
    cursor: CursorId,
}

pub(crate) struct OpenDeclaredEphemeralIndex {
    unopened: UnopenedEphemeralIndex,
}

/// The base symbolic stream of rows backed by an opened cursor.
#[derive(Clone)]
pub(crate) struct CursorRows {
    cursor: CursorId,
    row_cursor: CursorId,
    deferred_seek: Option<DeferredSeekCursors>,
    source: CursorRowSource,
}

#[derive(Clone)]
enum CursorRowSource {
    Scan(ScanDirection),
    Rowid(ValueId),
    TableRange(TableRangeSource),
    IndexRange(IndexRangeSource),
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const fn scan_cursor(cursor: CursorId, direction: ScanDirection) -> CursorRows {
    CursorRows {
        cursor,
        row_cursor: cursor,
        deferred_seek: None,
        source: CursorRowSource::Scan(direction),
    }
}

impl CursorRowSource {
    fn has_end_bound(&self) -> bool {
        match self {
            Self::TableRange(range) => range.end.is_some(),
            Self::IndexRange(range) => range.end.is_some(),
            Self::Scan(_) | Self::Rowid(_) => false,
        }
    }
}

#[derive(Clone)]
struct TableRangeSource {
    start: Option<TableBoundSpec>,
    end: Option<TableBoundSpec>,
    direction: ScanDirection,
    affinity: Affinity,
}

#[derive(Clone, Copy)]
struct TableBoundSpec {
    rowid: ValueId,
    op: SeekOp,
}

#[derive(Clone)]
struct IndexRangeSource {
    start: Option<IndexBoundSpec>,
    end: Option<IndexBoundSpec>,
    direction: ScanDirection,
}

#[derive(Clone)]
struct IndexBoundSpec {
    key: IndexKey,
    op: SeekOp,
}

#[derive(Clone, Copy)]
struct DeferredSeekCursors {
    index: CursorId,
    table: CursorId,
}

type BoxedRowConsumer<Item> = Box<dyn FnOnce(Item) -> BoxedCompile<()>>;
type BoxedRowFolder<Item> = Box<dyn FnOnce(Item, LoopState) -> BoxedCompile<LoopStep>>;
type BoxedDistinctKey<Item> = Box<dyn FnOnce(Item) -> BoxedCompile<ValuePack>>;

/// A compile-time row-program algebra analogous to [`Iterator`].
///
/// Stream operators compose compiler descriptions. They do not inspect rows or
/// advance cursors while the Rust expression is being constructed.
pub(crate) trait RowStream: Sized + 'static {
    type Item: 'static;

    fn for_each<BodyFn, Body>(self, body: BodyFn) -> BoxedCompile<()>
    where
        BodyFn: FnOnce(Self::Item) -> Body + 'static,
        Body: Compile<Output = ()> + 'static,
    {
        self.for_each_boxed(Box::new(move |item| body(item).boxed()))
    }

    /// Fold symbolic state through rows until the body returns a false
    /// continuation value.
    fn try_fold<Initial, BodyFn, Body>(
        self,
        initial: Initial,
        body: BodyFn,
    ) -> BoxedCompile<LoopState>
    where
        Initial: Compile<Output = LoopState> + 'static,
        BodyFn: FnOnce(Self::Item, LoopState) -> Body + 'static,
        Body: Compile<Output = LoopStep> + 'static,
    {
        self.try_fold_boxed(
            initial.boxed(),
            Box::new(move |item, state| body(item, state).boxed()),
        )
    }

    fn for_each_boxed(self, body: BoxedRowConsumer<Self::Item>) -> BoxedCompile<()>;

    fn try_fold_boxed(
        self,
        initial: BoxedCompile<LoopState>,
        body: BoxedRowFolder<Self::Item>,
    ) -> BoxedCompile<LoopState>;

    /// Return a symbolic SQL boolean indicating whether the stream yields a row.
    ///
    /// The terminal stops after the first row. Empty and non-empty paths join
    /// through the loop state, so the result remains an SSA value until lowering.
    fn has_rows(self) -> BoxedCompile<ValueId> {
        self.try_fold(
            constant(Value::from_i64(0)).map(LoopState::single),
            |_item, mut state| {
                constant(Value::from_i64(1))
                    .then(constant(Value::from_i64(0)))
                    .map(move |(is_non_empty, should_continue)| {
                        state.replace_single(is_non_empty);
                        LoopStep {
                            state,
                            should_continue,
                        }
                    })
            },
        )
        .map(LoopState::into_single)
        .boxed()
    }

    /// Return the first symbolic scalar yielded by the stream, or `initial`
    /// when the stream is empty.
    fn first_or<Initial>(self, initial: Initial) -> BoxedCompile<ValueId>
    where
        Self: RowStream<Item = ValueId>,
        Initial: Compile<Output = ValueId> + 'static,
    {
        self.try_fold(initial.map(LoopState::single), |item, mut state| {
            state.replace_single(item);
            constant(Value::from_i64(0)).map(move |should_continue| LoopStep {
                state,
                should_continue,
            })
        })
        .map(LoopState::into_single)
        .boxed()
    }

    fn map<MapperFn, Mapper>(self, mapper: MapperFn) -> MapRows<Self, MapperFn, Mapper>
    where
        MapperFn: FnOnce(Self::Item) -> Mapper + 'static,
        Mapper: Compile + 'static,
        Mapper::Output: 'static,
    {
        MapRows {
            source: self,
            mapper,
            compiler: PhantomData,
        }
    }

    /// Compile each item into another stream and yield that stream's items.
    ///
    /// The mapper itself is deferred, so it may use symbolic values produced
    /// by the outer stream when constructing the inner stream.
    fn flat_map<MapperFn, Mapper, Stream>(
        self,
        mapper: MapperFn,
    ) -> FlatMapRows<Self, MapperFn, Mapper, Stream>
    where
        MapperFn: FnOnce(Self::Item) -> Mapper + 'static,
        Mapper: Compile<Output = Stream> + 'static,
        Stream: RowStream + 'static,
    {
        FlatMapRows {
            source: self,
            mapper,
            compiler: PhantomData,
        }
    }

    fn filter<PredicateFn, Predicate>(
        self,
        predicate: PredicateFn,
    ) -> FilterRows<Self, PredicateFn, Predicate>
    where
        Self::Item: Clone,
        PredicateFn: FnOnce(Self::Item) -> Predicate + 'static,
        Predicate: Compile<Output = ValueId> + 'static,
    {
        FilterRows {
            source: self,
            predicate,
            compiler: PhantomData,
        }
    }

    fn take<Count>(self, count: Count) -> TakeRows<Self, Count>
    where
        Count: Compile<Output = ValueId> + 'static,
    {
        TakeRows {
            source: self,
            count,
        }
    }

    fn skip<Count>(self, count: Count) -> SkipRows<Self, Count>
    where
        Count: Compile<Output = ValueId> + 'static,
    {
        SkipRows {
            source: self,
            count,
        }
    }

    /// Buffer value packs, then yield them in the declared key order.
    fn sort(self, keys: SmallVec<[SortKey; 4]>, record_width: usize) -> SortRows<Self>
    where
        Self: RowStream<Item = ValuePack>,
    {
        SortRows {
            source: self,
            keys,
            record_width,
        }
    }

    /// Yield only the first value pack for each collation-aware key.
    fn distinct(self, collations: SmallVec<[CollationSeq; 4]>) -> DistinctRows<Self>
    where
        Self: RowStream<Item = ValuePack>,
    {
        self.distinct_by(collations, pure)
    }

    /// Yield the first item for each collation-aware derived key.
    fn distinct_by<KeyFn, Key>(
        self,
        collations: SmallVec<[CollationSeq; 4]>,
        key: KeyFn,
    ) -> DistinctRows<Self>
    where
        Self::Item: Clone,
        KeyFn: FnOnce(Self::Item) -> Key + 'static,
        Key: Compile<Output = ValuePack> + 'static,
    {
        DistinctRows {
            source: self,
            collations,
            key: Box::new(move |item| key(item).boxed()),
        }
    }
}

impl RowStream for CursorRows {
    type Item = Row;

    fn for_each_boxed(self, body: BoxedRowConsumer<Self::Item>) -> BoxedCompile<()> {
        ForEachRow {
            cursor: self.cursor,
            row_cursor: self.row_cursor,
            deferred_seek: self.deferred_seek,
            source: self.source,
            body,
            compiler: PhantomData,
        }
        .boxed()
    }

    fn try_fold_boxed(
        self,
        initial: BoxedCompile<LoopState>,
        body: BoxedRowFolder<Self::Item>,
    ) -> BoxedCompile<LoopState> {
        TryFoldRows {
            initial,
            cursor: self.cursor,
            row_cursor: self.row_cursor,
            deferred_seek: self.deferred_seek,
            source: self.source,
            body,
            compiler: PhantomData,
        }
        .boxed()
    }
}

/// A buffering row-stream boundary backed by a symbolic sorter resource.
pub(crate) struct SortRows<Source> {
    source: Source,
    keys: SmallVec<[SortKey; 4]>,
    record_width: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct SortedRow {
    sorter: SorterId,
    record_width: usize,
}

impl SortedRow {
    pub(crate) const fn column(self, column: usize) -> SorterColumn {
        SorterColumn {
            sorter: self.sorter,
            column,
            record_width: self.record_width,
        }
    }
}

pub(crate) struct SorterColumn {
    sorter: SorterId,
    column: usize,
    record_width: usize,
}

impl Compile for SorterColumn {
    type Output = ValueId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        if self.column >= self.record_width {
            return Err(LimboError::InternalError(format!(
                "compiler IR reads sorter column {} from width {}",
                self.column, self.record_width
            )));
        }
        builder.push(ScalarOp::SorterColumn {
            sorter: self.sorter,
            column: self.column,
        })
    }
}

struct InsertSorter {
    sorter: SorterId,
    pack: ValuePack,
}

impl Compile for InsertSorter {
    type Output = ();

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        builder.push_effect(EffectOp::SorterInsert {
            sorter: self.sorter,
            pack: self.pack,
        })
    }
}

struct ForEachSorted<Source> {
    source: Source,
    keys: SmallVec<[SortKey; 4]>,
    record_width: usize,
    body: BoxedRowConsumer<SortedRow>,
}

impl<Source> Compile for ForEachSorted<Source>
where
    Source: RowStream<Item = ValuePack>,
{
    type Output = ();

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let sorter = builder.allocate_sorter(self.keys, self.record_width)?;
        builder.push_effect(EffectOp::OpenSorter { sorter })?;
        self.source
            .for_each_boxed(Box::new(move |pack| InsertSorter { sorter, pack }.boxed()))
            .compile(builder)?;
        ForEachReadySorter {
            rows: ReadySorterRows {
                sorter,
                record_width: self.record_width,
            },
            body: self.body,
        }
        .compile(builder)
    }
}

struct TryFoldSorted<Source> {
    source: Source,
    keys: SmallVec<[SortKey; 4]>,
    record_width: usize,
    initial: BoxedCompile<LoopState>,
    body: BoxedRowFolder<SortedRow>,
}

impl<Source> Compile for TryFoldSorted<Source>
where
    Source: RowStream<Item = ValuePack>,
{
    type Output = LoopState;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let sorter = builder.allocate_sorter(self.keys, self.record_width)?;
        builder.push_effect(EffectOp::OpenSorter { sorter })?;
        self.source
            .for_each_boxed(Box::new(move |pack| InsertSorter { sorter, pack }.boxed()))
            .compile(builder)?;
        TryFoldReadySorter {
            rows: ReadySorterRows {
                sorter,
                record_width: self.record_width,
            },
            initial: self.initial,
            body: self.body,
        }
        .compile(builder)
    }
}

impl<Source> RowStream for SortRows<Source>
where
    Source: RowStream<Item = ValuePack>,
{
    type Item = SortedRow;

    fn for_each_boxed(self, body: BoxedRowConsumer<Self::Item>) -> BoxedCompile<()> {
        ForEachSorted {
            source: self.source,
            keys: self.keys,
            record_width: self.record_width,
            body,
        }
        .boxed()
    }

    fn try_fold_boxed(
        self,
        initial: BoxedCompile<LoopState>,
        body: BoxedRowFolder<Self::Item>,
    ) -> BoxedCompile<LoopState> {
        TryFoldSorted {
            source: self.source,
            keys: self.keys,
            record_width: self.record_width,
            initial,
            body,
        }
        .boxed()
    }
}

/// A sorter that has already been populated and is ready to yield rows.
pub(crate) struct ReadySorterRows {
    sorter: SorterId,
    record_width: usize,
}

struct ForEachReadySorter {
    rows: ReadySorterRows,
    body: BoxedRowConsumer<SortedRow>,
}

impl Compile for ForEachReadySorter {
    type Output = ();

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let row = builder.create_block()?;
        let exit = builder.create_block()?;
        builder.terminate(Terminator::SorterSort {
            sorter: self.rows.sorter,
            if_non_empty: row,
            if_empty: exit,
            arguments: SmallVec::new(),
        })?;

        builder.switch_to(row)?;
        builder.push_effect(EffectOp::SorterData {
            sorter: self.rows.sorter,
        })?;
        (self.body)(SortedRow {
            sorter: self.rows.sorter,
            record_width: self.rows.record_width,
        })
        .compile(builder)?;
        builder.terminate(Terminator::SorterNext {
            sorter: self.rows.sorter,
            if_next: row,
            if_done: exit,
            arguments: SmallVec::new(),
        })?;

        builder.switch_to(exit)
    }
}

struct TryFoldReadySorter {
    rows: ReadySorterRows,
    initial: BoxedCompile<LoopState>,
    body: BoxedRowFolder<SortedRow>,
}

impl Compile for TryFoldReadySorter {
    type Output = LoopState;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let initial = self.initial.compile(builder)?;
        let row = builder.create_block()?;
        let advance = builder.create_block()?;
        let stop = builder.create_block()?;
        let exit = builder.create_block()?;
        let mut row_state = SmallVec::with_capacity(initial.len());
        let mut result_state = SmallVec::with_capacity(initial.len());
        for _ in 0..initial.len() {
            row_state.push(builder.add_block_parameter(row)?);
            result_state.push(builder.add_block_parameter(exit)?);
        }
        builder.terminate(Terminator::SorterSort {
            sorter: self.rows.sorter,
            if_non_empty: row,
            if_empty: exit,
            arguments: initial.values,
        })?;

        builder.switch_to(row)?;
        builder.push_effect(EffectOp::SorterData {
            sorter: self.rows.sorter,
        })?;
        let step = (self.body)(
            SortedRow {
                sorter: self.rows.sorter,
                record_width: self.rows.record_width,
            },
            LoopState { values: row_state },
        )
        .compile(builder)?;
        if step.state.len() != result_state.len() {
            return Err(LimboError::InternalError(format!(
                "ready sorter loop body changed state arity from {} to {}",
                result_state.len(),
                step.state.len()
            )));
        }
        builder.terminate(Terminator::Branch {
            condition: step.should_continue,
            if_true: advance,
            if_false: stop,
        })?;

        builder.switch_to(advance)?;
        builder.terminate(Terminator::SorterNext {
            sorter: self.rows.sorter,
            if_next: row,
            if_done: exit,
            arguments: step.state.values.clone(),
        })?;

        builder.switch_to(stop)?;
        builder.terminate(Terminator::Jump {
            target: exit,
            arguments: step.state.values,
        })?;

        builder.switch_to(exit)?;
        Ok(LoopState {
            values: result_state,
        })
    }
}

impl RowStream for ReadySorterRows {
    type Item = SortedRow;

    fn for_each_boxed(self, body: BoxedRowConsumer<Self::Item>) -> BoxedCompile<()> {
        ForEachReadySorter { rows: self, body }.boxed()
    }

    fn try_fold_boxed(
        self,
        initial: BoxedCompile<LoopState>,
        body: BoxedRowFolder<Self::Item>,
    ) -> BoxedCompile<LoopState> {
        TryFoldReadySorter {
            rows: self,
            initial,
            body,
        }
        .boxed()
    }
}

pub(crate) struct DeferredLiteralValues {
    values: SmallVec<[BoxedCompile<ValueId>; 4]>,
    affinity: Affinity,
    collation: Option<CollationSeq>,
}

pub(crate) fn literal_values(
    values: SmallVec<[BoxedCompile<ValueId>; 4]>,
    affinity: Affinity,
    collation: Option<CollationSeq>,
) -> DeferredInValues {
    DeferredInValues::Literal(DeferredLiteralValues {
        values,
        affinity,
        collation,
    })
}

pub(crate) fn cursor_values(
    input: CursorInputId,
    collation: Option<CollationSeq>,
) -> DeferredInValues {
    DeferredInValues::Cursor { input, collation }
}

pub(crate) enum DeferredInValues {
    Literal(DeferredLiteralValues),
    Cursor {
        input: CursorInputId,
        collation: Option<CollationSeq>,
    },
}

impl DeferredInValues {
    const fn collation(&self) -> Option<CollationSeq> {
        match self {
            Self::Literal(values) => values.collation,
            Self::Cursor { collation, .. } => *collation,
        }
    }
}

pub(crate) enum InValueRows {
    Literal(ReadySorterRows),
    Cursor(CursorRows),
}

impl Compile for DeferredInValues {
    type Output = InValueRows;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        match self {
            Self::Literal(values) => {
                let sorter = builder.allocate_sorter_with_affinities(
                    smallvec![SortKey::new(SortOrder::Asc, values.collation, None, None,)],
                    1,
                    Some(smallvec![values.affinity]),
                )?;
                builder.push_effect(EffectOp::OpenSorter { sorter })?;
                for value in values.values {
                    let value = value.compile(builder)?;
                    builder.push_effect(EffectOp::SorterInsert {
                        sorter,
                        pack: ValuePack(smallvec![value]),
                    })?;
                }
                Ok(InValueRows::Literal(ReadySorterRows {
                    sorter,
                    record_width: 1,
                }))
            }
            Self::Cursor { input, .. } => {
                let cursor = builder.external_cursor(input)?;
                Ok(InValueRows::Cursor(CursorRows {
                    cursor,
                    row_cursor: cursor,
                    deferred_seek: None,
                    source: CursorRowSource::Scan(ScanDirection::Forward),
                }))
            }
        }
    }
}

impl RowStream for InValueRows {
    type Item = ValueId;

    fn for_each_boxed(self, body: BoxedRowConsumer<Self::Item>) -> BoxedCompile<()> {
        match self {
            Self::Literal(rows) => {
                rows.for_each_boxed(Box::new(move |row| row.column(0).and_then(body).boxed()))
            }
            Self::Cursor(rows) => {
                rows.for_each_boxed(Box::new(move |row| row.column(0).and_then(body).boxed()))
            }
        }
    }

    fn try_fold_boxed(
        self,
        initial: BoxedCompile<LoopState>,
        body: BoxedRowFolder<Self::Item>,
    ) -> BoxedCompile<LoopState> {
        match self {
            Self::Literal(rows) => rows.try_fold_boxed(
                initial,
                Box::new(move |row, state| {
                    row.column(0)
                        .and_then(move |value| body(value, state))
                        .boxed()
                }),
            ),
            Self::Cursor(rows) => rows.try_fold_boxed(
                initial,
                Box::new(move |row, state| {
                    row.column(0)
                        .and_then(move |value| body(value, state))
                        .boxed()
                }),
            ),
        }
    }
}

/// A streaming row stage that remembers derived keys and yields each first item.
pub(crate) struct DistinctRows<Source>
where
    Source: RowStream,
    Source::Item: Clone,
{
    source: Source,
    collations: SmallVec<[CollationSeq; 4]>,
    key: BoxedDistinctKey<Source::Item>,
}

struct CheckDistinct {
    set: DistinctSetId,
    pack: ValuePack,
}

impl Compile for CheckDistinct {
    type Output = ValueId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let unique = builder.create_block()?;
        let duplicate = builder.create_block()?;
        let merge = builder.create_block()?;
        let result = builder.add_block_parameter(merge)?;
        builder.terminate(Terminator::DistinctCheck {
            set: self.set,
            pack: self.pack,
            if_unique: unique,
            if_duplicate: duplicate,
        })?;

        builder.switch_to(unique)?;
        let is_unique = builder.push(ScalarOp::Constant(Value::from_i64(1)))?;
        builder.terminate(Terminator::Jump {
            target: merge,
            arguments: smallvec![is_unique],
        })?;

        builder.switch_to(duplicate)?;
        let is_duplicate = builder.push(ScalarOp::Constant(Value::from_i64(0)))?;
        builder.terminate(Terminator::Jump {
            target: merge,
            arguments: smallvec![is_duplicate],
        })?;

        builder.switch_to(merge)?;
        Ok(result)
    }
}

struct ForEachDistinct<Source>
where
    Source: RowStream,
    Source::Item: Clone,
{
    source: Source,
    collations: SmallVec<[CollationSeq; 4]>,
    key: BoxedDistinctKey<Source::Item>,
    body: BoxedRowConsumer<Source::Item>,
}

impl<Source> Compile for ForEachDistinct<Source>
where
    Source: RowStream,
    Source::Item: Clone,
{
    type Output = ();

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let set = builder.allocate_distinct_set(self.collations)?;
        builder.push_effect(EffectOp::OpenDistinctSet { set })?;
        let key = self.key;
        let body = self.body;
        self.source
            .for_each_boxed(Box::new(move |item| {
                key(item.clone())
                    .and_then(move |pack| CheckDistinct { set, pack })
                    .and_then(move |is_unique| when(is_unique, body(item)))
                    .boxed()
            }))
            .compile(builder)
    }
}

struct TryFoldDistinct<Source>
where
    Source: RowStream,
    Source::Item: Clone,
{
    source: Source,
    collations: SmallVec<[CollationSeq; 4]>,
    key: BoxedDistinctKey<Source::Item>,
    initial: BoxedCompile<LoopState>,
    body: BoxedRowFolder<Source::Item>,
}

impl<Source> Compile for TryFoldDistinct<Source>
where
    Source: RowStream,
    Source::Item: Clone,
{
    type Output = LoopState;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let initial = self.initial.compile(builder)?;
        let set = builder.allocate_distinct_set(self.collations)?;
        builder.push_effect(EffectOp::OpenDistinctSet { set })?;
        let key = self.key;
        let body = self.body;
        self.source
            .try_fold_boxed(
                pure(initial).boxed(),
                Box::new(move |item, state| {
                    key(item.clone())
                        .and_then(move |pack| CheckDistinct { set, pack })
                        .and_then(move |is_unique| {
                            pure(is_unique).branch(body(item, state.clone()), continue_loop(state))
                        })
                        .boxed()
                }),
            )
            .compile(builder)
    }
}

impl<Source> RowStream for DistinctRows<Source>
where
    Source: RowStream,
    Source::Item: Clone,
{
    type Item = Source::Item;

    fn for_each_boxed(self, body: BoxedRowConsumer<Self::Item>) -> BoxedCompile<()> {
        ForEachDistinct {
            source: self.source,
            collations: self.collations,
            key: self.key,
            body,
        }
        .boxed()
    }

    fn try_fold_boxed(
        self,
        initial: BoxedCompile<LoopState>,
        body: BoxedRowFolder<Self::Item>,
    ) -> BoxedCompile<LoopState> {
        TryFoldDistinct {
            source: self.source,
            collations: self.collations,
            key: self.key,
            initial,
            body,
        }
        .boxed()
    }
}

/// A row stream whose items are produced by a deferred compiler.
pub(crate) struct MapRows<Source, MapperFn, Mapper> {
    source: Source,
    mapper: MapperFn,
    compiler: PhantomData<fn() -> Mapper>,
}

/// A nested stream stage analogous to `Iterator::flat_map`.
pub(crate) struct FlatMapRows<Source, MapperFn, Mapper, Stream> {
    source: Source,
    mapper: MapperFn,
    compiler: PhantomData<fn() -> (Mapper, Stream)>,
}

impl<Source, MapperFn, Mapper, Stream> RowStream for FlatMapRows<Source, MapperFn, Mapper, Stream>
where
    Source: RowStream,
    MapperFn: FnOnce(Source::Item) -> Mapper + 'static,
    Mapper: Compile<Output = Stream> + 'static,
    Stream: RowStream + 'static,
{
    type Item = Stream::Item;

    fn for_each_boxed(self, body: BoxedRowConsumer<Self::Item>) -> BoxedCompile<()> {
        let Self { source, mapper, .. } = self;
        source.for_each_boxed(Box::new(move |item| {
            mapper(item)
                .and_then(move |stream| stream.for_each_boxed(body))
                .boxed()
        }))
    }

    fn try_fold_boxed(
        self,
        initial: BoxedCompile<LoopState>,
        body: BoxedRowFolder<Self::Item>,
    ) -> BoxedCompile<LoopState> {
        let Self { source, mapper, .. } = self;
        source.try_fold_boxed(
            initial,
            Box::new(move |outer_item, state| {
                mapper(outer_item)
                    .and_then(move |stream| {
                        constant(Value::from_i64(1))
                            .map(move |continue_outer| {
                                let mut state = state;
                                state.values.push(continue_outer);
                                state
                            })
                            .and_then(move |inner_initial| {
                                stream.try_fold_boxed(
                                    pure(inner_initial).boxed(),
                                    Box::new(move |inner_item, mut inner_state| {
                                        inner_state.values.pop().expect(
                                            "flat-map inner state must carry its continuation",
                                        );
                                        body(inner_item, inner_state)
                                            .map(|mut step| {
                                                step.state.values.push(step.should_continue);
                                                step
                                            })
                                            .boxed()
                                    }),
                                )
                            })
                            .map(|mut inner_result| {
                                let should_continue = inner_result
                                    .values
                                    .pop()
                                    .expect("flat-map result must carry its continuation");
                                LoopStep {
                                    state: inner_result,
                                    should_continue,
                                }
                            })
                    })
                    .boxed()
            }),
        )
    }
}

impl<Source, MapperFn, Mapper> RowStream for MapRows<Source, MapperFn, Mapper>
where
    Source: RowStream,
    MapperFn: FnOnce(Source::Item) -> Mapper + 'static,
    Mapper: Compile + 'static,
    Mapper::Output: 'static,
{
    type Item = Mapper::Output;

    fn for_each_boxed(self, body: BoxedRowConsumer<Self::Item>) -> BoxedCompile<()> {
        let Self { source, mapper, .. } = self;
        source.for_each_boxed(Box::new(move |item| mapper(item).and_then(body).boxed()))
    }

    fn try_fold_boxed(
        self,
        initial: BoxedCompile<LoopState>,
        body: BoxedRowFolder<Self::Item>,
    ) -> BoxedCompile<LoopState> {
        let Self { source, mapper, .. } = self;
        source.try_fold_boxed(
            initial,
            Box::new(move |item, state| {
                mapper(item)
                    .and_then(move |mapped| body(mapped, state))
                    .boxed()
            }),
        )
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
    Source::Item: Clone,
    PredicateFn: FnOnce(Source::Item) -> Predicate + 'static,
    Predicate: Compile<Output = ValueId> + 'static,
{
    type Item = Source::Item;

    fn for_each_boxed(self, body: BoxedRowConsumer<Self::Item>) -> BoxedCompile<()> {
        let Self {
            source, predicate, ..
        } = self;
        source.for_each_boxed(Box::new(move |item| {
            predicate(item.clone())
                .and_then(move |condition| when(condition, body(item)))
                .boxed()
        }))
    }

    fn try_fold_boxed(
        self,
        initial: BoxedCompile<LoopState>,
        body: BoxedRowFolder<Self::Item>,
    ) -> BoxedCompile<LoopState> {
        let Self {
            source, predicate, ..
        } = self;
        source.try_fold_boxed(
            initial,
            Box::new(move |item, state| {
                predicate(item.clone())
                    .and_then(move |condition| {
                        pure(condition).branch(body(item, state.clone()), continue_loop(state))
                    })
                    .boxed()
            }),
        )
    }
}

/// A row stream that discards a deferred number of upstream items.
pub(crate) struct SkipRows<Source, Count> {
    source: Source,
    count: Count,
}

impl<Source, Count> RowStream for SkipRows<Source, Count>
where
    Source: RowStream,
    Count: Compile<Output = ValueId> + 'static,
{
    type Item = Source::Item;

    fn for_each_boxed(self, body: BoxedRowConsumer<Self::Item>) -> BoxedCompile<()> {
        self.try_fold(pure(LoopState::empty()), move |item, state| {
            body(item).and_then(move |()| continue_loop(state))
        })
        .map(|_| ())
        .boxed()
    }

    fn try_fold_boxed(
        self,
        initial: BoxedCompile<LoopState>,
        body: BoxedRowFolder<Self::Item>,
    ) -> BoxedCompile<LoopState> {
        let Self { source, count } = self;
        initial
            .then(count)
            .and_then(move |(state, count)| {
                must_be_int(count).and_then(move |count| {
                    let mut active_state = state;
                    active_state.push(count);
                    source
                        .try_fold_boxed(
                            pure(active_state).boxed(),
                            Box::new(move |item, mut state| {
                                let remaining = state
                                    .pop()
                                    .expect("skip loop state must include its remaining-row count");
                                constant(Value::from_i64(0))
                                    .and_then(move |zero| {
                                        compare(
                                            remaining,
                                            zero,
                                            resolved_comparison(
                                                ComparisonOp::Greater,
                                                Affinity::Numeric,
                                                None,
                                            ),
                                        )
                                        .and_then(
                                            move |should_skip| {
                                                let mut skipped_state = state.clone();
                                                pure(should_skip).branch(
                                                    constant(Value::from_i64(-1)).and_then(
                                                        move |minus_one| {
                                                            add(remaining, minus_one).and_then(
                                                                move |next_remaining| {
                                                                    skipped_state
                                                                        .push(next_remaining);
                                                                    continue_loop(skipped_state)
                                                                },
                                                            )
                                                        },
                                                    ),
                                                    body(item, state).map(move |mut step| {
                                                        step.state.push(remaining);
                                                        step
                                                    }),
                                                )
                                            },
                                        )
                                    })
                                    .boxed()
                            }),
                        )
                        .map(|mut state| {
                            state
                                .pop()
                                .expect("skip result state must include its remaining-row count");
                            state
                        })
                })
            })
            .boxed()
    }
}

/// A row stream that stops after a deferred number of downstream items.
pub(crate) struct TakeRows<Source, Count> {
    source: Source,
    count: Count,
}

impl<Source, Count> RowStream for TakeRows<Source, Count>
where
    Source: RowStream,
    Count: Compile<Output = ValueId> + 'static,
{
    type Item = Source::Item;

    fn for_each_boxed(self, body: BoxedRowConsumer<Self::Item>) -> BoxedCompile<()> {
        self.try_fold(pure(LoopState::empty()), move |item, state| {
            body(item).and_then(move |()| continue_loop(state))
        })
        .map(|_| ())
        .boxed()
    }

    fn try_fold_boxed(
        self,
        initial: BoxedCompile<LoopState>,
        body: BoxedRowFolder<Self::Item>,
    ) -> BoxedCompile<LoopState> {
        let Self { source, count } = self;
        initial
            .then(count)
            .and_then(move |(state, count)| {
                must_be_int(count).and_then(move |count| {
                    let mut active_state = state.clone();
                    active_state.push(count);
                    pure(count).branch(
                        source
                            .try_fold_boxed(
                                pure(active_state).boxed(),
                                Box::new(move |item, mut state| {
                                    let remaining = state.pop().expect(
                                        "take loop state must include its remaining-row count",
                                    );
                                    body(item, state)
                                        .and_then(move |step| {
                                            constant(Value::from_i64(-1)).and_then(
                                                move |minus_one| {
                                                    add(remaining, minus_one).and_then(
                                                        move |next_remaining| {
                                                            constant(Value::from_i64(0)).and_then(
                                                                move |zero| {
                                                                    compare(
                                                                        next_remaining,
                                                                        zero,
                                                                        resolved_comparison(
                                                                            ComparisonOp::NotEqual,
                                                                            Affinity::Numeric,
                                                                            None,
                                                                        ),
                                                                    )
                                                                    .and_then(
                                                                        move |limit_continue| {
                                                                            logical(
                                                                                LogicalOp::And,
                                                                                step.should_continue,
                                                                                limit_continue,
                                                                            )
                                                                            .map(
                                                                                move |should_continue| {
                                                                                    let mut state =
                                                                                        step.state;
                                                                                    state.push(
                                                                                        next_remaining,
                                                                                    );
                                                                                    LoopStep {
                                                                                        state,
                                                                                        should_continue,
                                                                                    }
                                                                                },
                                                                            )
                                                                        },
                                                                    )
                                                                },
                                                            )
                                                        },
                                                    )
                                                },
                                            )
                                        })
                                        .boxed()
                                }),
                            )
                            .map(|mut state| {
                                state.pop().expect(
                                    "take result state must include its remaining-row count",
                                );
                                state
                            }),
                        pure(state),
                    )
                })
            })
            .boxed()
    }
}

fn continue_loop(state: LoopState) -> impl Compile<Output = LoopStep> {
    constant(Value::from_i64(1)).map(move |should_continue| LoopStep {
        state,
        should_continue,
    })
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

    pub(crate) const fn rowid(self) -> RowId {
        RowId {
            cursor: self.cursor,
        }
    }

    pub(crate) const fn index_rowid(self) -> IndexRowId {
        IndexRowId {
            cursor: self.cursor,
        }
    }
}

enum ScanBtreeSource {
    Table(OpenReadCursor),
    CoveringIndex(OpenReadCursor),
    IndexLookup {
        table: OpenReadCursor,
        index: OpenReadCursor,
    },
}

struct OpenedBtree {
    cursor: CursorId,
    row_cursor: CursorId,
    deferred_seek: Option<DeferredSeekCursors>,
}

impl ScanBtreeSource {
    fn open(self, builder: &mut IrBuilder) -> Result<OpenedBtree> {
        let (cursor, row_cursor, deferred_seek) = match self {
            Self::Table(table) => {
                let cursor = table.compile(builder)?;
                (cursor, cursor, None)
            }
            Self::CoveringIndex(index) => {
                let cursor = index.compile(builder)?;
                (cursor, cursor, None)
            }
            Self::IndexLookup { table, index } => {
                let table = table.compile(builder)?;
                let index = index.compile(builder)?;
                (index, table, Some(DeferredSeekCursors { index, table }))
            }
        };
        Ok(OpenedBtree {
            cursor,
            row_cursor,
            deferred_seek,
        })
    }
}

/// Opens a B-tree source when compiled and returns its symbolic row stream.
pub(crate) struct ScanBtree {
    source: ScanBtreeSource,
    start: ScanBtreeStart,
}

pub(crate) struct InSeekBtree {
    source: ScanBtreeSource,
    values: DeferredInValues,
    target: InSeekTarget,
    collation: CollationSeq,
}

#[derive(Clone, Copy)]
enum InSeekTarget {
    Rowid,
    Index,
}

pub(crate) struct InSeekRows {
    values: InValueRows,
    target: CursorRows,
    kind: InSeekTarget,
    collation: CollationSeq,
}

impl InSeekRows {
    fn target_for_key(mut target: CursorRows, kind: InSeekTarget, key: ValueId) -> CursorRows {
        target.source = match kind {
            InSeekTarget::Rowid => CursorRowSource::Rowid(key),
            InSeekTarget::Index => {
                let key = IndexKey {
                    pack: ValuePack(smallvec![key]),
                    // RHS values were coerced while the sorter record was
                    // built, matching the eager IN-list ephemeral index.
                    affinities: smallvec![Affinity::Blob],
                    null_policies: smallvec![IndexNullPolicy::AbortRange],
                };
                CursorRowSource::IndexRange(IndexRangeSource {
                    start: Some(IndexBoundSpec {
                        key: key.clone(),
                        op: SeekOp::GE { eq_only: false },
                    }),
                    end: Some(IndexBoundSpec {
                        key,
                        op: SeekOp::GT,
                    }),
                    direction: ScanDirection::Forward,
                })
            }
        };
        target
    }

    fn stream(self) -> impl RowStream<Item = Row> {
        let target = self.target;
        let kind = self.kind;
        self.values
            .distinct_by(smallvec![self.collation], |value| {
                pack_values(smallvec![pure(value).boxed()])
            })
            .flat_map(move |key| pure(Self::target_for_key(target, kind, key)))
    }
}

impl RowStream for InSeekRows {
    type Item = Row;

    fn for_each_boxed(self, body: BoxedRowConsumer<Self::Item>) -> BoxedCompile<()> {
        self.stream().for_each_boxed(body)
    }

    fn try_fold_boxed(
        self,
        initial: BoxedCompile<LoopState>,
        body: BoxedRowFolder<Self::Item>,
    ) -> BoxedCompile<LoopState> {
        self.stream().try_fold_boxed(initial, body)
    }
}

enum ScanBtreeStart {
    Full(ScanDirection),
    Rowid(BoxedCompile<ValueId>),
    TableRange(DeferredTableRange),
    IndexRange(DeferredIndexRange),
}

pub(crate) struct DeferredTableBound {
    rowid: Option<BoxedCompile<ValueId>>,
    op: SeekOp,
}

impl DeferredTableBound {
    pub(crate) const fn unbounded(op: SeekOp) -> Self {
        Self { rowid: None, op }
    }

    pub(crate) const fn expression(rowid: BoxedCompile<ValueId>, op: SeekOp) -> Self {
        Self {
            rowid: Some(rowid),
            op,
        }
    }

    fn compile(self, builder: &mut IrBuilder) -> Result<Option<TableBoundSpec>> {
        let Some(rowid) = self.rowid else {
            return Ok(None);
        };
        Ok(Some(TableBoundSpec {
            rowid: rowid.compile(builder)?,
            op: self.op,
        }))
    }
}

pub(crate) struct DeferredTableRange {
    start: DeferredTableBound,
    end: DeferredTableBound,
    direction: ScanDirection,
    affinity: Affinity,
}

impl DeferredTableRange {
    pub(crate) const fn new(
        start: DeferredTableBound,
        end: DeferredTableBound,
        direction: ScanDirection,
        affinity: Affinity,
    ) -> Self {
        Self {
            start,
            end,
            direction,
            affinity,
        }
    }

    fn compile(self, builder: &mut IrBuilder) -> Result<TableRangeSource> {
        Ok(TableRangeSource {
            start: self.start.compile(builder)?,
            end: self.end.compile(builder)?,
            direction: self.direction,
            affinity: self.affinity,
        })
    }
}

pub(crate) struct DeferredIndexBound {
    suffix: DeferredIndexSuffix,
    op: SeekOp,
}

impl DeferredIndexBound {
    pub(crate) const fn prefix(op: SeekOp) -> Self {
        Self {
            suffix: DeferredIndexSuffix::None,
            op,
        }
    }

    pub(crate) const fn null(op: SeekOp) -> Self {
        Self {
            suffix: DeferredIndexSuffix::Null,
            op,
        }
    }

    pub(crate) const fn expression(
        value: BoxedCompile<ValueId>,
        affinity: Affinity,
        op: SeekOp,
    ) -> Self {
        Self {
            suffix: DeferredIndexSuffix::Expression { value, affinity },
            op,
        }
    }

    fn compile(
        self,
        builder: &mut IrBuilder,
        prefix_values: &[ValueId],
        prefix_affinities: &[Affinity],
    ) -> Result<Option<IndexBoundSpec>> {
        let mut values = SmallVec::from_slice(prefix_values);
        let mut affinities = SmallVec::from_slice(prefix_affinities);
        let mut null_policies = smallvec![IndexNullPolicy::AbortRange; values.len()];
        match self.suffix {
            DeferredIndexSuffix::None => {}
            DeferredIndexSuffix::Null => {
                values.push(builder.push(ScalarOp::Constant(Value::Null))?);
                affinities.push(Affinity::Blob);
                null_policies.push(IndexNullPolicy::Compare);
            }
            DeferredIndexSuffix::Expression { value, affinity } => {
                values.push(value.compile(builder)?);
                affinities.push(affinity);
                null_policies.push(IndexNullPolicy::AbortRange);
            }
        }
        if values.is_empty() {
            return Ok(None);
        }
        Ok(Some(IndexBoundSpec {
            key: IndexKey::new(values, affinities, null_policies)?,
            op: self.op,
        }))
    }
}

/// Optional component after the equality prefix of one range endpoint.
enum DeferredIndexSuffix {
    None,
    Null,
    Expression {
        value: BoxedCompile<ValueId>,
        affinity: Affinity,
    },
}

pub(crate) struct DeferredIndexRange {
    prefix_values: SmallVec<[BoxedCompile<ValueId>; 4]>,
    prefix_affinities: SmallVec<[Affinity; 4]>,
    start: DeferredIndexBound,
    end: DeferredIndexBound,
    direction: ScanDirection,
}

impl DeferredIndexRange {
    pub(crate) const fn new(
        prefix_values: SmallVec<[BoxedCompile<ValueId>; 4]>,
        prefix_affinities: SmallVec<[Affinity; 4]>,
        start: DeferredIndexBound,
        end: DeferredIndexBound,
        direction: ScanDirection,
    ) -> Self {
        Self {
            prefix_values,
            prefix_affinities,
            start,
            end,
            direction,
        }
    }

    fn compile(self, builder: &mut IrBuilder) -> Result<IndexRangeSource> {
        if self.prefix_values.len() != self.prefix_affinities.len() {
            return Err(LimboError::InternalError(format!(
                "compiler index range has {} prefix values and {} affinities",
                self.prefix_values.len(),
                self.prefix_affinities.len()
            )));
        }
        let mut prefix_values = SmallVec::<[ValueId; 4]>::with_capacity(self.prefix_values.len());
        for value in self.prefix_values {
            prefix_values.push(value.compile(builder)?);
        }
        let start = self
            .start
            .compile(builder, &prefix_values, &self.prefix_affinities)?;
        let end = self
            .end
            .compile(builder, &prefix_values, &self.prefix_affinities)?;
        Ok(IndexRangeSource {
            start,
            end,
            direction: self.direction,
        })
    }
}

pub(crate) fn scan_table(
    table: Arc<BTreeTable>,
    db: usize,
    schema_cookie: u32,
    direction: ScanDirection,
) -> ScanBtree {
    ScanBtree {
        source: ScanBtreeSource::Table(open_read_table(table, db, schema_cookie)),
        start: ScanBtreeStart::Full(direction),
    }
}

pub(crate) fn seek_rowid(
    table: Arc<BTreeTable>,
    db: usize,
    schema_cookie: u32,
    rowid: BoxedCompile<ValueId>,
) -> ScanBtree {
    ScanBtree {
        source: ScanBtreeSource::Table(open_read_table(table, db, schema_cookie)),
        start: ScanBtreeStart::Rowid(rowid),
    }
}

pub(crate) fn seek_table_range(
    table: Arc<BTreeTable>,
    db: usize,
    schema_cookie: u32,
    range: DeferredTableRange,
) -> ScanBtree {
    ScanBtree {
        source: ScanBtreeSource::Table(open_read_table(table, db, schema_cookie)),
        start: ScanBtreeStart::TableRange(range),
    }
}

pub(crate) fn seek_in_values(
    table: Arc<BTreeTable>,
    index: Option<Arc<Index>>,
    covering: bool,
    db: usize,
    schema_cookie: u32,
    values: DeferredInValues,
) -> InSeekBtree {
    let collation = values.collation();
    let (source, target) = match index {
        Some(index) => (
            index_source(table, index, covering, db, schema_cookie),
            InSeekTarget::Index,
        ),
        None => (
            ScanBtreeSource::Table(open_read_table(table, db, schema_cookie)),
            InSeekTarget::Rowid,
        ),
    };
    InSeekBtree {
        source,
        values,
        target,
        collation: collation.unwrap_or(CollationSeq::Binary),
    }
}

pub(crate) fn scan_index(
    table: Arc<BTreeTable>,
    index: Arc<Index>,
    covering: bool,
    db: usize,
    schema_cookie: u32,
    direction: ScanDirection,
) -> ScanBtree {
    let source = index_source(table, index, covering, db, schema_cookie);
    ScanBtree {
        source,
        start: ScanBtreeStart::Full(direction),
    }
}

pub(crate) fn seek_index(
    table: Arc<BTreeTable>,
    index: Arc<Index>,
    covering: bool,
    db: usize,
    schema_cookie: u32,
    range: DeferredIndexRange,
) -> ScanBtree {
    ScanBtree {
        source: index_source(table, index, covering, db, schema_cookie),
        start: ScanBtreeStart::IndexRange(range),
    }
}

fn index_source(
    table: Arc<BTreeTable>,
    index: Arc<Index>,
    covering: bool,
    db: usize,
    schema_cookie: u32,
) -> ScanBtreeSource {
    let index = open_read_index(index, db, schema_cookie);
    if covering {
        ScanBtreeSource::CoveringIndex(index)
    } else {
        ScanBtreeSource::IndexLookup {
            table: open_read_table(table, db, schema_cookie),
            index,
        }
    }
}

impl Compile for ScanBtree {
    type Output = CursorRows;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let opened = self.source.open(builder)?;
        let source = match self.start {
            ScanBtreeStart::Full(direction) => CursorRowSource::Scan(direction),
            ScanBtreeStart::Rowid(rowid) => CursorRowSource::Rowid(rowid.compile(builder)?),
            ScanBtreeStart::TableRange(range) => {
                CursorRowSource::TableRange(range.compile(builder)?)
            }
            ScanBtreeStart::IndexRange(range) => {
                CursorRowSource::IndexRange(range.compile(builder)?)
            }
        };
        Ok(CursorRows {
            cursor: opened.cursor,
            row_cursor: opened.row_cursor,
            deferred_seek: opened.deferred_seek,
            source,
        })
    }
}

impl Compile for InSeekBtree {
    type Output = InSeekRows;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let opened = self.source.open(builder)?;
        let values = self.values.compile(builder)?;
        Ok(InSeekRows {
            values,
            target: CursorRows {
                cursor: opened.cursor,
                row_cursor: opened.row_cursor,
                deferred_seek: opened.deferred_seek,
                // Replaced with the current RHS key by `InSeekRows`.
                source: CursorRowSource::Scan(ScanDirection::Forward),
            },
            kind: self.target,
            collation: self.collation,
        })
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn open_read_table(
    table: Arc<BTreeTable>,
    db: usize,
    schema_cookie: u32,
) -> OpenReadCursor {
    let root_page = table.root_page;
    OpenReadCursor {
        resource: CursorType::BTreeTable(table),
        root_page,
        db,
        schema_cookie,
    }
}

pub(crate) fn open_ephemeral_index(index: Arc<Index>) -> OpenEphemeralIndex {
    OpenEphemeralIndex { index }
}

pub(crate) fn declare_ephemeral_index(index: Arc<Index>) -> DeclareEphemeralIndex {
    DeclareEphemeralIndex { index }
}

pub(crate) const fn open_declared_ephemeral_index(
    unopened: UnopenedEphemeralIndex,
) -> OpenDeclaredEphemeralIndex {
    OpenDeclaredEphemeralIndex { unopened }
}

fn open_read_index(index: Arc<Index>, db: usize, schema_cookie: u32) -> OpenReadCursor {
    let root_page = index.root_page;
    OpenReadCursor {
        resource: CursorType::BTreeIndex(index),
        root_page,
        db,
        schema_cookie,
    }
}

impl Compile for OpenReadCursor {
    type Output = CursorId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let cursor = builder.allocate_cursor(CursorResource::Owned(self.resource))?;
        builder.push_effect(EffectOp::OpenRead {
            cursor,
            root_page: self.root_page,
            db: self.db,
            schema_cookie: self.schema_cookie,
        })?;
        Ok(cursor)
    }
}

impl Compile for OpenEphemeralIndex {
    type Output = CursorId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let unopened = declare_ephemeral_index(self.index).compile(builder)?;
        open_declared_ephemeral_index(unopened).compile(builder)
    }
}

impl Compile for DeclareEphemeralIndex {
    type Output = UnopenedEphemeralIndex;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let cursor =
            builder.allocate_cursor(CursorResource::Owned(CursorType::BTreeIndex(self.index)))?;
        Ok(UnopenedEphemeralIndex { cursor })
    }
}

impl Compile for OpenDeclaredEphemeralIndex {
    type Output = CursorId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        let cursor = self.unopened.cursor;
        builder.push_effect(EffectOp::OpenEphemeralIndex { cursor })?;
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
        builder.external_input(self.0)
    }
}

pub(crate) struct ParameterValue(Variable);

pub(crate) const fn parameter(variable: Variable) -> ParameterValue {
    ParameterValue(variable)
}

impl Compile for ParameterValue {
    type Output = ValueId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        builder.push(ScalarOp::Parameter(self.0))
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

pub(crate) struct MustBeInt {
    value: ValueId,
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

pub(crate) struct RowId {
    cursor: CursorId,
}

pub(crate) struct IndexRowId {
    cursor: CursorId,
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct ResultRow {
    pack: ValuePack,
}

pub(crate) struct InsertIndex {
    cursor: CursorId,
    pack: ValuePack,
    index_name: String,
    affinity: Option<String>,
}

/// Compiles an ordered set of independently composed values into one pack.
pub(crate) struct PackValues {
    values: SmallVec<[BoxedCompile<ValueId>; 4]>,
}

/// Selects an ordered sub-pack without emitting a physical copy.
pub(crate) struct SelectPack {
    pack: ValuePack,
    start: usize,
    len: usize,
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

pub(crate) fn insert_index_pack(
    cursor: CursorId,
    pack: ValuePack,
    index_name: String,
    affinity: Option<String>,
) -> InsertIndex {
    InsertIndex {
        cursor,
        pack,
        index_name,
        affinity,
    }
}

pub(crate) fn pack_values(values: SmallVec<[BoxedCompile<ValueId>; 4]>) -> PackValues {
    PackValues { values }
}

pub(crate) const fn select_pack(pack: ValuePack, start: usize, len: usize) -> SelectPack {
    SelectPack { pack, start, len }
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

impl Compile for SelectPack {
    type Output = ValuePack;

    fn compile(self, _builder: &mut IrBuilder) -> Result<Self::Output> {
        if self.len == 0 {
            return Err(LimboError::InternalError(
                "compiler IR selected value pack must not be empty".to_owned(),
            ));
        }
        let end = self.start.checked_add(self.len).ok_or_else(|| {
            LimboError::InternalError("compiler IR value-pack selection overflow".to_owned())
        })?;
        let Some(values) = self.pack.values().get(self.start..end) else {
            return Err(LimboError::InternalError(format!(
                "compiler IR selects values {}..{end} from pack of width {}",
                self.start,
                self.pack.values().len()
            )));
        };
        Ok(ValuePack(values.iter().copied().collect()))
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

impl Compile for InsertIndex {
    type Output = ();

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        if self.pack.values().is_empty() {
            return Err(LimboError::InternalError(
                "compiler IR index insert must contain at least one value".to_owned(),
            ));
        }
        builder.push_effect(EffectOp::IndexInsert {
            cursor: self.cursor,
            pack: self.pack,
            index_name: self.index_name,
            affinity: self.affinity,
        })
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

impl Compile for RowId {
    type Output = ValueId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        builder.push(ScalarOp::RowId {
            cursor: self.cursor,
        })
    }
}

impl Compile for IndexRowId {
    type Output = ValueId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        builder.push(ScalarOp::IndexRowId {
            cursor: self.cursor,
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

pub(crate) const fn must_be_int(value: ValueId) -> MustBeInt {
    MustBeInt { value }
}

impl Compile for MustBeInt {
    type Output = ValueId;

    fn compile(self, builder: &mut IrBuilder) -> Result<Self::Output> {
        builder.push(ScalarOp::MustBeInt { value: self.value })
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
    use crate::{
        io::MemoryIO,
        schema::{BTreeTable, IndexColumn},
        sync::Arc,
        Database, SqliteDialect, Statement,
    };

    fn test_index(table: &BTreeTable, name: &str, root_page: i64) -> Arc<Index> {
        Arc::new(Index {
            name: name.to_owned(),
            table_name: table.name.clone(),
            root_page,
            columns: vec![IndexColumn::new("b", 1)],
            unique: false,
            ephemeral: false,
            has_rowid: table.has_rowid,
            where_clause: None,
            index_method: None,
            on_conflict: None,
        })
    }

    fn test_ephemeral_index(name: &str) -> Arc<Index> {
        Arc::new(Index {
            name: name.to_owned(),
            table_name: String::new(),
            root_page: 0,
            columns: vec![IndexColumn::new("key", 0)],
            unique: false,
            ephemeral: true,
            has_rowid: false,
            where_clause: None,
            index_method: None,
            on_conflict: None,
        })
    }

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
    fn parameters_register_bind_slots_only_during_lowering() {
        let compiler = parameter(Variable::named(":offset", 2.try_into().unwrap()))
            .then(parameter(Variable::indexed(1.try_into().unwrap())))
            .and_then(|(lhs, rhs)| add(lhs, rhs));
        let ir = compile_scalar(compiler).unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 3, 0));

        assert_eq!(program.parameters.count(), 0);
        assert_eq!(
            ir.to_string(),
            concat!(
                "block0:\n",
                "  %0 = parameter :offset @2\n",
                "  %1 = parameter ?1\n",
                "  %2 = add %0, %1\n",
                "  return %2\n",
            )
        );

        ir.lower_into(&mut program, 7).unwrap();

        assert_eq!(program.parameters.count(), 2);
        assert_eq!(
            program.parameters.name(1.try_into().unwrap()).as_deref(),
            Some("?1")
        );
        assert_eq!(
            program.parameters.name(2.try_into().unwrap()).as_deref(),
            Some(":offset")
        );
        assert_eq!(program.insns.len(), 3);
        assert!(matches!(
            program.insns[0].0,
            Insn::Variable { index, dest: 1 } if index.get() == 2
        ));
        assert!(matches!(
            program.insns[1].0,
            Insn::Variable { index, dest: 2 } if index.get() == 1
        ));
        assert!(matches!(
            program.insns[2].0,
            Insn::Add {
                lhs: 1,
                rhs: 2,
                dest: 7,
            }
        ));
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
        let compiler = scan_table(table, 0, 0, ScanDirection::Forward).and_then(|rows| {
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
                "  cursor_start Forward $0, block1(), block2()\n",
                "\n",
                "block1:\n",
                "  %0 = column $0[0]\n",
                "  result_row [%0]\n",
                "  cursor_advance Forward $0, block1(), block2()\n",
                "\n",
                "block2:\n",
                "  %1 = constant Null\n",
                "  return %1\n",
            )
        );
    }

    #[test]
    fn row_stream_non_empty_terminal_returns_an_ssa_boolean_and_stops() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE streamed(a)", 2).unwrap());
        let compiler =
            scan_table(table, 0, 0, ScanDirection::Forward).and_then(RowStream::has_rows);

        let ir = compile_scalar(compiler).unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "cursor $0 = btree_table \"streamed\" root 2\n",
                "\n",
                "block0:\n",
                "  open_read $0 root 2 db 0 schema 0\n",
                "  %0 = constant Numeric(Integer(0))\n",
                "  cursor_start Forward $0, block1(%0), block4(%0)\n",
                "\n",
                "block1(%1):\n",
                "  %3 = constant Numeric(Integer(1))\n",
                "  %4 = constant Numeric(Integer(0))\n",
                "  branch %4, block2, block3\n",
                "\n",
                "block2:\n",
                "  cursor_advance Forward $0, block1(%3), block4(%3)\n",
                "\n",
                "block3:\n",
                "  jump block4(%3)\n",
                "\n",
                "block4(%2):\n",
                "  return %2\n",
            )
        );
    }

    #[test]
    fn row_stream_first_or_joins_the_first_scalar_with_the_empty_default() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE streamed(a)", 2).unwrap());
        let compiler = scan_table(table, 0, 0, ScanDirection::Forward).and_then(|rows| {
            rows.map(|row| row.column(0))
                .first_or(constant(Value::Null))
        });

        let ir = compile_scalar(compiler).unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "cursor $0 = btree_table \"streamed\" root 2\n",
                "\n",
                "block0:\n",
                "  open_read $0 root 2 db 0 schema 0\n",
                "  %0 = constant Null\n",
                "  cursor_start Forward $0, block1(%0), block4(%0)\n",
                "\n",
                "block1(%1):\n",
                "  %3 = column $0[0]\n",
                "  %4 = constant Numeric(Integer(0))\n",
                "  branch %4, block2, block3\n",
                "\n",
                "block2:\n",
                "  cursor_advance Forward $0, block1(%3), block4(%3)\n",
                "\n",
                "block3:\n",
                "  jump block4(%3)\n",
                "\n",
                "block4(%2):\n",
                "  return %2\n",
            )
        );
    }

    #[test]
    fn reverse_row_stream_lowers_to_last_and_prev() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE reversed(a)", 2).unwrap());
        let compiler = scan_table(table, 0, 0, ScanDirection::Reverse).and_then(|rows| {
            rows.for_each(|row| {
                pack_values(smallvec![row.column(0).boxed()]).and_then(result_row_pack)
            })
        });
        let ir = compile_effect(compiler).unwrap();
        let rendered = ir.to_string();

        assert!(rendered.contains("cursor_start Reverse $0, block1(), block2()"));
        assert!(rendered.contains("cursor_advance Reverse $0, block1(), block2()"));

        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 2, 0));
        let target = program.alloc_register();
        ir.lower_into(&mut program, target).unwrap();
        program.resolve_labels().unwrap();

        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Last { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Prev { .. })));
        assert!(program.insns.iter().all(|(instruction, _)| !matches!(
            instruction,
            Insn::Rewind { .. } | Insn::Next { .. }
        )));
    }

    #[test]
    fn rowid_point_stream_seeks_once_without_building_a_scan_loop() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE pointed(a)", 2).unwrap());
        let compiler =
            seek_rowid(table, 0, 0, constant(Value::from_i64(7)).boxed()).and_then(|rows| {
                rows.for_each(|row| {
                    pack_values(smallvec![row.rowid().boxed(), row.column(0).boxed()])
                        .and_then(result_row_pack)
                })
            });

        let ir = compile_effect(compiler).unwrap();
        let rendered = ir.to_string();

        assert!(rendered.contains("%0 = constant Numeric(Integer(7))"));
        assert!(rendered.contains("cursor_seek_rowid $0, %0, block1(), block2()"));
        assert!(rendered.contains("%1 = rowid $0"));
        assert!(rendered.contains("%2 = column $0[0]"));
        assert!(!rendered.contains("cursor_start"));
        assert!(!rendered.contains("cursor_advance"));

        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 2, 0));
        let target = program.alloc_register();
        ir.lower_into(&mut program, target).unwrap();
        program.resolve_labels().unwrap();

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::SeekRowid { .. }))
                .count(),
            1
        );
        assert!(program.insns.iter().all(|(instruction, _)| !matches!(
            instruction,
            Insn::Rewind { .. } | Insn::Last { .. } | Insn::Next { .. } | Insn::Prev { .. }
        )));
    }

    #[test]
    fn in_values_flat_maps_a_coerced_distinct_stream_into_rowid_seeks() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE pointed(a)", 2).unwrap());
        let values = literal_values(
            smallvec![
                constant(Value::from_i64(3)).boxed(),
                constant(Value::from_text("1")).boxed(),
                constant(Value::from_i64(3)).boxed(),
                constant(Value::Null).boxed(),
            ],
            Affinity::Integer,
            None,
        );
        let compiler = seek_in_values(table, None, false, 0, 0, values).and_then(|rows| {
            rows.take(constant(Value::from_i64(2))).for_each(|row| {
                pack_values(smallvec![row.rowid().boxed()]).and_then(result_row_pack)
            })
        });

        let ir = compile_effect(compiler).unwrap();
        let rendered = ir.to_string();

        assert!(
            rendered.contains("sorter #0 keys 1 width 1 [Asc None None None] affinity [Integer]")
        );
        assert!(rendered.contains("distinct_set &0 width 1 [Binary]"));
        assert!(rendered.contains("distinct_check &0 ["));
        assert!(rendered.contains("cursor_seek_rowid $0,"));
        assert!(rendered.contains("sorter_next #0"));

        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 2, 0));
        let target = program.alloc_register();
        ir.lower_into(&mut program, target).unwrap();
        program.resolve_labels().unwrap();

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::SeekRowid { .. }))
                .count(),
            1
        );
        assert!(program.insns.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::MakeRecord {
                affinity_str: Some(affinity),
                ..
            } if affinity == "D"
        )));
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::SorterOpen { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::HashDistinct { .. })));
    }

    #[test]
    fn in_values_can_scan_an_external_cursor_without_reopening_it() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE pointed(a,b)", 2).unwrap());
        let values = cursor_values(CursorInputId::new(0), None);
        let compiler = seek_in_values(table.clone(), None, false, 0, 0, values).and_then(|rows| {
            rows.for_each(|row| {
                pack_values(smallvec![row.rowid().boxed()]).and_then(result_row_pack)
            })
        });
        let ir = compile_effect(compiler).unwrap();
        let rendered = ir.to_string();

        assert!(rendered.contains("cursor $1 = input &0"));
        assert!(rendered.contains("cursor_start Forward $1"));
        assert!(rendered.contains("cursor_seek_rowid $0"));
        assert!(!rendered.contains("open_read $1"));
        assert!(!rendered.contains("open_sorter"));

        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 2, 0));
        let bound_cursor =
            program.alloc_cursor_id(CursorType::BTreeIndex(test_index(&table, "in_values", 3)));
        let target = program.alloc_register();
        ir.lower_into_with_resources(&mut program, target, &[], &[bound_cursor])
            .unwrap();
        program.resolve_labels().unwrap();

        assert!(program.insns.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::Rewind { cursor_id, .. } if *cursor_id == bound_cursor
        )));
        assert!(program.insns.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::SeekRowid { cursor_id, .. } if *cursor_id != bound_cursor
        )));
        assert!(program
            .insns
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::SorterOpen { .. })));
    }

    #[test]
    fn index_insert_binds_an_external_cursor_without_opening_it() {
        let compiler = cursor_input(CursorInputId::new(0))
            .then(pack_values(smallvec![constant(Value::from_i64(7)).boxed()]))
            .and_then(|(cursor, pack)| {
                insert_index_pack(cursor, pack, "in_keys".to_owned(), Some("D".to_owned()))
            });
        let ir = compile_effect(compiler).unwrap();
        let rendered = ir.to_string();

        assert!(rendered.contains("cursor $0 = input &0"));
        assert!(rendered.contains("index_insert $0 [%0]"));
        assert!(!rendered.contains("open_read $0"));

        let table = BTreeTable::from_sql("CREATE TABLE pointed(a,b)", 2).unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 2, 0));
        let bound_cursor =
            program.alloc_cursor_id(CursorType::BTreeIndex(test_index(&table, "in_keys", 3)));
        let target = program.alloc_register();
        let lowered = ir
            .lower_into_with_resources(&mut program, target, &[], &[bound_cursor])
            .unwrap();
        lowered.expect_no_result_rows().unwrap();

        assert!(program.insns.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::MakeRecord {
                index_name: Some(name),
                affinity_str: Some(affinity),
                ..
            } if name == "in_keys" && affinity == "D"
        )));
        assert!(program.insns.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::IdxInsert {
                cursor_id,
                flags,
                ..
            } if *cursor_id == bound_cursor && flags.has(IdxInsertFlags::NO_OP_DUPLICATE)
        )));
        assert!(program.insns.iter().all(|(instruction, _)| !matches!(
            instruction,
            Insn::OpenRead { cursor_id, .. } | Insn::OpenEphemeral { cursor_id, .. }
                if *cursor_id == bound_cursor
        )));
    }

    #[test]
    fn table_range_builds_symbolic_seek_bound_and_advance_control_flow() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE ranged(a)", 2).unwrap());
        let range = DeferredTableRange::new(
            DeferredTableBound::expression(constant(Value::from_i64(2)).boxed(), SeekOp::GT),
            DeferredTableBound::expression(constant(Value::from_i64(5)).boxed(), SeekOp::GT),
            ScanDirection::Forward,
            Affinity::Numeric,
        );
        let compiler = seek_table_range(table, 0, 0, range).and_then(|rows| {
            rows.for_each(|row| {
                pack_values(smallvec![row.rowid().boxed(), row.column(0).boxed()])
                    .and_then(result_row_pack)
            })
        });

        let ir = compile_effect(compiler).unwrap();
        let rendered = ir.to_string();
        assert!(rendered.contains("table_seek GT $0, %0"));
        assert!(rendered.contains("table_bound GT affinity Numeric $0, %1"));
        assert!(rendered.contains("cursor_advance Forward $0"));
        assert!(!rendered.contains("cursor_start"));

        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 2, 0));
        let target = program.alloc_register();
        ir.lower_into(&mut program, target).unwrap();
        program.resolve_labels().unwrap();

        assert!(program.insns.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::SeekGT {
                is_index: false,
                num_regs: 1,
                ..
            }
        )));
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::RowId { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Gt { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Next { .. })));
    }

    #[test]
    fn verifier_rejects_table_range_control_on_an_index_cursor() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE ranged(a,b)", 2).unwrap());
        let index = test_index(&table, "ranged_b", 3);
        let range = DeferredTableRange::new(
            DeferredTableBound::expression(constant(Value::from_i64(2)).boxed(), SeekOp::GT),
            DeferredTableBound::unbounded(SeekOp::GT),
            ScanDirection::Forward,
            Affinity::Numeric,
        );
        let compiler = seek_table_range(table, 0, 0, range)
            .and_then(|rows| rows.for_each(|_| constant(Value::Null).map(|_| ())));
        let mut ir = compile_effect(compiler).unwrap();
        ir.cursor_resources[0] = CursorResource::Owned(CursorType::BTreeIndex(index));

        let error = ir.verify().unwrap_err();

        assert!(error
            .to_string()
            .contains("rowid control requires a B-tree table cursor"));
    }

    #[test]
    fn index_lookup_stream_seeks_the_table_before_reading_each_row() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE indexed(a,b)", 2).unwrap());
        let index = test_index(&table, "indexed_b", 3);
        let compiler =
            scan_index(table, index, false, 0, 0, ScanDirection::Forward).and_then(|rows| {
                rows.for_each(|row| {
                    pack_values(smallvec![row.column(0).boxed()]).and_then(result_row_pack)
                })
            });

        let ir = compile_effect(compiler).unwrap();
        let rendered = ir.to_string();
        assert!(rendered.contains("cursor $0 = btree_table \"indexed\" root 2"));
        assert!(rendered.contains("cursor $1 = btree_index \"indexed_b\" root 3"));
        assert!(rendered.contains("cursor_start Forward $1"));
        let seek = rendered.find("deferred_seek $1 -> $0").unwrap();
        let column = rendered.find("column $0[0]").unwrap();
        assert!(seek < column);
        assert!(rendered.contains("cursor_advance Forward $1"));

        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 2, 0));
        let target = program.alloc_register();
        ir.lower_into(&mut program, target).unwrap();
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::DeferredSeek { .. })));
    }

    #[test]
    fn exact_index_range_builds_seek_bound_and_advance_control_flow() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE indexed(a,b)", 2).unwrap());
        let index = test_index(&table, "indexed_b", 3);
        let range = DeferredIndexRange::new(
            smallvec![constant(Value::from_i64(7)).boxed()],
            smallvec![Affinity::Numeric],
            DeferredIndexBound::prefix(SeekOp::GE { eq_only: true }),
            DeferredIndexBound::prefix(SeekOp::GT),
            ScanDirection::Forward,
        );
        let compiler = seek_index(table, index, false, 0, 0, range).and_then(|rows| {
            rows.for_each(|row| {
                pack_values(smallvec![row.column(0).boxed()]).and_then(result_row_pack)
            })
        });

        let ir = compile_effect(compiler).unwrap();
        let rendered = ir.to_string();
        assert!(rendered.contains("index_seek GE { eq_only: true } $1 [%0] affinity [Numeric]"));
        assert!(rendered.contains("index_bound GT $1 [%0] affinity [Numeric]"));
        assert!(rendered.contains("deferred_seek $1 -> $0"));
        assert!(rendered.contains("cursor_advance Forward $1"));
        assert!(!rendered.contains("cursor_start"));

        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 2, 0));
        let target = program.alloc_register();
        ir.lower_into(&mut program, target).unwrap();
        program.resolve_labels().unwrap();

        assert!(program.insns.iter().any(|(instruction, _)| matches!(
            instruction,
            Insn::SeekGE {
                is_index: true,
                eq_only: true,
                num_regs: 1,
                ..
            }
        )));
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::IdxGT { num_regs: 1, .. })));
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Next { .. })));
        assert!(program
            .insns
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::Rewind { .. })));
    }

    #[test]
    fn index_range_distinguishes_null_sentinels_from_null_expression_bounds() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE indexed(a,b)", 2).unwrap());
        let index = test_index(&table, "indexed_b", 3);
        let range = DeferredIndexRange::new(
            SmallVec::new(),
            SmallVec::new(),
            DeferredIndexBound::null(SeekOp::GT),
            DeferredIndexBound::expression(
                constant(Value::from_i64(9)).boxed(),
                Affinity::Numeric,
                SeekOp::GE { eq_only: false },
            ),
            ScanDirection::Forward,
        );
        let compiler = seek_index(table, index, false, 0, 0, range).and_then(|rows| {
            rows.for_each(|row| {
                pack_values(smallvec![row.column(0).boxed()]).and_then(result_row_pack)
            })
        });

        let ir = compile_effect(compiler).unwrap();
        let rendered = ir.to_string();
        assert!(rendered.contains("%0 = constant Null"));
        assert!(rendered.contains("index_seek GT $1 [%0] affinity [Blob] null [Compare]"));
        assert!(rendered.contains("index_bound GE { eq_only: false } $1 [%1]"));

        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 2, 0));
        let target = program.alloc_register();
        ir.lower_into(&mut program, target).unwrap();
        program.resolve_labels().unwrap();

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::IsNull { .. }))
                .count(),
            1,
            "only the expression endpoint should abort the range when it is NULL"
        );
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::SeekGT { num_regs: 1, .. })));
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::IdxGE { num_regs: 1, .. })));
    }

    #[test]
    fn index_range_can_advance_without_a_termination_bound() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE indexed(a,b)", 2).unwrap());
        let index = test_index(&table, "indexed_b", 3);
        let range = DeferredIndexRange::new(
            SmallVec::new(),
            SmallVec::new(),
            DeferredIndexBound::expression(
                constant(Value::from_i64(1)).boxed(),
                Affinity::Numeric,
                SeekOp::GT,
            ),
            DeferredIndexBound::prefix(SeekOp::GT),
            ScanDirection::Forward,
        );
        let compiler = seek_index(table, index, true, 0, 0, range).and_then(|rows| {
            rows.for_each(|row| {
                pack_values(smallvec![row.column(0).boxed()]).and_then(result_row_pack)
            })
        });

        let ir = compile_effect(compiler).unwrap();
        let rendered = ir.to_string();

        assert!(rendered.contains("index_seek GT $0"));
        assert!(!rendered.contains("index_bound"));
        assert!(rendered.contains("cursor_advance Forward $0, block1(), block2()"));
    }

    #[test]
    fn verifier_rejects_index_control_on_a_table_cursor() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE indexed(a,b)", 2).unwrap());
        let index = test_index(&table, "indexed_b", 3);
        let range = DeferredIndexRange::new(
            smallvec![constant(Value::from_i64(7)).boxed()],
            smallvec![Affinity::Numeric],
            DeferredIndexBound::prefix(SeekOp::GE { eq_only: true }),
            DeferredIndexBound::prefix(SeekOp::GT),
            ScanDirection::Forward,
        );
        let compiler = seek_index(table.clone(), index, false, 0, 0, range)
            .and_then(|rows| rows.for_each(|_| constant(Value::Null).map(|_| ())));
        let mut ir = compile_effect(compiler).unwrap();
        ir.cursor_resources[1] = CursorResource::Owned(CursorType::BTreeTable(table));

        let error = ir.verify().unwrap_err();

        assert!(error
            .to_string()
            .contains("index seek requires a B-tree index cursor"));
    }

    #[test]
    fn covering_index_stream_reads_columns_and_rowid_from_the_index() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE indexed(a,b)", 2).unwrap());
        let index = test_index(&table, "indexed_b", 3);
        let compiler =
            scan_index(table, index, true, 0, 0, ScanDirection::Reverse).and_then(|rows| {
                rows.for_each(|row| {
                    pack_values(smallvec![row.column(0).boxed(), row.index_rowid().boxed(),])
                        .and_then(result_row_pack)
                })
            });

        let ir = compile_effect(compiler).unwrap();
        let rendered = ir.to_string();
        assert!(!rendered.contains("btree_table"));
        assert!(rendered.contains("cursor $0 = btree_index \"indexed_b\" root 3"));
        assert!(rendered.contains("column $0[0]"));
        assert!(rendered.contains("index_rowid $0"));
        assert!(!rendered.contains("deferred_seek"));

        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 2, 0));
        let target = program.alloc_register();
        ir.lower_into(&mut program, target).unwrap();
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::IdxRowId { .. })));
        assert!(program
            .insns
            .iter()
            .all(|(instruction, _)| !matches!(instruction, Insn::DeferredSeek { .. })));
    }

    #[test]
    fn row_stream_sort_materializes_then_yields_symbolic_records() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE sorted(a,b)", 2).unwrap());
        let compiler = scan_table(table, 0, 0, ScanDirection::Forward).and_then(|rows| {
            rows.map(|row| pack_values(smallvec![row.column(1).boxed(), row.column(0).boxed()]))
                .sort(
                    smallvec![SortKey::new(
                        SortOrder::Asc,
                        Some(CollationSeq::Binary),
                        None,
                        None,
                    )],
                    2,
                )
                .for_each(|row| {
                    pack_values(smallvec![row.column(1).boxed()]).and_then(result_row_pack)
                })
        });

        let ir = compile_effect(compiler).unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "cursor $0 = btree_table \"sorted\" root 2\n",
                "sorter #0 keys 1 width 2 [Asc Some(Binary) None None]\n",
                "\n",
                "block0:\n",
                "  open_read $0 root 2 db 0 schema 0\n",
                "  open_sorter #0\n",
                "  cursor_start Forward $0, block1(), block2()\n",
                "\n",
                "block1:\n",
                "  %0 = column $0[1]\n",
                "  %1 = column $0[0]\n",
                "  sorter_insert #0 [%0, %1]\n",
                "  cursor_advance Forward $0, block1(), block2()\n",
                "\n",
                "block2:\n",
                "  sort #0, block3(), block4()\n",
                "\n",
                "block3:\n",
                "  sorter_data #0\n",
                "  %2 = sorter_column #0[1]\n",
                "  result_row [%2]\n",
                "  sorter_next #0, block3(), block4()\n",
                "\n",
                "block4:\n",
                "  %3 = constant Null\n",
                "  return %3\n",
            )
        );
    }

    #[test]
    fn row_stream_distinct_checks_projected_packs_before_yielding() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE deduplicated(a,b)", 2).unwrap());
        let compiler = scan_table(table, 0, 0, ScanDirection::Forward).and_then(|rows| {
            rows.map(|row| pack_values(smallvec![row.column(0).boxed(), row.column(1).boxed()]))
                .distinct(smallvec![CollationSeq::NoCase, CollationSeq::Binary])
                .for_each(result_row_pack)
        });

        let ir = compile_effect(compiler).unwrap();
        let rendered = ir.to_string();

        assert!(rendered.starts_with(concat!(
            "cursor $0 = btree_table \"deduplicated\" root 2\n",
            "distinct_set &0 width 2 [NoCase, Binary]\n",
        )));
        let open = rendered
            .find("open_distinct_set &0")
            .expect("the distinct set must be reset before scanning");
        let rewind = rendered
            .find("cursor_start Forward $0")
            .expect("the source cursor must be entered");
        let check = rendered
            .find("distinct_check &0 [%0, %1]")
            .expect("the projected pack must be checked as one key");
        let result = rendered
            .find("result_row [%0, %1]")
            .expect("only admitted packs reach the consumer");

        assert!(open < rewind && rewind < check && check < result);
    }

    #[test]
    fn row_stream_distinct_by_preserves_records_for_downstream_sorting() {
        let table =
            Arc::new(BTreeTable::from_sql("CREATE TABLE ordered_distinct(a,b)", 2).unwrap());
        let compiler = scan_table(table, 0, 0, ScanDirection::Forward).and_then(|rows| {
            rows.map(|row| pack_values(smallvec![row.column(1).boxed(), row.column(0).boxed()]))
                .distinct_by(smallvec![CollationSeq::NoCase], |pack| {
                    select_pack(pack, 1, 1)
                })
                .sort(
                    smallvec![SortKey::new(
                        SortOrder::Asc,
                        Some(CollationSeq::Binary),
                        None,
                        None,
                    )],
                    2,
                )
                .for_each(|row| {
                    pack_values(smallvec![row.column(1).boxed()]).and_then(result_row_pack)
                })
        });

        let ir = compile_effect(compiler).unwrap();
        let rendered = ir.to_string();
        assert!(
            rendered.contains("distinct_check &0 [%1], block3, block4"),
            "only the derived SELECT-value suffix should be the distinct key:\n{rendered}"
        );
        assert!(
            rendered.contains("sorter_insert #0 [%0, %1]"),
            "the complete record should survive for downstream sorting:\n{rendered}"
        );
        assert!(
            rendered.contains("cursor_advance Forward $0, block1(), block2()")
                && rendered.contains("block2:\n  sort #0, block8(), block9()"),
            "sorting should begin only after the distinct source loop exits:\n{rendered}"
        );
    }

    #[test]
    fn selecting_a_key_outside_a_value_pack_is_rejected() {
        let compiler = pack_values(smallvec![constant(Value::from_i64(1)).boxed()])
            .and_then(|pack| select_pack(pack, 1, 1))
            .and_then(result_row_pack);

        let error = compile_effect(compiler).unwrap_err();

        assert!(error
            .to_string()
            .contains("selects values 1..2 from pack of width 1"));
    }

    #[test]
    fn verifier_rejects_distinct_keys_with_the_wrong_width() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE deduplicated(a)", 2).unwrap());
        let compiler = scan_table(table, 0, 0, ScanDirection::Forward).and_then(|rows| {
            rows.map(|row| pack_values(smallvec![row.column(0).boxed()]))
                .distinct(smallvec![CollationSeq::Binary])
                .for_each(result_row_pack)
        });
        let mut ir = compile_effect(compiler).unwrap();
        ir.distinct_set_resources[0]
            .collations
            .push(CollationSeq::NoCase);

        let error = ir.verify().unwrap_err();

        assert!(error
            .to_string()
            .contains("expects key width 2, received 1"));
    }

    #[test]
    fn verifier_rejects_distinct_checks_without_a_dominating_open() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE deduplicated(a)", 2).unwrap());
        let compiler = scan_table(table, 0, 0, ScanDirection::Forward).and_then(|rows| {
            rows.map(|row| pack_values(smallvec![row.column(0).boxed()]))
                .distinct(smallvec![CollationSeq::Binary])
                .for_each(result_row_pack)
        });
        let mut ir = compile_effect(compiler).unwrap();
        ir.blocks[0].instructions.retain(|instruction| {
            !matches!(
                instruction,
                Instruction::Effect(EffectOp::OpenDistinctSet { .. })
            )
        });

        let error = ir.verify().unwrap_err();

        assert!(error.to_string().contains("uses unopened distinct set"));
    }

    #[test]
    fn verifier_rejects_reading_a_sorter_before_sorting() {
        let mut builder = IrBuilder::new();
        let sorter = builder
            .allocate_sorter(smallvec![SortKey::new(SortOrder::Asc, None, None, None)], 1)
            .unwrap();
        builder
            .push_effect(EffectOp::OpenSorter { sorter })
            .unwrap();
        builder
            .push_effect(EffectOp::SorterData { sorter })
            .unwrap();
        let completion = builder.push(ScalarOp::Constant(Value::Null)).unwrap();

        let error = builder.finish(completion).unwrap_err();

        assert!(error
            .to_string()
            .contains("cannot read sorter SorterId(0) in phase Filling"));
    }

    #[test]
    fn verifier_rejects_sorter_records_with_the_wrong_width() {
        let mut builder = IrBuilder::new();
        let sorter = builder
            .allocate_sorter(smallvec![SortKey::new(SortOrder::Asc, None, None, None)], 2)
            .unwrap();
        builder
            .push_effect(EffectOp::OpenSorter { sorter })
            .unwrap();
        let value = builder
            .push(ScalarOp::Constant(Value::from_i64(1)))
            .unwrap();
        builder
            .push_effect(EffectOp::SorterInsert {
                sorter,
                pack: ValuePack(smallvec![value]),
            })
            .unwrap();
        let completion = builder.push(ScalarOp::Constant(Value::Null)).unwrap();

        let error = builder.finish(completion).unwrap_err();

        assert!(error
            .to_string()
            .contains("expects record width 2, received 1"));
    }

    #[test]
    fn row_stream_filters_and_maps_compose_in_source_order() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE filtered(a,b,c)", 2).unwrap());
        let compiler = scan_table(table, 0, 0, ScanDirection::Forward).and_then(|rows| {
            rows.filter(|row| row.column(0))
                .filter(|row| row.column(1))
                .map(|row| row.column(2))
                .for_each(|value| result_row([value]))
        });

        let ir = compile_effect(compiler).unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "cursor $0 = btree_table \"filtered\" root 2\n",
                "\n",
                "block0:\n",
                "  open_read $0 root 2 db 0 schema 0\n",
                "  cursor_start Forward $0, block1(), block2()\n",
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
                "  cursor_advance Forward $0, block1(), block2()\n",
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
    fn row_stream_take_short_circuits_before_cursor_advance() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE limited(a,b)", 2).unwrap());
        let compiler = scan_table(table, 0, 0, ScanDirection::Forward).and_then(|rows| {
            rows.filter(|row| row.column(0))
                .map(|row| row.column(1))
                .take(constant(Value::from_i64(2)))
                .for_each(|value| result_row([value]))
        });

        let ir = compile_effect(compiler).unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "cursor $0 = btree_table \"limited\" root 2\n",
                "\n",
                "block0:\n",
                "  open_read $0 root 2 db 0 schema 0\n",
                "  %0 = constant Numeric(Integer(2))\n",
                "  %1 = must_be_int %0\n",
                "  branch %1, block1, block2\n",
                "\n",
                "block1:\n",
                "  cursor_start Forward $0, block4(%1), block7(%1)\n",
                "\n",
                "block2:\n",
                "  jump block3()\n",
                "\n",
                "block3:\n",
                "  %18 = constant Null\n",
                "  return %18\n",
                "\n",
                "block4(%2):\n",
                "  %4 = column $0[0]\n",
                "  branch %4, block8, block9\n",
                "\n",
                "block5:\n",
                "  cursor_advance Forward $0, block4(%15), block7(%15)\n",
                "\n",
                "block6:\n",
                "  jump block7(%15)\n",
                "\n",
                "block7(%3):\n",
                "  jump block3()\n",
                "\n",
                "block8:\n",
                "  %5 = column $0[1]\n",
                "  result_row [%5]\n",
                "  %6 = constant Numeric(Integer(1))\n",
                "  %7 = constant Numeric(Integer(-1))\n",
                "  %8 = add %2, %7\n",
                "  %9 = constant Numeric(Integer(0))\n",
                "  compare NotEqual %8, %9 affinity Numeric collation None, block11, block12, block13\n",
                "\n",
                "block9:\n",
                "  %17 = constant Numeric(Integer(1))\n",
                "  jump block10(%2, %17)\n",
                "\n",
                "block10(%15, %16):\n",
                "  branch %16, block5, block6\n",
                "\n",
                "block11:\n",
                "  %11 = constant Numeric(Integer(1))\n",
                "  jump block14(%11)\n",
                "\n",
                "block12:\n",
                "  %12 = constant Numeric(Integer(0))\n",
                "  jump block14(%12)\n",
                "\n",
                "block13:\n",
                "  %13 = constant Null\n",
                "  jump block14(%13)\n",
                "\n",
                "block14(%10):\n",
                "  %14 = and %6, %10\n",
                "  jump block10(%8, %14)\n",
            )
        );
    }

    #[test]
    fn row_stream_skip_discards_items_before_projection() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE skipped(a,b)", 2).unwrap());
        let compiler = scan_table(table, 0, 0, ScanDirection::Forward).and_then(|rows| {
            rows.skip(constant(Value::from_i64(2)))
                .map(|row| row.column(1))
                .for_each(|value| result_row([value]))
        });

        let ir = compile_effect(compiler).unwrap();
        let rendered = ir.to_string();
        let offset_coercion = rendered
            .find("must_be_int")
            .expect("skip must coerce its deferred count");
        let rewind = rendered
            .find("cursor_start Forward $0")
            .expect("the source cursor must be entered");
        let skip_comparison = rendered
            .find("compare Greater")
            .expect("skip must discard only while its count is positive");
        let projection = rendered
            .find("column $0[1]")
            .expect("an admitted item must reach projection");
        let result_row = rendered
            .find("result_row")
            .expect("an admitted item must reach the terminal consumer");

        assert!(offset_coercion < rewind);
        assert!(skip_comparison < projection);
        assert!(projection < result_row);
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
                "  %1 = constant Numeric(Integer(10))\n",
                "  jump block3(%1)\n",
                "\n",
                "block2:\n",
                "  %3 = constant Numeric(Integer(20))\n",
                "  jump block3(%3)\n",
                "\n",
                "block3(%2):\n",
                "  return %2\n",
            )
        );
    }

    #[test]
    fn optimizer_folds_constant_branches_and_removes_unreachable_blocks() {
        let compiler = constant(Value::from_i64(1))
            .branch(constant(Value::from_i64(10)), constant(Value::from_i64(20)));

        let ir = compile_scalar(compiler).unwrap().optimize().unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "block0:\n",
                "  jump block1()\n",
                "\n",
                "block1:\n",
                "  %1 = constant Numeric(Integer(10))\n",
                "  jump block2(%1)\n",
                "\n",
                "block2(%2):\n",
                "  return %2\n",
            )
        );
    }

    #[test]
    fn optimizer_eliminates_recursively_dead_pure_values() {
        let discarded = constant(Value::from_i64(40))
            .then(constant(Value::from_i64(2)))
            .and_then(|(lhs, rhs)| add(lhs, rhs));
        let compiler = discarded
            .then(constant(Value::from_i64(7)))
            .map(|(_, result)| result);

        let ir = compile_scalar(compiler).unwrap().optimize().unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "block0:\n",
                "  %3 = constant Numeric(Integer(7))\n",
                "  return %3\n",
            )
        );
    }

    #[test]
    fn optimizer_removes_unused_join_parameters_and_edge_arguments() {
        let true_values = constant(Value::from_i64(10))
            .then(constant(Value::from_i64(20)))
            .map(|(first, second)| LoopState {
                values: smallvec![first, second],
            });
        let false_values = constant(Value::from_i64(30))
            .then(constant(Value::from_i64(40)))
            .map(|(first, second)| LoopState {
                values: smallvec![first, second],
            });
        let compiler = parameter(Variable::indexed(1.try_into().unwrap()))
            .branch(true_values, false_values)
            .map(|state| state.values[0]);

        let ir = compile_scalar(compiler).unwrap().optimize().unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "block0:\n",
                "  %0 = parameter ?1\n",
                "  branch %0, block1, block2\n",
                "\n",
                "block1:\n",
                "  %1 = constant Numeric(Integer(10))\n",
                "  jump block3(%1)\n",
                "\n",
                "block2:\n",
                "  %5 = constant Numeric(Integer(30))\n",
                "  jump block3(%5)\n",
                "\n",
                "block3(%3):\n",
                "  return %3\n",
            )
        );
    }

    #[test]
    fn dead_join_positions_do_not_emit_edge_copies() {
        let true_values = constant(Value::from_i64(10))
            .then(constant(Value::from_i64(20)))
            .map(|(first, second)| LoopState {
                values: smallvec![first, second],
            });
        let false_values = constant(Value::from_i64(30))
            .then(constant(Value::from_i64(40)))
            .map(|(first, second)| LoopState {
                values: smallvec![first, second],
            });
        let compiler = parameter(Variable::indexed(1.try_into().unwrap()))
            .branch(true_values, false_values)
            .map(|state| state.values[0]);
        let ir = compile_scalar(compiler).unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 16, 4));
        let target = program.alloc_register();

        ir.lower_into(&mut program, target).unwrap();

        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::Copy { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn optimizer_prunes_shared_cursor_edge_state_by_position() {
        let mut builder = IrBuilder::new();
        let cursor = builder.external_cursor(CursorInputId::new(0)).unwrap();
        let first = builder
            .push(ScalarOp::Constant(Value::from_i64(10)))
            .unwrap();
        let second = builder
            .push(ScalarOp::Constant(Value::from_i64(20)))
            .unwrap();
        let third = builder
            .push(ScalarOp::Constant(Value::from_i64(30)))
            .unwrap();
        let row = builder.create_block().unwrap();
        let exit = builder.create_block().unwrap();
        let row_first = builder.add_block_parameter(row).unwrap();
        let row_second = builder.add_block_parameter(row).unwrap();
        let row_third = builder.add_block_parameter(row).unwrap();
        builder.add_block_parameter(exit).unwrap();
        let exit_second = builder.add_block_parameter(exit).unwrap();
        builder.add_block_parameter(exit).unwrap();
        builder
            .terminate(Terminator::CursorStart {
                cursor,
                direction: ScanDirection::Forward,
                if_non_empty: row,
                if_empty: exit,
                arguments: smallvec![first, second, third],
            })
            .unwrap();
        builder.switch_to(row).unwrap();
        builder
            .push_effect(EffectOp::ResultRow {
                pack: ValuePack(smallvec![row_first]),
            })
            .unwrap();
        builder
            .terminate(Terminator::CursorAdvance {
                cursor,
                direction: ScanDirection::Forward,
                if_next: row,
                if_done: exit,
                arguments: smallvec![row_first, row_second, row_third],
            })
            .unwrap();
        builder.switch_to(exit).unwrap();

        let ir = builder.finish(exit_second).unwrap().optimize().unwrap();

        assert_eq!(
            ir.to_string(),
            concat!(
                "cursor $0 = input &0\n",
                "\n",
                "block0:\n",
                "  %0 = constant Numeric(Integer(10))\n",
                "  %1 = constant Numeric(Integer(20))\n",
                "  cursor_start Forward $0, block1(%0, %1), block2(%0, %1)\n",
                "\n",
                "block1(%3, %4):\n",
                "  result_row [%3]\n",
                "  cursor_advance Forward $0, block1(%3, %4), block2(%3, %4)\n",
                "\n",
                "block2(%6, %7):\n",
                "  return %7\n",
            )
        );
    }

    #[test]
    fn dead_value_holes_do_not_allocate_registers() {
        let discarded = constant(Value::from_i64(40))
            .then(constant(Value::from_i64(2)))
            .and_then(|(lhs, rhs)| add(lhs, rhs));
        let compiler = discarded
            .then(constant(Value::from_i64(7)))
            .map(|(_, result)| result);
        let ir = compile_scalar(compiler).unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 3, 0));
        let target = program.alloc_register();

        ir.lower_into(&mut program, target).unwrap();

        assert_eq!(program.peek_next_register(), target + 1);
        assert!(matches!(
            program.insns.as_slice(),
            [(Insn::Integer { value: 7, dest }, _)] if *dest == target
        ));
    }

    #[test]
    fn straight_line_lifetimes_reuse_non_overlapping_registers() {
        let compiler = constant(Value::from_i64(7))
            .and_then(must_be_int)
            .and_then(must_be_int)
            .and_then(must_be_int);
        let ir = compile_scalar(compiler).unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 4, 0));
        let target = program.alloc_register();

        ir.lower_into(&mut program, target).unwrap();

        assert_eq!(program.peek_next_register(), target + 3);
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { dest: first, .. }, _),
                (Insn::Copy { dst_reg: second, .. }, _),
                (Insn::MustBeInt { reg: second_check, .. }, _),
                (Insn::Copy { dst_reg: third, .. }, _),
                (Insn::MustBeInt { reg: third_check, .. }, _),
                (Insn::Copy { dst_reg: result, .. }, _),
                (Insn::MustBeInt { reg: result_check, .. }, _),
            ] if *first == *third
                && *second == *second_check
                && *third == *third_check
                && *result == target
                && *result_check == target
        ));
    }

    #[test]
    fn mutually_exclusive_branch_values_share_a_register() {
        let compiler = parameter(Variable::indexed(1.try_into().unwrap()))
            .branch(constant(Value::from_i64(10)), constant(Value::from_i64(20)));
        let ir = compile_scalar(compiler).unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 4, 0));
        let target = program.alloc_register();

        ir.lower_into(&mut program, target).unwrap();

        assert_eq!(program.peek_next_register(), target + 2);
        let destinations = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Variable { dest, .. } | Insn::Integer { dest, .. } => Some(*dest),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(destinations.len(), 3);
        assert!(destinations
            .iter()
            .all(|destination| { *destination == destinations[0] && *destination != target }));
    }

    #[test]
    fn loop_temporaries_reuse_preheader_storage() {
        let compiler = constant(Value::from_i64(3)).loop_while(pure, |state| {
            constant(Value::from_i64(-1)).and_then(move |step| add(state, step))
        });
        let ir = compile_scalar(compiler).unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 4, 0));
        let target = program.alloc_register();

        ir.lower_into(&mut program, target).unwrap();

        assert_eq!(program.peek_next_register(), target + 3);
        let constant_destinations = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Integer { dest, .. } => Some(*dest),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(constant_destinations.len(), 2);
        assert_eq!(constant_destinations[0], constant_destinations[1]);
    }

    #[test]
    fn dead_value_elimination_preserves_integer_coercion_errors() {
        let compiler = constant(Value::Text("not an integer".into()))
            .and_then(must_be_int)
            .then(constant(Value::from_i64(7)))
            .map(|(_, result)| result);
        let ir = compile_scalar(compiler).unwrap();
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        let mut builder =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 8, 0));
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
            .build(connection.clone(), false, "dead value coercion test")
            .unwrap();
        let mut statement = Statement::new(program, connection.get_pager(), QueryMode::Normal, 0);

        let error = statement.run_collect_rows().unwrap_err();

        assert!(error.to_string().contains("datatype mismatch"));
    }

    #[test]
    fn pruned_owned_cursors_do_not_allocate_backend_resources() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE dead(a)", 2).unwrap());
        let dead_branch = open_read_table(table, 0, 0).and_then(|cursor| column(cursor, 0));
        let compiler =
            constant(Value::from_i64(1)).branch(constant(Value::from_i64(10)), dead_branch);
        let ir = compile_scalar(compiler).unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 8, 0));
        let target = program.alloc_register();

        ir.lower_into(&mut program, target).unwrap();

        assert!(program.cursor_ref.is_empty());
        assert!(program
            .insns
            .iter()
            .all(|(insn, _)| !matches!(insn, Insn::OpenRead { .. } | Insn::Column { .. })));
    }

    #[test]
    fn constant_branch_folding_uses_vdbe_truthiness() {
        for (condition, expected_target) in [
            (Value::Null, BlockId(2)),
            (Value::Text("0".into()), BlockId(2)),
            (Value::Text("2".into()), BlockId(1)),
        ] {
            let compiler = constant(condition).branch(constant(Value::Null), constant(Value::Null));
            let mut ir = compile_scalar(compiler).unwrap();

            assert!(ir.fold_constant_branches());
            assert!(matches!(
                ir.blocks[0].terminator,
                Terminator::Jump { target, .. } if target == expected_target
            ));
        }
    }

    #[test]
    fn optimizer_retains_a_constant_branch_that_guards_the_only_return() {
        let compiler =
            constant(Value::from_i64(0)).loop_while(|_| constant(Value::from_i64(1)), pure);

        let ir = compile_scalar(compiler).unwrap().optimize().unwrap();

        assert!(ir.to_string().contains("branch"));
    }

    #[test]
    fn lowering_pruned_parameter_branches_preserves_bind_slots() {
        let compiler = constant(Value::from_i64(1)).branch(
            constant(Value::from_i64(10)),
            parameter(Variable::indexed(1.try_into().unwrap())),
        );
        let ir = compile_scalar(compiler).unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 3, 0));

        ir.lower_into(&mut program, 7).unwrap();

        assert_eq!(program.parameters.count(), 1);
        assert!(program
            .insns
            .iter()
            .all(|(insn, _)| !matches!(insn, Insn::Variable { .. } | Insn::IfNot { .. })));
    }

    #[test]
    fn eliminating_an_unused_parameter_preserves_its_bind_slot() {
        let compiler = parameter(Variable::indexed(1.try_into().unwrap()))
            .then(constant(Value::from_i64(7)))
            .map(|(_, result)| result);
        let ir = compile_scalar(compiler).unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 3, 0));
        let target = program.alloc_register();

        ir.lower_into(&mut program, target).unwrap();

        assert_eq!(program.parameters.count(), 1);
        assert!(program
            .insns
            .iter()
            .all(|(insn, _)| !matches!(insn, Insn::Variable { .. })));
    }

    #[test]
    fn optimization_preserves_external_input_bindings() {
        let compiler = constant(Value::from_i64(1))
            .branch(constant(Value::from_i64(10)), input(InputId::new(0)));
        let ir = compile_scalar(compiler).unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 3, 0));

        let error = ir.lower_into(&mut program, 7).unwrap_err();

        assert!(error.to_string().contains("expects 1 inputs, received 0"));
        assert!(program.insns.is_empty());
    }

    #[test]
    fn optimizer_keeps_parameter_control_flow_dynamic() {
        let compiler = parameter(Variable::indexed(1.try_into().unwrap()))
            .branch(constant(Value::from_i64(10)), constant(Value::from_i64(20)));
        let ir = compile_scalar(compiler).unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 3, 0));

        ir.lower_into(&mut program, 7).unwrap();

        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Variable { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::IfNot { .. })));
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
    fn parallel_edge_copies_preserve_loop_carried_swaps() {
        let mut ir = IrBuilder::new();
        let first = ir.push(ScalarOp::Constant(Value::from_i64(1))).unwrap();
        let second = ir.push(ScalarOp::Constant(Value::from_i64(2))).unwrap();
        let remaining = ir.push(ScalarOp::Constant(Value::from_i64(1))).unwrap();
        let header = ir.create_block().unwrap();
        let body = ir.create_block().unwrap();
        let exit = ir.create_block().unwrap();
        let carried_first = ir.add_block_parameter(header).unwrap();
        let carried_second = ir.add_block_parameter(header).unwrap();
        let carried_remaining = ir.add_block_parameter(header).unwrap();
        ir.terminate(Terminator::Jump {
            target: header,
            arguments: smallvec![first, second, remaining],
        })
        .unwrap();

        ir.switch_to(header).unwrap();
        ir.terminate(Terminator::Branch {
            condition: carried_remaining,
            if_true: body,
            if_false: exit,
        })
        .unwrap();

        ir.switch_to(body).unwrap();
        let decrement = ir.push(ScalarOp::Constant(Value::from_i64(-1))).unwrap();
        let next_remaining = ir
            .push(ScalarOp::Add {
                lhs: carried_remaining,
                rhs: decrement,
            })
            .unwrap();
        ir.terminate(Terminator::Jump {
            target: header,
            arguments: smallvec![carried_second, carried_first, next_remaining],
        })
        .unwrap();

        ir.switch_to(exit).unwrap();
        ir.push_effect(EffectOp::ResultRow {
            pack: ValuePack(smallvec![carried_first, carried_second]),
        })
        .unwrap();
        let ir = ir.finish(carried_first).unwrap();

        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        let mut builder =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 10, 0));
        let result = builder.alloc_register();
        ir.lower_into(&mut builder, result).unwrap();
        builder.emit_insn(Insn::Halt {
            err_code: 0,
            description: String::new(),
            on_error: None,
            description_reg: None,
        });
        let program = builder
            .build(connection.clone(), false, "parallel edge copy test")
            .unwrap();
        let mut statement = Statement::new(program, connection.get_pager(), QueryMode::Normal, 0);

        assert_eq!(
            statement.run_collect_rows().unwrap(),
            vec![vec![Value::from_i64(2), Value::from_i64(1)]]
        );
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
                "  cursor_start Forward $0, block1(%0), block2(%0)\n",
                "\n",
                "block1(%1):\n",
                "  %3 = column $0[2]\n",
                "  %4 = add %1, %3\n",
                "  cursor_advance Forward $0, block1(%4), block2(%4)\n",
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
    fn owned_ephemeral_index_can_be_written_then_scanned_in_one_region() {
        let index = test_ephemeral_index("owned_ephemeral");
        let compiler = open_ephemeral_index(index).and_then(|cursor| {
            pack_values(smallvec![constant(Value::from_i64(7)).boxed()])
                .and_then(move |pack| {
                    insert_index_pack(
                        cursor,
                        pack,
                        "owned_ephemeral".to_owned(),
                        Some("D".to_owned()),
                    )
                })
                .and_then(move |()| {
                    scan_cursor(cursor, ScanDirection::Forward).for_each(|row| {
                        pack_values(smallvec![row.column(0).boxed()]).and_then(result_row_pack)
                    })
                })
        });
        let ir = compile_effect(compiler).unwrap();
        let rendered = ir.to_string();

        assert!(rendered.contains("cursor $0 = btree_index \"owned_ephemeral\" root 0"));
        assert!(rendered.contains("open_ephemeral_index $0"));
        assert!(rendered.contains("index_insert $0 [%0]"));
        assert!(rendered.contains("cursor_start Forward $0"));

        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 2, 0));
        let target = program.alloc_register();
        let lowered = ir.lower_into(&mut program, target).unwrap();
        assert_eq!(lowered.single_result_row_pack().unwrap().1, 1);
        program.resolve_labels().unwrap();

        assert_eq!(program.cursor_ref.len(), 1);
        assert!(matches!(
            &program.cursor_ref[0].1,
            CursorType::BTreeIndex(index) if index.ephemeral
        ));
        let open = program
            .insns
            .iter()
            .position(|(instruction, _)| {
                matches!(instruction, Insn::OpenEphemeral { cursor_id: 0, .. })
            })
            .unwrap();
        let insert = program
            .insns
            .iter()
            .position(|(instruction, _)| {
                matches!(instruction, Insn::IdxInsert { cursor_id: 0, .. })
            })
            .unwrap();
        let rewind = program
            .insns
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::Rewind { cursor_id: 0, .. }))
            .unwrap();
        assert!(open < insert && insert < rewind);
    }

    #[test]
    fn ephemeral_index_declaration_is_separate_from_its_open_effect() {
        let compiler = declare_ephemeral_index(test_ephemeral_index("declared_ephemeral"))
            .and_then(|unopened| {
                open_declared_ephemeral_index(unopened).and_then(|cursor| {
                    pack_values(smallvec![constant(Value::from_i64(7)).boxed()])
                        .and_then(move |pack| {
                            insert_index_pack(
                                cursor,
                                pack,
                                "declared_ephemeral".to_owned(),
                                Some("D".to_owned()),
                            )
                        })
                        .and_then(move |()| {
                            scan_cursor(cursor, ScanDirection::Forward).for_each(|row| {
                                pack_values(smallvec![row.column(0).boxed()])
                                    .and_then(result_row_pack)
                            })
                        })
                })
            });
        let ir = compile_effect(compiler).unwrap();
        let rendered = ir.to_string();

        assert!(rendered.contains("cursor $0 = btree_index \"declared_ephemeral\" root 0"));
        assert!(rendered.contains("open_ephemeral_index $0"));
        assert!(rendered.contains("index_insert $0 [%0]"));
        assert!(rendered.contains("cursor_start Forward $0"));

        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 2, 0));
        let target = program.alloc_register();
        let lowered = ir.lower_into(&mut program, target).unwrap();
        assert_eq!(lowered.single_result_row_pack().unwrap().1, 1);
        assert_eq!(program.cursor_ref.len(), 1);
    }

    #[test]
    fn once_initialized_cursor_can_be_consumed_after_its_guarded_region() {
        let compiler =
            declare_ephemeral_index(test_ephemeral_index("once_ephemeral")).and_then(|unopened| {
                initialize_cursor_once(open_declared_ephemeral_index(unopened).and_then(|cursor| {
                    pack_values(smallvec![constant(Value::from_i64(7)).boxed()])
                        .and_then(move |pack| {
                            insert_index_pack(
                                cursor,
                                pack,
                                "once_ephemeral".to_owned(),
                                Some("D".to_owned()),
                            )
                        })
                        .map(move |()| cursor)
                }))
                .and_then(|cursor| {
                    scan_cursor(cursor, ScanDirection::Forward).for_each(|row| {
                        pack_values(smallvec![row.column(0).boxed()]).and_then(result_row_pack)
                    })
                })
            });
        let ir = compile_effect(compiler).unwrap();
        let rendered = ir.to_string();

        assert!(rendered.contains("once block1, block2"));
        assert!(rendered.contains("block1:\n  open_ephemeral_index $0"));
        assert!(rendered.contains("block2:\n  cursor_start Forward $0"));

        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 2, 0));
        let target = program.alloc_register();
        ir.lower_into(&mut program, target).unwrap();
        program.resolve_labels().unwrap();

        let once = program
            .insns
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::Once { .. }))
            .unwrap();
        let open = program
            .insns
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::OpenEphemeral { .. }))
            .unwrap();
        let insert = program
            .insns
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::IdxInsert { .. }))
            .unwrap();
        let rewind = program
            .insns
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::Rewind { .. }))
            .unwrap();
        let Insn::Once {
            target_pc_when_reentered,
        } = &program.insns[once].0
        else {
            unreachable!();
        };
        assert_eq!(target_pc_when_reentered.as_offset_int() as usize, rewind);
        assert!(once < open && open < insert && insert < rewind);
    }

    #[test]
    fn verifier_rejects_a_declared_but_unopened_cursor() {
        let compiler = declare_ephemeral_index(test_ephemeral_index("unopened_ephemeral"))
            .and_then(|unopened| {
                scan_cursor(unopened.cursor, ScanDirection::Forward).for_each(|_| pure(()))
            });

        let error = compile_effect(compiler).unwrap_err();

        assert!(error.to_string().contains("uses unopened cursor"));
    }

    #[test]
    fn cursor_input_binding_composes_descriptions_over_one_owned_cursor() {
        let index = test_ephemeral_index("bound_ephemeral");
        let input = CursorInputId::new(0);
        let compiler = open_ephemeral_index(index).and_then(move |cursor| {
            let producer = cursor_input(input)
                .then(pack_values(smallvec![constant(Value::from_i64(7)).boxed()]))
                .and_then(|(destination, pack)| {
                    insert_index_pack(
                        destination,
                        pack,
                        "bound_ephemeral".to_owned(),
                        Some("D".to_owned()),
                    )
                });
            let consumer = producer
                .and_then(move |()| cursor_input(input))
                .and_then(|source| {
                    scan_cursor(source, ScanDirection::Forward).for_each(|row| {
                        pack_values(smallvec![row.column(0).boxed()]).and_then(result_row_pack)
                    })
                });
            bind_cursor_input(input, cursor, consumer)
        });
        let ir = compile_effect(compiler).unwrap();
        let rendered = ir.to_string();

        assert!(rendered.contains("cursor $0 = btree_index \"bound_ephemeral\" root 0"));
        assert!(!rendered.contains("external_cursor"));
        assert!(rendered.contains("open_ephemeral_index $0"));
        assert!(rendered.contains("index_insert $0 [%0]"));
        assert!(rendered.contains("cursor_start Forward $0"));

        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 2, 0));
        let target = program.alloc_register();
        let lowered = ir.lower_into(&mut program, target).unwrap();
        assert_eq!(lowered.single_result_row_pack().unwrap().1, 1);
        assert_eq!(program.cursor_ref.len(), 1);
    }

    #[test]
    fn scalar_input_binding_composes_descriptions_without_a_lowering_input() {
        let input_id = InputId::new(0);
        let compiler = constant(Value::from_i64(7)).and_then(move |value| {
            bind_input(
                input_id,
                value,
                input(input_id)
                    .then(constant(Value::from_i64(1)))
                    .and_then(|(lhs, rhs)| add(lhs, rhs)),
            )
        });

        let ir = compile_scalar(compiler).unwrap();

        assert!(!ir.to_string().contains("input @0"));
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 1, 0));
        ir.lower_into(&mut program, 1).unwrap();
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
        assert_eq!(goto_count, 5);
        assert!(program
            .insns
            .iter()
            .all(|(insn, _)| !matches!(insn, Insn::IfNot { .. })));
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
            sorter_resources: SmallVec::new(),
            distinct_set_resources: SmallVec::new(),
            parameter_declarations: SmallVec::new(),
        };

        let error = ir.verify().unwrap_err();
        assert!(error.to_string().contains("does not dominate"));
    }

    #[test]
    fn once_initialization_does_not_weaken_value_ssa_dominance() {
        let mut builder = IrBuilder::new();
        let initialize = builder.create_block().unwrap();
        let ready = builder.create_block().unwrap();
        builder
            .terminate(Terminator::Once { initialize, ready })
            .unwrap();
        builder.switch_to(initialize).unwrap();
        let initialized_value = builder
            .push(ScalarOp::Constant(Value::from_i64(1)))
            .unwrap();
        builder
            .terminate(Terminator::Jump {
                target: ready,
                arguments: SmallVec::new(),
            })
            .unwrap();
        builder.switch_to(ready).unwrap();

        let error = builder.finish(initialized_value).unwrap_err();

        assert!(error.to_string().contains("does not dominate"));
    }

    #[test]
    fn verifier_rejects_a_used_hole_in_the_value_arena() {
        let ir = IrProgram {
            blocks: smallvec![BasicBlock {
                id: BlockId(0),
                parameters: SmallVec::new(),
                instructions: smallvec![Instruction::Value {
                    result: ValueId(0),
                    op: ScalarOp::Constant(Value::Null),
                }],
                terminator: Terminator::Return(ValueId(1)),
            }],
            value_count: 2,
            input_count: 0,
            cursor_input_count: 0,
            cursor_resources: SmallVec::new(),
            sorter_resources: SmallVec::new(),
            distinct_set_resources: SmallVec::new(),
            parameter_declarations: SmallVec::new(),
        };

        let error = ir.verify().unwrap_err();

        assert!(error
            .to_string()
            .contains("uses undefined value ValueId(1)"));
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
            sorter_resources: SmallVec::new(),
            distinct_set_resources: SmallVec::new(),
            parameter_declarations: SmallVec::new(),
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
            .terminate(Terminator::CursorStart {
                cursor,
                direction: ScanDirection::Forward,
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
            sorter_resources: SmallVec::new(),
            distinct_set_resources: SmallVec::new(),
            parameter_declarations: SmallVec::new(),
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
    fn verifier_checks_both_deferred_seek_cursors() {
        let table = Arc::new(BTreeTable::from_sql("CREATE TABLE t(a,b)", 2).unwrap());
        let index = test_index(&table, "t_b", 3);
        let mut builder = IrBuilder::new();
        let table_cursor = builder
            .allocate_cursor(CursorResource::Owned(CursorType::BTreeTable(table)))
            .unwrap();
        let index_cursor = builder
            .allocate_cursor(CursorResource::Owned(CursorType::BTreeIndex(index)))
            .unwrap();
        builder
            .push_effect(EffectOp::OpenRead {
                cursor: index_cursor,
                root_page: 3,
                db: 0,
                schema_cookie: 0,
            })
            .unwrap();
        builder
            .push_effect(EffectOp::DeferredSeek {
                index: index_cursor,
                table: table_cursor,
            })
            .unwrap();
        let value = builder.push(ScalarOp::Constant(Value::Null)).unwrap();

        let error = builder.finish(value).unwrap_err();

        assert!(error
            .to_string()
            .contains("uses unopened cursor CursorId(0)"));
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
    fn ordered_table_scan_runs_symbolic_sort_pipeline_through_vdbe() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE sorted(key, payload, value)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO sorted VALUES \
                 (NULL, 'null', 0), ('b', 'bee', 3), ('A', 'aye', 2), \
                 ('a', 'lower', 1), ('C', 'see', 4)",
            )
            .unwrap();

        let rows = connection
            .prepare(
                "SELECT payload, value + 1 FROM sorted WHERE value >= 0 \
                 ORDER BY key COLLATE NOCASE DESC NULLS FIRST, value ASC \
                 LIMIT 3 OFFSET 1",
            )
            .unwrap()
            .run_collect_rows()
            .unwrap();

        assert_eq!(
            rows,
            vec![
                vec![Value::from_text("see"), Value::from_i64(5)],
                vec![Value::from_text("bee"), Value::from_i64(4)],
                vec![Value::from_text("lower"), Value::from_i64(2)],
            ]
        );
    }

    #[test]
    fn distinct_table_scan_runs_symbolic_stream_pipeline_through_vdbe() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE deduplicated(key COLLATE NOCASE, payload)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO deduplicated VALUES \
                 ('A', 1), ('a', 1), ('b', 2), (NULL, 3), (NULL, 3), ('c', 4)",
            )
            .unwrap();

        let rows = connection
            .prepare("SELECT DISTINCT key, payload FROM deduplicated LIMIT 2 OFFSET 1")
            .unwrap()
            .run_collect_rows()
            .unwrap();

        assert_eq!(
            rows,
            vec![
                vec![Value::from_text("b"), Value::from_i64(2)],
                vec![Value::Null, Value::from_i64(3)],
            ]
        );
    }

    #[test]
    fn distinct_ordered_table_scan_composes_symbolic_stream_stages() {
        let io = Arc::new(MemoryIO::new());
        let database = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let connection = database.connect().unwrap();
        connection
            .execute("CREATE TABLE ordered_distinct(key, sort_key)")
            .unwrap();
        connection
            .execute(
                "INSERT INTO ordered_distinct VALUES \
                 ('a', 2), ('A', 1), ('b', 4), ('B', 3), ('c', 0)",
            )
            .unwrap();

        let rows = connection
            .prepare(
                "SELECT DISTINCT key COLLATE NOCASE FROM ordered_distinct \
                 ORDER BY sort_key DESC LIMIT 2",
            )
            .unwrap()
            .run_collect_rows()
            .unwrap();

        assert_eq!(
            rows,
            vec![vec![Value::from_text("b")], vec![Value::from_text("a")]]
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
