use std::ops::ControlFlow;

use crate::error::{
    SQLITE_CONSTRAINT_CHECK, SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_TRIGGER, SQLITE_ERROR,
};
#[cfg(feature = "json")]
use crate::function::JsonFunc;
use crate::function::{Func, FuncCtx, MathFuncArity, ScalarFunc};
use crate::translate::{expr, semantic::hir};
use crate::util::parse_numeric_literal;
use crate::vdbe::{
    builder::{CursorType, ProgramBuilder, SourceBinding, SubqueryBinding},
    insn::{CmpInsFlags, Insn},
    BranchOffset, CursorID,
};
use crate::{LimboError, Numeric, Result, Value};
use turso_parser::ast::{Literal, Operator, ResolveType, UnaryOperator};

#[derive(Clone, Copy)]
enum BinaryOperands {
    Shared(usize),
    Pair { lhs: usize, rhs: usize },
}

#[derive(Clone, Copy)]
enum BinaryRegisters {
    Ordinary(BinaryOperands),
    Custom(CustomBinaryRegisters),
}

#[derive(Clone, Copy)]
enum CustomBinaryRegisters {
    Direct {
        arguments: usize,
    },
    EncodeLiteral {
        inputs: usize,
        arguments: usize,
        encoder_arguments_start: usize,
    },
}

#[derive(Clone, Copy)]
struct BetweenRegisters {
    value: usize,
    start: usize,
    end: usize,
    width: usize,
    lower: usize,
    upper: usize,
}

#[derive(Clone, Copy)]
struct CaseRegisters {
    base: Option<usize>,
    when: usize,
    return_label: BranchOffset,
    next_label: BranchOffset,
}

#[derive(Clone, Copy)]
struct InListRegisters {
    lhs: usize,
    rhs: usize,
    width: usize,
    null_flag: usize,
    result: usize,
    match_label: BranchOffset,
    false_label: BranchOffset,
    null_label: BranchOffset,
}

#[derive(Clone, Copy)]
struct InSubqueryRegisters {
    lhs: usize,
    width: usize,
    null_rewind_label: BranchOffset,
}

#[derive(Clone, Copy)]
struct LikeRegisters {
    start: usize,
    result: usize,
}

#[derive(Clone, Copy)]
struct ConcatWsRegisters {
    result: usize,
    arguments: usize,
}

#[derive(Clone, Copy)]
struct IfNullRegisters {
    value: usize,
    copy_label: BranchOffset,
}

#[derive(Clone, Copy)]
struct IifRegisters {
    condition: usize,
    end_label: BranchOffset,
    false_label: BranchOffset,
}

#[derive(Clone, Copy)]
struct CoalesceRegisters {
    end_label: BranchOffset,
}

#[derive(Clone, Copy)]
struct SubscriptRegisters {
    base: usize,
    index: usize,
}

#[derive(Clone, Copy)]
struct CursorColumn {
    cursor: usize,
    column: usize,
}

enum ColumnChild<'expr> {
    Generated(&'expr hir::Expr),
    Default(&'expr hir::Expr),
    Program {
        phase: ColumnProgram,
        child: SchemaCallChild<'expr>,
    },
}

enum SchemaCallChild<'expr> {
    Argument {
        call: usize,
        argument: usize,
        argument_count: usize,
        expression: &'expr hir::Expr,
    },
    Body {
        call: usize,
        argument_count: usize,
        input_source: hir::SourceId,
        expression: &'expr hir::Expr,
    },
}

enum CastChild<'expr> {
    Value(&'expr hir::Expr),
    Program {
        phase: CastProgram,
        child: SchemaCallChild<'expr>,
    },
}

enum BinaryChild<'expr> {
    Operand(&'expr hir::Expr),
    Encoder(SchemaCallChild<'expr>),
}

impl<'expr> ColumnChild<'expr> {
    const fn expression(&self) -> &'expr hir::Expr {
        match self {
            Self::Generated(expression) | Self::Default(expression) => expression,
            Self::Program { child, .. } => child.expression(),
        }
    }
}

impl<'expr> SchemaCallChild<'expr> {
    const fn expression(&self) -> &'expr hir::Expr {
        match self {
            Self::Argument { expression, .. } | Self::Body { expression, .. } => expression,
        }
    }
}

impl<'expr> CastChild<'expr> {
    const fn expression(&self) -> &'expr hir::Expr {
        match self {
            Self::Value(expression) => expression,
            Self::Program { child, .. } => child.expression(),
        }
    }
}

impl<'expr> BinaryChild<'expr> {
    const fn expression(&self) -> &'expr hir::Expr {
        match self {
            Self::Operand(expression) => expression,
            Self::Encoder(child) => child.expression(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ColumnProgram {
    Encode,
    Decode,
}

#[derive(Clone, Copy)]
struct ColumnProgramRegisters {
    call: Option<(ColumnProgram, usize)>,
    arguments_start: usize,
    stored_value: Option<BranchOffset>,
    decode_done: Option<BranchOffset>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CastProgram {
    Encode,
    DomainCheck,
}

#[derive(Clone, Copy)]
struct PendingDomainCheck {
    check: usize,
    result: usize,
}

#[derive(Clone, Copy)]
struct CastProgramRegisters {
    call: Option<(CastProgram, usize)>,
    arguments_start: usize,
    domain_not_null_emitted: bool,
    pending_domain_check: Option<PendingDomainCheck>,
}

#[derive(Clone, Copy)]
enum ExprRegisters {
    None,
    Binary(BinaryRegisters),
    Between(BetweenRegisters),
    Case(CaseRegisters),
    InList(InListRegisters),
    InSubquery(InSubqueryRegisters),
    Like(LikeRegisters),
    ConcatWs(ConcatWsRegisters),
    IfNull(IfNullRegisters),
    Iif(IifRegisters),
    Coalesce(CoalesceRegisters),
    Array(usize),
    Subscript(SubscriptRegisters),
    FieldAccess(usize),
    RaiseMessage(usize),
    Function(usize),
    ColumnPrograms(ColumnProgramRegisters),
    CastPrograms(CastProgramRegisters),
}

enum NullTest {
    IsNull,
    NotNull,
}

struct LoweringContext {
    target: usize,
    registers: ExprRegisters,
}

impl LoweringContext {
    const fn new(target: usize) -> Self {
        Self {
            target,
            registers: ExprRegisters::None,
        }
    }
}

fn binary_registers(registers: ExprRegisters) -> BinaryOperands {
    let ExprRegisters::Binary(BinaryRegisters::Ordinary(operands)) = registers else {
        unreachable!("binary operand registers were allocated")
    };
    operands
}

const fn custom_operand_position(operand: hir::BinaryOperand, swap_args: bool) -> usize {
    match (operand, swap_args) {
        (hir::BinaryOperand::Left, false) | (hir::BinaryOperand::Right, true) => 0,
        (hir::BinaryOperand::Right, false) | (hir::BinaryOperand::Left, true) => 1,
    }
}

fn comparison_flags(component: &hir::ComparisonComponent) -> CmpInsFlags {
    let flags = CmpInsFlags::default().with_affinity(component.affinity);
    if component.array {
        flags.array_cmp()
    } else {
        flags
    }
}

fn comparison_collation(
    component: &hir::ComparisonComponent,
) -> Option<crate::translate::collate::CollationSeq> {
    component
        .collation
        .as_ref()
        .map(|collation| *collation.value())
}

#[derive(Clone, Copy)]
enum EmptyArgumentStart {
    UseTarget,
    UseZero,
    UseNextRegister,
    ReserveOneRegister,
}

fn plain_function_lowering(function: &Func) -> Option<EmptyArgumentStart> {
    match function {
        Func::External(_) | Func::Dialect(_) | Func::Vector(_) => {
            Some(EmptyArgumentStart::UseTarget)
        }
        #[cfg(all(feature = "fts", not(target_family = "wasm")))]
        Func::Fts(_) => Some(EmptyArgumentStart::UseNextRegister),
        Func::Math(function) if matches!(function.arity(), MathFuncArity::Nullary) => {
            Some(EmptyArgumentStart::UseZero)
        }
        Func::Math(_) => Some(EmptyArgumentStart::UseTarget),
        Func::Scalar(
            ScalarFunc::Changes
            | ScalarFunc::TotalChanges
            | ScalarFunc::Random
            | ScalarFunc::Date
            | ScalarFunc::DateTime
            | ScalarFunc::JulianDay
            | ScalarFunc::UnixEpoch
            | ScalarFunc::Time
            | ScalarFunc::StrfTime,
        ) => Some(EmptyArgumentStart::ReserveOneRegister),
        #[cfg(feature = "test_helper")]
        Func::Scalar(ScalarFunc::TestNondetCounter) => Some(EmptyArgumentStart::ReserveOneRegister),
        Func::Scalar(
            ScalarFunc::Abs
            | ScalarFunc::Like
            | ScalarFunc::Glob
            | ScalarFunc::Lower
            | ScalarFunc::Upper
            | ScalarFunc::Length
            | ScalarFunc::OctetLength
            | ScalarFunc::Typeof
            | ScalarFunc::Unicode
            | ScalarFunc::Unistr
            | ScalarFunc::UnistrQuote
            | ScalarFunc::Quote
            | ScalarFunc::RandomBlob
            | ScalarFunc::Sign
            | ScalarFunc::Soundex
            | ScalarFunc::ZeroBlob
            | ScalarFunc::CurrVal
            | ScalarFunc::SequenceWatermark
            | ScalarFunc::TimeDiff
            | ScalarFunc::Hex
            | ScalarFunc::Nullif
            | ScalarFunc::Instr
            | ScalarFunc::Replace
            | ScalarFunc::Trim
            | ScalarFunc::LTrim
            | ScalarFunc::RTrim
            | ScalarFunc::Round
            | ScalarFunc::Unhex
            | ScalarFunc::Min
            | ScalarFunc::Max
            | ScalarFunc::Concat
            | ScalarFunc::Char
            | ScalarFunc::Printf
            | ScalarFunc::GetByte
            | ScalarFunc::SetByte
            | ScalarFunc::ArrayLength
            | ScalarFunc::ArrayAppend
            | ScalarFunc::ArrayPrepend
            | ScalarFunc::ArrayCat
            | ScalarFunc::ArrayRemove
            | ScalarFunc::ArrayContains
            | ScalarFunc::ArrayPosition
            | ScalarFunc::ArraySlice
            | ScalarFunc::StringToArray
            | ScalarFunc::ArrayToString
            | ScalarFunc::ArrayOverlap
            | ScalarFunc::ArrayContainsAll
            | ScalarFunc::TestUintEncode
            | ScalarFunc::TestUintDecode
            | ScalarFunc::TestUintAdd
            | ScalarFunc::TestUintSub
            | ScalarFunc::TestUintMul
            | ScalarFunc::TestUintDiv
            | ScalarFunc::TestUintLt
            | ScalarFunc::TestUintEq
            | ScalarFunc::StringReverse
            | ScalarFunc::Gcd
            | ScalarFunc::Lcm
            | ScalarFunc::Repeat
            | ScalarFunc::Lpad
            | ScalarFunc::Rpad
            | ScalarFunc::BooleanToInt
            | ScalarFunc::IntToBoolean
            | ScalarFunc::ValidateIpAddr
            | ScalarFunc::NumericEncode
            | ScalarFunc::NumericDecode
            | ScalarFunc::NumericAdd
            | ScalarFunc::NumericSub
            | ScalarFunc::NumericMul
            | ScalarFunc::NumericDiv
            | ScalarFunc::NumericLt
            | ScalarFunc::NumericEq
            | ScalarFunc::TableColumnsJsonArray
            | ScalarFunc::BinRecordJsonObject,
        ) => Some(EmptyArgumentStart::UseNextRegister),
        #[cfg(all(feature = "fs", not(target_family = "wasm")))]
        Func::Scalar(ScalarFunc::LoadExtension) => Some(EmptyArgumentStart::UseNextRegister),
        #[cfg(feature = "json")]
        Func::Json(JsonFunc::JsonRemove) => Some(EmptyArgumentStart::ReserveOneRegister),
        #[cfg(feature = "json")]
        Func::Json(function) if !function.is_internal() => {
            Some(EmptyArgumentStart::UseNextRegister)
        }
        _ => None,
    }
}

/// One expression node whose value was already produced in a register.
///
/// The node is matched by identity. This keeps temporary runtime inputs scoped
/// to the exact resolved expression being lowered.
#[derive(Clone, Copy)]
pub(crate) struct ExprRegisterInput<'expr> {
    expression: &'expr hir::Expr,
    register: usize,
}

impl<'expr> ExprRegisterInput<'expr> {
    pub(crate) const fn new(expression: &'expr hir::Expr, register: usize) -> Self {
        Self {
            expression,
            register,
        }
    }
}

struct ExprLowerer<'program, 'document, 'inputs> {
    program: &'program mut ProgramBuilder,
    document: &'document hir::HirDocument,
    inputs: &'inputs [ExprRegisterInput<'document>],
}

impl<'expr> ExprLowerer<'_, 'expr, '_> {
    fn input_register(&self, expression: &hir::Expr) -> Option<usize> {
        self.inputs
            .iter()
            .find(|input| std::ptr::eq(input.expression, expression))
            .map(|input| input.register)
    }

    fn schema_call_child(
        &self,
        calls: impl IntoIterator<Item = &'expr hir::BoundSchemaCall>,
        mut index: usize,
    ) -> Option<SchemaCallChild<'expr>> {
        for (call_index, call) in calls.into_iter().enumerate() {
            if let Some(expression) = call.arguments.get(index) {
                return Some(SchemaCallChild::Argument {
                    call: call_index,
                    argument: index,
                    argument_count: call.arguments.len(),
                    expression,
                });
            }
            index -= call.arguments.len();
            if index == 0 {
                let program = self.document.schema_program(call.program)?;
                return Some(SchemaCallChild::Body {
                    call: call_index,
                    argument_count: call.arguments.len(),
                    input_source: program.input_source,
                    expression: &program.body,
                });
            }
            index -= 1;
        }
        None
    }

    fn column_child(
        &self,
        reference: hir::ColumnRef,
        mut index: usize,
    ) -> Option<ColumnChild<'expr>> {
        let source = self.document.source(reference.source)?;
        let generated = source.generated_expressions.get(reference.column)?;
        if let hir::ColumnReadExpression::Planned(expression) = generated {
            if index == 0 {
                return Some(ColumnChild::Generated(expression));
            }
            index -= 1;
        }

        let column = source.columns.get(reference.column)?;
        let programs = source
            .column_type_programs
            .get(reference.column)?
            .as_ref()?;
        if !self.program.flags.suppress_custom_type_decode() {
            if let hir::ColumnReadExpression::Planned(default) =
                source.default_expressions.get(reference.column)?
            {
                if !programs.encode.is_empty() {
                    if index == 0 {
                        return Some(ColumnChild::Default(default));
                    }
                    index -= 1;
                    let encode_children: usize = programs
                        .encode
                        .iter()
                        .map(|call| call.arguments.len() + 1)
                        .sum();
                    if index < encode_children {
                        return self
                            .schema_call_child(&programs.encode, index)
                            .map(|child| ColumnChild::Program {
                                phase: ColumnProgram::Encode,
                                child,
                            });
                    }
                    index -= encode_children;
                }
            }

            if column.type_fact.array_dimensions == 0 {
                return self
                    .schema_call_child(&programs.decode, index)
                    .map(|child| ColumnChild::Program {
                        phase: ColumnProgram::Decode,
                        child,
                    });
            }
        }
        None
    }

    fn cast_child(
        &self,
        expression: &'expr hir::Expr,
        target: &'expr hir::TypeName,
        mut index: usize,
    ) -> Option<CastChild<'expr>> {
        if index == 0 {
            return Some(CastChild::Value(expression));
        }
        index -= 1;

        let encode_children: usize = target
            .programs
            .encode
            .iter()
            .map(|call| call.arguments.len() + 1)
            .sum();
        if index < encode_children {
            return self
                .schema_call_child(&target.programs.encode, index)
                .map(|child| CastChild::Program {
                    phase: CastProgram::Encode,
                    child,
                });
        }
        index -= encode_children;

        let checks = target.programs.domain.as_ref()?.checks.as_slice();
        self.schema_call_child(checks.iter().map(|check| &check.call), index)
            .map(|child| CastChild::Program {
                phase: CastProgram::DomainCheck,
                child,
            })
    }

    fn binary_child(
        &self,
        lhs: &'expr hir::Expr,
        rhs: &'expr hir::Expr,
        custom: Option<&'expr hir::CustomBinaryOperator>,
        mut index: usize,
    ) -> Option<BinaryChild<'expr>> {
        let (first, second) = match custom {
            Some(custom) if custom.swap_args => (rhs, lhs),
            _ => (lhs, rhs),
        };
        match index {
            0 => return Some(BinaryChild::Operand(first)),
            1 => return Some(BinaryChild::Operand(second)),
            _ => index -= 2,
        }

        let encoder = custom?.literal_encoding.as_ref()?.encoder.as_ref()?;
        self.schema_call_child(std::iter::once(encoder), index)
            .map(BinaryChild::Encoder)
    }
}

impl<'expr> hir::ExprVisitor<'expr> for ExprLowerer<'_, 'expr, '_> {
    type Context = LoweringContext;
    type Output = usize;
    type Error = LimboError;

    fn child(&mut self, expression: &'expr hir::Expr, index: usize) -> Option<&'expr hir::Expr> {
        if self.input_register(expression).is_some() {
            return None;
        }
        match expression {
            hir::Expr::Column(reference) => self
                .column_child(*reference, index)
                .map(|child| child.expression()),
            hir::Expr::Cast { expr, target } => self
                .cast_child(expr, target, index)
                .map(|child| child.expression()),
            hir::Expr::Binary {
                lhs, rhs, custom, ..
            } => self
                .binary_child(lhs, rhs, custom.as_ref(), index)
                .map(|child| child.expression()),
            hir::Expr::Subquery(hir::SubqueryExpr::In { lhs, .. }) => match lhs.as_ref() {
                hir::Expr::Row(values) => values.get(index),
                expression => (index == 0).then_some(expression),
            },
            _ => expression.child(index),
        }
    }

    fn pre_order(
        &mut self,
        parent: &hir::Expr,
        context: &mut LoweringContext,
        child_index: usize,
        child: &hir::Expr,
    ) -> Result<ControlFlow<(), LoweringContext>> {
        // Query lowering evaluates aggregate/window inputs while stepping the
        // function. Reading its result must not evaluate those inputs again.
        if matches!(
            parent,
            hir::Expr::Function(hir::FunctionCall {
                evaluation: hir::FunctionEvaluation::Aggregate { .. }
                    | hir::FunctionEvaluation::Window { .. },
                ..
            })
        ) {
            return Ok(ControlFlow::Break(()));
        }

        let target = match parent {
            hir::Expr::Column(reference) => {
                let Some(column_child) = self.column_child(*reference, child_index) else {
                    return Err(LimboError::InternalError(format!(
                        "HIR column {}.{} has an invalid linked child {child_index}",
                        reference.source, reference.column
                    )));
                };
                match column_child {
                    ColumnChild::Generated(_) => context.target,
                    ColumnChild::Default(_) => {
                        self.begin_encoded_default(*reference, context)?;
                        context.target
                    }
                    ColumnChild::Program { phase, child } => match child {
                        SchemaCallChild::Argument {
                            call,
                            argument,
                            argument_count,
                            ..
                        } => {
                            if phase == ColumnProgram::Decode {
                                self.begin_column_decode(*reference, context)?;
                            }
                            let ExprRegisters::ColumnPrograms(registers) = &mut context.registers
                            else {
                                unreachable!("column program registers were allocated")
                            };
                            if registers.call != Some((phase, call)) {
                                registers.call = Some((phase, call));
                                registers.arguments_start =
                                    self.program.alloc_registers(argument_count);
                            }
                            registers.arguments_start + argument
                        }
                        SchemaCallChild::Body {
                            call,
                            argument_count,
                            input_source,
                            ..
                        } => {
                            if phase == ColumnProgram::Decode {
                                self.begin_column_decode(*reference, context)?;
                            }
                            let ExprRegisters::ColumnPrograms(registers) = &mut context.registers
                            else {
                                unreachable!("column program registers were allocated")
                            };
                            if registers.call != Some((phase, call)) {
                                registers.call = Some((phase, call));
                                registers.arguments_start =
                                    self.program.alloc_registers(argument_count);
                            }
                            self.program.bind_source(
                                input_source,
                                SourceBinding::SchemaInputs {
                                    value: context.target,
                                    arguments_start: registers.arguments_start,
                                },
                            );
                            context.target
                        }
                    },
                }
            }
            hir::Expr::MergedColumn(column) => match column.value {
                hir::MergedColumnValue::Left | hir::MergedColumnValue::Right => context.target,
                hir::MergedColumnValue::Coalesce => {
                    if matches!(context.registers, ExprRegisters::None) {
                        context.registers = ExprRegisters::Coalesce(CoalesceRegisters {
                            end_label: self.program.allocate_label(),
                        });
                    }
                    let ExprRegisters::Coalesce(registers) = context.registers else {
                        unreachable!("merged-column registers were allocated")
                    };
                    if child_index == 1 {
                        self.program.emit_insn(Insn::NotNull {
                            reg: context.target,
                            target_pc: registers.end_label,
                        });
                    }
                    context.target
                }
            },
            hir::Expr::Unary { operator, .. } => match operator {
                UnaryOperator::Positive => context.target,
                UnaryOperator::Negative
                    if matches!(child, hir::Expr::Literal(Literal::Numeric(_))) =>
                {
                    return Ok(ControlFlow::Break(()));
                }
                UnaryOperator::BitwiseNot
                    if matches!(
                        child,
                        hir::Expr::Literal(Literal::Numeric(_) | Literal::Null)
                    ) =>
                {
                    return Ok(ControlFlow::Break(()));
                }
                UnaryOperator::Negative | UnaryOperator::BitwiseNot | UnaryOperator::Not => {
                    self.program.alloc_register()
                }
            },
            hir::Expr::IsNull(_) | hir::Expr::NotNull(_) | hir::Expr::TruthTest { .. } => {
                self.program.alloc_register()
            }
            hir::Expr::Collate { .. } => context.target,
            hir::Expr::Cast {
                expr,
                target: cast_target,
            } => {
                let Some(cast_child) = self.cast_child(expr, cast_target, child_index) else {
                    return Err(LimboError::InternalError(format!(
                        "HIR CAST has an invalid linked child {child_index}"
                    )));
                };
                match cast_child {
                    CastChild::Value(_) => context.target,
                    CastChild::Program { phase, child } => match child {
                        SchemaCallChild::Argument {
                            call,
                            argument,
                            argument_count,
                            ..
                        } => {
                            let arguments_start = self.begin_cast_call(
                                cast_target,
                                context,
                                phase,
                                call,
                                argument_count,
                            )?;
                            arguments_start + argument
                        }
                        SchemaCallChild::Body {
                            call,
                            argument_count,
                            input_source,
                            ..
                        } => {
                            let arguments_start = self.begin_cast_call(
                                cast_target,
                                context,
                                phase,
                                call,
                                argument_count,
                            )?;
                            self.program.bind_source(
                                input_source,
                                SourceBinding::SchemaInputs {
                                    value: context.target,
                                    arguments_start,
                                },
                            );
                            match phase {
                                CastProgram::Encode => context.target,
                                CastProgram::DomainCheck => {
                                    let result = self.program.alloc_register();
                                    let ExprRegisters::CastPrograms(registers) =
                                        &mut context.registers
                                    else {
                                        unreachable!("CAST program registers were allocated")
                                    };
                                    registers.pending_domain_check = Some(PendingDomainCheck {
                                        check: call,
                                        result,
                                    });
                                    result
                                }
                            }
                        }
                    },
                }
            }
            hir::Expr::Binary {
                lhs,
                rhs,
                custom,
                comparison,
                ..
            } => {
                if matches!(context.registers, ExprRegisters::None) {
                    let registers = if let Some(custom) = custom {
                        BinaryRegisters::Custom(
                            if let Some(encoder) = custom
                                .literal_encoding
                                .as_ref()
                                .and_then(|encoding| encoding.encoder.as_ref())
                            {
                                CustomBinaryRegisters::EncodeLiteral {
                                    inputs: self.program.alloc_registers(2),
                                    arguments: self.program.alloc_registers(2),
                                    encoder_arguments_start: self
                                        .program
                                        .alloc_registers(encoder.arguments.len()),
                                }
                            } else {
                                CustomBinaryRegisters::Direct {
                                    arguments: self.program.alloc_registers(2),
                                }
                            },
                        )
                    } else {
                        let width = comparison
                            .as_ref()
                            .map_or(1, |comparison| comparison.components.len());
                        let operands = if lhs.equivalent(rhs) {
                            BinaryOperands::Shared(self.program.alloc_registers(width))
                        } else {
                            let lhs = self.program.alloc_registers(width * 2);
                            BinaryOperands::Pair {
                                lhs,
                                rhs: lhs + width,
                            }
                        };
                        BinaryRegisters::Ordinary(operands)
                    };
                    context.registers = ExprRegisters::Binary(registers);
                }
                match (context.registers, child_index) {
                    (
                        ExprRegisters::Binary(BinaryRegisters::Ordinary(BinaryOperands::Shared(
                            register,
                        ))),
                        0,
                    ) => register,
                    (
                        ExprRegisters::Binary(BinaryRegisters::Ordinary(BinaryOperands::Shared(_))),
                        1,
                    ) => {
                        return Ok(ControlFlow::Break(()));
                    }
                    (
                        ExprRegisters::Binary(BinaryRegisters::Ordinary(BinaryOperands::Pair {
                            lhs,
                            ..
                        })),
                        0,
                    ) => lhs,
                    (
                        ExprRegisters::Binary(BinaryRegisters::Ordinary(BinaryOperands::Pair {
                            rhs,
                            ..
                        })),
                        1,
                    ) => rhs,
                    (
                        ExprRegisters::Binary(BinaryRegisters::Custom(
                            CustomBinaryRegisters::Direct { arguments },
                        )),
                        0 | 1,
                    ) => arguments + child_index,
                    (
                        ExprRegisters::Binary(BinaryRegisters::Custom(
                            CustomBinaryRegisters::EncodeLiteral {
                                inputs,
                                arguments,
                                encoder_arguments_start,
                            },
                        )),
                        _,
                    ) => {
                        let custom = custom
                            .as_ref()
                            .expect("custom binary registers require a custom operator");
                        let encoding = custom
                            .literal_encoding
                            .as_ref()
                            .expect("literal encoding registers require literal encoding");
                        if child_index == 2 {
                            let literal =
                                custom_operand_position(encoding.operand, custom.swap_args);
                            let column = 1 - literal;
                            self.program.emit_insn(Insn::Copy {
                                src_reg: inputs + column,
                                dst_reg: arguments + column,
                                extra_amount: 0,
                            });
                        }
                        let Some(child) = self.binary_child(lhs, rhs, Some(custom), child_index)
                        else {
                            return Err(LimboError::InternalError(format!(
                                "HIR custom binary operator has an invalid linked child {child_index}"
                            )));
                        };
                        match child {
                            BinaryChild::Operand(_) => inputs + child_index,
                            BinaryChild::Encoder(SchemaCallChild::Argument {
                                call,
                                argument,
                                argument_count,
                                ..
                            }) => {
                                debug_assert_eq!(call, 0);
                                debug_assert_eq!(
                                    argument_count,
                                    encoding.encoder.as_ref().unwrap().arguments.len()
                                );
                                encoder_arguments_start + argument
                            }
                            BinaryChild::Encoder(SchemaCallChild::Body {
                                call,
                                argument_count,
                                input_source,
                                ..
                            }) => {
                                debug_assert_eq!(call, 0);
                                debug_assert_eq!(
                                    argument_count,
                                    encoding.encoder.as_ref().unwrap().arguments.len()
                                );
                                let literal =
                                    custom_operand_position(encoding.operand, custom.swap_args);
                                self.program.bind_source(
                                    input_source,
                                    SourceBinding::SchemaInputs {
                                        value: inputs + literal,
                                        arguments_start: encoder_arguments_start,
                                    },
                                );
                                arguments + literal
                            }
                        }
                    }
                    (_, _) => unreachable!("binary expression has valid linked children"),
                }
            }
            hir::Expr::Between {
                expr,
                negated,
                start,
                end,
                start_comparison,
                ..
            } => {
                if matches!(context.registers, ExprRegisters::None) {
                    let width = start_comparison.components.len();
                    if width == 0 {
                        return Err(LimboError::InternalError(
                            "HIR BETWEEN comparison has no components".to_string(),
                        ));
                    }
                    let value = self.program.alloc_registers(width);
                    let lower = self.program.alloc_register();
                    let start_register = if expr.equivalent(start) {
                        value
                    } else {
                        self.program.alloc_registers(width)
                    };
                    let upper = self.program.alloc_register();
                    let end_register = if expr.equivalent(end) {
                        value
                    } else {
                        self.program.alloc_registers(width)
                    };
                    context.registers = ExprRegisters::Between(BetweenRegisters {
                        value,
                        start: start_register,
                        end: end_register,
                        width,
                        lower,
                        upper,
                    });
                }
                let ExprRegisters::Between(registers) = context.registers else {
                    unreachable!("BETWEEN registers were allocated")
                };
                debug_assert_eq!(registers.width, start_comparison.components.len());
                match child_index {
                    0 => registers.value,
                    1 if registers.start == registers.value => {
                        return Ok(ControlFlow::Break(()));
                    }
                    1 => registers.start,
                    2 => {
                        self.emit_comparison(
                            if *negated {
                                Operator::Less
                            } else {
                                Operator::GreaterEquals
                            },
                            start_comparison,
                            registers.value,
                            registers.start,
                            registers.lower,
                        )?;
                        if registers.end == registers.value {
                            return Ok(ControlFlow::Break(()));
                        }
                        registers.end
                    }
                    _ => unreachable!("BETWEEN has three children"),
                }
            }
            hir::Expr::Case {
                base,
                when_then,
                base_comparisons,
                ..
            } => {
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers = ExprRegisters::Case(CaseRegisters {
                        base: base.as_ref().map(|_| self.program.alloc_register()),
                        when: self.program.alloc_register(),
                        return_label: self.program.allocate_label(),
                        next_label: self.program.allocate_label(),
                    });
                }
                let ExprRegisters::Case(registers) = &mut context.registers else {
                    unreachable!("CASE registers were allocated")
                };
                let pair_start = usize::from(base.is_some());
                if child_index < pair_start {
                    registers.base.expect("simple CASE has a base register")
                } else if child_index < pair_start + when_then.len() * 2 {
                    let pair_child = child_index - pair_start;
                    let pair_index = pair_child / 2;
                    if pair_child % 2 == 0 {
                        if pair_index > 0 {
                            self.finish_case_arm(registers);
                        }
                        registers.when
                    } else {
                        self.emit_case_test(registers, base_comparisons.get(pair_index))?;
                        context.target
                    }
                } else {
                    debug_assert!(when_then.len() * 2 + pair_start == child_index);
                    if !when_then.is_empty() {
                        self.finish_case_arm(registers);
                    }
                    context.target
                }
            }
            hir::Expr::InList {
                values,
                comparisons,
                ..
            } => {
                if values.is_empty() {
                    if child_index == 0 {
                        return Ok(ControlFlow::Break(()));
                    }
                    unreachable!("empty IN list only has its skipped left operand")
                }
                if comparisons.len() != values.len() {
                    return Err(LimboError::InternalError(
                        "HIR IN list has mismatched values and comparisons".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    let width = comparisons[0].components.len();
                    if width == 0
                        || comparisons
                            .iter()
                            .any(|comparison| comparison.components.len() != width)
                    {
                        return Err(LimboError::InternalError(
                            "HIR IN list has invalid comparison widths".to_string(),
                        ));
                    }
                    let false_label = self.program.allocate_label();
                    let null_label = self.program.allocate_label();
                    let result = self.program.alloc_register();
                    self.program.emit_no_constant_insn(Insn::Null {
                        dest: result,
                        dest_end: None,
                    });
                    let lhs = self.program.alloc_registers(width);
                    let match_label = self.program.allocate_label();
                    let null_flag = self.program.alloc_register();
                    context.registers = ExprRegisters::InList(InListRegisters {
                        lhs,
                        rhs: 0,
                        width,
                        null_flag,
                        result,
                        match_label,
                        false_label,
                        null_label,
                    });
                }
                let ExprRegisters::InList(registers) = &mut context.registers else {
                    unreachable!("IN list registers were allocated")
                };
                match child_index {
                    0 => registers.lhs,
                    value_child => {
                        if value_child == 1 {
                            self.program.emit_insn(Insn::BitAnd {
                                lhs: registers.lhs,
                                rhs: registers.lhs,
                                dest: registers.null_flag,
                            });
                        } else {
                            self.finish_in_list_value(registers, &comparisons[value_child - 2])?;
                        }
                        registers.rhs = self.program.alloc_registers(registers.width);
                        registers.rhs
                    }
                }
            }
            hir::Expr::Subquery(hir::SubqueryExpr::In { comparison, .. }) => {
                let width = comparison.components.len();
                if width == 0 {
                    return Err(LimboError::InternalError(
                        "HIR IN-subquery comparison has no components".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    self.program.emit_insn(Insn::Integer {
                        value: 0,
                        dest: context.target,
                    });
                    context.registers = ExprRegisters::InSubquery(InSubqueryRegisters {
                        lhs: self.program.alloc_registers(width),
                        width,
                        null_rewind_label: self.program.allocate_label(),
                    });
                }
                let ExprRegisters::InSubquery(registers) = context.registers else {
                    unreachable!("IN-subquery registers were allocated")
                };
                if child_index >= registers.width {
                    return Err(LimboError::InternalError(format!(
                        "HIR IN-subquery operand {child_index} exceeds comparison width {}",
                        registers.width
                    )));
                }
                if child_index > 0 {
                    self.emit_in_subquery_null_check(
                        registers.lhs + child_index - 1,
                        registers.null_rewind_label,
                    );
                }
                registers.lhs + child_index
            }
            hir::Expr::Like {
                negated,
                operator,
                argument_count,
                ..
            } => {
                if *argument_count < 2 {
                    return Err(LimboError::InternalError(
                        "HIR LIKE-family expression has fewer than two arguments".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers = ExprRegisters::Like(LikeRegisters {
                        start: self.program.alloc_registers(*argument_count),
                        result: if *negated {
                            self.program.alloc_register()
                        } else {
                            context.target
                        },
                    });
                }
                let ExprRegisters::Like(registers) = context.registers else {
                    unreachable!("LIKE-family registers were allocated")
                };
                match (operator, child_index) {
                    (turso_parser::ast::LikeOperator::Match, 0) => registers.start,
                    (turso_parser::ast::LikeOperator::Match, 1) => {
                        registers.start + argument_count - 1
                    }
                    (turso_parser::ast::LikeOperator::Match, _) => {
                        unreachable!("MATCH has two expression children")
                    }
                    (_, 0) => registers.start + 1,
                    (_, 1) => registers.start,
                    (_, 2) => registers.start + 2,
                    (_, _) => unreachable!("LIKE-family expression has at most three children"),
                }
            }
            hir::Expr::Array(elements) => {
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers =
                        ExprRegisters::Array(self.program.alloc_registers(elements.len()));
                }
                let ExprRegisters::Array(start) = context.registers else {
                    unreachable!("array element registers were allocated")
                };
                start + child_index
            }
            hir::Expr::Subscript { .. } => {
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers = ExprRegisters::Subscript(SubscriptRegisters {
                        base: self.program.alloc_register(),
                        index: self.program.alloc_register(),
                    });
                }
                let ExprRegisters::Subscript(registers) = context.registers else {
                    unreachable!("subscript registers were allocated")
                };
                match child_index {
                    0 => registers.base,
                    1 => registers.index,
                    _ => unreachable!("subscript has two children"),
                }
            }
            hir::Expr::FieldAccess(_) => {
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers = ExprRegisters::FieldAccess(self.program.alloc_register());
                }
                let ExprRegisters::FieldAccess(base) = context.registers else {
                    unreachable!("field-access base register was allocated")
                };
                debug_assert_eq!(child_index, 0);
                base
            }
            hir::Expr::Raise {
                action, message, ..
            } => {
                debug_assert_eq!(child_index, 0);
                if *action == ResolveType::Ignore
                    || matches!(
                        message.as_deref(),
                        Some(hir::Expr::Literal(Literal::String(_)))
                    )
                {
                    return Ok(ControlFlow::Break(()));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers = ExprRegisters::RaiseMessage(self.program.alloc_register());
                }
                let ExprRegisters::RaiseMessage(message) = context.registers else {
                    unreachable!("RAISE message register was allocated")
                };
                message
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::CustomType(_)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "custom-type function has invalid HIR arguments".to_string(),
                    ));
                };
                let runtime_argument = match &call.operation {
                    hir::FunctionOperation::CustomType(hir::CustomTypeOperation::UnionValue {
                        ..
                    }) if values.len() == 2 => 1,
                    hir::FunctionOperation::CustomType(hir::CustomTypeOperation::UnionTag {
                        ..
                    }) if values.len() == 1 => 0,
                    hir::FunctionOperation::CustomType(
                        hir::CustomTypeOperation::UnionExtract { .. }
                        | hir::CustomTypeOperation::StructExtract { .. },
                    ) if values.len() == 2 => 0,
                    _ => {
                        return Err(LimboError::InternalError(
                            "custom-type function has invalid HIR arguments".to_string(),
                        ));
                    }
                };
                if !order_by.is_empty() {
                    return Err(LimboError::InternalError(
                        "custom-type function has argument ordering".to_string(),
                    ));
                }
                if child_index != runtime_argument {
                    return Ok(ControlFlow::Break(()));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers = ExprRegisters::Function(self.program.alloc_register());
                }
                let ExprRegisters::Function(argument) = context.registers else {
                    unreachable!("custom-type function register was allocated")
                };
                argument
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Sequence(_)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "sequence function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() {
                    return Err(LimboError::InternalError(
                        "sequence function has argument ordering".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers =
                        ExprRegisters::Function(self.program.alloc_registers(values.len()));
                }
                let ExprRegisters::Function(start) = context.registers else {
                    unreachable!("sequence function registers were allocated")
                };
                start + child_index
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(
                        call.function.value(),
                        Func::Scalar(
                            ScalarFunc::Likely | ScalarFunc::Likelihood | ScalarFunc::Unlikely
                        )
                    ) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                let expected = match call.function.value() {
                    Func::Scalar(ScalarFunc::Likely | ScalarFunc::Unlikely) => 1,
                    Func::Scalar(ScalarFunc::Likelihood) => 2,
                    _ => unreachable!("planner hint was checked by match guard"),
                };
                if !order_by.is_empty() || values.len() != expected {
                    return Err(LimboError::InternalError(
                        "planner hint has invalid HIR arguments".to_string(),
                    ));
                }
                if child_index == 0 {
                    context.target
                } else {
                    debug_assert_eq!(child_index, 1);
                    return Ok(ControlFlow::Break(()));
                }
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(
                        call.function.value(),
                        Func::Scalar(ScalarFunc::Substr | ScalarFunc::Substring)
                    ) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || !matches!(values.len(), 2 | 3) {
                    return Err(LimboError::InternalError(
                        "substring has invalid HIR arguments".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers = ExprRegisters::Function(self.program.alloc_registers(3));
                }
                let ExprRegisters::Function(start) = context.registers else {
                    unreachable!("substring registers were allocated")
                };
                start + child_index
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(
                        call.function.value(),
                        Func::Scalar(ScalarFunc::LastInsertRowid)
                    ) =>
            {
                return Ok(ControlFlow::Break(()));
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(call.function.value(), Func::Scalar(ScalarFunc::Coalesce)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || values.len() < 2 {
                    return Err(LimboError::InternalError(
                        "coalesce has invalid HIR arguments".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers = ExprRegisters::Coalesce(CoalesceRegisters {
                        end_label: self.program.allocate_label(),
                    });
                }
                let ExprRegisters::Coalesce(registers) = context.registers else {
                    unreachable!("coalesce registers were allocated")
                };
                if child_index > 0 {
                    self.program.emit_insn(Insn::NotNull {
                        reg: context.target,
                        target_pc: registers.end_label,
                    });
                }
                context.target
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(call.function.value(), Func::Scalar(ScalarFunc::Iif)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || values.len() < 2 {
                    return Err(LimboError::InternalError(
                        "iif has invalid HIR arguments".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    let end_label = self.program.allocate_label();
                    let condition = self.program.alloc_register();
                    let false_label = self.program.allocate_label();
                    context.registers = ExprRegisters::Iif(IifRegisters {
                        condition,
                        end_label,
                        false_label,
                    });
                }
                let target = context.target;
                let ExprRegisters::Iif(registers) = &mut context.registers else {
                    unreachable!("iif registers were allocated")
                };
                if child_index % 2 == 1 {
                    self.program.emit_insn(Insn::IfNot {
                        reg: registers.condition,
                        target_pc: registers.false_label,
                        jump_if_null: true,
                    });
                    target
                } else if child_index == 0 {
                    registers.condition
                } else {
                    self.program.emit_insn(Insn::Goto {
                        target_pc: registers.end_label,
                    });
                    self.program
                        .preassign_label_to_next_insn(registers.false_label);
                    if child_index == values.len() - 1 {
                        target
                    } else {
                        registers.false_label = self.program.allocate_label();
                        registers.condition
                    }
                }
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(call.function.value(), Func::Scalar(ScalarFunc::IfNull)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || values.len() != 2 {
                    return Err(LimboError::InternalError(
                        "ifnull has invalid HIR arguments".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers = ExprRegisters::IfNull(IfNullRegisters {
                        value: self.program.alloc_register(),
                        copy_label: self.program.allocate_label(),
                    });
                }
                let ExprRegisters::IfNull(registers) = context.registers else {
                    unreachable!("ifnull registers were allocated")
                };
                match child_index {
                    0 => registers.value,
                    1 => {
                        self.program.emit_insn(Insn::NotNull {
                            reg: registers.value,
                            target_pc: registers.copy_label,
                        });
                        registers.value
                    }
                    _ => unreachable!("ifnull has two arguments"),
                }
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(call.function.value(), Func::Scalar(ScalarFunc::ConcatWs)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has argument ordering".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    let result = self.program.alloc_registers(values.len() + 1);
                    context.registers = ExprRegisters::ConcatWs(ConcatWsRegisters {
                        result,
                        arguments: result + 1,
                    });
                }
                let ExprRegisters::ConcatWs(registers) = context.registers else {
                    unreachable!("concat_ws registers were allocated")
                };
                registers.arguments + child_index
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(call.function.value(), Func::Scalar(ScalarFunc::StructPack)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "struct_pack has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() {
                    return Err(LimboError::InternalError(
                        "struct_pack has argument ordering".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers =
                        ExprRegisters::Function(self.program.alloc_registers(values.len()));
                }
                let ExprRegisters::Function(start) = context.registers else {
                    unreachable!("struct_pack registers were allocated")
                };
                start + child_index
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && plain_function_lowering(call.function.value()).is_some() =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has argument ordering".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers =
                        ExprRegisters::Function(self.program.alloc_registers(values.len()));
                }
                let ExprRegisters::Function(start) = context.registers else {
                    unreachable!("function argument registers were allocated")
                };
                start + child_index
            }
            hir::Expr::Row(_) => context.target + child_index,
            _ => return Ok(ControlFlow::Break(())),
        };
        Ok(ControlFlow::Continue(LoweringContext::new(target)))
    }

    fn post_order(
        &mut self,
        expression: &hir::Expr,
        context: LoweringContext,
        children: &[usize],
    ) -> Result<usize> {
        let target = context.target;
        if let Some(register) = self.input_register(expression) {
            debug_assert!(children.is_empty());
            if register != target {
                self.program.emit_insn(Insn::Copy {
                    src_reg: register,
                    dst_reg: target,
                    extra_amount: 0,
                });
            }
            return Ok(target);
        }
        match expression {
            hir::Expr::Literal(literal) => emit_literal(self.program, literal, target),
            hir::Expr::Parameter(parameter) => Ok(emit_parameter(self.program, parameter, target)),
            hir::Expr::Column(column) => {
                self.emit_column(*column, target, context.registers, children)
            }
            hir::Expr::MergedColumn(column) => {
                match column.value {
                    hir::MergedColumnValue::Left | hir::MergedColumnValue::Right => {
                        debug_assert_eq!(children, [target]);
                    }
                    hir::MergedColumnValue::Coalesce => {
                        debug_assert_eq!(children, [target, target]);
                        let ExprRegisters::Coalesce(registers) = context.registers else {
                            unreachable!("merged-column registers were allocated")
                        };
                        self.program
                            .preassign_label_to_next_insn(registers.end_label);
                    }
                }
                Ok(target)
            }
            hir::Expr::RowId(source) => self.emit_rowid(*source, target),
            hir::Expr::Output(output) => {
                let binding = self.program.output_binding(*output).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "HIR output {output:?} has no runtime binding"
                    ))
                })?;
                if binding.register != target {
                    self.program.emit_insn(Insn::Copy {
                        src_reg: binding.register,
                        dst_reg: target,
                        extra_amount: 0,
                    });
                }
                Ok(target)
            }
            hir::Expr::Subquery(hir::SubqueryExpr::Scalar { query, output }) => {
                let Some(SubqueryBinding::RowValue { start, count }) =
                    self.program.subquery_binding(*query)
                else {
                    return Err(LimboError::InternalError(format!(
                        "HIR scalar subquery {query:?} has no row-value runtime binding"
                    )));
                };
                if *output >= count {
                    return Err(LimboError::InternalError(format!(
                        "HIR scalar subquery {query:?} output {output} exceeds runtime width {count}"
                    )));
                }
                self.program.emit_insn(Insn::Copy {
                    src_reg: start + *output,
                    dst_reg: target,
                    extra_amount: 0,
                });
                Ok(target)
            }
            hir::Expr::Subquery(hir::SubqueryExpr::Row { query }) => {
                let Some(SubqueryBinding::RowValue { start, count }) =
                    self.program.subquery_binding(*query)
                else {
                    return Err(LimboError::InternalError(format!(
                        "HIR row subquery {query:?} has no row-value runtime binding"
                    )));
                };
                let expected = self
                    .document
                    .query(*query)
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "HIR row subquery references missing query {query:?}"
                        ))
                    })?
                    .output
                    .len();
                if count != expected {
                    return Err(LimboError::InternalError(format!(
                        "HIR row subquery {query:?} runtime width {count} does not match output width {expected}"
                    )));
                }
                expr::assert_vector_register_range_allocated(self.program, target, count)?;
                self.program.emit_insn(Insn::Copy {
                    src_reg: start,
                    dst_reg: target,
                    extra_amount: count - 1,
                });
                Ok(target)
            }
            hir::Expr::Subquery(hir::SubqueryExpr::Exists(query)) => {
                let Some(SubqueryBinding::Exists { register }) =
                    self.program.subquery_binding(*query)
                else {
                    return Err(LimboError::InternalError(format!(
                        "HIR EXISTS subquery {query:?} has no EXISTS runtime binding"
                    )));
                };
                self.program.emit_insn(Insn::Copy {
                    src_reg: register,
                    dst_reg: target,
                    extra_amount: 0,
                });
                Ok(target)
            }
            hir::Expr::Subquery(hir::SubqueryExpr::In {
                query,
                negated,
                comparison,
                ..
            }) => {
                let ExprRegisters::InSubquery(registers) = context.registers else {
                    unreachable!("IN-subquery registers were allocated")
                };
                if children.len() != registers.width {
                    return Err(LimboError::InternalError(format!(
                        "HIR IN-subquery has {} lowered operands for comparison width {}",
                        children.len(),
                        registers.width
                    )));
                }
                let Some(SubqueryBinding::InIndex { cursor }) =
                    self.program.subquery_binding(*query)
                else {
                    return Err(LimboError::InternalError(format!(
                        "HIR IN subquery {query:?} has no index runtime binding"
                    )));
                };
                self.emit_in_subquery(registers, cursor, *negated, comparison, target)
            }
            hir::Expr::Function(hir::FunctionCall {
                evaluation: hir::FunctionEvaluation::Aggregate { id, .. },
                ..
            }) => {
                let register = self.program.aggregate_result_register(*id).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "HIR aggregate {id:?} has no runtime result register"
                    ))
                })?;
                self.program.emit_insn(Insn::Copy {
                    src_reg: register,
                    dst_reg: target,
                    extra_amount: 0,
                });
                Ok(target)
            }
            hir::Expr::Function(hir::FunctionCall {
                evaluation: hir::FunctionEvaluation::Window { id, .. },
                ..
            }) => {
                let register = self.program.window_result_register(*id).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "HIR window function {id:?} has no runtime result register"
                    ))
                })?;
                self.program.emit_insn(Insn::Copy {
                    src_reg: register,
                    dst_reg: target,
                    extra_amount: 0,
                });
                Ok(target)
            }
            hir::Expr::Unary { operator, expr } => {
                self.emit_unary(*operator, expr, target, children)
            }
            hir::Expr::IsNull(_) => self.emit_null_test(NullTest::IsNull, target, children),
            hir::Expr::NotNull(_) => self.emit_null_test(NullTest::NotNull, target, children),
            hir::Expr::TruthTest {
                is_true, negated, ..
            } => {
                let [value] = children else {
                    unreachable!("truth test has one lowered child")
                };
                self.program.emit_insn(Insn::IsTrue {
                    reg: *value,
                    dest: target,
                    null_value: *negated,
                    invert: *negated == *is_true,
                });
                Ok(target)
            }
            hir::Expr::Collate { .. } => {
                let [value] = children else {
                    unreachable!("COLLATE has one lowered child")
                };
                debug_assert_eq!(*value, target);
                Ok(target)
            }
            hir::Expr::Cast {
                target: cast_target,
                ..
            } => self.emit_cast(cast_target, target, context.registers, children),
            hir::Expr::Binary {
                custom: Some(custom),
                ..
            } => {
                let ExprRegisters::Binary(BinaryRegisters::Custom(registers)) = context.registers
                else {
                    unreachable!("custom binary registers were allocated")
                };
                self.emit_custom_binary(custom, registers, target, children)
            }
            hir::Expr::Binary {
                operator: Operator::Concat,
                array_concat,
                custom: None,
                comparison: None,
                ..
            } => self.emit_concat(
                binary_registers(context.registers),
                target,
                children,
                *array_concat,
            ),
            hir::Expr::Binary {
                operator,
                array_concat: false,
                custom: None,
                comparison: Some(comparison),
                ..
            } => self.emit_binary_comparison(
                *operator,
                comparison,
                binary_registers(context.registers),
                target,
                children,
            ),
            hir::Expr::Binary {
                operator: operator @ (Operator::ArrayContains | Operator::ArrayOverlap),
                array_concat: false,
                custom: None,
                comparison: None,
                ..
            } => self.emit_array_binary(
                *operator,
                binary_registers(context.registers),
                target,
                children,
            ),
            hir::Expr::Binary {
                operator,
                array_concat: false,
                custom: None,
                comparison: None,
                ..
            } => self.emit_binary(
                *operator,
                binary_registers(context.registers),
                target,
                children,
            ),
            hir::Expr::Between {
                negated,
                end_comparison,
                ..
            } => {
                let ExprRegisters::Between(registers) = context.registers else {
                    unreachable!("BETWEEN registers were allocated")
                };
                debug_assert_eq!(registers.width, end_comparison.components.len());
                self.emit_comparison(
                    if *negated {
                        Operator::Greater
                    } else {
                        Operator::LessEquals
                    },
                    end_comparison,
                    registers.value,
                    registers.end,
                    registers.upper,
                )?;
                self.program.emit_insn(if *negated {
                    Insn::Or {
                        lhs: registers.lower,
                        rhs: registers.upper,
                        dest: target,
                    }
                } else {
                    Insn::And {
                        lhs: registers.lower,
                        rhs: registers.upper,
                        dest: target,
                    }
                });
                Ok(target)
            }
            hir::Expr::Case {
                when_then,
                else_expr,
                ..
            } => {
                let ExprRegisters::Case(mut registers) = context.registers else {
                    unreachable!("CASE registers were allocated")
                };
                if else_expr.is_none() {
                    if !when_then.is_empty() {
                        self.finish_case_arm(&mut registers);
                    }
                    self.program.emit_insn(Insn::Null {
                        dest: target,
                        dest_end: None,
                    });
                }
                self.program
                    .preassign_label_to_next_insn(registers.return_label);
                Ok(target)
            }
            hir::Expr::InList {
                negated,
                values,
                comparisons,
                ..
            } => {
                if values.is_empty() {
                    debug_assert!(children.is_empty());
                    self.program.emit_insn(Insn::Integer {
                        value: i64::from(*negated),
                        dest: target,
                    });
                    return Ok(target);
                }
                let ExprRegisters::InList(registers) = context.registers else {
                    unreachable!("IN list registers were allocated")
                };
                debug_assert_eq!(children.len(), values.len() + 1);
                debug_assert_eq!(children[0], registers.lhs);
                self.finish_in_list_value(
                    &registers,
                    comparisons.last().expect("non-empty IN has a comparison"),
                )?;
                self.program.emit_insn(Insn::IsNull {
                    reg: registers.null_flag,
                    target_pc: registers.null_label,
                });
                self.program.emit_insn(Insn::Goto {
                    target_pc: registers.false_label,
                });
                self.program
                    .preassign_label_to_next_insn(registers.match_label);
                self.program.emit_insn(Insn::Integer {
                    value: 1,
                    dest: registers.result,
                });
                self.program
                    .preassign_label_to_next_insn(registers.false_label);
                self.program.emit_insn(Insn::AddImm {
                    register: registers.result,
                    value: 0,
                });
                if *negated {
                    self.program.emit_insn(Insn::Not {
                        reg: registers.result,
                        dest: registers.result,
                    });
                }
                self.program
                    .preassign_label_to_next_insn(registers.null_label);
                self.program.emit_insn(Insn::Copy {
                    src_reg: registers.result,
                    dst_reg: target,
                    extra_amount: 0,
                });
                Ok(target)
            }
            hir::Expr::Like {
                negated,
                operator,
                function,
                argument_count,
                rhs,
                escape,
                ..
            } => {
                let ExprRegisters::Like(registers) = context.registers else {
                    unreachable!("LIKE-family registers were allocated")
                };
                let expected_children = 2 + usize::from(escape.is_some());
                debug_assert_eq!(children.len(), expected_children);
                let constant_mask = if matches!(
                    operator,
                    turso_parser::ast::LikeOperator::Like | turso_parser::ast::LikeOperator::Glob
                ) && matches!(rhs.as_ref(), hir::Expr::Literal(_))
                {
                    self.program.mark_last_insn_constant();
                    1
                } else {
                    0
                };
                self.program.emit_insn(Insn::Function {
                    constant_mask,
                    start_reg: registers.start,
                    dest: registers.result,
                    func: FuncCtx {
                        func: function.value().clone(),
                        arg_count: *argument_count,
                    },
                });
                if *negated {
                    self.program.emit_insn(Insn::Not {
                        reg: registers.result,
                        dest: target,
                    });
                }
                Ok(target)
            }
            hir::Expr::Array(elements) => {
                let start = match context.registers {
                    ExprRegisters::Array(start) => start,
                    ExprRegisters::None if elements.is_empty() => self.program.alloc_registers(0),
                    _ => unreachable!("array element registers were allocated"),
                };
                debug_assert!(children
                    .iter()
                    .enumerate()
                    .all(|(index, register)| *register == start + index));
                self.program.emit_insn(Insn::MakeArray {
                    start_reg: start,
                    count: elements.len(),
                    dest: target,
                });
                Ok(target)
            }
            hir::Expr::Subscript { .. } => {
                let ExprRegisters::Subscript(registers) = context.registers else {
                    unreachable!("subscript registers were allocated")
                };
                debug_assert_eq!(children, [registers.base, registers.index]);
                self.program.emit_insn(Insn::ArrayElement {
                    array_reg: registers.base,
                    index_reg: registers.index,
                    dest: target,
                });
                Ok(target)
            }
            hir::Expr::FieldAccess(access) => {
                let ExprRegisters::FieldAccess(base) = context.registers else {
                    unreachable!("field-access base register was allocated")
                };
                debug_assert_eq!(children, [base]);
                self.program.emit_insn(match access.kind {
                    hir::FieldAccessKind::Struct { field_index } => Insn::StructField {
                        src_reg: base,
                        field_index,
                        dest: target,
                    },
                    hir::FieldAccessKind::Union { tag_index } => Insn::UnionExtract {
                        src_reg: base,
                        expected_tag: tag_index,
                        dest: target,
                    },
                });
                Ok(target)
            }
            hir::Expr::Raise { action, message } => {
                let in_trigger = self.program.trigger.is_some();
                match action {
                    ResolveType::Ignore => {
                        if !in_trigger {
                            crate::bail_parse_error!(
                                "RAISE() may only be used within a trigger-program"
                            );
                        }
                        if message.is_some() {
                            return Err(LimboError::InternalError(
                                "RAISE(IGNORE) has a message".to_string(),
                            ));
                        }
                        debug_assert!(children.is_empty());
                        self.program.emit_insn(Insn::Halt {
                            err_code: 0,
                            description: String::new(),
                            on_error: Some(ResolveType::Ignore),
                            description_reg: None,
                        });
                    }
                    action @ (ResolveType::Fail | ResolveType::Abort | ResolveType::Rollback) => {
                        if !in_trigger && *action != ResolveType::Abort {
                            crate::bail_parse_error!(
                                "RAISE() may only be used within a trigger-program"
                            );
                        }
                        let Some(message) = message else {
                            crate::bail_parse_error!("RAISE requires an error message");
                        };
                        let (description, description_reg) = match message.as_ref() {
                            hir::Expr::Literal(Literal::String(value)) => {
                                debug_assert!(children.is_empty());
                                (expr::sanitize_string(value), None)
                            }
                            _ => {
                                let [message] = children else {
                                    unreachable!("dynamic RAISE has one lowered message")
                                };
                                (String::new(), Some(*message))
                            }
                        };
                        self.program.emit_insn(Insn::Halt {
                            err_code: if in_trigger {
                                SQLITE_CONSTRAINT_TRIGGER
                            } else {
                                SQLITE_ERROR
                            },
                            description,
                            on_error: Some(*action),
                            description_reg,
                        });
                    }
                    ResolveType::Replace => {
                        crate::bail_parse_error!("REPLACE is not valid for RAISE");
                    }
                }
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(
                        call.function.value(),
                        Func::Scalar(
                            ScalarFunc::Likely | ScalarFunc::Likelihood | ScalarFunc::Unlikely
                        )
                    ) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                let expected = match call.function.value() {
                    Func::Scalar(ScalarFunc::Likely | ScalarFunc::Unlikely) => 1,
                    Func::Scalar(ScalarFunc::Likelihood) => 2,
                    _ => unreachable!("planner hint was checked by match guard"),
                };
                if !order_by.is_empty() || values.len() != expected || children != [target] {
                    return Err(LimboError::InternalError(
                        "planner hint has invalid lowered arguments".to_string(),
                    ));
                }
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(
                        call.function.value(),
                        Func::Scalar(ScalarFunc::Substr | ScalarFunc::Substring)
                    ) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty()
                    || !matches!(values.len(), 2 | 3)
                    || children.len() != values.len()
                {
                    return Err(LimboError::InternalError(
                        "substring has invalid lowered arguments".to_string(),
                    ));
                }
                let ExprRegisters::Function(start) = context.registers else {
                    unreachable!("substring registers were allocated")
                };
                debug_assert!(children
                    .iter()
                    .enumerate()
                    .all(|(index, register)| *register == start + index));
                self.program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg: start,
                    dest: target,
                    func: FuncCtx {
                        func: call.function.value().clone(),
                        arg_count: values.len(),
                    },
                });
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::CustomType(_)) =>
            {
                let [argument] = children else {
                    return Err(LimboError::InternalError(
                        "custom-type function has invalid lowered arguments".to_string(),
                    ));
                };
                let ExprRegisters::Function(expected_argument) = context.registers else {
                    unreachable!("custom-type function register was allocated")
                };
                debug_assert_eq!(*argument, expected_argument);
                self.program.emit_insn(match &call.operation {
                    hir::FunctionOperation::CustomType(hir::CustomTypeOperation::UnionValue {
                        tag_index,
                        ..
                    }) => Insn::UnionPack {
                        tag_index: *tag_index,
                        value_reg: *argument,
                        dest: target,
                    },
                    hir::FunctionOperation::CustomType(hir::CustomTypeOperation::UnionTag {
                        tag_names,
                        ..
                    }) => Insn::UnionTag {
                        src_reg: *argument,
                        dest: target,
                        tag_names: tag_names.clone(),
                    },
                    hir::FunctionOperation::CustomType(
                        hir::CustomTypeOperation::UnionExtract { tag_index, .. },
                    ) => Insn::UnionExtract {
                        src_reg: *argument,
                        expected_tag: *tag_index,
                        dest: target,
                    },
                    hir::FunctionOperation::CustomType(
                        hir::CustomTypeOperation::StructExtract { field_index, .. },
                    ) => Insn::StructField {
                        src_reg: *argument,
                        field_index: *field_index,
                        dest: target,
                    },
                    _ => unreachable!("custom-type operation was checked by match guard"),
                });
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Sequence(_)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "sequence function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || children.len() != values.len() {
                    return Err(LimboError::InternalError(
                        "sequence function has invalid lowered arguments".to_string(),
                    ));
                }
                let ExprRegisters::Function(start_reg) = context.registers else {
                    unreachable!("sequence function registers were allocated")
                };
                debug_assert!(children
                    .iter()
                    .enumerate()
                    .all(|(index, register)| *register == start_reg + index));

                let hir::FunctionOperation::Sequence(operation) = &call.operation else {
                    unreachable!("sequence operation was checked by match guard")
                };
                let database = operation.sequence.database().ok_or_else(|| {
                    LimboError::InternalError("HIR sequence has no owning database".to_string())
                })?;
                let snapshot = self.document.database(database).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "HIR database {} is absent from the catalog snapshot",
                        database.index()
                    ))
                })?;
                self.program
                    .begin_write_on_database(database.index(), snapshot.schema_version)?;

                let backing_table = operation.backing_table.value().require_btree()?;
                let sqlite_sequence = operation
                    .sqlite_sequence
                    .as_ref()
                    .map(|table| table.value().require_btree())
                    .transpose()?;
                match operation.kind {
                    hir::SequenceOperationKind::NextValue => {
                        crate::translate::sequence::emit_disk_read_nextval(
                            self.program,
                            database.index(),
                            backing_table,
                            sqlite_sequence,
                            &operation.normalized_name,
                            operation.sequence.value(),
                            target,
                            Some(start_reg),
                        )?;
                    }
                    hir::SequenceOperationKind::SetValue => {
                        crate::translate::sequence::emit_disk_setval(
                            self.program,
                            database.index(),
                            backing_table,
                            sqlite_sequence,
                            &operation.normalized_name,
                            operation.sequence.value(),
                            start_reg,
                            values.len(),
                            target,
                            FuncCtx {
                                func: call.function.value().clone(),
                                arg_count: values.len(),
                            },
                        )?;
                    }
                }
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(
                        call.function.value(),
                        Func::Scalar(
                            ScalarFunc::SqliteVersion
                                | ScalarFunc::TursoVersion
                                | ScalarFunc::SqliteSourceId
                        )
                    ) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !values.is_empty() || !order_by.is_empty() || !children.is_empty() {
                    return Err(LimboError::InternalError(
                        "version function has invalid lowered arguments".to_string(),
                    ));
                }
                let output = self.program.alloc_register();
                self.program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg: output,
                    dest: output,
                    func: FuncCtx {
                        func: call.function.value().clone(),
                        arg_count: 0,
                    },
                });
                self.program.emit_insn(Insn::Copy {
                    src_reg: output,
                    dst_reg: target,
                    extra_amount: 0,
                });
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(
                        call.function.value(),
                        Func::Scalar(ScalarFunc::LastInsertRowid)
                    ) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || !children.is_empty() {
                    return Err(LimboError::InternalError(
                        "last_insert_rowid has invalid lowered arguments".to_string(),
                    ));
                }
                let start_reg = self.program.alloc_register();
                self.program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg,
                    dest: target,
                    func: FuncCtx {
                        func: call.function.value().clone(),
                        arg_count: values.len(),
                    },
                });
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(call.function.value(), Func::Scalar(ScalarFunc::Coalesce)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || values.len() < 2 || children.len() != values.len() {
                    return Err(LimboError::InternalError(
                        "coalesce has invalid lowered arguments".to_string(),
                    ));
                }
                let ExprRegisters::Coalesce(registers) = context.registers else {
                    unreachable!("coalesce registers were allocated")
                };
                debug_assert!(children.iter().all(|child| *child == target));
                self.program
                    .preassign_label_to_next_insn(registers.end_label);
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(call.function.value(), Func::Scalar(ScalarFunc::Iif)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || values.len() < 2 || children.len() != values.len() {
                    return Err(LimboError::InternalError(
                        "iif has invalid lowered arguments".to_string(),
                    ));
                }
                let ExprRegisters::Iif(registers) = context.registers else {
                    unreachable!("iif registers were allocated")
                };
                if values.len() % 2 == 0 {
                    self.program.emit_insn(Insn::Goto {
                        target_pc: registers.end_label,
                    });
                    self.program
                        .preassign_label_to_next_insn(registers.false_label);
                    self.program.emit_insn(Insn::Null {
                        dest: target,
                        dest_end: None,
                    });
                }
                self.program
                    .preassign_label_to_next_insn(registers.end_label);
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(call.function.value(), Func::Scalar(ScalarFunc::IfNull)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || values.len() != 2 || children.len() != 2 {
                    return Err(LimboError::InternalError(
                        "ifnull has invalid lowered arguments".to_string(),
                    ));
                }
                let ExprRegisters::IfNull(registers) = context.registers else {
                    unreachable!("ifnull registers were allocated")
                };
                debug_assert_eq!(children, [registers.value, registers.value]);
                self.program
                    .preassign_label_to_next_insn(registers.copy_label);
                self.program.emit_insn(Insn::Copy {
                    src_reg: registers.value,
                    dst_reg: target,
                    extra_amount: 0,
                });
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(call.function.value(), Func::Scalar(ScalarFunc::ConcatWs)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || children.len() != values.len() {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid lowered arguments".to_string(),
                    ));
                }
                let ExprRegisters::ConcatWs(registers) = context.registers else {
                    unreachable!("concat_ws registers were allocated")
                };
                debug_assert!(children
                    .iter()
                    .enumerate()
                    .all(|(index, register)| *register == registers.arguments + index));
                self.program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg: registers.arguments,
                    dest: registers.result,
                    func: FuncCtx {
                        func: call.function.value().clone(),
                        arg_count: values.len(),
                    },
                });
                self.program.emit_insn(Insn::Copy {
                    src_reg: registers.result,
                    dst_reg: target,
                    extra_amount: 0,
                });
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(call.function.value(), Func::Scalar(ScalarFunc::StructPack)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "struct_pack has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || children.len() != values.len() {
                    return Err(LimboError::InternalError(
                        "struct_pack has invalid lowered arguments".to_string(),
                    ));
                }
                let start = match context.registers {
                    ExprRegisters::Function(start) => start,
                    ExprRegisters::None if values.is_empty() => self.program.alloc_registers(0),
                    _ => unreachable!("struct_pack registers were allocated"),
                };
                debug_assert!(children
                    .iter()
                    .enumerate()
                    .all(|(index, register)| *register == start + index));
                self.program.emit_insn(Insn::MakeArray {
                    start_reg: start,
                    count: values.len(),
                    dest: target,
                });
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && plain_function_lowering(call.function.value()).is_some() =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                    ..
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || children.len() != values.len() {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid lowered arguments".to_string(),
                    ));
                }
                debug_assert!(children
                    .windows(2)
                    .all(|registers| registers[1] == registers[0] + 1));
                let lowering = plain_function_lowering(call.function.value())
                    .expect("plain function lowering was checked by the match guard");
                let start_reg = if let Some(start) = children.first() {
                    *start
                } else {
                    match lowering {
                        EmptyArgumentStart::UseTarget => target,
                        EmptyArgumentStart::UseZero => 0,
                        EmptyArgumentStart::UseNextRegister => self.program.alloc_registers(0),
                        EmptyArgumentStart::ReserveOneRegister => self.program.alloc_register(),
                    }
                };
                self.program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg,
                    dest: target,
                    func: FuncCtx {
                        func: call.function.value().clone(),
                        arg_count: values.len(),
                    },
                });
                Ok(target)
            }
            hir::Expr::Row(values) => {
                debug_assert_eq!(children.len(), values.len());
                debug_assert!(children
                    .iter()
                    .enumerate()
                    .all(|(index, register)| *register == target + index));
                Ok(target)
            }
            _ => Err(LimboError::InternalError(
                "HIR expression lowering is not implemented for this expression".to_string(),
            )),
        }
    }
}

impl ExprLowerer<'_, '_, '_> {
    fn begin_cast_call(
        &mut self,
        target: &hir::TypeName,
        context: &mut LoweringContext,
        phase: CastProgram,
        call: usize,
        argument_count: usize,
    ) -> Result<usize> {
        let mut registers = match context.registers {
            ExprRegisters::None => CastProgramRegisters {
                call: None,
                arguments_start: 0,
                domain_not_null_emitted: false,
                pending_domain_check: None,
            },
            ExprRegisters::CastPrograms(registers) => registers,
            _ => unreachable!("CAST program registers were allocated"),
        };
        if phase == CastProgram::DomainCheck
            && registers.call != Some((CastProgram::DomainCheck, call))
        {
            self.finish_pending_domain_check(target, &mut registers)?;
            self.emit_domain_not_null(target, context.target, &mut registers)?;
        }
        if registers.call != Some((phase, call)) {
            registers.call = Some((phase, call));
            registers.arguments_start = self.program.alloc_registers(argument_count);
        }
        let arguments_start = registers.arguments_start;
        context.registers = ExprRegisters::CastPrograms(registers);
        Ok(arguments_start)
    }

    fn emit_domain_not_null(
        &mut self,
        target: &hir::TypeName,
        value: usize,
        registers: &mut CastProgramRegisters,
    ) -> Result<()> {
        if registers.domain_not_null_emitted {
            return Ok(());
        }
        let domain = target.programs.domain.as_ref().ok_or_else(|| {
            LimboError::InternalError("HIR domain CHECK has no domain metadata".to_string())
        })?;
        if let Some(description) = &domain.not_null_description {
            self.program.emit_insn(Insn::HaltIfNull {
                target_reg: value,
                err_code: SQLITE_CONSTRAINT_NOTNULL,
                description: description.clone(),
            });
        }
        registers.domain_not_null_emitted = true;
        Ok(())
    }

    fn finish_pending_domain_check(
        &mut self,
        target: &hir::TypeName,
        registers: &mut CastProgramRegisters,
    ) -> Result<()> {
        let Some(pending) = registers.pending_domain_check.take() else {
            return Ok(());
        };
        let check = target
            .programs
            .domain
            .as_ref()
            .and_then(|domain| domain.checks.get(pending.check))
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "HIR domain CHECK {} has no metadata",
                    pending.check
                ))
            })?;
        let passed = self.program.allocate_label();
        self.program.emit_insn(Insn::IsNull {
            reg: pending.result,
            target_pc: passed,
        });
        self.program.emit_insn(Insn::If {
            reg: pending.result,
            target_pc: passed,
            jump_if_null: false,
        });
        self.program.emit_insn(Insn::Halt {
            err_code: SQLITE_CONSTRAINT_CHECK,
            description: check.failure_description.clone(),
            on_error: None,
            description_reg: None,
        });
        self.program.preassign_label_to_next_insn(passed);
        Ok(())
    }

    fn emit_cast(
        &mut self,
        target: &hir::TypeName,
        value: usize,
        registers: ExprRegisters,
        children: &[usize],
    ) -> Result<usize> {
        let encode_children: usize = target
            .programs
            .encode
            .iter()
            .map(|call| call.arguments.len() + 1)
            .sum();
        let domain_children: usize = target.programs.domain.as_ref().map_or(0, |domain| {
            domain
                .checks
                .iter()
                .map(|check| check.call.arguments.len() + 1)
                .sum()
        });
        let expected_children = 1 + encode_children + domain_children;
        if children.len() != expected_children {
            return Err(LimboError::InternalError(format!(
                "HIR CAST lowered {} linked children, expected {expected_children}",
                children.len()
            )));
        }
        debug_assert_eq!(children[0], value);

        if target.programs.apply_builtin_affinity {
            if encode_children != 0 || target.programs.domain.is_some() {
                return Err(LimboError::InternalError(
                    "HIR built-in CAST contains custom programs".to_string(),
                ));
            }
            self.program.emit_insn(Insn::Cast {
                reg: value,
                affinity: target.affinity,
            });
            return Ok(value);
        }

        let mut registers = match registers {
            ExprRegisters::None => CastProgramRegisters {
                call: None,
                arguments_start: 0,
                domain_not_null_emitted: false,
                pending_domain_check: None,
            },
            ExprRegisters::CastPrograms(registers) => registers,
            _ => unreachable!("CAST program registers were allocated"),
        };
        if target.programs.domain.is_some() {
            self.emit_domain_not_null(target, value, &mut registers)?;
            self.finish_pending_domain_check(target, &mut registers)?;
        }
        Ok(value)
    }

    fn emit_column(
        &mut self,
        reference: hir::ColumnRef,
        target: usize,
        registers: ExprRegisters,
        children: &[usize],
    ) -> Result<usize> {
        let Some(source) = self.document.source(reference.source) else {
            return Err(LimboError::InternalError(format!(
                "HIR column references missing source {}",
                reference.source
            )));
        };
        let Some(column) = source.columns.get(reference.column) else {
            return Err(LimboError::InternalError(format!(
                "HIR column {}.{} is out of bounds",
                reference.source, reference.column
            )));
        };
        let Some(generated) = source.generated_expressions.get(reference.column) else {
            return Err(LimboError::InternalError(
                "HIR source generated-expression metadata is incomplete".to_string(),
            ));
        };
        let Some(default) = source.default_expressions.get(reference.column) else {
            return Err(LimboError::InternalError(
                "HIR source default-expression metadata is incomplete".to_string(),
            ));
        };
        let Some(type_program) = source.column_type_programs.get(reference.column) else {
            return Err(LimboError::InternalError(
                "HIR source custom-type metadata is incomplete".to_string(),
            ));
        };
        if matches!(default, hir::ColumnReadExpression::NotRequired) {
            return Err(LimboError::InternalError(format!(
                "HIR default for column {}.{} was not planned",
                reference.source, reference.column
            )));
        }
        let encoded_default = matches!(default, hir::ColumnReadExpression::Planned(_))
            && type_program
                .as_ref()
                .is_some_and(|programs| !programs.encode.is_empty());

        let suppress_custom_type_decode = self.program.flags.suppress_custom_type_decode();
        let decode_children =
            if column.type_fact.array_dimensions > 0 || suppress_custom_type_decode {
                0
            } else {
                type_program.as_ref().map_or(0, |programs| {
                    programs
                        .decode
                        .iter()
                        .map(|call| call.arguments.len() + 1)
                        .sum()
                })
            };
        let encode_default_children = if encoded_default && !suppress_custom_type_decode {
            1 + type_program.as_ref().map_or(0, |programs| {
                programs
                    .encode
                    .iter()
                    .map(|call| call.arguments.len() + 1)
                    .sum::<usize>()
            })
        } else {
            0
        };
        let generated_children =
            usize::from(matches!(generated, hir::ColumnReadExpression::Planned(_)));
        let expected_children = generated_children + encode_default_children + decode_children;
        if children.len() != expected_children {
            return Err(LimboError::InternalError(format!(
                "HIR column {}.{} lowered {} linked children, expected {}",
                reference.source,
                reference.column,
                children.len(),
                expected_children
            )));
        }

        match generated {
            hir::ColumnReadExpression::Planned(_) => {
                debug_assert_eq!(children[0], target);
                if decode_children == 0 && column.has_affinity {
                    self.program.emit_column_affinity(target, column.affinity);
                }
            }
            hir::ColumnReadExpression::Absent => {}
            hir::ColumnReadExpression::NotRequired => {
                return Err(LimboError::InternalError(format!(
                    "HIR generated column {}.{} was not planned",
                    reference.source, reference.column
                )));
            }
        }

        if let ExprRegisters::ColumnPrograms(registers) = registers {
            if let Some(stored_value) = registers.stored_value {
                self.program.preassign_label_to_next_insn(stored_value);
            }
            if let Some(decode_done) = registers.decode_done {
                self.program.preassign_label_to_next_insn(decode_done);
            }
            self.set_column_collation(column);
            return Ok(target);
        }
        if matches!(generated, hir::ColumnReadExpression::Planned(_)) {
            self.set_column_collation(column);
            return Ok(target);
        }

        if encoded_default {
            self.program.flags.set_suppress_column_default(true);
        }
        self.emit_stored_column(reference, column, target, type_program.is_none())?;
        self.set_column_collation(column);
        Ok(target)
    }

    fn begin_encoded_default(
        &mut self,
        reference: hir::ColumnRef,
        context: &mut LoweringContext,
    ) -> Result<()> {
        let source = self.document.source(reference.source).ok_or_else(|| {
            LimboError::InternalError(format!(
                "HIR column references missing source {}",
                reference.source
            ))
        })?;
        let column = source.columns.get(reference.column).ok_or_else(|| {
            LimboError::InternalError(format!(
                "HIR column {}.{} is out of bounds",
                reference.source, reference.column
            ))
        })?;
        if column.rowid_alias {
            return Err(LimboError::InternalError(
                "HIR custom-type default cannot be read from a rowid alias".to_string(),
            ));
        }
        let read = self.resolve_btree_column(reference)?;
        self.program.flags.set_suppress_column_default(true);
        self.program
            .emit_column_or_rowid(read.cursor, read.column, context.target);
        let stored_value = self.program.allocate_label();
        self.program
            .emit_column_has_field(read.cursor, reference.column, stored_value);
        context.registers = ExprRegisters::ColumnPrograms(ColumnProgramRegisters {
            call: None,
            arguments_start: 0,
            stored_value: Some(stored_value),
            decode_done: None,
        });
        Ok(())
    }

    fn begin_column_decode(
        &mut self,
        reference: hir::ColumnRef,
        context: &mut LoweringContext,
    ) -> Result<()> {
        let source = self.document.source(reference.source).ok_or_else(|| {
            LimboError::InternalError(format!(
                "HIR column references missing source {}",
                reference.source
            ))
        })?;
        let column = source.columns.get(reference.column).ok_or_else(|| {
            LimboError::InternalError(format!(
                "HIR column {}.{} is out of bounds",
                reference.source, reference.column
            ))
        })?;
        let generated = source
            .generated_expressions
            .get(reference.column)
            .ok_or_else(|| {
                LimboError::InternalError(
                    "HIR source generated-expression metadata is incomplete".to_string(),
                )
            })?;
        if matches!(context.registers, ExprRegisters::None) {
            match generated {
                hir::ColumnReadExpression::Planned(_) => {
                    if column.has_affinity {
                        self.program
                            .emit_column_affinity(context.target, column.affinity);
                    }
                }
                hir::ColumnReadExpression::Absent => {
                    self.emit_stored_column(reference, column, context.target, false)?;
                }
                hir::ColumnReadExpression::NotRequired => {
                    return Err(LimboError::InternalError(format!(
                        "HIR generated column {}.{} was not planned",
                        reference.source, reference.column
                    )));
                }
            }
            context.registers = ExprRegisters::ColumnPrograms(ColumnProgramRegisters {
                call: None,
                arguments_start: 0,
                stored_value: None,
                decode_done: None,
            });
        }

        let ExprRegisters::ColumnPrograms(registers) = &mut context.registers else {
            unreachable!("column program registers were allocated")
        };
        if registers.decode_done.is_some() {
            return Ok(());
        }
        if let Some(stored_value) = registers.stored_value.take() {
            self.program.preassign_label_to_next_insn(stored_value);
        }
        let decode_done = self.program.allocate_label();
        self.program.emit_insn(Insn::IsNull {
            reg: context.target,
            target_pc: decode_done,
        });
        registers.decode_done = Some(decode_done);
        Ok(())
    }

    fn emit_stored_column(
        &mut self,
        reference: hir::ColumnRef,
        column: &hir::SourceColumn,
        target: usize,
        apply_storage_affinity: bool,
    ) -> Result<()> {
        let Some(binding) = self.program.source_binding(reference.source).copied() else {
            return Err(LimboError::InternalError(format!(
                "HIR source {} has no physical binding",
                reference.source
            )));
        };

        match binding {
            SourceBinding::Registers { start, .. } => {
                self.program.emit_insn(Insn::Copy {
                    src_reg: start + reference.column,
                    dst_reg: target,
                    extra_amount: 0,
                });
            }
            SourceBinding::SchemaInputs {
                value,
                arguments_start,
            } => {
                let src_reg = if reference.column == 0 {
                    value
                } else {
                    arguments_start + reference.column - 1
                };
                self.program.emit_insn(Insn::Copy {
                    src_reg,
                    dst_reg: target,
                    extra_amount: 0,
                });
            }
            SourceBinding::Virtual { cursor } => {
                self.program.emit_insn(Insn::VColumn {
                    cursor_id: cursor,
                    column: reference.column,
                    dest: target,
                });
            }
            SourceBinding::BTree { scan_cursor, .. } => {
                if column.rowid_alias {
                    self.emit_btree_rowid(scan_cursor, target)?;
                } else {
                    let read = self.resolve_btree_column(reference)?;
                    self.program
                        .emit_column_or_rowid(read.cursor, read.column, target);
                }
                if apply_storage_affinity {
                    let Some(storage) = column.type_fact.storage else {
                        return Ok(());
                    };
                    expr::maybe_apply_affinity(storage, target, self.program);
                }
            }
        }
        Ok(())
    }

    fn resolve_btree_column(&self, reference: hir::ColumnRef) -> Result<CursorColumn> {
        let Some(SourceBinding::BTree {
            scan_cursor,
            table_cursor,
        }) = self.program.source_binding(reference.source).copied()
        else {
            return Err(LimboError::InternalError(format!(
                "HIR source {} is not bound to a B-tree cursor",
                reference.source
            )));
        };
        match self.program.get_cursor_type(scan_cursor) {
            Some(CursorType::BTreeTable(_)) => Ok(CursorColumn {
                cursor: scan_cursor,
                column: reference.column,
            }),
            Some(CursorType::BTreeIndex(index)) => {
                if let Some(column) = index.column_table_pos_to_index_pos(reference.column) {
                    Ok(CursorColumn {
                        cursor: scan_cursor,
                        column,
                    })
                } else {
                    let Some(table_cursor) = table_cursor else {
                        return Err(LimboError::InternalError(format!(
                            "HIR source {} index does not contain column {} and has no table cursor",
                            reference.source, reference.column
                        )));
                    };
                    Ok(CursorColumn {
                        cursor: table_cursor,
                        column: reference.column,
                    })
                }
            }
            Some(cursor) => Err(LimboError::InternalError(format!(
                "HIR B-tree source uses incompatible cursor {cursor:?}"
            ))),
            None => Err(LimboError::InternalError(
                "HIR B-tree source cursor does not exist".to_string(),
            )),
        }
    }

    fn set_column_collation(&mut self, column: &hir::SourceColumn) {
        self.program.set_collation(
            column
                .collation
                .as_ref()
                .map(|collation| (collation.value().clone(), false)),
        );
    }

    fn emit_rowid(&mut self, source: hir::SourceId, target: usize) -> Result<usize> {
        let Some(definition) = self.document.source(source) else {
            return Err(LimboError::InternalError(format!(
                "HIR rowid references missing source {source}"
            )));
        };
        if !definition.rowid_available {
            return Err(LimboError::InternalError(format!(
                "HIR source {source} has no rowid"
            )));
        }
        let Some(binding) = self.program.source_binding(source).copied() else {
            return Err(LimboError::InternalError(format!(
                "HIR source {source} has no physical binding"
            )));
        };
        match binding {
            SourceBinding::BTree { scan_cursor, .. } => {
                self.emit_btree_rowid(scan_cursor, target)?;
            }
            SourceBinding::Registers {
                rowid: Some(register),
                ..
            } => self.program.emit_insn(Insn::Copy {
                src_reg: register,
                dst_reg: target,
                extra_amount: 0,
            }),
            _ => {
                return Err(LimboError::InternalError(format!(
                    "HIR rowid source {source} has no rowid runtime binding"
                )))
            }
        }
        Ok(target)
    }

    fn emit_btree_rowid(&mut self, cursor: usize, target: usize) -> Result<()> {
        match self.program.get_cursor_type(cursor) {
            Some(CursorType::BTreeTable(_)) => self.program.emit_insn(Insn::RowId {
                cursor_id: cursor,
                dest: target,
            }),
            Some(CursorType::BTreeIndex(_)) => self.program.emit_insn(Insn::IdxRowId {
                cursor_id: cursor,
                dest: target,
            }),
            Some(cursor) => {
                return Err(LimboError::InternalError(format!(
                    "HIR rowid uses incompatible cursor {cursor:?}"
                )));
            }
            None => {
                return Err(LimboError::InternalError(
                    "HIR rowid cursor does not exist".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn finish_in_list_value(
        &mut self,
        registers: &InListRegisters,
        comparison: &hir::ComparisonSemantics,
    ) -> Result<()> {
        if comparison.components.len() != registers.width {
            return Err(LimboError::InternalError(
                "HIR IN comparison width changed between values".to_string(),
            ));
        }
        self.program.emit_insn(Insn::BitAnd {
            lhs: registers.null_flag,
            rhs: registers.rhs,
            dest: registers.null_flag,
        });
        match comparison.components.as_slice() {
            [] => unreachable!("IN comparison width was checked"),
            [component] => {
                self.program.emit_insn(Insn::Eq {
                    lhs: registers.lhs,
                    rhs: registers.rhs,
                    target_pc: registers.match_label,
                    flags: comparison_flags(component),
                    collation: comparison_collation(component),
                });
            }
            components => {
                let skip_label = self.program.allocate_label();
                for (index, component) in components.iter().enumerate() {
                    if index + 1 == components.len() {
                        self.program.emit_insn(Insn::Eq {
                            lhs: registers.lhs + index,
                            rhs: registers.rhs + index,
                            target_pc: registers.match_label,
                            flags: comparison_flags(component),
                            collation: comparison_collation(component),
                        });
                    } else {
                        self.program.emit_insn(Insn::Ne {
                            lhs: registers.lhs + index,
                            rhs: registers.rhs + index,
                            target_pc: skip_label,
                            flags: comparison_flags(component),
                            collation: comparison_collation(component),
                        });
                    }
                }
                self.program.preassign_label_to_next_insn(skip_label);
            }
        }
        Ok(())
    }

    fn emit_in_subquery_null_check(&mut self, register: usize, null_rewind_label: BranchOffset) {
        // NullRow can make a NOT NULL column read as NULL, so this check cannot
        // be removed based on the column's declared nullability.
        self.program.emit_insn(Insn::IsNull {
            reg: register,
            target_pc: null_rewind_label,
        });
    }

    fn emit_in_subquery(
        &mut self,
        registers: InSubqueryRegisters,
        cursor: CursorID,
        negated: bool,
        comparison: &hir::ComparisonSemantics,
        target: usize,
    ) -> Result<usize> {
        if comparison.components.len() != registers.width {
            return Err(LimboError::InternalError(
                "HIR IN-subquery comparison width changed while lowering".to_string(),
            ));
        }
        self.emit_in_subquery_null_check(
            registers.lhs + registers.width - 1,
            registers.null_rewind_label,
        );

        if comparison
            .components
            .iter()
            .any(|component| component.affinity != crate::vdbe::affinity::Affinity::Blob)
        {
            let count = std::num::NonZeroUsize::new(registers.width)
                .expect("IN-subquery comparison width was checked");
            self.program.emit_insn(Insn::Affinity {
                start_reg: registers.lhs,
                count,
                affinities: comparison
                    .components
                    .iter()
                    .map(|component| component.affinity.aff_mask())
                    .collect(),
            });
        }

        let skip_label = self.program.allocate_label();
        let include_label = self.program.allocate_label();
        let null_result_label = self.program.allocate_label();
        let null_loop_label = self.program.allocate_label();
        let null_next_label = self.program.allocate_label();
        let no_null_label = if negated { include_label } else { skip_label };

        if negated {
            self.program.emit_insn(Insn::Found {
                cursor_id: cursor,
                target_pc: skip_label,
                record_reg: registers.lhs,
                num_regs: registers.width,
            });
        } else {
            self.program.emit_insn(Insn::NotFound {
                cursor_id: cursor,
                target_pc: registers.null_rewind_label,
                record_reg: registers.lhs,
                num_regs: registers.width,
            });
            self.program.emit_insn(Insn::Goto {
                target_pc: include_label,
            });
        }

        self.program
            .preassign_label_to_next_insn(registers.null_rewind_label);
        self.program.emit_insn(Insn::Rewind {
            cursor_id: cursor,
            pc_if_empty: no_null_label,
        });
        self.program.preassign_label_to_next_insn(null_loop_label);
        let column = self.program.alloc_register();
        for (index, component) in comparison.components.iter().enumerate() {
            self.program.emit_insn(Insn::Column {
                cursor_id: cursor,
                column: index,
                dest: column,
                default: None,
            });
            self.program.emit_insn(Insn::Ne {
                lhs: registers.lhs + index,
                rhs: column,
                target_pc: null_next_label,
                flags: comparison_flags(component),
                collation: comparison_collation(component),
            });
        }
        self.program.emit_insn(Insn::Goto {
            target_pc: null_result_label,
        });
        self.program.preassign_label_to_next_insn(null_next_label);
        self.program.emit_insn(Insn::Next {
            cursor_id: cursor,
            pc_if_next: null_loop_label,
            fullscan: false,
        });
        self.program.emit_insn(Insn::Goto {
            target_pc: no_null_label,
        });

        let done_label = self.program.allocate_label();
        self.program.preassign_label_to_next_insn(include_label);
        self.program.emit_insn(Insn::Integer {
            value: 1,
            dest: target,
        });
        self.program.emit_insn(Insn::Goto {
            target_pc: done_label,
        });
        self.program.preassign_label_to_next_insn(skip_label);
        self.program.emit_insn(Insn::Integer {
            value: 0,
            dest: target,
        });
        self.program.emit_insn(Insn::Goto {
            target_pc: done_label,
        });
        self.program.preassign_label_to_next_insn(null_result_label);
        self.program.emit_insn(Insn::Null {
            dest: target,
            dest_end: None,
        });
        self.program.preassign_label_to_next_insn(done_label);
        Ok(target)
    }

    fn finish_case_arm(&mut self, registers: &mut CaseRegisters) {
        self.program.emit_insn(Insn::Goto {
            target_pc: registers.return_label,
        });
        self.program
            .preassign_label_to_next_insn(registers.next_label);
        registers.next_label = self.program.allocate_label();
    }

    fn emit_case_test(
        &mut self,
        registers: &CaseRegisters,
        comparison: Option<&hir::ComparisonSemantics>,
    ) -> Result<()> {
        match (registers.base, comparison) {
            (None, None) => {
                self.program.emit_insn(Insn::IfNot {
                    reg: registers.when,
                    target_pc: registers.next_label,
                    jump_if_null: true,
                });
            }
            (Some(base), Some(comparison)) => {
                let [component] = comparison.components.as_slice() else {
                    return Err(LimboError::InternalError(
                        "simple CASE comparison must have one component".to_string(),
                    ));
                };
                let flags = CmpInsFlags::default()
                    .with_affinity(component.affinity)
                    .jump_if_null();
                self.program.emit_insn(Insn::Ne {
                    lhs: base,
                    rhs: registers.when,
                    target_pc: registers.next_label,
                    flags: if component.array {
                        flags.array_cmp()
                    } else {
                        flags
                    },
                    collation: component
                        .collation
                        .as_ref()
                        .map(|collation| *collation.value()),
                });
            }
            (None, Some(_)) => {
                return Err(LimboError::InternalError(
                    "searched CASE contains comparison metadata".to_string(),
                ));
            }
            (Some(_), None) => {
                return Err(LimboError::InternalError(
                    "simple CASE is missing comparison metadata".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn emit_null_test(
        &mut self,
        test: NullTest,
        target: usize,
        children: &[usize],
    ) -> Result<usize> {
        let [value] = children else {
            unreachable!("NULL test has one lowered child")
        };
        let if_true_label = self.program.allocate_label();
        let instruction = match test {
            NullTest::IsNull => Insn::IsNull {
                reg: *value,
                target_pc: if_true_label,
            },
            NullTest::NotNull => Insn::NotNull {
                reg: *value,
                target_pc: if_true_label,
            },
        };
        expr::functions::wrap_eval_jump_expr(self.program, instruction, target, if_true_label);
        Ok(target)
    }

    fn emit_array_binary(
        &mut self,
        operator: Operator,
        operands: BinaryOperands,
        target: usize,
        children: &[usize],
    ) -> Result<usize> {
        let start_reg = match operands {
            BinaryOperands::Shared(source) => {
                debug_assert_eq!(children, [source]);
                let start = self.program.alloc_registers(2);
                self.program.emit_insn(Insn::Copy {
                    src_reg: source,
                    dst_reg: start,
                    extra_amount: 0,
                });
                self.program.emit_insn(Insn::Copy {
                    src_reg: source,
                    dst_reg: start + 1,
                    extra_amount: 0,
                });
                start
            }
            BinaryOperands::Pair { lhs, rhs } => {
                debug_assert_eq!(children, [lhs, rhs]);
                debug_assert_eq!(rhs, lhs + 1);
                lhs
            }
        };
        let function = match operator {
            Operator::ArrayContains => ScalarFunc::ArrayContainsAll,
            Operator::ArrayOverlap => ScalarFunc::ArrayOverlap,
            _ => unreachable!("array binary operator was matched by the caller"),
        };
        self.program.emit_insn(Insn::Function {
            constant_mask: 0,
            start_reg,
            dest: target,
            func: FuncCtx {
                func: Func::Scalar(function),
                arg_count: 2,
            },
        });
        Ok(target)
    }

    fn emit_custom_binary(
        &mut self,
        custom: &hir::CustomBinaryOperator,
        registers: CustomBinaryRegisters,
        target: usize,
        children: &[usize],
    ) -> Result<usize> {
        let arguments = match registers {
            CustomBinaryRegisters::Direct { arguments } => {
                debug_assert_eq!(children, [arguments, arguments + 1]);
                arguments
            }
            CustomBinaryRegisters::EncodeLiteral {
                inputs,
                arguments,
                encoder_arguments_start: _,
            } => {
                let encoding = custom
                    .literal_encoding
                    .as_ref()
                    .expect("literal encoding registers require literal encoding");
                let encoder = encoding
                    .encoder
                    .as_ref()
                    .expect("literal encoding registers require an encoder");
                debug_assert_eq!(children[0], inputs);
                debug_assert_eq!(children[1], inputs + 1);
                debug_assert_eq!(children.len(), encoder.arguments.len() + 3);
                let literal = custom_operand_position(encoding.operand, custom.swap_args);
                debug_assert_eq!(children.last(), Some(&(arguments + literal)));
                arguments
            }
        };

        let result = self.program.alloc_register();
        self.program.emit_insn(Insn::Function {
            constant_mask: 0,
            start_reg: arguments,
            dest: result,
            func: FuncCtx {
                func: custom.function.value().clone(),
                arg_count: 2,
            },
        });
        if custom.negate {
            self.program.emit_insn(Insn::Not {
                reg: result,
                dest: result,
            });
        }
        if result != target {
            self.program.emit_insn(Insn::Copy {
                src_reg: result,
                dst_reg: target,
                extra_amount: 0,
            });
        }
        Ok(target)
    }

    fn emit_binary_comparison(
        &mut self,
        operator: Operator,
        comparison: &hir::ComparisonSemantics,
        operands: BinaryOperands,
        target: usize,
        children: &[usize],
    ) -> Result<usize> {
        let (lhs, rhs) = match operands {
            BinaryOperands::Shared(register) => {
                debug_assert_eq!(children, [register]);
                (register, register)
            }
            BinaryOperands::Pair { lhs, rhs } => {
                debug_assert_eq!(children, [lhs, rhs]);
                (lhs, rhs)
            }
        };
        self.emit_comparison(operator, comparison, lhs, rhs, target)
    }

    fn emit_comparison(
        &mut self,
        operator: Operator,
        comparison: &hir::ComparisonSemantics,
        lhs: usize,
        rhs: usize,
        target: usize,
    ) -> Result<usize> {
        match comparison.components.as_slice() {
            [] => Err(LimboError::InternalError(
                "HIR comparison has no components".to_string(),
            )),
            [_] => self.emit_scalar_comparison(operator, comparison, lhs, rhs, target),
            components => {
                self.emit_row_comparison(operator, components, lhs, rhs, target)?;
                Ok(target)
            }
        }
    }

    fn emit_scalar_comparison(
        &mut self,
        operator: Operator,
        comparison: &hir::ComparisonSemantics,
        lhs: usize,
        rhs: usize,
        target: usize,
    ) -> Result<usize> {
        let [component] = comparison.components.as_slice() else {
            unreachable!("scalar comparison has one component")
        };
        let base_flags = CmpInsFlags::default().with_affinity(component.affinity);
        let comparison_flags = if component.array {
            base_flags.array_cmp()
        } else {
            base_flags
        };
        let collation = component
            .collation
            .as_ref()
            .map(|resolved| *resolved.value());
        let if_true_label = self.program.allocate_label();
        let (instruction, null_equal) = match operator {
            Operator::Equals => (
                Insn::Eq {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: comparison_flags,
                    collation,
                },
                false,
            ),
            Operator::NotEquals => (
                Insn::Ne {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: comparison_flags,
                    collation,
                },
                false,
            ),
            Operator::Less => (
                Insn::Lt {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: comparison_flags,
                    collation,
                },
                false,
            ),
            Operator::LessEquals => (
                Insn::Le {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: comparison_flags,
                    collation,
                },
                false,
            ),
            Operator::Greater => (
                Insn::Gt {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: comparison_flags,
                    collation,
                },
                false,
            ),
            Operator::GreaterEquals => (
                Insn::Ge {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: comparison_flags,
                    collation,
                },
                false,
            ),
            Operator::Is => (
                Insn::Eq {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: base_flags.null_eq(),
                    collation,
                },
                true,
            ),
            Operator::IsNot => (
                Insn::Ne {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: base_flags.null_eq(),
                    collation,
                },
                true,
            ),
            _ => {
                return Err(LimboError::InternalError(format!(
                    "HIR comparison lowering is not implemented for {operator:?}"
                )));
            }
        };
        if null_equal {
            expr::functions::wrap_eval_jump_expr(self.program, instruction, target, if_true_label);
        } else {
            expr::functions::wrap_eval_jump_expr_zero_or_null(
                self.program,
                instruction,
                target,
                if_true_label,
                lhs,
                rhs,
            );
        }
        Ok(target)
    }

    fn emit_row_comparison(
        &mut self,
        operator: Operator,
        components: &[hir::ComparisonComponent],
        lhs_start: usize,
        rhs_start: usize,
        target: usize,
    ) -> Result<()> {
        enum Ordering {
            Less,
            Greater,
        }

        let mut emit_equality = |target: usize, null_equal: bool| {
            let null_seen = if null_equal {
                None
            } else {
                let register = self.program.alloc_register();
                self.program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: register,
                });
                Some(register)
            };

            let done = self.program.allocate_label();
            for (index, component) in components.iter().enumerate() {
                let next = self.program.allocate_label();
                let lhs = lhs_start + index;
                let rhs = rhs_start + index;
                self.program.emit_insn(Insn::Eq {
                    lhs,
                    rhs,
                    target_pc: next,
                    flags: if null_equal {
                        CmpInsFlags::default()
                            .null_eq()
                            .with_affinity(component.affinity)
                    } else {
                        CmpInsFlags::default().with_affinity(component.affinity)
                    },
                    collation: component
                        .collation
                        .as_ref()
                        .map(|collation| *collation.value()),
                });
                if null_equal {
                    self.program.emit_insn(Insn::Integer {
                        value: 0,
                        dest: target,
                    });
                    self.program.emit_insn(Insn::Goto { target_pc: done });
                } else {
                    let mark_null = self.program.allocate_label();
                    self.program.emit_insn(Insn::IsNull {
                        reg: lhs,
                        target_pc: mark_null,
                    });
                    self.program.emit_insn(Insn::IsNull {
                        reg: rhs,
                        target_pc: mark_null,
                    });
                    self.program.emit_insn(Insn::Integer {
                        value: 0,
                        dest: target,
                    });
                    self.program.emit_insn(Insn::Goto { target_pc: done });
                    self.program.preassign_label_to_next_insn(mark_null);
                    self.program.emit_insn(Insn::Integer {
                        value: 1,
                        dest: null_seen.expect("null tracking register must exist"),
                    });
                }
                self.program.preassign_label_to_next_insn(next);
            }
            self.program.emit_insn(Insn::Integer {
                value: 1,
                dest: target,
            });
            if !null_equal {
                let finish = self.program.allocate_label();
                self.program.emit_insn(Insn::IfNot {
                    reg: null_seen.expect("null tracking register must exist"),
                    target_pc: finish,
                    jump_if_null: true,
                });
                self.program.emit_insn(Insn::Null {
                    dest: target,
                    dest_end: None,
                });
                self.program.preassign_label_to_next_insn(finish);
            }
            self.program.preassign_label_to_next_insn(done);
        };

        let emit_ordering =
            |program: &mut ProgramBuilder, ordering: Ordering, include_equal: bool| {
                let done = program.allocate_label();
                let null_result = program.allocate_label();
                for (index, component) in components.iter().enumerate() {
                    let next = program.allocate_label();
                    let lhs = lhs_start + index;
                    let rhs = rhs_start + index;
                    let flags = CmpInsFlags::default().with_affinity(component.affinity);
                    let collation = component
                        .collation
                        .as_ref()
                        .map(|collation| *collation.value());
                    program.emit_insn(Insn::IsNull {
                        reg: lhs,
                        target_pc: null_result,
                    });
                    program.emit_insn(Insn::IsNull {
                        reg: rhs,
                        target_pc: null_result,
                    });
                    program.emit_insn(Insn::Eq {
                        lhs,
                        rhs,
                        target_pc: next,
                        flags,
                        collation,
                    });
                    let if_true = program.allocate_label();
                    program.emit_insn(match ordering {
                        Ordering::Less => Insn::Lt {
                            lhs,
                            rhs,
                            target_pc: if_true,
                            flags,
                            collation,
                        },
                        Ordering::Greater => Insn::Gt {
                            lhs,
                            rhs,
                            target_pc: if_true,
                            flags,
                            collation,
                        },
                    });
                    program.emit_insn(Insn::Integer {
                        value: 0,
                        dest: target,
                    });
                    program.emit_insn(Insn::Goto { target_pc: done });
                    program.preassign_label_to_next_insn(if_true);
                    program.emit_insn(Insn::Integer {
                        value: 1,
                        dest: target,
                    });
                    program.emit_insn(Insn::Goto { target_pc: done });
                    program.preassign_label_to_next_insn(next);
                }
                program.emit_insn(Insn::Integer {
                    value: if include_equal { 1 } else { 0 },
                    dest: target,
                });
                program.emit_insn(Insn::Goto { target_pc: done });
                program.preassign_label_to_next_insn(null_result);
                program.emit_insn(Insn::Null {
                    dest: target,
                    dest_end: None,
                });
                program.preassign_label_to_next_insn(done);
            };

        match operator {
            Operator::Equals => emit_equality(target, false),
            Operator::NotEquals => {
                emit_equality(target, false);
                self.program.emit_insn(Insn::Not {
                    reg: target,
                    dest: target,
                });
            }
            Operator::Is => emit_equality(target, true),
            Operator::IsNot => {
                emit_equality(target, true);
                self.program.emit_insn(Insn::Not {
                    reg: target,
                    dest: target,
                });
            }
            Operator::Less => emit_ordering(self.program, Ordering::Less, false),
            Operator::LessEquals => emit_ordering(self.program, Ordering::Less, true),
            Operator::Greater => emit_ordering(self.program, Ordering::Greater, false),
            Operator::GreaterEquals => emit_ordering(self.program, Ordering::Greater, true),
            _ => {
                return Err(LimboError::InternalError(format!(
                    "HIR row comparison lowering is not implemented for {operator:?}"
                )));
            }
        }
        Ok(())
    }

    fn emit_concat(
        &mut self,
        operands: BinaryOperands,
        target: usize,
        children: &[usize],
        array_concat: bool,
    ) -> Result<usize> {
        let (lhs, rhs) = match operands {
            BinaryOperands::Shared(register) => {
                debug_assert_eq!(children, [register]);
                (register, register)
            }
            BinaryOperands::Pair { lhs, rhs } => {
                debug_assert_eq!(children, [lhs, rhs]);
                (lhs, rhs)
            }
        };
        let instruction = if array_concat {
            Insn::ArrayConcat {
                lhs,
                rhs,
                dest: target,
            }
        } else {
            Insn::Concat {
                lhs,
                rhs,
                dest: target,
            }
        };
        self.program.emit_insn(instruction);
        Ok(target)
    }

    fn emit_binary(
        &mut self,
        operator: Operator,
        operands: BinaryOperands,
        target: usize,
        children: &[usize],
    ) -> Result<usize> {
        let (lhs, rhs) = match operands {
            BinaryOperands::Shared(register) => {
                debug_assert_eq!(children, [register]);
                (register, register)
            }
            BinaryOperands::Pair { lhs, rhs } => {
                debug_assert_eq!(children, [lhs, rhs]);
                (lhs, rhs)
            }
        };
        let instruction = match operator {
            Operator::Add => Insn::Add {
                lhs,
                rhs,
                dest: target,
            },
            Operator::Subtract => Insn::Subtract {
                lhs,
                rhs,
                dest: target,
            },
            Operator::Multiply => Insn::Multiply {
                lhs,
                rhs,
                dest: target,
            },
            Operator::Divide => Insn::Divide {
                lhs,
                rhs,
                dest: target,
            },
            Operator::Modulus => Insn::Remainder {
                lhs,
                rhs,
                dest: target,
            },
            Operator::And => Insn::And {
                lhs,
                rhs,
                dest: target,
            },
            Operator::Or => Insn::Or {
                lhs,
                rhs,
                dest: target,
            },
            Operator::BitwiseAnd => Insn::BitAnd {
                lhs,
                rhs,
                dest: target,
            },
            Operator::BitwiseOr => Insn::BitOr {
                lhs,
                rhs,
                dest: target,
            },
            Operator::LeftShift => Insn::ShiftLeft {
                lhs,
                rhs,
                dest: target,
            },
            Operator::RightShift => Insn::ShiftRight {
                lhs,
                rhs,
                dest: target,
            },
            #[cfg(feature = "json")]
            operator @ (Operator::ArrowRight | Operator::ArrowRightShift) => {
                let function = match operator {
                    Operator::ArrowRight => JsonFunc::JsonArrowExtract,
                    Operator::ArrowRightShift => JsonFunc::JsonArrowShiftExtract,
                    _ => unreachable!("JSON arrow operator was matched"),
                };
                self.program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg: lhs,
                    dest: target,
                    func: FuncCtx {
                        func: Func::Json(function),
                        arg_count: 2,
                    },
                });
                return Ok(target);
            }
            _ => {
                return Err(LimboError::InternalError(format!(
                    "HIR binary lowering is not implemented for {operator:?}"
                )));
            }
        };
        self.program.emit_insn(instruction);
        Ok(target)
    }

    fn emit_unary(
        &mut self,
        operator: UnaryOperator,
        expression: &hir::Expr,
        target: usize,
        children: &[usize],
    ) -> Result<usize> {
        match (operator, expression) {
            (UnaryOperator::Positive, _) => {
                debug_assert_eq!(children, [target]);
            }
            (UnaryOperator::Negative, hir::Expr::Literal(Literal::Numeric(value))) => {
                let value = format!("-{value}");
                match parse_numeric_literal(&value)? {
                    Value::Numeric(Numeric::Integer(value)) => {
                        self.program.emit_insn(Insn::Integer {
                            value,
                            dest: target,
                        });
                    }
                    Value::Numeric(Numeric::Float(value)) => {
                        self.program.emit_insn(Insn::Real {
                            value: value.into(),
                            dest: target,
                        });
                    }
                    _ => unreachable!("numeric parser returns a numeric value"),
                }
            }
            (UnaryOperator::Negative, _) => {
                let [value] = children else {
                    unreachable!("non-literal negation has one lowered child")
                };
                let zero = self.program.alloc_register();
                self.program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: zero,
                });
                self.program.mark_last_insn_constant();
                self.program.emit_insn(Insn::Subtract {
                    lhs: zero,
                    rhs: *value,
                    dest: target,
                });
            }
            (UnaryOperator::BitwiseNot, hir::Expr::Literal(Literal::Numeric(value))) => {
                let value = match parse_numeric_literal(value)? {
                    Value::Numeric(Numeric::Integer(value)) => !value,
                    Value::Numeric(Numeric::Float(value)) => !(f64::from(value) as i64),
                    _ => unreachable!("numeric parser returns a numeric value"),
                };
                self.program.emit_insn(Insn::Integer {
                    value,
                    dest: target,
                });
            }
            (UnaryOperator::BitwiseNot, hir::Expr::Literal(Literal::Null)) => {
                self.program.emit_insn(Insn::Null {
                    dest: target,
                    dest_end: None,
                });
            }
            (UnaryOperator::BitwiseNot, _) => {
                let [value] = children else {
                    unreachable!("non-literal bitwise negation has one lowered child")
                };
                self.program.emit_insn(Insn::BitNot {
                    reg: *value,
                    dest: target,
                });
            }
            (UnaryOperator::Not, _) => {
                let [value] = children else {
                    unreachable!("logical negation has one lowered child")
                };
                self.program.emit_insn(Insn::Not {
                    reg: *value,
                    dest: target,
                });
            }
        }
        Ok(target)
    }
}

/// Lower a resolved expression into an existing target register.
pub(crate) fn translate_expr(
    program: &mut ProgramBuilder,
    document: &hir::HirDocument,
    expression: &hir::Expr,
    target: usize,
) -> Result<usize> {
    translate_expr_with_inputs(program, document, expression, target, &[])
}

/// Lower a resolved expression while taking selected node values from
/// registers prepared by its caller.
pub(crate) fn translate_expr_with_inputs<'expr>(
    program: &mut ProgramBuilder,
    document: &'expr hir::HirDocument,
    expression: &'expr hir::Expr,
    target: usize,
    inputs: &[ExprRegisterInput<'expr>],
) -> Result<usize> {
    expression.walk(
        LoweringContext::new(target),
        &mut ExprLowerer {
            program,
            document,
            inputs,
        },
    )
}

#[derive(Clone, Copy)]
enum ConditionContext {
    Predicate(expr::ConditionMetadata),
    RightOperand {
        metadata: expr::ConditionMetadata,
        next_operand: BranchOffset,
    },
}

impl ConditionContext {
    fn metadata(self) -> expr::ConditionMetadata {
        match self {
            Self::Predicate(metadata) | Self::RightOperand { metadata, .. } => metadata,
        }
    }
}

struct ConditionLowerer<'program, 'document> {
    program: &'program mut ProgramBuilder,
    document: &'document hir::HirDocument,
}

impl<'expr> hir::ExprVisitor<'expr> for ConditionLowerer<'_, 'expr> {
    type Context = ConditionContext;
    type Output = ();
    type Error = LimboError;

    fn child(&mut self, expression: &'expr hir::Expr, index: usize) -> Option<&'expr hir::Expr> {
        let hir::Expr::Binary {
            lhs,
            operator: Operator::And | Operator::Or,
            rhs,
            custom: None,
            ..
        } = expression
        else {
            return None;
        };
        [lhs.as_ref(), rhs.as_ref()].get(index).copied()
    }

    fn pre_order(
        &mut self,
        parent: &hir::Expr,
        context: &mut ConditionContext,
        child_index: usize,
        _child: &hir::Expr,
    ) -> Result<ControlFlow<(), ConditionContext>> {
        let hir::Expr::Binary { operator, .. } = parent else {
            unreachable!("condition visitor only exposes binary AND/OR children")
        };
        match (operator, child_index) {
            (Operator::And, 0) => {
                let next_operand = self.program.allocate_label();
                let metadata = context.metadata();
                *context = ConditionContext::RightOperand {
                    metadata,
                    next_operand,
                };
                Ok(ControlFlow::Continue(ConditionContext::Predicate(
                    expr::ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true: next_operand,
                        ..metadata
                    },
                )))
            }
            (Operator::Or, 0) => {
                let next_operand = self.program.allocate_label();
                let metadata = context.metadata();
                *context = ConditionContext::RightOperand {
                    metadata,
                    next_operand,
                };
                Ok(ControlFlow::Continue(ConditionContext::Predicate(
                    expr::ConditionMetadata {
                        jump_if_condition_is_true: true,
                        jump_target_when_false: next_operand,
                        jump_target_when_null: next_operand,
                        ..metadata
                    },
                )))
            }
            (Operator::And | Operator::Or, 1) => {
                let ConditionContext::RightOperand {
                    metadata,
                    next_operand,
                } = *context
                else {
                    return Err(LimboError::InternalError(
                        "HIR condition is missing its right-operand state".to_string(),
                    ));
                };
                *context = ConditionContext::Predicate(metadata);
                self.program.preassign_label_to_next_insn(next_operand);
                Ok(ControlFlow::Continue(ConditionContext::Predicate(metadata)))
            }
            _ => Err(LimboError::InternalError(format!(
                "HIR condition visitor received invalid child {child_index} for {operator:?}"
            ))),
        }
    }

    fn post_order(
        &mut self,
        expression: &hir::Expr,
        context: ConditionContext,
        _children: &[()],
    ) -> Result<()> {
        if matches!(
            expression,
            hir::Expr::Binary {
                operator: Operator::And | Operator::Or,
                custom: None,
                ..
            }
        ) {
            return Ok(());
        }
        let register = self.program.alloc_register();
        translate_expr(self.program, self.document, expression, register)?;
        expr::emit_cond_jump(self.program, context.metadata(), register);
        Ok(())
    }
}

/// Lower a resolved predicate while preserving AND/OR short-circuiting.
pub(crate) fn translate_condition_expr(
    program: &mut ProgramBuilder,
    document: &hir::HirDocument,
    expression: &hir::Expr,
    metadata: expr::ConditionMetadata,
) -> Result<()> {
    expression.walk(
        ConditionContext::Predicate(metadata),
        &mut ConditionLowerer { program, document },
    )
}

/// Lower an expression while keeping its instructions at the current use.
pub(crate) fn translate_expr_no_constant_opt(
    program: &mut ProgramBuilder,
    document: &hir::HirDocument,
    expression: &hir::Expr,
    target: usize,
) -> Result<usize> {
    translate_expr_with_inputs_no_constant_opt(program, document, expression, target, &[])
}

/// Keep register-dependent expressions inside the row or subroutine using them.
pub(crate) fn translate_expr_with_inputs_no_constant_opt<'expr>(
    program: &mut ProgramBuilder,
    document: &'expr hir::HirDocument,
    expression: &'expr hir::Expr,
    target: usize,
    inputs: &[ExprRegisterInput<'expr>],
) -> Result<usize> {
    let first_new_span = program.constant_spans_next_idx();
    let result = translate_expr_with_inputs(program, document, expression, target, inputs);
    program.constant_spans_invalidate_after(first_new_span);
    result
}

/// Apply bound schema calls to a value already stored in `target`.
pub(crate) fn translate_schema_calls_in_place(
    program: &mut ProgramBuilder,
    document: &hir::HirDocument,
    calls: &[hir::BoundSchemaCall],
    target: usize,
) -> Result<()> {
    for call in calls {
        let schema_program = document.schema_program(call.program).ok_or_else(|| {
            LimboError::InternalError(format!(
                "HIR schema call references missing program {}",
                call.program
            ))
        })?;
        let arguments_start = program.alloc_registers(call.arguments.len());
        for (position, argument) in call.arguments.iter().enumerate() {
            translate_expr_no_constant_opt(
                program,
                document,
                argument,
                arguments_start + position,
            )?;
        }
        program.bind_source(
            schema_program.input_source,
            SourceBinding::SchemaInputs {
                value: target,
                arguments_start,
            },
        );
        translate_expr_no_constant_opt(program, document, &schema_program.body, target)?;
    }
    Ok(())
}

/// Register parameter slots in an expression that runtime lowering may skip.
pub(crate) fn register_parameters(program: &mut ProgramBuilder, expression: &hir::Expr) {
    expression.for_each(&mut |expression| {
        if let hir::Expr::Parameter(parameter) = expression {
            program.register_parameter(parameter.index, &parameter.spelling);
        }
    });
}

/// Emit a literal already selected by semantic analysis.
pub(crate) fn emit_literal(
    program: &mut ProgramBuilder,
    literal: &turso_parser::ast::Literal,
    target: usize,
) -> Result<usize> {
    expr::emit_literal(program, literal, target)
}

/// Emit a parameter using its resolved slot and original SQL spelling.
pub(crate) fn emit_parameter(
    program: &mut ProgramBuilder,
    parameter: &hir::Parameter,
    target: usize,
) -> usize {
    let index = program.register_parameter(parameter.index, &parameter.spelling);
    program.emit_insn(Insn::Variable {
        index,
        dest: target,
    });
    target
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use crate::dialect::SqliteDialect;
    use crate::parameters::ParameterSpelling;
    use crate::schema::{BTreeTable, Index, IndexColumn, Table, Type};
    use crate::sync::Arc;
    use crate::translate::semantic::{
        catalog::{SemanticCatalog, SemanticCatalogDatabase},
        context::DoubleQuotedDml,
        hir::TypeFact,
        SemanticOptions, SemanticRootInput,
    };
    use crate::vdbe::affinity::Affinity;
    use crate::vdbe::builder::{CursorType, ProgramBuilderOpts, QueryMode, SourceBinding};
    use crate::{SymbolTable, MAIN_DB_ID};
    use turso_parser::{ast::SortOrder, parser::Parser};

    fn program() -> ProgramBuilder {
        ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 4, 0))
    }

    fn document(databases: Vec<hir::DatabaseSnapshot>) -> hir::HirDocument {
        hir::HirDocument {
            snapshot: hir::CatalogSnapshot::from_id(1),
            databases,
            root: hir::HirRoot::Query(hir::QueryRoot {
                query: hir::QueryId::new(0),
            }),
            queries: Vec::new(),
            sources: Vec::new(),
            ctes: Vec::new(),
            schema_programs: Vec::new(),
            cdc: None,
        }
    }

    fn analyze_sql(mut schema: crate::schema::Schema, sql: &str) -> hir::HirDocument {
        schema
            .resolve_all_custom_type_affinities()
            .expect("custom affinities resolve");
        let catalog = SemanticCatalog {
            databases: vec![SemanticCatalogDatabase {
                id: hir::DatabaseId::new(MAIN_DB_ID),
                name: "main".to_string(),
                schema: Arc::new(schema),
            }],
            unqualified_database_search_path: vec![hir::DatabaseId::new(MAIN_DB_ID)],
        };
        let statement = match Parser::new(sql.as_bytes())
            .next_cmd()
            .expect("SQL parses")
            .expect("SQL contains a command")
        {
            turso_parser::ast::Cmd::Stmt(statement) => statement,
            _ => panic!("SQL contains a statement"),
        };
        let document = crate::translate::semantic::analyze_root(
            &catalog,
            &SymbolTable::new(),
            SemanticOptions {
                dialect: Arc::new(SqliteDialect),
                custom_types_enabled: true,
                dqs_dml: DoubleQuotedDml::Enabled,
            },
            SemanticRootInput::Statement(&statement),
        )
        .expect("SQL analyzes");
        document.validate().expect("HIR validates");
        document
    }

    fn root_output(document: &hir::HirDocument) -> &hir::Expr {
        let hir::HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces a query root");
        };
        &document.query(root.query).expect("query exists").blocks[0].outputs[0].expr
    }

    fn root_filter(document: &hir::HirDocument) -> &hir::Expr {
        let hir::HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces a query root");
        };
        let hir::QueryBlockBody::Select {
            filter: Some(filter),
            ..
        } = &document.query(root.query).expect("query exists").blocks[0].body
        else {
            panic!("SELECT has a filter");
        };
        filter
    }

    fn custom_operator_schema() -> crate::schema::Schema {
        let mut schema = crate::schema::Schema::new();
        schema
            .add_type_from_sql(
                "CREATE TYPE amount(value INTEGER, factor INTEGER) BASE INTEGER \
                 ENCODE value * factor DECODE value / factor \
                 OPERATOR '+' numeric_add OPERATOR '<' numeric_lt OPERATOR '=' numeric_eq",
            )
            .expect("custom operator type parses");
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql(
                    "CREATE TABLE custom_values(a amount(4), b amount(4)) STRICT",
                    2,
                )
                .expect("custom operator table parses"),
            ))
            .expect("custom operator table name is unique");
        schema
    }

    fn join_schema() -> crate::schema::Schema {
        let mut schema = crate::schema::Schema::new();
        for (sql, root_page) in [
            ("CREATE TABLE left_values(id INTEGER)", 2),
            ("CREATE TABLE right_values(id INTEGER)", 3),
        ] {
            schema
                .add_btree_table(Arc::new(
                    BTreeTable::from_sql(sql, root_page).expect("join table parses"),
                ))
                .expect("join table name is unique");
        }
        schema
    }

    fn lower_btree_output(document: &hir::HirDocument, target: usize) -> ProgramBuilder {
        let hir::HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces a query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let source_id = block.from.as_ref().expect("query has FROM").first;
        let source = document.source(source_id).expect("source exists");
        let hir::SourceKind::Table(table) = &source.kind else {
            panic!("source is a table");
        };
        let Table::BTree(table) = table.value() else {
            panic!("source is a B-tree table");
        };
        let mut program = program();
        let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
        program.bind_source(
            source_id,
            SourceBinding::BTree {
                scan_cursor: cursor,
                table_cursor: None,
            },
        );
        super::translate_expr(&mut program, document, &block.outputs[0].expr, target)
            .expect("analyzed output lowers");
        program
    }

    fn lower_join_output(
        document: &hir::HirDocument,
        target: usize,
    ) -> (ProgramBuilder, Vec<usize>) {
        let hir::HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces a query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let from = block.from.as_ref().expect("query has FROM");
        let mut program = program();
        let mut cursors = Vec::new();
        for source_id in std::iter::once(from.first).chain(from.joins.iter().map(|join| join.right))
        {
            let source = document.source(source_id).expect("source exists");
            let hir::SourceKind::Table(table) = &source.kind else {
                panic!("source is a table");
            };
            let Table::BTree(table) = table.value() else {
                panic!("source is a B-tree table");
            };
            let cursor = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
            program.bind_source(
                source_id,
                SourceBinding::BTree {
                    scan_cursor: cursor,
                    table_cursor: None,
                },
            );
            cursors.push(cursor);
        }
        super::translate_expr(&mut program, document, &block.outputs[0].expr, target)
            .expect("analyzed join output lowers");
        (program, cursors)
    }

    fn translate_expr(
        program: &mut ProgramBuilder,
        expression: &hir::Expr,
        target: usize,
    ) -> Result<usize> {
        super::translate_expr(program, &document(Vec::new()), expression, target)
    }

    fn resolved_function(function: Func) -> hir::ResolvedFunction {
        hir::CatalogObject::new(
            hir::CatalogObjectId::new(1),
            hir::CatalogSnapshot::from_id(1),
            None,
            crate::sync::Arc::new(function),
        )
    }

    fn source_column(
        name: &str,
        storage: Type,
        affinity: Affinity,
        rowid_alias: bool,
    ) -> hir::SourceColumn {
        hir::SourceColumn {
            name: name.to_string(),
            type_fact: TypeFact::known(storage),
            affinity,
            has_affinity: true,
            collation: None,
            hidden: false,
            rowid_alias,
        }
    }

    fn source(
        id: hir::SourceId,
        kind: hir::SourceKind,
        columns: Vec<hir::SourceColumn>,
        rowid_available: bool,
    ) -> hir::Source {
        let width = columns.len();
        hir::Source {
            id,
            owner: hir::SourceOwner::Root,
            database: None,
            name: "items".to_string(),
            alias: None,
            kind,
            columns,
            generated_expressions: vec![hir::ColumnReadExpression::Absent; width],
            default_expressions: vec![hir::ColumnReadExpression::Absent; width],
            column_type_programs: vec![None; width],
            check_constraints: None,
            rowid_available,
            index_hint: hir::IndexHint::None,
            index_expressions: Vec::new(),
            index_coverage: hir::IndexCoverage::Selective,
            index_method_patterns: Vec::new(),
        }
    }

    #[test]
    fn output_references_read_the_bound_result_register() {
        let document = analyze_sql(
            crate::schema::Schema::new(),
            "SELECT random() AS value ORDER BY value",
        );
        let hir::HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces a query root");
        };
        let query = document.query(root.query).expect("query exists");
        let expression = &query.order_by[0].expr;
        let hir::Expr::Output(output) = expression else {
            panic!("ORDER BY alias becomes an output reference");
        };

        let mut lowered = program();
        lowered.bind_output(*output, 7);
        super::translate_expr(&mut lowered, &document, expression, 9).expect("bound output lowers");

        assert!(matches!(
            lowered.insns.as_slice(),
            [(
                Insn::Copy {
                    src_reg: 7,
                    dst_reg: 9,
                    extra_amount: 0,
                },
                _
            )]
        ));

        let mut same_register = program();
        same_register.bind_output(*output, 7);
        super::translate_expr(&mut same_register, &document, expression, 7)
            .expect("output already in its target lowers");
        assert!(same_register.insns.is_empty());
    }

    #[test]
    fn unbound_output_reference_is_rejected() {
        let block = hir::QueryBlockId::new(hir::QueryId::new(0), 0);
        let expression = hir::Expr::output(hir::OutputId::query(block, 0));
        let error = translate_expr(&mut program(), &expression, 7)
            .expect_err("output needs a runtime binding");

        assert!(error.to_string().contains("has no runtime binding"));
    }

    #[test]
    fn aggregate_results_use_the_register_bound_by_aggregate_identity() {
        let document = analyze_sql(crate::schema::Schema::new(), "SELECT sum(random())");
        let expression = root_output(&document);
        let hir::Expr::Function(hir::FunctionCall {
            evaluation: hir::FunctionEvaluation::Aggregate { id, .. },
            ..
        }) = expression
        else {
            panic!("sum becomes an aggregate function");
        };

        let mut program = program();
        program.bind_aggregate_result(*id, 7);
        super::translate_expr(&mut program, &document, expression, 7)
            .expect("bound aggregate lowers");

        assert!(matches!(
            program.insns.as_slice(),
            [(
                Insn::Copy {
                    src_reg: 7,
                    dst_reg: 7,
                    extra_amount: 0,
                },
                _
            )]
        ));
    }

    #[test]
    fn window_results_use_the_register_bound_by_window_identity() {
        let document = analyze_sql(crate::schema::Schema::new(), "SELECT sum(random()) OVER ()");
        let expression = root_output(&document);
        let hir::Expr::Function(hir::FunctionCall {
            evaluation: hir::FunctionEvaluation::Window { id, .. },
            ..
        }) = expression
        else {
            panic!("sum OVER becomes a window function");
        };

        let mut program = program();
        program.bind_window_result(*id, 7);
        super::translate_expr(&mut program, &document, expression, 9)
            .expect("bound window function lowers");

        assert!(matches!(
            program.insns.as_slice(),
            [(
                Insn::Copy {
                    src_reg: 7,
                    dst_reg: 9,
                    extra_amount: 0,
                },
                _
            )]
        ));
    }

    #[test]
    fn function_result_lowering_requires_a_runtime_binding() {
        for sql in ["SELECT count(*)", "SELECT row_number() OVER ()"] {
            let document = analyze_sql(crate::schema::Schema::new(), sql);
            let error = super::translate_expr(&mut program(), &document, root_output(&document), 7)
                .expect_err("query function needs a result register");
            assert!(error.to_string().contains("has no runtime result register"));
        }
    }

    #[test]
    fn query_function_validation_rejects_out_of_range_result_identities() {
        for sql in ["SELECT count(*)", "SELECT row_number() OVER ()"] {
            let mut document = analyze_sql(crate::schema::Schema::new(), sql);
            let hir::HirRoot::Query(root) = &document.root else {
                panic!("SELECT produces a query root");
            };
            let block = &mut document.queries[root.query.index()].blocks[0];
            let aggregate_count = block.aggregate_count;
            let window_function_count = block.window_function_count;
            let hir::Expr::Function(call) = &mut block.outputs[0].expr else {
                panic!("output is a query function");
            };
            match &mut call.evaluation {
                hir::FunctionEvaluation::Aggregate { id, .. } => {
                    id.index = aggregate_count;
                }
                hir::FunctionEvaluation::Window { id, .. } => {
                    id.index = window_function_count;
                }
                hir::FunctionEvaluation::Scalar => panic!("query function is not scalar"),
            }

            assert!(document.validate().is_err());
        }
    }

    #[test]
    fn scalar_and_exists_subqueries_copy_their_bound_results() {
        for (sql, exists) in [
            ("SELECT (SELECT random())", false),
            ("SELECT EXISTS (SELECT random())", true),
        ] {
            let document = analyze_sql(crate::schema::Schema::new(), sql);
            let expression = root_output(&document);
            let query = match expression {
                hir::Expr::Subquery(hir::SubqueryExpr::Scalar { query, output: 0 }) => {
                    assert!(!exists);
                    *query
                }
                hir::Expr::Subquery(hir::SubqueryExpr::Exists(query)) => {
                    assert!(exists);
                    *query
                }
                _ => panic!("output is the expected subquery kind"),
            };
            let mut program = program();
            let result = program.alloc_register();
            let target = program.alloc_register();
            program.bind_subquery(
                query,
                if exists {
                    SubqueryBinding::Exists { register: result }
                } else {
                    SubqueryBinding::RowValue {
                        start: result,
                        count: 1,
                    }
                },
            );

            super::translate_expr(&mut program, &document, expression, target)
                .expect("bound subquery result lowers");

            assert!(matches!(
                program.insns.as_slice(),
                [(Insn::Copy {
                    src_reg,
                    dst_reg,
                    extra_amount: 0,
                }, _)] if *src_reg == result && *dst_reg == target
            ));
        }
    }

    #[test]
    fn row_subquery_copies_its_bound_register_range() {
        let document = analyze_sql(
            crate::schema::Schema::new(),
            "SELECT (1, 2) = (SELECT random(), random())",
        );
        let hir::Expr::Binary { rhs, .. } = root_output(&document) else {
            panic!("output is a row comparison");
        };
        let hir::Expr::Subquery(hir::SubqueryExpr::Row { query }) = rhs.as_ref() else {
            panic!("comparison RHS is a row subquery");
        };
        let mut program = program();
        let result = program.alloc_registers(2);
        let target = program.alloc_registers(2);
        program.bind_subquery(
            *query,
            SubqueryBinding::RowValue {
                start: result,
                count: 2,
            },
        );

        super::translate_expr(&mut program, &document, rhs, target)
            .expect("bound row subquery lowers");

        assert!(matches!(
            program.insns.as_slice(),
            [(Insn::Copy {
                src_reg,
                dst_reg,
                extra_amount: 1,
            }, _)] if *src_reg == result && *dst_reg == target
        ));
    }

    #[test]
    fn subquery_result_binding_must_match_the_hir_shape() {
        let scalar = analyze_sql(crate::schema::Schema::new(), "SELECT (SELECT 1)");
        let hir::Expr::Subquery(hir::SubqueryExpr::Scalar { query, .. }) = root_output(&scalar)
        else {
            panic!("output is a scalar subquery");
        };
        let mut wrong_kind = program();
        wrong_kind.bind_subquery(*query, SubqueryBinding::Exists { register: 1 });
        let error = super::translate_expr(&mut wrong_kind, &scalar, root_output(&scalar), 1)
            .expect_err("scalar subquery needs row-value storage");
        assert!(error.to_string().contains("no row-value runtime binding"));

        let row = analyze_sql(
            crate::schema::Schema::new(),
            "SELECT (1, 2) = (SELECT 3, 4)",
        );
        let hir::Expr::Binary { rhs, .. } = root_output(&row) else {
            panic!("output is a row comparison");
        };
        let hir::Expr::Subquery(hir::SubqueryExpr::Row { query }) = rhs.as_ref() else {
            panic!("comparison RHS is a row subquery");
        };
        let mut wrong_width = program();
        wrong_width.bind_subquery(*query, SubqueryBinding::RowValue { start: 1, count: 1 });
        let error = super::translate_expr(&mut wrong_width, &row, rhs, 1)
            .expect_err("row subquery runtime width must match HIR");
        assert!(error.to_string().contains("does not match output width"));
    }

    #[test]
    fn in_subquery_uses_the_bound_index_and_legacy_null_scan() {
        let document = analyze_sql(
            crate::schema::Schema::new(),
            "SELECT (1, 2) IN (SELECT 3, 4)",
        );
        let expression = root_output(&document);
        let hir::Expr::Subquery(hir::SubqueryExpr::In {
            query,
            negated: false,
            comparison,
            ..
        }) = expression
        else {
            panic!("output is an IN subquery");
        };
        assert_eq!(comparison.components.len(), 2);

        let mut program = program();
        let target = program.alloc_register();
        let cursor = 11;
        program.bind_subquery(*query, SubqueryBinding::InIndex { cursor });
        super::translate_expr(&mut program, &document, expression, target)
            .expect("bound IN subquery lowers");

        // Keep the legacy row evaluation order: after each component is
        // evaluated, branch before evaluating the next component if it is NULL.
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { value: 0, dest }, _),
                (Insn::Integer { value: 1, dest: first }, _),
                (Insn::IsNull { reg, .. }, _),
                (Insn::Integer { value: 2, dest: second }, _),
                (Insn::IsNull { reg: last, .. }, _),
                ..
            ] if *dest == target && *reg == *first && *last == *second
        ));
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::NotFound {
                cursor_id,
                num_regs: 2,
                ..
            } if *cursor_id == cursor
        )));
        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::Rewind { cursor_id, .. } if *cursor_id == cursor
        )));
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::Column { cursor_id, .. } if *cursor_id == cursor))
                .count(),
            2
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(insn, _)| matches!(insn, Insn::Ne { .. }))
                .count(),
            2
        );
        assert!(program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Null { dest, .. } if *dest == target)));
    }

    #[test]
    fn not_in_subquery_uses_found_for_the_negated_probe() {
        let document = analyze_sql(crate::schema::Schema::new(), "SELECT 1 NOT IN (SELECT 2)");
        let expression = root_output(&document);
        let hir::Expr::Subquery(hir::SubqueryExpr::In {
            query,
            negated: true,
            ..
        }) = expression
        else {
            panic!("output is a NOT IN subquery");
        };

        let mut program = program();
        program.bind_subquery(*query, SubqueryBinding::InIndex { cursor: 12 });
        super::translate_expr(&mut program, &document, expression, 7)
            .expect("bound NOT IN subquery lowers");

        assert!(program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::Found {
                cursor_id: 12,
                num_regs: 1,
                ..
            }
        )));
        assert!(!program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::NotFound { .. })));
    }

    #[test]
    fn in_subquery_requires_an_index_binding() {
        let document = analyze_sql(crate::schema::Schema::new(), "SELECT 1 IN (SELECT 2)");
        let expression = root_output(&document);
        let hir::Expr::Subquery(hir::SubqueryExpr::In { query, .. }) = expression else {
            panic!("output is an IN subquery");
        };
        let mut program = program();
        program.bind_subquery(*query, SubqueryBinding::RowValue { start: 1, count: 1 });

        let error = super::translate_expr(&mut program, &document, expression, 7)
            .expect_err("IN subquery needs an index cursor");
        assert!(error.to_string().contains("no index runtime binding"));
    }

    #[test]
    fn scalar_subquery_validation_rejects_an_out_of_range_output() {
        let mut document = analyze_sql(crate::schema::Schema::new(), "SELECT (SELECT 1)");
        let hir::HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces a query root");
        };
        let root_query = root.query;
        let hir::Expr::Subquery(hir::SubqueryExpr::Scalar { query, .. }) =
            &document.queries[root_query.index()].blocks[0].outputs[0].expr
        else {
            panic!("output is a scalar subquery");
        };
        let output = document.queries[query.index()].output.len();
        let hir::Expr::Subquery(hir::SubqueryExpr::Scalar {
            output: invalid_output,
            ..
        }) = &mut document.queries[root_query.index()].blocks[0].outputs[0].expr
        else {
            unreachable!("scalar subquery shape was checked");
        };
        *invalid_output = output;

        assert!(document.validate().is_err());
    }

    #[test]
    fn columns_copy_from_bound_source_registers() {
        let source_id = hir::SourceId::new(0);
        let mut document = document(Vec::new());
        document.sources.push(source(
            source_id,
            hir::SourceKind::SchemaExpression,
            vec![source_column(
                "value",
                Type::Integer,
                Affinity::Integer,
                false,
            )],
            false,
        ));
        let mut program = program();
        program.bind_source(
            source_id,
            SourceBinding::Registers {
                start: 7,
                rowid: None,
            },
        );

        super::translate_expr(&mut program, &document, &hir::Expr::column(source_id, 0), 3)
            .expect("bound register column lowers");

        assert!(matches!(
            program.insns.as_slice(),
            [(
                Insn::Copy {
                    src_reg: 7,
                    dst_reg: 3,
                    extra_amount: 0,
                },
                _
            )]
        ));
    }

    #[test]
    fn generated_columns_use_linked_expression_and_declared_affinity() {
        let source_id = hir::SourceId::new(0);
        let mut definition = source(
            source_id,
            hir::SourceKind::SchemaExpression,
            vec![
                source_column("value", Type::Integer, Affinity::Integer, false),
                source_column("generated", Type::Real, Affinity::Real, false),
            ],
            false,
        );
        definition.generated_expressions[1] =
            hir::ColumnReadExpression::Planned(hir::Expr::column(source_id, 0));
        let mut document = document(Vec::new());
        document.sources.push(definition);
        let mut program = program();
        program.bind_source(
            source_id,
            SourceBinding::Registers {
                start: 7,
                rowid: None,
            },
        );

        super::translate_expr(&mut program, &document, &hir::Expr::column(source_id, 1), 3)
            .expect("generated column lowers");

        assert!(matches!(
            program.insns.as_slice(),
            [
                (
                    Insn::Copy {
                        src_reg: 7,
                        dst_reg: 3,
                        extra_amount: 0,
                    },
                    _
                ),
                (Insn::Affinity { start_reg: 3, .. }, _),
            ]
        ));
    }

    #[test]
    fn custom_columns_run_validated_default_encode_and_decode_programs() {
        let mut schema = crate::schema::Schema::new();
        schema
            .add_type_from_sql(
                "CREATE TYPE scaled(value INTEGER, factor INTEGER) BASE INTEGER \
                 ENCODE value * factor DECODE value / factor",
            )
            .expect("custom type parses");
        schema
            .add_btree_table(Arc::new(
                BTreeTable::from_sql(
                    "CREATE TABLE typed_values(value scaled(4) DEFAULT 3) STRICT",
                    2,
                )
                .expect("custom table parses"),
            ))
            .expect("table name is unique");
        let document = analyze_sql(schema, "SELECT value FROM typed_values");
        let hir::HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces a query root");
        };
        let block = &document.query(root.query).expect("query exists").blocks[0];
        let source_id = block.from.as_ref().expect("query has FROM").first;
        let source = document.source(source_id).expect("source exists");
        let hir::SourceKind::Table(table) = &source.kind else {
            panic!("source is a table");
        };
        let Table::BTree(table) = table.value() else {
            panic!("source is a B-tree table");
        };
        let expression = &block.outputs[0].expr;
        let mut decoded = program();
        let cursor = decoded.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
        decoded.bind_source(
            source_id,
            SourceBinding::BTree {
                scan_cursor: cursor,
                table_cursor: None,
            },
        );

        super::translate_expr(&mut decoded, &document, expression, 3)
            .expect("custom column lowers");

        let instruction = |predicate: fn(&Insn) -> bool| {
            decoded
                .insns
                .iter()
                .position(|(instruction, _)| predicate(instruction))
                .expect("expected instruction was emitted")
        };
        let column = instruction(|instruction| {
            matches!(
                instruction,
                Insn::Column {
                    dest: 3,
                    default: None,
                    ..
                }
            )
        });
        let has_field =
            instruction(|instruction| matches!(instruction, Insn::ColumnHasField { .. }));
        let default =
            instruction(|instruction| matches!(instruction, Insn::Integer { value: 3, dest: 3 }));
        let encode =
            instruction(|instruction| matches!(instruction, Insn::Multiply { dest: 3, .. }));
        let null_guard =
            instruction(|instruction| matches!(instruction, Insn::IsNull { reg: 3, .. }));
        let decode = instruction(|instruction| matches!(instruction, Insn::Divide { dest: 3, .. }));
        assert!(matches!(
            decoded.insns[column].0,
            Insn::Column { cursor_id, .. } if cursor_id == cursor
        ));
        assert!(column < has_field);
        assert!(has_field < default);
        assert!(default < encode);
        assert!(encode < null_guard);
        assert!(null_guard < decode);

        let mut suppressed = program();
        suppressed.flags.set_suppress_custom_type_decode(true);
        let suppressed_cursor = suppressed.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
        suppressed.bind_source(
            source_id,
            SourceBinding::BTree {
                scan_cursor: suppressed_cursor,
                table_cursor: None,
            },
        );
        super::translate_expr(&mut suppressed, &document, expression, 3)
            .expect("encoded custom column lowers without decode");
        assert!(matches!(
            suppressed.insns.as_slice(),
            [(
                Insn::Column {
                    cursor_id,
                    dest: 3,
                    default: None,
                    ..
                },
                _
            )] if *cursor_id == suppressed_cursor
        ));
    }

    #[test]
    fn custom_binary_operator_uses_resolved_function_and_swap_order() {
        let document = analyze_sql(custom_operator_schema(), "SELECT a > b FROM custom_values");
        let program = lower_btree_output(&document, 20);

        let columns = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Column { column, .. } => Some(*column),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(columns, [1, 0], "swapped operator evaluates b before a");

        let (function_index, result) = program
            .insns
            .iter()
            .enumerate()
            .find_map(|(index, (instruction, _))| match instruction {
                Insn::Function {
                    dest,
                    func:
                        FuncCtx {
                            func: Func::Scalar(ScalarFunc::NumericLt),
                            arg_count: 2,
                        },
                    ..
                } => Some((index, *dest)),
                _ => None,
            })
            .expect("resolved less-than function is called");
        assert!(!program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Not { .. })));
        assert!(matches!(
            program.insns[function_index + 1].0,
            Insn::Copy {
                src_reg,
                dst_reg: 20,
                extra_amount: 0,
            } if src_reg == result
        ));
    }

    #[test]
    fn custom_binary_operator_encodes_literal_into_resolved_argument_slot() {
        for (sql, literal_position, negate, literal_before_column) in [
            ("SELECT a >= 7 FROM custom_values", 1, true, false),
            ("SELECT 7 < a FROM custom_values", 0, false, true),
        ] {
            let document = analyze_sql(custom_operator_schema(), sql);
            let program = lower_btree_output(&document, 20);
            let column_index = program
                .insns
                .iter()
                .position(|(instruction, _)| matches!(instruction, Insn::Column { column: 0, .. }))
                .expect("custom column is read");
            let literal_index = program
                .insns
                .iter()
                .position(|(instruction, _)| matches!(instruction, Insn::Integer { value: 7, .. }))
                .expect("literal is evaluated");
            assert_eq!(literal_index < column_index, literal_before_column);

            let (arguments, result) = program
                .insns
                .iter()
                .find_map(|(instruction, _)| match instruction {
                    Insn::Function {
                        start_reg,
                        dest,
                        func:
                            FuncCtx {
                                func: Func::Scalar(ScalarFunc::NumericLt),
                                arg_count: 2,
                            },
                        ..
                    } => Some((*start_reg, *dest)),
                    _ => None,
                })
                .expect("resolved less-than function is called");
            assert!(program.insns.iter().any(|(instruction, _)| matches!(
                instruction,
                Insn::Multiply { dest, .. } if *dest == arguments + literal_position
            )));
            assert!(program.insns.iter().any(|(instruction, _)| matches!(
                instruction,
                Insn::Copy { dst_reg, .. } if *dst_reg == arguments + (1 - literal_position)
            )));
            assert_eq!(
                program.insns.iter().any(|(instruction, _)| matches!(
                    instruction,
                    Insn::Not { reg, dest } if *reg == result && *dest == result
                )),
                negate
            );
            assert!(program.insns.iter().any(|(instruction, _)| matches!(
                instruction,
                Insn::Copy {
                    src_reg,
                    dst_reg: 20,
                    extra_amount: 0,
                } if *src_reg == result
            )));
        }
    }

    #[test]
    fn merged_join_columns_lower_only_the_resolved_runtime_values() {
        for (join, expected_columns, coalesces) in [
            ("LEFT JOIN", vec![0], false),
            ("RIGHT JOIN", vec![1], false),
            ("FULL JOIN", vec![0, 1], true),
        ] {
            let document = analyze_sql(
                join_schema(),
                &format!("SELECT id FROM left_values {join} right_values USING(id)"),
            );
            let (program, cursors) = lower_join_output(&document, 20);
            let reads = program
                .insns
                .iter()
                .filter_map(|(instruction, _)| match instruction {
                    Insn::Column {
                        cursor_id,
                        column: 0,
                        dest: 20,
                        ..
                    } => Some(*cursor_id),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let expected = expected_columns
                .into_iter()
                .map(|position| cursors[position])
                .collect::<Vec<_>>();
            assert_eq!(reads, expected);

            let null_guards = program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::NotNull { reg: 20, .. }))
                .count();
            assert_eq!(null_guards, usize::from(coalesces));
            if coalesces {
                let first = program
                    .insns
                    .iter()
                    .position(|(instruction, _)| {
                        matches!(instruction, Insn::Column { cursor_id, .. } if *cursor_id == cursors[0])
                    })
                    .expect("left merged value is read");
                let guard = program
                    .insns
                    .iter()
                    .position(|(instruction, _)| matches!(instruction, Insn::NotNull { .. }))
                    .expect("full join merge short-circuits");
                let second = program
                    .insns
                    .iter()
                    .position(|(instruction, _)| {
                        matches!(instruction, Insn::Column { cursor_id, .. } if *cursor_id == cursors[1])
                    })
                    .expect("right merged value is read");
                assert!(first < guard && guard < second);
            }
        }
    }

    #[test]
    fn merged_join_column_validation_requires_a_right_source_column() {
        let mut document = analyze_sql(
            join_schema(),
            "SELECT id FROM left_values FULL JOIN right_values USING(id)",
        );
        let hir::HirRoot::Query(root) = &document.root else {
            panic!("SELECT produces a query root");
        };
        let query = root.query;
        let hir::Expr::MergedColumn(merged) =
            &mut document.queries[query.index()].blocks[0].outputs[0].expr
        else {
            panic!("USING output is a merged column");
        };
        merged.right = Box::new(hir::Expr::Literal(Literal::Numeric("1".to_string())));

        let error = document
            .validate()
            .expect_err("merged right side must retain its source identity");
        assert!(error
            .to_string()
            .contains("merged-column right side is not a source column"));
    }

    #[test]
    fn ordinary_defaults_keep_the_column_instruction_default() {
        let source_id = hir::SourceId::new(0);
        let table = Arc::new(
            BTreeTable::from_sql("CREATE TABLE items(value TEXT DEFAULT 7)", 2)
                .expect("table parses"),
        );
        let resolved_table = hir::CatalogObject::new(
            hir::CatalogObjectId::new(1),
            hir::CatalogSnapshot::from_id(1),
            None,
            Arc::new(Table::BTree(table.clone())),
        );
        let mut definition = source(
            source_id,
            hir::SourceKind::Table(resolved_table),
            vec![source_column("value", Type::Text, Affinity::Text, false)],
            true,
        );
        definition.default_expressions[0] = hir::ColumnReadExpression::Planned(hir::Expr::Literal(
            Literal::Numeric("7".to_string()),
        ));
        let mut document = document(Vec::new());
        document.sources.push(definition);
        let mut program = program();
        let table_cursor = program.alloc_cursor_id(CursorType::BTreeTable(table));
        program.bind_source(
            source_id,
            SourceBinding::BTree {
                scan_cursor: table_cursor,
                table_cursor: None,
            },
        );

        super::translate_expr(&mut program, &document, &hir::Expr::column(source_id, 0), 3)
            .expect("column with an ordinary default lowers");

        assert!(matches!(
            program.insns.as_slice(),
            [(
                Insn::Column {
                    cursor_id,
                    column: 0,
                    dest: 3,
                    default: Some(_),
                },
                _
            )] if *cursor_id == table_cursor
        ));
    }

    #[test]
    fn linked_generated_columns_do_not_use_the_call_stack() {
        const WIDTH: usize = 20_000;

        let source_id = hir::SourceId::new(0);
        let mut columns = Vec::with_capacity(WIDTH);
        for index in 0..WIDTH {
            let mut column = source_column(
                &format!("column_{index}"),
                Type::Integer,
                Affinity::Integer,
                false,
            );
            column.has_affinity = false;
            columns.push(column);
        }
        let mut definition = source(source_id, hir::SourceKind::SchemaExpression, columns, false);
        for index in 1..WIDTH {
            definition.generated_expressions[index] =
                hir::ColumnReadExpression::Planned(hir::Expr::column(source_id, index - 1));
        }
        let mut document = document(Vec::new());
        document.sources.push(definition);
        let mut program = program();
        program.bind_source(
            source_id,
            SourceBinding::Registers {
                start: 7,
                rowid: None,
            },
        );

        super::translate_expr(
            &mut program,
            &document,
            &hir::Expr::column(source_id, WIDTH - 1),
            3,
        )
        .expect("deep generated chain lowers iteratively");

        assert!(matches!(
            program.insns.as_slice(),
            [(
                Insn::Copy {
                    src_reg: 7,
                    dst_reg: 3,
                    extra_amount: 0,
                },
                _
            )]
        ));
    }

    #[test]
    fn btree_columns_and_rowids_use_the_scan_index() {
        let source_id = hir::SourceId::new(0);
        let table = Arc::new(
            BTreeTable::from_sql(
                "CREATE TABLE items(id INTEGER PRIMARY KEY, value REAL, note TEXT)",
                2,
            )
            .expect("table parses"),
        );
        let resolved_table = hir::CatalogObject::new(
            hir::CatalogObjectId::new(1),
            hir::CatalogSnapshot::from_id(1),
            None,
            Arc::new(Table::BTree(table.clone())),
        );
        let mut document = document(Vec::new());
        document.sources.push(source(
            source_id,
            hir::SourceKind::Table(resolved_table),
            vec![
                source_column("id", Type::Integer, Affinity::Integer, true),
                source_column("value", Type::Real, Affinity::Real, false),
                source_column("note", Type::Text, Affinity::Text, false),
            ],
            true,
        ));

        let index = Arc::new(Index {
            name: "items_value".to_string(),
            table_name: "items".to_string(),
            root_page: 3,
            columns: vec![IndexColumn {
                name: "value".to_string(),
                order: SortOrder::Asc,
                nulls_order: None,
                pos_in_table: 1,
                collation: None,
                default: None,
                expr: None,
            }],
            unique: false,
            ephemeral: false,
            has_rowid: true,
            where_clause: None,
            index_method: None,
            on_conflict: None,
        });
        let mut program = program();
        let table_cursor = program.alloc_cursor_id(CursorType::BTreeTable(table));
        let index_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index));
        program.bind_source(
            source_id,
            SourceBinding::BTree {
                scan_cursor: index_cursor,
                table_cursor: Some(table_cursor),
            },
        );

        super::translate_expr(&mut program, &document, &hir::Expr::column(source_id, 1), 4)
            .expect("indexed column lowers");
        super::translate_expr(&mut program, &document, &hir::Expr::column(source_id, 2), 6)
            .expect("table fallback column lowers");
        super::translate_expr(&mut program, &document, &hir::Expr::rowid(source_id), 5)
            .expect("index rowid lowers");

        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Column {
                    cursor_id,
                    column: 0,
                    dest: 4,
                    ..
                }, _),
                (Insn::RealAffinity { register: 4 }, _),
                (Insn::Column {
                    cursor_id: fallback_cursor,
                    column: 2,
                    dest: 6,
                    ..
                }, _),
                (Insn::IdxRowId { cursor_id: rowid_cursor, dest: 5 }, _),
            ] if *cursor_id == index_cursor
                && *fallback_cursor == table_cursor
                && *rowid_cursor == index_cursor
        ));
    }

    fn ordinary_scalar_call(function: Func, values: Vec<hir::Expr>) -> hir::Expr {
        hir::Expr::Function(hir::FunctionCall {
            function: resolved_function(function),
            evaluation: hir::FunctionEvaluation::Scalar,
            arguments: hir::FunctionArguments::Expressions {
                facts: vec![
                    hir::FunctionArgumentFacts {
                        type_fact: TypeFact::dynamic(),
                        collation: None,
                    };
                    values.len()
                ],
                values,
                distinctness: None,
                order_by: Vec::new(),
            },
            result_type: TypeFact::dynamic(),
            operation: hir::FunctionOperation::Ordinary,
        })
    }

    fn custom_type_call(
        function: ScalarFunc,
        values: Vec<hir::Expr>,
        operation: hir::CustomTypeOperation,
    ) -> hir::Expr {
        hir::Expr::Function(hir::FunctionCall {
            function: resolved_function(Func::Scalar(function)),
            evaluation: hir::FunctionEvaluation::Scalar,
            arguments: hir::FunctionArguments::Expressions {
                facts: vec![
                    hir::FunctionArgumentFacts {
                        type_fact: TypeFact::dynamic(),
                        collation: None,
                    };
                    values.len()
                ],
                values,
                distinctness: None,
                order_by: Vec::new(),
            },
            result_type: TypeFact::dynamic(),
            operation: hir::FunctionOperation::CustomType(operation),
        })
    }

    fn sequence_call(kind: hir::SequenceOperationKind, values: Vec<hir::Expr>) -> hir::Expr {
        let database = hir::DatabaseId::new(2);
        let normalized_name = "seq".to_string();
        let backing_sql = crate::translate::sequence::sequence_backing_table_sql("seq");
        let backing_table = crate::schema::BTreeTable::from_sql(&backing_sql, 9).unwrap();
        let sequence = crate::schema::Sequence::new(
            normalized_name.clone(),
            Some(1),
            Some(1),
            Some(1),
            Some(100),
            false,
        )
        .unwrap();
        let function = match kind {
            hir::SequenceOperationKind::NextValue => ScalarFunc::NextVal,
            hir::SequenceOperationKind::SetValue => ScalarFunc::SetVal,
        };
        hir::Expr::Function(hir::FunctionCall {
            function: resolved_function(Func::Scalar(function)),
            evaluation: hir::FunctionEvaluation::Scalar,
            arguments: hir::FunctionArguments::Expressions {
                facts: vec![
                    hir::FunctionArgumentFacts {
                        type_fact: TypeFact::dynamic(),
                        collation: None,
                    };
                    values.len()
                ],
                values,
                distinctness: None,
                order_by: Vec::new(),
            },
            result_type: TypeFact::known(crate::schema::Type::Integer),
            operation: hir::FunctionOperation::Sequence(hir::SequenceOperation {
                kind,
                user_name: "aux.seq".to_string(),
                normalized_name,
                sequence: hir::CatalogObject::new(
                    hir::CatalogObjectId::new(2),
                    hir::CatalogSnapshot::from_id(1),
                    Some(database),
                    crate::sync::Arc::new(sequence),
                ),
                backing_table: hir::CatalogObject::new(
                    hir::CatalogObjectId::new(3),
                    hir::CatalogSnapshot::from_id(1),
                    Some(database),
                    crate::sync::Arc::new(crate::schema::Table::BTree(crate::sync::Arc::new(
                        backing_table,
                    ))),
                ),
                sqlite_sequence: None,
            }),
        })
    }

    fn resolved_type(kind: crate::schema::TypeDefKind) -> hir::ResolvedType {
        hir::CatalogObject::new(
            hir::CatalogObjectId::new(2),
            hir::CatalogSnapshot::from_id(1),
            None,
            crate::sync::Arc::new(crate::schema::TypeDef {
                name: "container".to_string(),
                is_builtin: false,
                not_null: false,
                is_domain: false,
                sql: String::new(),
                domain_checks: Vec::new(),
                kind,
            }),
        )
    }

    #[test]
    fn custom_type_calls_use_resolved_metadata_and_skip_name_arguments() {
        #[derive(Debug)]
        enum ExpectedInsn {
            UnionPack,
            UnionTag,
            UnionExtract,
            StructField,
        }

        let union_type =
            resolved_type(crate::schema::TypeDefKind::Union(crate::schema::UnionDef {
                variants: Vec::new(),
                tag_names: crate::sync::Arc::from(Vec::<String>::new()),
            }));
        let struct_type = resolved_type(crate::schema::TypeDefKind::Struct(
            crate::schema::StructDef { fields: Vec::new() },
        ));
        let runtime_value = || hir::Expr::Literal(Literal::Numeric("7".to_string()));
        let semantic_name = || hir::Expr::Literal(Literal::String("'ignored'".to_string()));
        let cases = [
            (
                custom_type_call(
                    ScalarFunc::UnionValueFunc,
                    vec![semantic_name(), runtime_value()],
                    hir::CustomTypeOperation::UnionValue {
                        union_type: union_type.clone(),
                        tag_index: 3,
                    },
                ),
                ExpectedInsn::UnionPack,
            ),
            (
                custom_type_call(
                    ScalarFunc::UnionTagFunc,
                    vec![runtime_value()],
                    hir::CustomTypeOperation::UnionTag {
                        union_type: union_type.clone(),
                        tag_names: crate::sync::Arc::from([
                            "email".to_string(),
                            "chat".to_string(),
                        ]),
                    },
                ),
                ExpectedInsn::UnionTag,
            ),
            (
                custom_type_call(
                    ScalarFunc::UnionExtractFunc,
                    vec![runtime_value(), semantic_name()],
                    hir::CustomTypeOperation::UnionExtract {
                        union_type,
                        tag_index: 4,
                    },
                ),
                ExpectedInsn::UnionExtract,
            ),
            (
                custom_type_call(
                    ScalarFunc::StructExtractFunc,
                    vec![runtime_value(), semantic_name()],
                    hir::CustomTypeOperation::StructExtract {
                        struct_type,
                        field_index: 5,
                    },
                ),
                ExpectedInsn::StructField,
            ),
        ];

        for (expression, expected_operation) in cases {
            let mut program = program();
            translate_expr(&mut program, &expression, 8).unwrap();

            assert_eq!(program.insns.len(), 2);
            assert!(matches!(
                program.insns[0].0,
                Insn::Integer { value: 7, dest: 1 }
            ));
            match (&program.insns[1].0, expected_operation) {
                (
                    Insn::UnionPack {
                        tag_index: 3,
                        value_reg: 1,
                        dest: 8,
                    },
                    ExpectedInsn::UnionPack,
                ) => {}
                (
                    Insn::UnionTag {
                        src_reg: 1,
                        dest: 8,
                        tag_names,
                    },
                    ExpectedInsn::UnionTag,
                ) if tag_names.as_ref() == ["email", "chat"] => {}
                (
                    Insn::UnionExtract {
                        src_reg: 1,
                        expected_tag: 4,
                        dest: 8,
                    },
                    ExpectedInsn::UnionExtract,
                )
                | (
                    Insn::StructField {
                        src_reg: 1,
                        field_index: 5,
                        dest: 8,
                    },
                    ExpectedInsn::StructField,
                ) => {}
                unexpected => panic!("unexpected custom-type lowering: {unexpected:?}"),
            }
        }
    }

    fn trigger_program() -> ProgramBuilder {
        let mut program = program();
        program.trigger = Some(crate::sync::Arc::new(crate::schema::Trigger::new(
            "trigger".to_string(),
            String::new(),
            "items".to_string(),
            None,
            turso_parser::ast::TriggerEvent::Insert,
            true,
            None,
            Vec::new(),
            false,
            None,
        )));
        program
    }

    #[test]
    fn resolved_parameter_spelling_reaches_program_metadata() {
        let cases = [
            (1, ParameterSpelling::Anonymous, None),
            (5, ParameterSpelling::Numbered, Some("?5")),
            (
                6,
                ParameterSpelling::Named(":name".to_string()),
                Some(":name"),
            ),
        ];

        for (index, spelling, expected_name) in cases {
            let mut program = program();
            let parameter = hir::Parameter {
                index: NonZeroU32::new(index).unwrap(),
                spelling,
                type_fact: TypeFact::dynamic(),
            };

            assert_eq!(emit_parameter(&mut program, &parameter, 2), 2);

            let index = usize::try_from(index).unwrap().try_into().unwrap();
            assert_eq!(program.parameters.name(index).as_deref(), expected_name);
            assert!(matches!(
                program.insns.as_slice(),
                [(Insn::Variable { index: emitted, dest: 2 }, _)] if *emitted == index
            ));
        }
    }

    #[test]
    fn resolved_literal_uses_existing_opcode_emission() {
        let mut program = program();

        assert_eq!(
            emit_literal(
                &mut program,
                &turso_parser::ast::Literal::String("'value'".to_string()),
                3,
            )
            .expect("literal emits"),
            3
        );
        assert!(matches!(
            program.insns.as_slice(),
            [(Insn::String8 { value, dest: 3 }, _)] if value == "value"
        ));
    }

    #[test]
    fn unary_lowering_keeps_existing_register_and_opcode_order() {
        let mut program = program();
        let expression = hir::Expr::Unary {
            operator: UnaryOperator::Negative,
            expr: Box::new(hir::Expr::Parameter(hir::Parameter {
                index: NonZeroU32::new(1).unwrap(),
                spelling: ParameterSpelling::Anonymous,
                type_fact: TypeFact::dynamic(),
            })),
        };

        assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Variable { index, dest: 1 }, _),
                (Insn::Integer { value: 0, dest: 2 }, _),
                (Insn::Subtract { lhs: 2, rhs: 1, dest: 8 }, _),
            ] if index.get() == 1
        ));
    }

    #[test]
    fn unary_literal_fast_paths_skip_child_emission() {
        let cases = [
            (
                UnaryOperator::Negative,
                Literal::Numeric("3".to_string()),
                -3,
            ),
            (
                UnaryOperator::BitwiseNot,
                Literal::Numeric("3".to_string()),
                !3,
            ),
        ];

        for (operator, literal, expected) in cases {
            let mut program = program();
            let expression = hir::Expr::Unary {
                operator,
                expr: Box::new(hir::Expr::Literal(literal)),
            };
            translate_expr(&mut program, &expression, 4).unwrap();
            assert!(matches!(
                program.insns.as_slice(),
                [(Insn::Integer { value, dest: 4 }, _)] if *value == expected
            ));
        }

        let mut program = program();
        let expression = hir::Expr::Unary {
            operator: UnaryOperator::BitwiseNot,
            expr: Box::new(hir::Expr::Literal(Literal::Null)),
        };
        translate_expr(&mut program, &expression, 4).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [(
                Insn::Null {
                    dest: 4,
                    dest_end: None
                },
                _
            )]
        ));
    }

    #[test]
    fn null_test_lowering_keeps_existing_sequence_for_nested_expressions() {
        for is_null in [true, false] {
            let value = hir::Expr::Binary {
                lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                operator: Operator::Add,
                rhs: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
                array_concat: false,
                custom: None,
                comparison: None,
            };
            let expression = if is_null {
                hir::Expr::IsNull(Box::new(value))
            } else {
                hir::Expr::NotNull(Box::new(value))
            };
            let mut program = program();

            assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
            assert!(matches!(
                &program.insns[..4],
                [
                    (Insn::Integer { value: 2, dest: 2 }, _),
                    (Insn::Integer { value: 3, dest: 3 }, _),
                    (
                        Insn::Add {
                            lhs: 2,
                            rhs: 3,
                            dest: 1
                        },
                        _
                    ),
                    (Insn::Integer { value: 1, dest: 8 }, _),
                ]
            ));
            if is_null {
                assert!(matches!(program.insns[4].0, Insn::IsNull { reg: 1, .. }));
            } else {
                assert!(matches!(program.insns[4].0, Insn::NotNull { reg: 1, .. }));
            }
            assert!(matches!(
                program.insns[5].0,
                Insn::Integer { value: 0, dest: 8 }
            ));
        }
    }

    #[test]
    fn truth_test_lowering_keeps_existing_flags_for_nested_expressions() {
        for (is_true, negated, expected_invert) in [
            (true, false, false),
            (false, false, true),
            (true, true, true),
            (false, true, false),
        ] {
            let expression = hir::Expr::TruthTest {
                expr: Box::new(hir::Expr::Binary {
                    lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                    operator: Operator::Add,
                    rhs: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
                    array_concat: false,
                    custom: None,
                    comparison: None,
                }),
                is_true,
                negated,
            };
            let mut program = program();

            assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
            assert!(matches!(
                program.insns.as_slice(),
                [
                    (Insn::Integer { value: 2, dest: 2 }, _),
                    (Insn::Integer { value: 3, dest: 3 }, _),
                    (
                        Insn::Add {
                            lhs: 2,
                            rhs: 3,
                            dest: 1
                        },
                        _
                    ),
                    (
                        Insn::IsTrue {
                            reg: 1,
                            dest: 8,
                            null_value,
                            invert,
                        },
                        _
                    ),
                ] if *null_value == negated && *invert == expected_invert
            ));
        }
    }

    #[test]
    fn collate_lowers_only_its_child_and_comparison_uses_resolved_metadata() {
        let collation = hir::CatalogObject::new(
            hir::CatalogObjectId::new(1),
            hir::CatalogSnapshot::from_id(1),
            None,
            crate::sync::Arc::new(crate::translate::collate::CollationSeq::NoCase),
        );
        let expression = hir::Expr::Binary {
            lhs: Box::new(hir::Expr::Collate {
                expr: Box::new(hir::Expr::Literal(Literal::String("'left'".to_string()))),
                collation: collation.clone(),
            }),
            operator: Operator::Equals,
            rhs: Box::new(hir::Expr::Literal(Literal::String("'right'".to_string()))),
            array_concat: false,
            custom: None,
            comparison: Some(hir::ComparisonSemantics {
                components: vec![hir::ComparisonComponent {
                    affinity: crate::vdbe::affinity::Affinity::Text,
                    collation: Some(collation),
                    array: false,
                }],
            }),
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            &program.insns[..2],
            [
                (Insn::String8 { value, dest: 1 }, _),
                (Insn::String8 { dest: 2, .. }, _),
            ] if value == "left"
        ));
        assert!(matches!(
            program.insns[3].0,
            Insn::Eq {
                collation: Some(crate::translate::collate::CollationSeq::NoCase),
                ..
            }
        ));
        assert_eq!(program.curr_collation_ctx(), None);
    }

    #[test]
    fn builtin_cast_uses_resolved_affinity_and_skips_type_parameters() {
        for affinity in [
            crate::vdbe::affinity::Affinity::Numeric,
            crate::vdbe::affinity::Affinity::Text,
        ] {
            let expression = hir::Expr::Cast {
                expr: Box::new(hir::Expr::Binary {
                    lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                    operator: Operator::Add,
                    rhs: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
                    array_concat: false,
                    custom: None,
                    comparison: None,
                }),
                target: hir::TypeName {
                    name: "resolved".to_string(),
                    parameters: vec![hir::Expr::Literal(Literal::Numeric("99".to_string()))],
                    array_dimensions: 0,
                    type_fact: TypeFact::dynamic(),
                    affinity,
                    programs: hir::BoundCastPrograms {
                        encode: Vec::new(),
                        domain: None,
                        apply_builtin_affinity: true,
                    },
                },
            };
            let mut program = program();

            assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
            assert!(matches!(
                program.insns.as_slice(),
                [
                    (Insn::Integer { value: 2, dest: 1 }, _),
                    (Insn::Integer { value: 3, dest: 2 }, _),
                    (
                        Insn::Add {
                            lhs: 1,
                            rhs: 2,
                            dest: 8
                        },
                        _
                    ),
                    (
                        Insn::Cast {
                            reg: 8,
                            affinity: emitted_affinity
                        },
                        _
                    ),
                ] if *emitted_affinity == affinity
            ));
        }
    }

    #[test]
    fn custom_cast_passes_resolved_type_parameters_to_the_encoder() {
        let mut schema = crate::schema::Schema::new();
        schema
            .add_type_from_sql(
                "CREATE TYPE scaled(value INTEGER, factor INTEGER) BASE INTEGER \
                 ENCODE value * factor DECODE value / factor",
            )
            .expect("custom type parses");
        let document = analyze_sql(schema, "SELECT CAST(2 AS scaled(4))");
        let expression = root_output(&document);
        let mut program = program();

        super::translate_expr(&mut program, &document, expression, 8)
            .expect("parameterized custom CAST lowers");

        let parameter = program
            .insns
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::Integer { value: 4, .. }))
            .expect("resolved type parameter is evaluated");
        let encode = program
            .insns
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::Multiply { dest: 8, .. }))
            .expect("custom encoder is emitted");
        assert!(parameter < encode);
        assert!(!program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Cast { .. })));
    }

    #[test]
    fn custom_domain_cast_runs_validated_encode_and_constraint_programs() {
        let mut schema = crate::schema::Schema::new();
        schema
            .add_type_from_sql(
                "CREATE TYPE shifted(value INTEGER) BASE INTEGER \
                 ENCODE value + 1 DECODE value - 1",
            )
            .expect("custom type parses");
        schema
            .add_type_from_sql(
                "CREATE DOMAIN positive_shifted AS shifted \
                 CONSTRAINT positive CHECK (value > 0) \
                 CONSTRAINT small CHECK (value < 10)",
            )
            .expect("parent domain parses");
        schema
            .add_type_from_sql("CREATE DOMAIN required_positive AS positive_shifted NOT NULL")
            .expect("child domain parses");
        let document = analyze_sql(schema, "SELECT CAST(2 AS required_positive)");
        let expression = root_output(&document);
        let mut program = program();

        super::translate_expr(&mut program, &document, expression, 8)
            .expect("custom domain CAST lowers");

        let instruction = |predicate: fn(&Insn) -> bool| {
            program
                .insns
                .iter()
                .position(|(instruction, _)| predicate(instruction))
                .expect("expected instruction was emitted")
        };
        let value =
            instruction(|instruction| matches!(instruction, Insn::Integer { value: 2, dest: 8 }));
        let encode = instruction(|instruction| matches!(instruction, Insn::Add { dest: 8, .. }));
        let not_null = instruction(|instruction| {
            matches!(
                instruction,
                Insn::HaltIfNull {
                    err_code: SQLITE_CONSTRAINT_NOTNULL,
                    description,
                    ..
                } if description == "domain required_positive does not allow null values"
            )
        });
        let check = instruction(|instruction| matches!(instruction, Insn::Gt { .. }));
        let check_null = instruction(|instruction| matches!(instruction, Insn::IsNull { .. }));
        let check_truth = instruction(|instruction| matches!(instruction, Insn::If { .. }));
        let first_failure = instruction(|instruction| {
            matches!(
                instruction,
                Insn::Halt {
                    err_code: SQLITE_CONSTRAINT_CHECK,
                    description,
                    ..
                } if description
                    == "value for domain positive_shifted violates check constraint \"positive\""
            )
        });
        let second_check = instruction(|instruction| matches!(instruction, Insn::Lt { .. }));
        let second_failure = instruction(|instruction| {
            matches!(
                instruction,
                Insn::Halt {
                    err_code: SQLITE_CONSTRAINT_CHECK,
                    description,
                    ..
                } if description
                    == "value for domain positive_shifted violates check constraint \"small\""
            )
        });
        assert!(value < encode);
        assert!(encode < not_null);
        assert!(not_null < check);
        assert!(check < check_null);
        assert!(check_null < check_truth);
        assert!(check_truth < first_failure);
        assert!(first_failure < second_check);
        assert!(second_check < second_failure);
        assert!(!program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Cast { .. })));
    }

    #[test]
    fn simple_case_evaluates_base_once_and_uses_resolved_comparisons() {
        let collation = hir::CatalogObject::new(
            hir::CatalogObjectId::new(1),
            hir::CatalogSnapshot::from_id(1),
            None,
            crate::sync::Arc::new(crate::translate::collate::CollationSeq::NoCase),
        );
        let expression = hir::Expr::Case {
            base: Some(Box::new(hir::Expr::Literal(Literal::Numeric(
                "99".to_string(),
            )))),
            when_then: vec![
                (
                    hir::Expr::Literal(Literal::Numeric("1".to_string())),
                    hir::Expr::Literal(Literal::String("'one'".to_string())),
                ),
                (
                    hir::Expr::Literal(Literal::Numeric("2".to_string())),
                    hir::Expr::Literal(Literal::String("'two'".to_string())),
                ),
            ],
            else_expr: Some(Box::new(hir::Expr::Literal(Literal::String(
                "'other'".to_string(),
            )))),
            base_comparisons: vec![
                hir::ComparisonSemantics {
                    components: vec![hir::ComparisonComponent {
                        affinity: crate::vdbe::affinity::Affinity::Text,
                        collation: Some(collation),
                        array: false,
                    }],
                },
                numeric_comparison(),
            ],
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| {
                    matches!(instruction, Insn::Integer { value: 99, .. })
                })
                .count(),
            1
        );
        let comparisons = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Ne {
                    lhs,
                    rhs,
                    flags,
                    collation,
                    ..
                } => Some((
                    *lhs,
                    *rhs,
                    flags.get_affinity(),
                    flags.has_jump_if_null(),
                    *collation,
                )),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            comparisons,
            vec![
                (
                    1,
                    2,
                    crate::vdbe::affinity::Affinity::Text,
                    true,
                    Some(crate::translate::collate::CollationSeq::NoCase),
                ),
                (1, 2, crate::vdbe::affinity::Affinity::Numeric, true, None,),
            ]
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Goto { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn searched_case_uses_if_not_and_missing_else_writes_null() {
        let expression = hir::Expr::Case {
            base: None,
            when_then: vec![
                (
                    hir::Expr::Literal(Literal::Null),
                    hir::Expr::Literal(Literal::String("'null'".to_string())),
                ),
                (
                    hir::Expr::Literal(Literal::Numeric("1".to_string())),
                    hir::Expr::Literal(Literal::String("'true'".to_string())),
                ),
            ],
            else_expr: None,
            base_comparisons: Vec::new(),
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        let tests = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::IfNot {
                    reg, jump_if_null, ..
                } => Some((*reg, *jump_if_null)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(tests, vec![(1, true), (1, true)]);
        assert!(matches!(
            program.insns.last().map(|(instruction, _)| instruction),
            Some(Insn::Null {
                dest: 8,
                dest_end: None,
            })
        ));
    }

    #[test]
    fn case_lowering_does_not_use_the_call_stack() {
        let mut expression = hir::Expr::Literal(Literal::Numeric("1".to_string()));
        for _ in 0..10_000 {
            expression = hir::Expr::Case {
                base: None,
                when_then: vec![(
                    hir::Expr::Literal(Literal::Numeric("1".to_string())),
                    expression,
                )],
                else_expr: Some(Box::new(hir::Expr::Literal(Literal::Null))),
                base_comparisons: Vec::new(),
            };
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::IfNot { .. }))
                .count(),
            10_000
        );

        std::mem::forget(expression);
    }

    #[test]
    fn empty_in_list_skips_its_left_operand() {
        for (negated, expected) in [(false, 0), (true, 1)] {
            let expression = hir::Expr::InList {
                lhs: Box::new(hir::Expr::Parameter(hir::Parameter {
                    index: NonZeroU32::new(1).unwrap(),
                    spelling: ParameterSpelling::Anonymous,
                    type_fact: TypeFact::dynamic(),
                })),
                negated,
                values: Vec::new(),
                comparisons: Vec::new(),
            };
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(matches!(
                program.insns.as_slice(),
                [(Insn::Integer { value, dest: 8 }, _)] if *value == expected
            ));
        }
    }

    #[test]
    fn scalar_in_list_uses_resolved_comparisons_and_tracks_nulls() {
        let collation = hir::CatalogObject::new(
            hir::CatalogObjectId::new(1),
            hir::CatalogSnapshot::from_id(1),
            None,
            crate::sync::Arc::new(crate::translate::collate::CollationSeq::NoCase),
        );
        let comparison = hir::ComparisonSemantics {
            components: vec![hir::ComparisonComponent {
                affinity: crate::vdbe::affinity::Affinity::Text,
                collation: Some(collation),
                array: true,
            }],
        };
        let expression = hir::Expr::InList {
            lhs: Box::new(hir::Expr::Literal(Literal::String("'a'".to_string()))),
            negated: true,
            values: vec![
                hir::Expr::Literal(Literal::String("'b'".to_string())),
                hir::Expr::Literal(Literal::Null),
            ],
            comparisons: vec![comparison.clone(), comparison],
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::BitAnd { .. }))
                .count(),
            3
        );
        let comparisons = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Eq {
                    flags, collation, ..
                } => Some((flags.get_affinity(), flags.has_array_cmp(), *collation)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            comparisons,
            vec![
                (
                    crate::vdbe::affinity::Affinity::Text,
                    true,
                    Some(crate::translate::collate::CollationSeq::NoCase),
                );
                2
            ]
        );
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::IsNull { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Not { .. })));
        assert!(matches!(
            program.insns.last().map(|(instruction, _)| instruction),
            Some(Insn::Copy {
                dst_reg: 8,
                extra_amount: 0,
                ..
            })
        ));
    }

    #[test]
    fn row_in_list_compares_each_component() {
        let expression = hir::Expr::InList {
            lhs: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::Numeric("1".to_string())),
                hir::Expr::Literal(Literal::Numeric("2".to_string())),
            ])),
            negated: false,
            values: vec![hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::Numeric("3".to_string())),
                hir::Expr::Literal(Literal::Numeric("4".to_string())),
            ])],
            comparisons: vec![numeric_row_comparison(2)],
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        let comparisons = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Ne { lhs, rhs, .. } => Some(("ne", *lhs, *rhs)),
                Insn::Eq { lhs, rhs, .. } => Some(("eq", *lhs, *rhs)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(comparisons, vec![("ne", 2, 5), ("eq", 3, 6)]);
    }

    #[test]
    fn nested_in_list_lowering_does_not_use_the_call_stack() {
        let mut expression = hir::Expr::Literal(Literal::Numeric("1".to_string()));
        for _ in 0..10_000 {
            expression = hir::Expr::InList {
                lhs: Box::new(expression),
                negated: false,
                values: vec![hir::Expr::Literal(Literal::Numeric("1".to_string()))],
                comparisons: vec![numeric_comparison()],
            };
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Eq { .. }))
                .count(),
            10_000
        );

        std::mem::forget(expression);
    }

    #[test]
    fn like_uses_legacy_argument_order_and_resolved_function() {
        let expression = hir::Expr::Like {
            lhs: Box::new(hir::Expr::Literal(Literal::String("'value'".to_string()))),
            negated: false,
            operator: turso_parser::ast::LikeOperator::Like,
            function: resolved_function(Func::Scalar(ScalarFunc::Like)),
            argument_count: 2,
            rhs: Box::new(hir::Expr::Literal(Literal::String("'pattern'".to_string()))),
            escape: None,
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::String8 { value: lhs, dest: 2 }, _),
                (Insn::String8 { value: rhs, dest: 1 }, _),
                (
                    Insn::Function {
                        constant_mask: 1,
                        start_reg: 1,
                        dest: 8,
                        func:
                            FuncCtx {
                                func: Func::Scalar(ScalarFunc::Like),
                                arg_count: 2,
                            },
                    },
                    _,
                ),
            ] if lhs == "value" && rhs == "pattern"
        ));
    }

    #[test]
    fn like_escape_and_negation_keep_existing_register_shape() {
        let expression = hir::Expr::Like {
            lhs: Box::new(hir::Expr::Literal(Literal::String("'value'".to_string()))),
            negated: true,
            operator: turso_parser::ast::LikeOperator::Like,
            function: resolved_function(Func::Scalar(ScalarFunc::Like)),
            argument_count: 3,
            rhs: Box::new(hir::Expr::Literal(Literal::String("'pattern'".to_string()))),
            escape: Some(Box::new(hir::Expr::Literal(Literal::String(
                "'!'".to_string(),
            )))),
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::String8 { dest: 2, .. }, _),
                (Insn::String8 { dest: 1, .. }, _),
                (Insn::String8 { dest: 3, .. }, _),
                (
                    Insn::Function {
                        constant_mask: 1,
                        start_reg: 1,
                        dest: 4,
                        func: FuncCtx { arg_count: 3, .. },
                    },
                    _,
                ),
                (Insn::Not { reg: 4, dest: 8 }, _),
            ]
        ));
    }

    #[test]
    fn regexp_uses_the_function_resolved_by_semantic_analysis() {
        let expression = hir::Expr::Like {
            lhs: Box::new(hir::Expr::Literal(Literal::String("'value'".to_string()))),
            negated: false,
            operator: turso_parser::ast::LikeOperator::Regexp,
            function: resolved_function(Func::Scalar(ScalarFunc::Abs)),
            argument_count: 2,
            rhs: Box::new(hir::Expr::Literal(Literal::String("'pattern'".to_string()))),
            escape: None,
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.last().map(|(instruction, _)| instruction),
            Some(Insn::Function {
                constant_mask: 0,
                start_reg: 1,
                dest: 8,
                func: FuncCtx {
                    func: Func::Scalar(ScalarFunc::Abs),
                    arg_count: 2,
                },
            })
        ));
    }

    #[test]
    fn match_places_row_columns_before_the_query() {
        let expression = hir::Expr::Like {
            lhs: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::String("'one'".to_string())),
                hir::Expr::Literal(Literal::String("'two'".to_string())),
            ])),
            negated: false,
            operator: turso_parser::ast::LikeOperator::Match,
            function: resolved_function(Func::Scalar(ScalarFunc::Abs)),
            argument_count: 3,
            rhs: Box::new(hir::Expr::Literal(Literal::String("'query'".to_string()))),
            escape: None,
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::String8 { dest: 1, .. }, _),
                (Insn::String8 { dest: 2, .. }, _),
                (Insn::String8 { dest: 3, .. }, _),
                (
                    Insn::Function {
                        constant_mask: 0,
                        start_reg: 1,
                        dest: 8,
                        func: FuncCtx { arg_count: 3, .. },
                    },
                    _,
                ),
            ]
        ));
    }

    #[test]
    fn nested_like_lowering_does_not_use_the_call_stack() {
        let function = resolved_function(Func::Scalar(ScalarFunc::Like));
        let mut expression = hir::Expr::Literal(Literal::String("'value'".to_string()));
        for _ in 0..10_000 {
            expression = hir::Expr::Like {
                lhs: Box::new(expression),
                negated: false,
                operator: turso_parser::ast::LikeOperator::Like,
                function: function.clone(),
                argument_count: 2,
                rhs: Box::new(hir::Expr::Literal(Literal::String("'pattern'".to_string()))),
                escape: None,
            };
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Function { .. }))
                .count(),
            10_000
        );

        std::mem::forget(expression);
    }

    #[test]
    fn empty_array_uses_the_existing_make_array_opcode() {
        let mut program = program();

        translate_expr(&mut program, &hir::Expr::Array(Vec::new()), 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [(
                Insn::MakeArray {
                    start_reg: 1,
                    count: 0,
                    dest: 8,
                },
                _,
            )]
        ));
    }

    #[test]
    fn array_elements_use_consecutive_registers() {
        let expression = hir::Expr::Array(vec![
            hir::Expr::Literal(Literal::Numeric("1".to_string())),
            hir::Expr::Binary {
                lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                operator: Operator::Add,
                rhs: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
                array_concat: false,
                custom: None,
                comparison: None,
            },
        ]);
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { value: 1, dest: 1 }, _),
                (Insn::Integer { value: 2, dest: 3 }, _),
                (Insn::Integer { value: 3, dest: 4 }, _),
                (
                    Insn::Add {
                        lhs: 3,
                        rhs: 4,
                        dest: 2,
                    },
                    _,
                ),
                (
                    Insn::MakeArray {
                        start_reg: 1,
                        count: 2,
                        dest: 8,
                    },
                    _,
                ),
            ]
        ));
    }

    #[test]
    fn subscript_uses_separate_base_and_index_registers() {
        let expression = hir::Expr::Subscript {
            base: Box::new(hir::Expr::Array(vec![
                hir::Expr::Literal(Literal::Numeric("10".to_string())),
                hir::Expr::Literal(Literal::Numeric("20".to_string())),
            ])),
            index: Box::new(hir::Expr::Literal(Literal::Numeric("1".to_string()))),
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { value: 10, dest: 3 }, _),
                (Insn::Integer { value: 20, dest: 4 }, _),
                (
                    Insn::MakeArray {
                        start_reg: 3,
                        count: 2,
                        dest: 1,
                    },
                    _,
                ),
                (Insn::Integer { value: 1, dest: 2 }, _),
                (
                    Insn::ArrayElement {
                        array_reg: 1,
                        index_reg: 2,
                        dest: 8,
                    },
                    _,
                ),
            ]
        ));
    }

    #[test]
    fn nested_array_lowering_does_not_use_the_call_stack() {
        let mut expression = hir::Expr::Literal(Literal::Numeric("1".to_string()));
        for _ in 0..10_000 {
            expression = hir::Expr::Array(vec![expression]);
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::MakeArray { .. }))
                .count(),
            10_000
        );

        std::mem::forget(expression);
    }

    #[test]
    fn field_access_uses_the_resolved_member_index() {
        let cases = [
            (
                hir::FieldAccessKind::Struct { field_index: 7 },
                resolved_type(crate::schema::TypeDefKind::Struct(
                    crate::schema::StructDef { fields: Vec::new() },
                )),
            ),
            (
                hir::FieldAccessKind::Union { tag_index: 3 },
                resolved_type(crate::schema::TypeDefKind::Union(crate::schema::UnionDef {
                    variants: Vec::new(),
                    tag_names: crate::sync::Arc::from(Vec::<String>::new()),
                })),
            ),
        ];

        for (kind, container_type) in cases {
            let expression = hir::Expr::FieldAccess(hir::FieldAccess {
                base: Box::new(hir::Expr::Literal(Literal::Numeric("1".to_string()))),
                field_name: "already_resolved".to_string(),
                kind,
                container_type,
                result_type: TypeFact::dynamic(),
            });
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(matches!(
                (&program.insns[0].0, &program.insns[1].0, kind),
                (
                    Insn::Integer { dest: 1, .. },
                    Insn::StructField {
                        src_reg: 1,
                        field_index: 7,
                        dest: 8,
                    },
                    hir::FieldAccessKind::Struct { .. },
                ) | (
                    Insn::Integer { dest: 1, .. },
                    Insn::UnionExtract {
                        src_reg: 1,
                        expected_tag: 3,
                        dest: 8,
                    },
                    hir::FieldAccessKind::Union { .. },
                )
            ));
        }
    }

    #[test]
    fn nested_field_access_writes_each_result_into_its_parent_register() {
        let struct_type = resolved_type(crate::schema::TypeDefKind::Struct(
            crate::schema::StructDef { fields: Vec::new() },
        ));
        let union_type =
            resolved_type(crate::schema::TypeDefKind::Union(crate::schema::UnionDef {
                variants: Vec::new(),
                tag_names: crate::sync::Arc::from(Vec::<String>::new()),
            }));
        let expression = hir::Expr::FieldAccess(hir::FieldAccess {
            base: Box::new(hir::Expr::FieldAccess(hir::FieldAccess {
                base: Box::new(hir::Expr::Literal(Literal::Numeric("1".to_string()))),
                field_name: "variant".to_string(),
                kind: hir::FieldAccessKind::Union { tag_index: 2 },
                container_type: union_type,
                result_type: TypeFact::dynamic(),
            })),
            field_name: "field".to_string(),
            kind: hir::FieldAccessKind::Struct { field_index: 4 },
            container_type: struct_type,
            result_type: TypeFact::dynamic(),
        });
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { dest: 2, .. }, _),
                (
                    Insn::UnionExtract {
                        src_reg: 2,
                        expected_tag: 2,
                        dest: 1,
                    },
                    _,
                ),
                (
                    Insn::StructField {
                        src_reg: 1,
                        field_index: 4,
                        dest: 8,
                    },
                    _,
                ),
            ]
        ));
    }

    #[test]
    fn nested_field_access_lowering_does_not_use_the_call_stack() {
        let container_type = resolved_type(crate::schema::TypeDefKind::Struct(
            crate::schema::StructDef { fields: Vec::new() },
        ));
        let mut expression = hir::Expr::Literal(Literal::Numeric("1".to_string()));
        for _ in 0..10_000 {
            expression = hir::Expr::FieldAccess(hir::FieldAccess {
                base: Box::new(expression),
                field_name: "field".to_string(),
                kind: hir::FieldAccessKind::Struct { field_index: 0 },
                container_type: container_type.clone(),
                result_type: TypeFact::dynamic(),
            });
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::StructField { .. }))
                .count(),
            10_000
        );

        std::mem::forget(expression);
    }

    #[test]
    fn standalone_abort_embeds_a_literal_message() {
        let expression = hir::Expr::Raise {
            action: ResolveType::Abort,
            message: Some(Box::new(hir::Expr::Literal(Literal::String(
                "'stop'' now'".to_string(),
            )))),
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [(
                Insn::Halt {
                    err_code: SQLITE_ERROR,
                    description,
                    on_error: Some(ResolveType::Abort),
                    description_reg: None,
                },
                _,
            )] if description == "stop' now"
        ));
    }

    #[test]
    fn dynamic_raise_message_uses_one_result_register() {
        let expression = hir::Expr::Raise {
            action: ResolveType::Abort,
            message: Some(Box::new(hir::Expr::Binary {
                lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                operator: Operator::Add,
                rhs: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
                array_concat: false,
                custom: None,
                comparison: None,
            })),
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { value: 2, dest: 2 }, _),
                (Insn::Integer { value: 3, dest: 3 }, _),
                (
                    Insn::Add {
                        lhs: 2,
                        rhs: 3,
                        dest: 1,
                    },
                    _,
                ),
                (
                    Insn::Halt {
                        err_code: SQLITE_ERROR,
                        description_reg: Some(1),
                        on_error: Some(ResolveType::Abort),
                        ..
                    },
                    _,
                ),
            ]
        ));
    }

    #[test]
    fn trigger_raise_uses_trigger_error_codes_and_ignore_shape() {
        let cases = [
            (
                hir::Expr::Raise {
                    action: ResolveType::Ignore,
                    message: None,
                },
                ResolveType::Ignore,
                0,
            ),
            (
                hir::Expr::Raise {
                    action: ResolveType::Fail,
                    message: Some(Box::new(hir::Expr::Literal(Literal::String(
                        "'stop'".to_string(),
                    )))),
                },
                ResolveType::Fail,
                SQLITE_CONSTRAINT_TRIGGER,
            ),
        ];

        for (expression, expected_action, expected_code) in cases {
            let mut program = trigger_program();
            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(matches!(
                program.insns.as_slice(),
                [(Insn::Halt { err_code, on_error: Some(action), .. }, _)]
                    if *err_code == expected_code && *action == expected_action
            ));
        }
    }

    #[test]
    fn nested_raise_lowering_does_not_use_the_call_stack() {
        let mut expression = hir::Expr::Literal(Literal::Numeric("1".to_string()));
        for _ in 0..10_000 {
            expression = hir::Expr::Raise {
                action: ResolveType::Abort,
                message: Some(Box::new(expression)),
            };
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Halt { .. }))
                .count(),
            10_000
        );

        std::mem::forget(expression);
    }

    #[derive(Clone, Copy)]
    enum ExpectedBinaryInsn {
        Add,
        Subtract,
        Multiply,
        Divide,
        Remainder,
        And,
        Or,
        BitAnd,
        BitOr,
        ShiftLeft,
        ShiftRight,
    }

    #[derive(Clone, Copy)]
    enum ExpectedComparisonInsn {
        Eq,
        Ne,
        Lt,
        Le,
        Gt,
        Ge,
    }

    fn comparison_expression(operator: Operator, component: hir::ComparisonComponent) -> hir::Expr {
        hir::Expr::Binary {
            lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
            operator,
            rhs: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
            array_concat: false,
            custom: None,
            comparison: Some(hir::ComparisonSemantics {
                components: vec![component],
            }),
        }
    }

    fn row_comparison_expression(
        operator: Operator,
        components: Vec<hir::ComparisonComponent>,
    ) -> hir::Expr {
        hir::Expr::Binary {
            lhs: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::String("'a'".to_string())),
                hir::Expr::Literal(Literal::Numeric("2".to_string())),
            ])),
            operator,
            rhs: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::String("'b'".to_string())),
                hir::Expr::Literal(Literal::Numeric("3".to_string())),
            ])),
            array_concat: false,
            custom: None,
            comparison: Some(hir::ComparisonSemantics { components }),
        }
    }

    #[test]
    fn ordinary_binary_lowering_keeps_existing_register_and_opcode_shape() {
        let cases = [
            (Operator::Add, ExpectedBinaryInsn::Add),
            (Operator::Subtract, ExpectedBinaryInsn::Subtract),
            (Operator::Multiply, ExpectedBinaryInsn::Multiply),
            (Operator::Divide, ExpectedBinaryInsn::Divide),
            (Operator::Modulus, ExpectedBinaryInsn::Remainder),
            (Operator::And, ExpectedBinaryInsn::And),
            (Operator::Or, ExpectedBinaryInsn::Or),
            (Operator::BitwiseAnd, ExpectedBinaryInsn::BitAnd),
            (Operator::BitwiseOr, ExpectedBinaryInsn::BitOr),
            (Operator::LeftShift, ExpectedBinaryInsn::ShiftLeft),
            (Operator::RightShift, ExpectedBinaryInsn::ShiftRight),
        ];

        for (operator, expected) in cases {
            let expression = hir::Expr::Binary {
                lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                operator,
                rhs: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
                array_concat: false,
                custom: None,
                comparison: None,
            };
            let mut program = program();

            assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
            assert!(matches!(
                &program.insns[..2],
                [
                    (Insn::Integer { value: 2, dest: 1 }, _),
                    (Insn::Integer { value: 3, dest: 2 }, _),
                ]
            ));
            let instruction = &program.insns[2].0;
            match expected {
                ExpectedBinaryInsn::Add => assert!(matches!(
                    instruction,
                    Insn::Add {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::Subtract => assert!(matches!(
                    instruction,
                    Insn::Subtract {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::Multiply => assert!(matches!(
                    instruction,
                    Insn::Multiply {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::Divide => assert!(matches!(
                    instruction,
                    Insn::Divide {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::Remainder => assert!(matches!(
                    instruction,
                    Insn::Remainder {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::And => assert!(matches!(
                    instruction,
                    Insn::And {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::Or => assert!(matches!(
                    instruction,
                    Insn::Or {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::BitAnd => assert!(matches!(
                    instruction,
                    Insn::BitAnd {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::BitOr => assert!(matches!(
                    instruction,
                    Insn::BitOr {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::ShiftLeft => assert!(matches!(
                    instruction,
                    Insn::ShiftLeft {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::ShiftRight => assert!(matches!(
                    instruction,
                    Insn::ShiftRight {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
            }
        }
    }

    #[cfg(feature = "json")]
    #[test]
    fn json_arrow_lowering_keeps_existing_function_and_register_shape() {
        for (operator, expected) in [
            (Operator::ArrowRight, JsonFunc::JsonArrowExtract),
            (Operator::ArrowRightShift, JsonFunc::JsonArrowShiftExtract),
        ] {
            let expression = hir::Expr::Binary {
                lhs: Box::new(hir::Expr::Literal(Literal::String(
                    "'{\"value\": 3}'".to_string(),
                ))),
                operator,
                rhs: Box::new(hir::Expr::Literal(Literal::String("'$.value'".to_string()))),
                array_concat: false,
                custom: None,
                comparison: None,
            };
            let mut program = program();

            assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
            assert!(matches!(
                &program.insns[..2],
                [
                    (Insn::String8 { dest: 1, .. }, _),
                    (Insn::String8 { dest: 2, .. }, _),
                ]
            ));
            let Insn::Function {
                constant_mask: 0,
                start_reg: 1,
                dest: 8,
                func:
                    FuncCtx {
                        func: Func::Json(actual),
                        arg_count: 2,
                    },
            } = &program.insns[2].0
            else {
                panic!("JSON arrow emits its resolved built-in function")
            };
            assert_eq!(actual, &expected);
        }
    }

    fn numeric_comparison() -> hir::ComparisonSemantics {
        hir::ComparisonSemantics {
            components: vec![hir::ComparisonComponent {
                affinity: crate::vdbe::affinity::Affinity::Numeric,
                collation: None,
                array: false,
            }],
        }
    }

    fn numeric_row_comparison(width: usize) -> hir::ComparisonSemantics {
        hir::ComparisonSemantics {
            components: (0..width)
                .map(|_| hir::ComparisonComponent {
                    affinity: crate::vdbe::affinity::Affinity::Numeric,
                    collation: None,
                    array: false,
                })
                .collect(),
        }
    }

    #[test]
    fn scalar_between_lowering_keeps_existing_comparison_order() {
        for negated in [false, true] {
            let expression = hir::Expr::Between {
                expr: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                negated,
                start: Box::new(hir::Expr::Literal(Literal::Numeric("1".to_string()))),
                end: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
                start_comparison: numeric_comparison(),
                end_comparison: numeric_comparison(),
            };
            let mut program = program();

            assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
            assert!(matches!(
                &program.insns[..3],
                [
                    (Insn::Integer { value: 2, dest: 1 }, _),
                    (Insn::Integer { value: 1, dest: 3 }, _),
                    (Insn::Integer { value: 1, dest: 2 }, _),
                ]
            ));
            if negated {
                assert!(matches!(
                    program.insns[3].0,
                    Insn::Lt { lhs: 1, rhs: 3, .. }
                ));
            } else {
                assert!(matches!(
                    program.insns[3].0,
                    Insn::Ge { lhs: 1, rhs: 3, .. }
                ));
            }
            assert!(matches!(
                program.insns[4].0,
                Insn::ZeroOrNull {
                    rg1: 1,
                    rg2: 3,
                    dest: 2,
                }
            ));
            assert!(matches!(
                &program.insns[5..7],
                [
                    (Insn::Integer { value: 3, dest: 5 }, _),
                    (Insn::Integer { value: 1, dest: 4 }, _),
                ]
            ));
            if negated {
                assert!(matches!(
                    program.insns[7].0,
                    Insn::Gt { lhs: 1, rhs: 5, .. }
                ));
                assert!(matches!(
                    program.insns[9].0,
                    Insn::Or {
                        lhs: 2,
                        rhs: 4,
                        dest: 8,
                    }
                ));
            } else {
                assert!(matches!(
                    program.insns[7].0,
                    Insn::Le { lhs: 1, rhs: 5, .. }
                ));
                assert!(matches!(
                    program.insns[9].0,
                    Insn::And {
                        lhs: 2,
                        rhs: 4,
                        dest: 8,
                    }
                ));
            }
        }
    }

    #[test]
    fn scalar_between_reuses_equivalent_value_bounds() {
        let value = hir::Expr::Literal(Literal::Numeric("7".to_string()));
        let expression = hir::Expr::Between {
            expr: Box::new(value.clone()),
            negated: false,
            start: Box::new(value.clone()),
            end: Box::new(value),
            start_comparison: numeric_comparison(),
            end_comparison: numeric_comparison(),
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| {
                    matches!(instruction, Insn::Integer { value: 7, .. })
                })
                .count(),
            1
        );
        assert!(matches!(
            program.insns[2].0,
            Insn::Ge { lhs: 1, rhs: 1, .. }
        ));
        assert!(matches!(
            program.insns[5].0,
            Insn::Le { lhs: 1, rhs: 1, .. }
        ));
    }

    #[test]
    fn row_between_uses_register_ranges_resolved_facts_and_existing_order() {
        let collation = hir::CatalogObject::new(
            hir::CatalogObjectId::new(1),
            hir::CatalogSnapshot::from_id(1),
            None,
            crate::sync::Arc::new(crate::translate::collate::CollationSeq::NoCase),
        );
        let comparison = hir::ComparisonSemantics {
            components: vec![
                hir::ComparisonComponent {
                    affinity: crate::vdbe::affinity::Affinity::Text,
                    collation: Some(collation),
                    array: false,
                },
                hir::ComparisonComponent {
                    affinity: crate::vdbe::affinity::Affinity::Numeric,
                    collation: None,
                    array: false,
                },
            ],
        };
        let expression = hir::Expr::Between {
            expr: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::Numeric("20".to_string())),
                hir::Expr::Literal(Literal::Numeric("21".to_string())),
            ])),
            negated: false,
            start: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::Numeric("10".to_string())),
                hir::Expr::Literal(Literal::Numeric("11".to_string())),
            ])),
            end: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::Numeric("30".to_string())),
                hir::Expr::Literal(Literal::Numeric("31".to_string())),
            ])),
            start_comparison: comparison.clone(),
            end_comparison: comparison,
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 12).unwrap();
        let value_destinations = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Integer { value, dest } if matches!(*value, 10 | 11 | 20 | 21 | 30 | 31) => {
                    Some(*dest)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(value_destinations, vec![1, 2, 4, 5, 7, 8]);

        let lower_position = program
            .insns
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::Gt { .. }))
            .expect("lower comparison is emitted");
        let end_position = program
            .insns
            .iter()
            .position(|(instruction, _)| {
                matches!(instruction, Insn::Integer { value: 30, dest: 7 })
            })
            .expect("end row is evaluated");
        assert!(lower_position < end_position);

        let comparisons = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Gt {
                    lhs,
                    rhs,
                    flags,
                    collation,
                    ..
                }
                | Insn::Lt {
                    lhs,
                    rhs,
                    flags,
                    collation,
                    ..
                } => Some((*lhs, *rhs, flags.get_affinity(), *collation)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            comparisons,
            vec![
                (
                    1,
                    4,
                    crate::vdbe::affinity::Affinity::Text,
                    Some(crate::translate::collate::CollationSeq::NoCase)
                ),
                (2, 5, crate::vdbe::affinity::Affinity::Numeric, None),
                (
                    1,
                    7,
                    crate::vdbe::affinity::Affinity::Text,
                    Some(crate::translate::collate::CollationSeq::NoCase)
                ),
                (2, 8, crate::vdbe::affinity::Affinity::Numeric, None),
            ]
        );
        assert!(matches!(
            program.insns.last().map(|(instruction, _)| instruction),
            Some(Insn::And {
                lhs: 3,
                rhs: 6,
                dest: 12,
            })
        ));
    }

    #[test]
    fn row_not_between_uses_less_greater_and_or() {
        let expression = hir::Expr::Between {
            expr: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::Numeric("2".to_string())),
                hir::Expr::Literal(Literal::Numeric("3".to_string())),
            ])),
            negated: true,
            start: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::Numeric("1".to_string())),
                hir::Expr::Literal(Literal::Numeric("1".to_string())),
            ])),
            end: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::Numeric("4".to_string())),
                hir::Expr::Literal(Literal::Numeric("4".to_string())),
            ])),
            start_comparison: numeric_row_comparison(2),
            end_comparison: numeric_row_comparison(2),
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 12).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Lt { .. }))
                .count(),
            2
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Gt { .. }))
                .count(),
            2
        );
        assert!(matches!(
            program.insns.last().map(|(instruction, _)| instruction),
            Some(Insn::Or {
                lhs: 3,
                rhs: 6,
                dest: 12,
            })
        ));
    }

    #[test]
    fn row_between_reuses_equivalent_value_ranges() {
        let value = hir::Expr::Row(vec![
            hir::Expr::Literal(Literal::Numeric("7".to_string())),
            hir::Expr::Literal(Literal::Numeric("8".to_string())),
        ]);
        let expression = hir::Expr::Between {
            expr: Box::new(value.clone()),
            negated: false,
            start: Box::new(value.clone()),
            end: Box::new(value),
            start_comparison: numeric_row_comparison(2),
            end_comparison: numeric_row_comparison(2),
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 12).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(
                    instruction,
                    Insn::Integer { value: 7 | 8, .. }
                ))
                .count(),
            2
        );
        let comparisons = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Gt { lhs, rhs, .. } | Insn::Lt { lhs, rhs, .. } => Some((lhs, rhs)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(comparisons.len(), 4);
        assert!(comparisons.into_iter().all(|(lhs, rhs)| lhs == rhs));
    }

    #[test]
    fn row_comparison_uses_consecutive_registers_and_resolved_component_facts() {
        let collation = hir::CatalogObject::new(
            hir::CatalogObjectId::new(1),
            hir::CatalogSnapshot::from_id(1),
            None,
            crate::sync::Arc::new(crate::translate::collate::CollationSeq::NoCase),
        );
        let expression = row_comparison_expression(
            Operator::Less,
            vec![
                hir::ComparisonComponent {
                    affinity: crate::vdbe::affinity::Affinity::Text,
                    collation: Some(collation),
                    array: false,
                },
                hir::ComparisonComponent {
                    affinity: crate::vdbe::affinity::Affinity::Numeric,
                    collation: None,
                    array: false,
                },
            ],
        );
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            &program.insns[..4],
            [
                (Insn::String8 { dest: 1, .. }, _),
                (Insn::Integer { value: 2, dest: 2 }, _),
                (Insn::String8 { dest: 3, .. }, _),
                (Insn::Integer { value: 3, dest: 4 }, _),
            ]
        ));

        let comparisons = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Lt {
                    lhs,
                    rhs,
                    flags,
                    collation,
                    ..
                } => Some((*lhs, *rhs, flags.get_affinity(), *collation)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            comparisons,
            vec![
                (
                    1,
                    3,
                    crate::vdbe::affinity::Affinity::Text,
                    Some(crate::translate::collate::CollationSeq::NoCase),
                ),
                (2, 4, crate::vdbe::affinity::Affinity::Numeric, None,),
            ]
        );
    }

    #[test]
    fn row_equality_preserves_null_and_is_uses_null_equal() {
        for (operator, null_equal) in [(Operator::Equals, false), (Operator::Is, true)] {
            let expression = row_comparison_expression(
                operator,
                vec![
                    hir::ComparisonComponent {
                        affinity: crate::vdbe::affinity::Affinity::Text,
                        collation: None,
                        array: false,
                    },
                    hir::ComparisonComponent {
                        affinity: crate::vdbe::affinity::Affinity::Numeric,
                        collation: None,
                        array: false,
                    },
                ],
            );
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            let equality_flags = program
                .insns
                .iter()
                .filter_map(|(instruction, _)| match instruction {
                    Insn::Eq { flags, .. } => Some(flags),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(equality_flags.len(), 2);
            assert!(equality_flags
                .iter()
                .all(|flags| flags.has_nulleq() == null_equal));
            assert_eq!(
                program.insns.iter().any(|(instruction, _)| matches!(
                    instruction,
                    Insn::Null {
                        dest: 8,
                        dest_end: None,
                    }
                )),
                !null_equal
            );
        }
    }

    #[test]
    fn row_ordering_emits_each_component_in_lexicographic_order() {
        for (operator, inclusive) in [
            (Operator::Less, false),
            (Operator::LessEquals, true),
            (Operator::Greater, false),
            (Operator::GreaterEquals, true),
        ] {
            let expression = row_comparison_expression(
                operator,
                vec![
                    hir::ComparisonComponent {
                        affinity: crate::vdbe::affinity::Affinity::Text,
                        collation: None,
                        array: false,
                    },
                    hir::ComparisonComponent {
                        affinity: crate::vdbe::affinity::Affinity::Numeric,
                        collation: None,
                        array: false,
                    },
                ],
            );
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            let equality_pairs = program
                .insns
                .iter()
                .filter_map(|(instruction, _)| match instruction {
                    Insn::Eq { lhs, rhs, .. } => Some((*lhs, *rhs)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(equality_pairs, vec![(1, 3), (2, 4)]);
            let ordering_pairs = program
                .insns
                .iter()
                .filter_map(|(instruction, _)| match instruction {
                    Insn::Lt { lhs, rhs, .. } | Insn::Gt { lhs, rhs, .. } => Some((*lhs, *rhs)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(ordering_pairs, vec![(1, 3), (2, 4)]);
            assert!(program.insns.iter().any(|(instruction, _)| matches!(
                instruction,
                Insn::Integer { value, dest: 8 } if *value == i64::from(inclusive)
            )));
        }
    }

    #[test]
    fn scalar_comparison_lowering_keeps_existing_opcode_and_null_shape() {
        let cases = [
            (Operator::Equals, ExpectedComparisonInsn::Eq, false),
            (Operator::NotEquals, ExpectedComparisonInsn::Ne, false),
            (Operator::Less, ExpectedComparisonInsn::Lt, false),
            (Operator::LessEquals, ExpectedComparisonInsn::Le, false),
            (Operator::Greater, ExpectedComparisonInsn::Gt, false),
            (Operator::GreaterEquals, ExpectedComparisonInsn::Ge, false),
            (Operator::Is, ExpectedComparisonInsn::Eq, true),
            (Operator::IsNot, ExpectedComparisonInsn::Ne, true),
        ];

        for (operator, expected, null_equal) in cases {
            let expression = comparison_expression(
                operator,
                hir::ComparisonComponent {
                    affinity: crate::vdbe::affinity::Affinity::Numeric,
                    collation: None,
                    array: false,
                },
            );
            let mut program = program();

            assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
            assert!(matches!(
                &program.insns[..3],
                [
                    (Insn::Integer { value: 2, dest: 1 }, _),
                    (Insn::Integer { value: 3, dest: 2 }, _),
                    (Insn::Integer { value: 1, dest: 8 }, _),
                ]
            ));
            let flags = match (expected, &program.insns[3].0) {
                (
                    ExpectedComparisonInsn::Eq,
                    Insn::Eq {
                        lhs: 1,
                        rhs: 2,
                        flags,
                        collation: None,
                        ..
                    },
                )
                | (
                    ExpectedComparisonInsn::Ne,
                    Insn::Ne {
                        lhs: 1,
                        rhs: 2,
                        flags,
                        collation: None,
                        ..
                    },
                )
                | (
                    ExpectedComparisonInsn::Lt,
                    Insn::Lt {
                        lhs: 1,
                        rhs: 2,
                        flags,
                        collation: None,
                        ..
                    },
                )
                | (
                    ExpectedComparisonInsn::Le,
                    Insn::Le {
                        lhs: 1,
                        rhs: 2,
                        flags,
                        collation: None,
                        ..
                    },
                )
                | (
                    ExpectedComparisonInsn::Gt,
                    Insn::Gt {
                        lhs: 1,
                        rhs: 2,
                        flags,
                        collation: None,
                        ..
                    },
                )
                | (
                    ExpectedComparisonInsn::Ge,
                    Insn::Ge {
                        lhs: 1,
                        rhs: 2,
                        flags,
                        collation: None,
                        ..
                    },
                ) => flags,
                _ => panic!("comparison uses expected instruction"),
            };
            assert_eq!(
                flags.get_affinity(),
                crate::vdbe::affinity::Affinity::Numeric
            );
            assert_eq!(flags.has_nulleq(), null_equal);
            assert!(!flags.has_array_cmp());
            if null_equal {
                assert!(matches!(
                    program.insns[4].0,
                    Insn::Integer { value: 0, dest: 8 }
                ));
            } else {
                assert!(matches!(
                    program.insns[4].0,
                    Insn::ZeroOrNull {
                        rg1: 1,
                        rg2: 2,
                        dest: 8
                    }
                ));
            }
        }
    }

    #[test]
    fn scalar_comparison_uses_resolved_array_and_collation_metadata() {
        let collation = hir::CatalogObject::new(
            hir::CatalogObjectId::new(1),
            hir::CatalogSnapshot::from_id(1),
            None,
            crate::sync::Arc::new(crate::translate::collate::CollationSeq::NoCase),
        );
        let expression = comparison_expression(
            Operator::Equals,
            hir::ComparisonComponent {
                affinity: crate::vdbe::affinity::Affinity::Text,
                collation: Some(collation),
                array: true,
            },
        );
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        let Insn::Eq {
            flags, collation, ..
        } = &program.insns[3].0
        else {
            panic!("equals comparison emits Eq")
        };
        assert_eq!(flags.get_affinity(), crate::vdbe::affinity::Affinity::Text);
        assert!(flags.has_array_cmp());
        assert_eq!(
            *collation,
            Some(crate::translate::collate::CollationSeq::NoCase)
        );
    }

    #[test]
    fn array_binary_lowering_keeps_existing_function_and_register_shape() {
        for operator in [Operator::ArrayContains, Operator::ArrayOverlap] {
            for shared in [false, true] {
                let rhs = if shared { "2" } else { "3" };
                let expression = hir::Expr::Binary {
                    lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                    operator,
                    rhs: Box::new(hir::Expr::Literal(Literal::Numeric(rhs.to_string()))),
                    array_concat: false,
                    custom: None,
                    comparison: None,
                };
                let mut program = program();

                assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
                let function_index = if shared {
                    assert!(matches!(
                        &program.insns[..3],
                        [
                            (Insn::Integer { value: 2, dest: 1 }, _),
                            (
                                Insn::Copy {
                                    src_reg: 1,
                                    dst_reg: 2,
                                    extra_amount: 0
                                },
                                _
                            ),
                            (
                                Insn::Copy {
                                    src_reg: 1,
                                    dst_reg: 3,
                                    extra_amount: 0
                                },
                                _
                            ),
                        ]
                    ));
                    3
                } else {
                    assert!(matches!(
                        &program.insns[..2],
                        [
                            (Insn::Integer { value: 2, dest: 1 }, _),
                            (Insn::Integer { value: 3, dest: 2 }, _),
                        ]
                    ));
                    2
                };
                let expected_start = if shared { 2 } else { 1 };
                let Insn::Function {
                    constant_mask: 0,
                    start_reg,
                    dest: 8,
                    func:
                        FuncCtx {
                            func: Func::Scalar(function),
                            arg_count: 2,
                        },
                } = &program.insns[function_index].0
                else {
                    panic!("array operator emits scalar function")
                };
                assert_eq!(*start_reg, expected_start);
                assert!(matches!(
                    (operator, function),
                    (Operator::ArrayContains, ScalarFunc::ArrayContainsAll)
                        | (Operator::ArrayOverlap, ScalarFunc::ArrayOverlap)
                ));
            }
        }
    }

    #[test]
    fn concat_lowering_uses_resolved_array_semantics() {
        for array_concat in [false, true] {
            let expression = hir::Expr::Binary {
                lhs: Box::new(hir::Expr::Literal(Literal::String("'left'".to_string()))),
                operator: Operator::Concat,
                rhs: Box::new(hir::Expr::Literal(Literal::String("'right'".to_string()))),
                array_concat,
                custom: None,
                comparison: None,
            };
            let mut program = program();

            assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
            assert!(matches!(
                &program.insns[..2],
                [
                    (Insn::String8 { value, dest: 1 }, _),
                    (Insn::String8 { dest: 2, .. }, _),
                ] if value == "left"
            ));
            if array_concat {
                assert!(matches!(
                    program.insns[2].0,
                    Insn::ArrayConcat {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                ));
            } else {
                assert!(matches!(
                    program.insns[2].0,
                    Insn::Concat {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                ));
            }
        }
    }

    #[test]
    fn concat_can_share_equivalent_operand_registers() {
        let expression = hir::Expr::Binary {
            lhs: Box::new(hir::Expr::Literal(Literal::String("'value'".to_string()))),
            operator: Operator::Concat,
            rhs: Box::new(hir::Expr::Literal(Literal::String("'value'".to_string()))),
            array_concat: true,
            custom: None,
            comparison: None,
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::String8 { value, dest: 1 }, _),
                (
                    Insn::ArrayConcat {
                        lhs: 1,
                        rhs: 1,
                        dest: 8
                    },
                    _
                ),
            ] if value == "value"
        ));
    }

    #[test]
    fn equivalent_binary_operands_share_one_register() {
        let expression = hir::Expr::Binary {
            lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
            operator: Operator::Add,
            rhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
            array_concat: false,
            custom: None,
            comparison: None,
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { value: 2, dest: 1 }, _),
                (
                    Insn::Add {
                        lhs: 1,
                        rhs: 1,
                        dest: 8
                    },
                    _
                ),
            ]
        ));
    }

    #[test]
    fn nested_binary_operands_are_allocated_before_lowering_children() {
        let expression = hir::Expr::Binary {
            lhs: Box::new(hir::Expr::Binary {
                lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                operator: Operator::Add,
                rhs: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
                array_concat: false,
                custom: None,
                comparison: None,
            }),
            operator: Operator::Multiply,
            rhs: Box::new(hir::Expr::Literal(Literal::Numeric("4".to_string()))),
            array_concat: false,
            custom: None,
            comparison: None,
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { value: 2, dest: 3 }, _),
                (Insn::Integer { value: 3, dest: 4 }, _),
                (
                    Insn::Add {
                        lhs: 3,
                        rhs: 4,
                        dest: 1
                    },
                    _
                ),
                (Insn::Integer { value: 4, dest: 2 }, _),
                (
                    Insn::Multiply {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    },
                    _
                ),
            ]
        ));
    }

    #[test]
    fn binary_lowering_does_not_use_the_call_stack() {
        let mut expression = hir::Expr::Literal(Literal::Numeric("1".to_string()));
        for value in 2..=10_000 {
            expression = hir::Expr::Binary {
                lhs: Box::new(expression),
                operator: Operator::Add,
                rhs: Box::new(hir::Expr::Literal(Literal::Numeric(value.to_string()))),
                array_concat: false,
                custom: None,
                comparison: None,
            };
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 4).unwrap();
        assert_eq!(program.insns.len(), 19_999);

        std::mem::forget(expression);
    }

    #[test]
    fn dialect_scalar_function_uses_consecutive_argument_registers() {
        let expression = ordinary_scalar_call(
            Func::Dialect("catalog_value".to_string()),
            vec![
                hir::Expr::Literal(Literal::Numeric("1".to_string())),
                hir::Expr::Binary {
                    lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                    operator: Operator::Add,
                    rhs: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
                    array_concat: false,
                    custom: None,
                    comparison: None,
                },
            ],
        );
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { value: 1, dest: 1 }, _),
                (Insn::Integer { value: 2, dest: 3 }, _),
                (Insn::Integer { value: 3, dest: 4 }, _),
                (
                    Insn::Add {
                        lhs: 3,
                        rhs: 4,
                        dest: 2,
                    },
                    _,
                ),
                (
                    Insn::Function {
                        constant_mask: 0,
                        start_reg: 1,
                        dest: 8,
                        func: FuncCtx {
                            func: Func::Dialect(name),
                            arg_count: 2,
                        },
                    },
                    _,
                ),
            ] if name == "catalog_value"
        ));
    }

    #[test]
    fn zero_argument_dialect_function_keeps_legacy_start_register() {
        let expression =
            ordinary_scalar_call(Func::Dialect("catalog_value".to_string()), Vec::new());
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [(
                Insn::Function {
                    start_reg: 8,
                    dest: 8,
                    func: FuncCtx { arg_count: 0, .. },
                    ..
                },
                _,
            )]
        ));
    }

    #[test]
    fn math_functions_keep_legacy_argument_registers() {
        use crate::function::MathFunc;

        let cases = [
            (MathFunc::Pi, Vec::new(), 0, 0),
            (
                MathFunc::Sqrt,
                vec![hir::Expr::Literal(Literal::Numeric("4".to_string()))],
                1,
                1,
            ),
            (
                MathFunc::Pow,
                vec![
                    hir::Expr::Literal(Literal::Numeric("2".to_string())),
                    hir::Expr::Literal(Literal::Numeric("3".to_string())),
                ],
                1,
                2,
            ),
            (
                MathFunc::Log,
                vec![hir::Expr::Literal(Literal::Numeric("8".to_string()))],
                1,
                1,
            ),
            (
                MathFunc::Log,
                vec![
                    hir::Expr::Literal(Literal::Numeric("2".to_string())),
                    hir::Expr::Literal(Literal::Numeric("8".to_string())),
                ],
                1,
                2,
            ),
        ];

        for (function, arguments, expected_start, expected_count) in cases {
            let mut program = program();
            let expression = ordinary_scalar_call(Func::Math(function), arguments);

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(matches!(
                program.insns.last().map(|(instruction, _)| instruction),
                Some(Insn::Function {
                    constant_mask: 0,
                    start_reg,
                    dest: 8,
                    func: FuncCtx {
                        func: Func::Math(_),
                        arg_count,
                    },
                }) if *start_reg == expected_start && *arg_count == expected_count
            ));
        }
    }

    #[cfg(all(feature = "fts", not(target_family = "wasm")))]
    #[test]
    fn direct_fts_functions_use_consecutive_argument_registers() {
        use crate::function::FtsFunc;

        for (function, argument_count) in [
            (FtsFunc::Score, 2),
            (FtsFunc::Match, 3),
            (FtsFunc::Highlight, 4),
        ] {
            let arguments = (0..argument_count)
                .map(|value| hir::Expr::Literal(Literal::Numeric(value.to_string())))
                .collect();
            let expression = ordinary_scalar_call(Func::Fts(function.clone()), arguments);
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(program.insns[..argument_count].iter().enumerate().all(
                |(index, (instruction, _))| {
                    matches!(instruction, Insn::Integer { dest, .. } if *dest == index + 1)
                }
            ));
            assert!(matches!(
                program.insns.last().map(|(instruction, _)| instruction),
                Some(Insn::Function {
                    constant_mask: 0,
                    start_reg: 1,
                    dest: 8,
                    func: FuncCtx {
                        func: Func::Fts(emitted),
                        arg_count,
                    },
                }) if *emitted == function && *arg_count == argument_count
            ));
        }
    }

    #[test]
    fn vector_functions_use_consecutive_argument_registers() {
        use crate::function::VectorFunc;

        let cases = [
            (VectorFunc::Vector, 1),
            (VectorFunc::VectorDistanceL2, 2),
            (VectorFunc::VectorSlice, 3),
        ];

        for (function, argument_count) in cases {
            let arguments = (0..argument_count)
                .map(|value| hir::Expr::Literal(Literal::Numeric(value.to_string())))
                .collect();
            let expression = ordinary_scalar_call(Func::Vector(function), arguments);
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(program.insns[..argument_count].iter().enumerate().all(
                |(index, (instruction, _))| {
                    matches!(instruction, Insn::Integer { dest, .. } if *dest == index + 1)
                }
            ));
            assert!(matches!(
                program.insns.last().map(|(instruction, _)| instruction),
                Some(Insn::Function {
                    constant_mask: 0,
                    start_reg: 1,
                    dest: 8,
                    func: FuncCtx {
                        func: Func::Vector(_),
                        arg_count,
                    },
                }) if *arg_count == argument_count
            ));
        }
    }

    #[test]
    fn plain_builtin_functions_use_legacy_argument_registers() {
        let mut empty_program = program();
        let expression = ordinary_scalar_call(Func::Scalar(ScalarFunc::Char), Vec::new());

        translate_expr(&mut empty_program, &expression, 8).unwrap();
        assert!(matches!(
            empty_program.insns.as_slice(),
            [(
                Insn::Function {
                    start_reg: 1,
                    dest: 8,
                    func: FuncCtx { arg_count: 0, .. },
                    ..
                },
                _,
            )]
        ));
        assert_eq!(empty_program.alloc_register(), 1);

        for (function, argument_count) in [
            (ScalarFunc::Char, 2),
            (ScalarFunc::GetByte, 2),
            (ScalarFunc::ArraySlice, 3),
            (ScalarFunc::Abs, 1),
            (ScalarFunc::SequenceWatermark, 1),
            (ScalarFunc::TimeDiff, 2),
            (ScalarFunc::Hex, 1),
            (ScalarFunc::Nullif, 2),
            (ScalarFunc::Instr, 2),
            (ScalarFunc::Replace, 3),
            (ScalarFunc::Trim, 1),
            (ScalarFunc::LTrim, 2),
            (ScalarFunc::RTrim, 1),
            (ScalarFunc::Round, 2),
            (ScalarFunc::Unhex, 2),
            (ScalarFunc::Min, 2),
            (ScalarFunc::Max, 3),
            (ScalarFunc::Concat, 1),
            (ScalarFunc::Concat, 3),
            (ScalarFunc::DateTime, 2),
            (ScalarFunc::StrfTime, 2),
            (ScalarFunc::StringReverse, 1),
            (ScalarFunc::Gcd, 2),
            (ScalarFunc::NumericEncode, 3),
            (ScalarFunc::TableColumnsJsonArray, 1),
            (ScalarFunc::BinRecordJsonObject, 2),
        ] {
            let arguments = (0..argument_count)
                .map(|value| hir::Expr::Literal(Literal::Numeric(value.to_string())))
                .collect();
            let expression = ordinary_scalar_call(Func::Scalar(function.clone()), arguments);
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(program.insns[..argument_count].iter().enumerate().all(
                |(index, (instruction, _))| {
                    matches!(instruction, Insn::Integer { dest, .. } if *dest == index + 1)
                }
            ));
            assert!(matches!(
                program.insns.last().map(|(instruction, _)| instruction),
                Some(Insn::Function {
                    constant_mask: 0,
                    start_reg: 1,
                    dest: 8,
                    func: FuncCtx {
                        func: Func::Scalar(emitted),
                        arg_count,
                    },
                }) if *emitted == function && *arg_count == argument_count
            ));
        }
    }

    #[test]
    fn struct_pack_uses_legacy_make_array_register_shape() {
        let mut empty_program = program();
        let empty = ordinary_scalar_call(Func::Scalar(ScalarFunc::StructPack), Vec::new());

        translate_expr(&mut empty_program, &empty, 8).unwrap();
        assert!(matches!(
            empty_program.insns.as_slice(),
            [(
                Insn::MakeArray {
                    start_reg: 1,
                    count: 0,
                    dest: 8,
                },
                _,
            )]
        ));
        assert_eq!(empty_program.alloc_register(), 1);

        let expression = ordinary_scalar_call(
            Func::Scalar(ScalarFunc::StructPack),
            vec![
                hir::Expr::Literal(Literal::Numeric("10".to_string())),
                hir::Expr::Literal(Literal::Numeric("20".to_string())),
            ],
        );
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { value: 10, dest: 1 }, _),
                (Insn::Integer { value: 20, dest: 2 }, _),
                (
                    Insn::MakeArray {
                        start_reg: 1,
                        count: 2,
                        dest: 8,
                    },
                    _,
                ),
            ]
        ));
    }

    #[test]
    fn currval_uses_ordinary_function_lowering() {
        let expression = ordinary_scalar_call(
            Func::Scalar(ScalarFunc::CurrVal),
            vec![hir::Expr::Literal(Literal::String(
                "'main.seq'".to_string(),
            ))],
        );
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::String8 { value, dest: 1 }, _),
                (
                    Insn::Function {
                        constant_mask: 0,
                        start_reg: 1,
                        dest: 8,
                        func: FuncCtx {
                            func: Func::Scalar(ScalarFunc::CurrVal),
                            arg_count: 1,
                        },
                    },
                    _,
                ),
            ] if value == "main.seq"
        ));
    }

    #[test]
    fn sequence_calls_use_resolved_hir_objects_and_database_snapshot() {
        let database = hir::DatabaseId::new(2);
        let document = document(vec![hir::DatabaseSnapshot {
            database,
            schema_version: 37,
        }]);

        let nextval = sequence_call(
            hir::SequenceOperationKind::NextValue,
            vec![hir::Expr::Literal(Literal::String("'aux.seq'".to_string()))],
        );
        let mut nextval_program = program();
        nextval_program.prologue();
        super::translate_expr(&mut nextval_program, &document, &nextval, 8).unwrap();
        nextval_program.epilogue(&crate::schema::Schema::new());
        assert!(nextval_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::SequenceComputeNext { db: 2, .. })));
        assert!(nextval_program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::Transaction {
                db: 2,
                schema_cookie: 37,
                tx_mode: crate::translate::emitter::TransactionMode::Write,
            }
        )));

        let setval = sequence_call(
            hir::SequenceOperationKind::SetValue,
            vec![
                hir::Expr::Literal(Literal::String("'aux.seq'".to_string())),
                hir::Expr::Literal(Literal::Numeric("12".to_string())),
            ],
        );
        let mut setval_program = program();
        super::translate_expr(&mut setval_program, &document, &setval, 8).unwrap();
        assert!(setval_program.insns.iter().any(|(insn, _)| matches!(
            insn,
            Insn::Function {
                func: FuncCtx {
                    func: Func::Scalar(ScalarFunc::SetVal),
                    arg_count: 2,
                },
                ..
            }
        )));
        assert!(setval_program
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::SetSequenceCurrval { .. })));
    }

    #[test]
    fn direct_like_functions_use_plain_legacy_argument_registers() {
        for function in [ScalarFunc::Like, ScalarFunc::Glob] {
            for argument_count in [2, 3] {
                let arguments = (0..argument_count)
                    .map(|value| hir::Expr::Literal(Literal::Numeric(value.to_string())))
                    .collect();
                let expression = ordinary_scalar_call(Func::Scalar(function.clone()), arguments);
                let mut program = program();

                translate_expr(&mut program, &expression, 8).unwrap();
                assert!(program.insns[..argument_count].iter().enumerate().all(
                    |(index, (instruction, _))| {
                        matches!(instruction, Insn::Integer { dest, .. } if *dest == index + 1)
                    }
                ));
                assert!(matches!(
                    program.insns.last().map(|(instruction, _)| instruction),
                    Some(Insn::Function {
                        constant_mask: 0,
                        start_reg: 1,
                        dest: 8,
                        func: FuncCtx {
                            func: Func::Scalar(emitted),
                            arg_count,
                        },
                    }) if *emitted == function && *arg_count == argument_count
                ));
            }
        }
    }

    #[test]
    fn planner_hints_only_lower_the_value_expression() {
        for (function, arguments) in [
            (
                ScalarFunc::Likely,
                vec![hir::Expr::Literal(Literal::Numeric("7".to_string()))],
            ),
            (
                ScalarFunc::Unlikely,
                vec![hir::Expr::Literal(Literal::Numeric("7".to_string()))],
            ),
            (
                ScalarFunc::Likelihood,
                vec![
                    hir::Expr::Literal(Literal::Numeric("7".to_string())),
                    hir::Expr::Literal(Literal::Numeric("0.5".to_string())),
                ],
            ),
        ] {
            let expression = ordinary_scalar_call(Func::Scalar(function), arguments);
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(matches!(
                program.insns.as_slice(),
                [(Insn::Integer { value: 7, dest: 8 }, _)]
            ));
            assert_eq!(program.alloc_register(), 1);
        }
    }

    #[test]
    fn concat_ws_keeps_legacy_result_and_argument_registers() {
        for argument_count in [2, 3] {
            let arguments = (0..argument_count)
                .map(|value| hir::Expr::Literal(Literal::Numeric(value.to_string())))
                .collect();
            let expression = ordinary_scalar_call(Func::Scalar(ScalarFunc::ConcatWs), arguments);
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(program.insns[..argument_count].iter().enumerate().all(
                |(index, (instruction, _))| {
                    matches!(instruction, Insn::Integer { dest, .. } if *dest == index + 2)
                }
            ));
            assert!(matches!(
                program
                    .insns
                    .get(argument_count)
                    .map(|(instruction, _)| instruction),
                Some(Insn::Function {
                    constant_mask: 0,
                    start_reg: 2,
                    dest: 1,
                    func: FuncCtx {
                        func: Func::Scalar(ScalarFunc::ConcatWs),
                        arg_count,
                    },
                }) if *arg_count == argument_count
            ));
            assert!(matches!(
                program.insns.last().map(|(instruction, _)| instruction),
                Some(Insn::Copy {
                    src_reg: 1,
                    dst_reg: 8,
                    extra_amount: 0,
                })
            ));
        }
    }

    #[test]
    fn ifnull_keeps_legacy_short_circuit_register_and_opcode_shape() {
        let expression = ordinary_scalar_call(
            Func::Scalar(ScalarFunc::IfNull),
            vec![
                hir::Expr::Literal(Literal::Numeric("1".to_string())),
                hir::Expr::Literal(Literal::Numeric("2".to_string())),
            ],
        );
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { dest: 1, .. }, _),
                (Insn::NotNull { reg: 1, .. }, _),
                (Insn::Integer { dest: 1, .. }, _),
                (
                    Insn::Copy {
                        src_reg: 1,
                        dst_reg: 8,
                        extra_amount: 0,
                    },
                    _
                ),
            ]
        ));
        assert_eq!(program.alloc_register(), 2);
    }

    #[test]
    fn iif_keeps_legacy_branch_register_and_opcode_shapes() {
        for (argument_count, expected_shape) in [
            (2, "condition if-not value goto null"),
            (
                4,
                "condition if-not value goto condition if-not value goto null",
            ),
            (
                5,
                "condition if-not value goto condition if-not value goto value",
            ),
        ] {
            let arguments = (0..argument_count)
                .map(|value| hir::Expr::Literal(Literal::Numeric(value.to_string())))
                .collect();
            let expression = ordinary_scalar_call(Func::Scalar(ScalarFunc::Iif), arguments);
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            let shape = program
                .insns
                .iter()
                .map(|(instruction, _)| match instruction {
                    Insn::Integer { dest: 1, .. } => "condition",
                    Insn::Integer { dest: 8, .. } => "value",
                    Insn::IfNot {
                        reg: 1,
                        jump_if_null: true,
                        ..
                    } => "if-not",
                    Insn::Goto { .. } => "goto",
                    Insn::Null {
                        dest: 8,
                        dest_end: None,
                    } => "null",
                    instruction => panic!("unexpected IIF instruction: {instruction:?}"),
                })
                .collect::<Vec<_>>()
                .join(" ");
            assert_eq!(shape, expected_shape);
            assert_eq!(program.alloc_register(), 2);
        }
    }

    #[test]
    fn coalesce_keeps_legacy_target_register_and_opcode_shape() {
        for argument_count in [2, 3] {
            let arguments = (0..argument_count)
                .map(|value| hir::Expr::Literal(Literal::Numeric(value.to_string())))
                .collect();
            let expression = ordinary_scalar_call(Func::Scalar(ScalarFunc::Coalesce), arguments);
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert_eq!(program.insns.len(), argument_count * 2 - 1);
            for (index, (instruction, _)) in program.insns.iter().enumerate() {
                if index % 2 == 0 {
                    assert!(matches!(instruction, Insn::Integer { dest: 8, .. }));
                } else {
                    assert!(matches!(instruction, Insn::NotNull { reg: 8, .. }));
                }
            }
            assert_eq!(program.alloc_register(), 1);
        }
    }

    #[test]
    fn last_insert_rowid_keeps_legacy_ignored_argument_behavior() {
        for argument_count in [0, 2] {
            let arguments = (0..argument_count)
                .map(|value| hir::Expr::Literal(Literal::Numeric(value.to_string())))
                .collect();
            let expression =
                ordinary_scalar_call(Func::Scalar(ScalarFunc::LastInsertRowid), arguments);
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(matches!(
                program.insns.as_slice(),
                [(
                    Insn::Function {
                        constant_mask: 0,
                        start_reg: 1,
                        dest: 8,
                        func: FuncCtx {
                            func: Func::Scalar(ScalarFunc::LastInsertRowid),
                            arg_count,
                        },
                    },
                    _,
                )] if *arg_count == argument_count
            ));
            assert_eq!(program.alloc_register(), 2);
        }
    }

    #[test]
    fn version_functions_keep_legacy_output_register_and_copy_shape() {
        for function in [
            ScalarFunc::SqliteVersion,
            ScalarFunc::TursoVersion,
            ScalarFunc::SqliteSourceId,
        ] {
            let expression = ordinary_scalar_call(Func::Scalar(function.clone()), Vec::new());
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(matches!(
                program.insns.as_slice(),
                [
                    (
                        Insn::Function {
                            constant_mask: 0,
                            start_reg: 1,
                            dest: 1,
                            func: FuncCtx {
                                func: Func::Scalar(emitted),
                                arg_count: 0,
                            },
                        },
                        _,
                    ),
                    (
                        Insn::Copy {
                            src_reg: 1,
                            dst_reg: 8,
                            extra_amount: 0,
                        },
                        _,
                    ),
                ] if *emitted == function
            ));
            assert_eq!(program.alloc_register(), 2);
        }
    }

    #[test]
    fn substring_functions_always_reserve_three_legacy_argument_registers() {
        for function in [ScalarFunc::Substr, ScalarFunc::Substring] {
            for argument_count in [2, 3] {
                let arguments = (0..argument_count)
                    .map(|value| hir::Expr::Literal(Literal::Numeric(value.to_string())))
                    .collect();
                let expression = ordinary_scalar_call(Func::Scalar(function.clone()), arguments);
                let mut program = program();

                translate_expr(&mut program, &expression, 8).unwrap();
                assert!(program.insns[..argument_count].iter().enumerate().all(
                    |(index, (instruction, _))| {
                        matches!(instruction, Insn::Integer { dest, .. } if *dest == index + 1)
                    }
                ));
                assert!(matches!(
                    program.insns.last().map(|(instruction, _)| instruction),
                    Some(Insn::Function {
                        constant_mask: 0,
                        start_reg: 1,
                        dest: 8,
                        func: FuncCtx {
                            func: Func::Scalar(emitted),
                            arg_count,
                        },
                    }) if *emitted == function && *arg_count == argument_count
                ));
                assert_eq!(program.alloc_register(), 4);
            }
        }
    }

    #[test]
    fn empty_plain_functions_reserve_the_legacy_start_register() {
        for function in [
            ScalarFunc::Changes,
            ScalarFunc::TotalChanges,
            ScalarFunc::Random,
            ScalarFunc::Date,
            ScalarFunc::DateTime,
            ScalarFunc::JulianDay,
            ScalarFunc::UnixEpoch,
            ScalarFunc::Time,
            ScalarFunc::StrfTime,
            #[cfg(feature = "test_helper")]
            ScalarFunc::TestNondetCounter,
        ] {
            let expression = ordinary_scalar_call(Func::Scalar(function), Vec::new());
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(matches!(
                program.insns.as_slice(),
                [(
                    Insn::Function {
                        start_reg: 1,
                        dest: 8,
                        func: FuncCtx { arg_count: 0, .. },
                        ..
                    },
                    _,
                )]
            ));
            assert_eq!(program.alloc_register(), 2);
        }
    }

    #[cfg(feature = "json")]
    #[test]
    fn json_functions_preserve_empty_and_nonempty_register_shapes() {
        let mut empty_array_program = program();
        let expression = ordinary_scalar_call(Func::Json(JsonFunc::JsonArray), Vec::new());

        translate_expr(&mut empty_array_program, &expression, 8).unwrap();
        assert!(matches!(
            empty_array_program.insns.as_slice(),
            [(
                Insn::Function {
                    start_reg: 1,
                    dest: 8,
                    func: FuncCtx { arg_count: 0, .. },
                    ..
                },
                _,
            )]
        ));
        assert_eq!(empty_array_program.alloc_register(), 1);

        let mut empty_remove_program = program();
        let expression = ordinary_scalar_call(Func::Json(JsonFunc::JsonRemove), Vec::new());

        translate_expr(&mut empty_remove_program, &expression, 8).unwrap();
        assert!(matches!(
            empty_remove_program.insns.as_slice(),
            [(
                Insn::Function {
                    start_reg: 1,
                    dest: 8,
                    func: FuncCtx { arg_count: 0, .. },
                    ..
                },
                _,
            )]
        ));
        assert_eq!(empty_remove_program.alloc_register(), 2);

        let mut object_program = program();
        let expression = ordinary_scalar_call(
            Func::Json(JsonFunc::JsonObject),
            vec![
                hir::Expr::Literal(Literal::String("'key'".to_string())),
                hir::Expr::Literal(Literal::Numeric("1".to_string())),
            ],
        );

        translate_expr(&mut object_program, &expression, 8).unwrap();
        assert!(matches!(
            object_program.insns.as_slice(),
            [
                (Insn::String8 { dest: 1, .. }, _),
                (Insn::Integer { dest: 2, .. }, _),
                (
                    Insn::Function {
                        start_reg: 1,
                        dest: 8,
                        func: FuncCtx { arg_count: 2, .. },
                        ..
                    },
                    _,
                ),
            ]
        ));
    }

    #[cfg(feature = "json")]
    #[test]
    fn nested_json_functions_do_not_use_the_call_stack() {
        let mut expression = hir::Expr::Literal(Literal::String("'value'".to_string()));
        for _ in 0..10_000 {
            expression = ordinary_scalar_call(Func::Json(JsonFunc::JsonQuote), vec![expression]);
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Function { .. }))
                .count(),
            10_000
        );

        std::mem::forget(expression);
    }

    #[test]
    fn nested_dialect_functions_do_not_use_the_call_stack() {
        let mut expression = hir::Expr::Literal(Literal::Numeric("1".to_string()));
        for _ in 0..10_000 {
            expression =
                ordinary_scalar_call(Func::Dialect("catalog_value".to_string()), vec![expression]);
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Function { .. }))
                .count(),
            10_000
        );

        std::mem::forget(expression);
    }

    #[test]
    fn expression_lowering_does_not_use_call_stack() {
        let mut expression = hir::Expr::Literal(Literal::Numeric("1".to_string()));
        for _ in 0..10_000 {
            expression = hir::Expr::Unary {
                operator: UnaryOperator::Positive,
                expr: Box::new(expression),
            };
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 4).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [(Insn::Integer { value: 1, dest: 4 }, _)]
        ));

        std::mem::forget(expression);
    }

    #[test]
    fn condition_lowering_short_circuits_and_and_or() {
        #[derive(Clone, Copy)]
        enum JumpKind {
            If,
            IfNot,
        }
        for (sql, expected_jump) in [
            ("SELECT 1 WHERE 0 AND random()", JumpKind::IfNot),
            ("SELECT 1 WHERE 1 OR random()", JumpKind::If),
        ] {
            let document = analyze_sql(crate::schema::Schema::new(), sql);
            let mut program = program();
            let when_true = program.allocate_label();
            let when_false = program.allocate_label();
            super::translate_condition_expr(
                &mut program,
                &document,
                root_filter(&document),
                expr::ConditionMetadata {
                    jump_if_condition_is_true: false,
                    jump_target_when_true: when_true,
                    jump_target_when_false: when_false,
                    jump_target_when_null: when_false,
                },
            )
            .expect("condition lowers");

            let jump = program
                .insns
                .iter()
                .position(|(instruction, _)| match expected_jump {
                    JumpKind::If => matches!(instruction, Insn::If { .. }),
                    JumpKind::IfNot => matches!(instruction, Insn::IfNot { .. }),
                })
                .expect("left operand emits its short-circuit jump");
            let function = program
                .insns
                .iter()
                .position(|(instruction, _)| matches!(instruction, Insn::Function { .. }))
                .expect("right operand is emitted");
            assert!(jump < function, "left jump must guard right operand");
        }
    }

    #[test]
    fn condition_lowering_does_not_use_call_stack() {
        let mut expression = hir::Expr::Literal(Literal::Numeric("1".to_string()));
        for _ in 0..10_000 {
            expression = hir::Expr::Binary {
                lhs: Box::new(expression),
                operator: Operator::And,
                rhs: Box::new(hir::Expr::Literal(Literal::Numeric("1".to_string()))),
                array_concat: false,
                custom: None,
                comparison: None,
            };
        }
        let mut program = program();
        let when_true = program.allocate_label();
        let when_false = program.allocate_label();
        super::translate_condition_expr(
            &mut program,
            &document(Vec::new()),
            &expression,
            expr::ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: when_true,
                jump_target_when_false: when_false,
                jump_target_when_null: when_false,
            },
        )
        .expect("deep condition lowers");
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::IfNot { .. }))
                .count(),
            10_001
        );
        std::mem::forget(expression);
    }
}
