//! Deferred, typed building blocks for VDBE compilation.
//!
//! Compiler combinators describe work without mutating [`ProgramBuilder`]. The
//! completed description is first interpreted into symbolic IR and only then
//! lowered into physical VDBE registers and instructions.

use std::{fmt, marker::PhantomData};

use smallvec::SmallVec;

use crate::{
    numeric::Numeric,
    types::Value,
    vdbe::{builder::ProgramBuilder, insn::Insn},
    LimboError, Result,
};

/// A deferred compilation step with one typed output.
pub(crate) trait Compile: Sized {
    type Output;

    fn compile(self, builder: &mut ScalarIrBuilder) -> Result<Self::Output>;

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

    fn compile(self, builder: &mut ScalarIrBuilder) -> Result<Self::Output> {
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

    fn compile(self, builder: &mut ScalarIrBuilder) -> Result<Self::Output> {
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

    fn compile(self, builder: &mut ScalarIrBuilder) -> Result<Self::Output> {
        let output = self.compiler.compile(builder)?;
        (self.next)(output).compile(builder)
    }
}

/// The symbolic result of one scalar operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ValueId(u32);

impl ValueId {
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
struct ScalarInstruction {
    result: ValueId,
    op: ScalarOp,
}

/// Builds a straight-line scalar SSA region.
#[derive(Default)]
pub(crate) struct ScalarIrBuilder {
    instructions: SmallVec<[ScalarInstruction; 8]>,
}

impl ScalarIrBuilder {
    fn push(&mut self, op: ScalarOp) -> Result<ValueId> {
        let id = u32::try_from(self.instructions.len()).map_err(|_| {
            LimboError::InternalError("scalar IR value identifier overflow".to_owned())
        })?;
        let result = ValueId(id);
        self.instructions.push(ScalarInstruction { result, op });
        Ok(result)
    }

    fn finish(self, output: ValueId) -> Result<ScalarProgram> {
        let program = ScalarProgram {
            instructions: self.instructions,
            output,
        };
        program.verify()?;
        Ok(program)
    }
}

/// A verified, straight-line scalar SSA program.
pub(crate) struct ScalarProgram {
    instructions: SmallVec<[ScalarInstruction; 8]>,
    output: ValueId,
}

impl ScalarProgram {
    fn verify(&self) -> Result<()> {
        for (index, instruction) in self.instructions.iter().enumerate() {
            if instruction.result.index() != index {
                return Err(LimboError::InternalError(format!(
                    "scalar IR instruction {index} defines non-canonical value {:?}",
                    instruction.result
                )));
            }
            for operand in instruction.op.operands() {
                if operand.index() >= index {
                    return Err(LimboError::InternalError(format!(
                        "scalar IR value {operand:?} is used before its definition"
                    )));
                }
            }
        }

        if self.output.index() >= self.instructions.len() {
            return Err(LimboError::InternalError(format!(
                "scalar IR output {:?} is not defined",
                self.output
            )));
        }
        Ok(())
    }

    /// Assign physical registers and append equivalent instructions to the VDBE builder.
    pub(crate) fn lower_into(
        self,
        program: &mut ProgramBuilder,
        target_register: usize,
    ) -> Result<()> {
        self.verify()?;
        let mut registers = SmallVec::<[usize; 8]>::with_capacity(self.instructions.len());

        for instruction in self.instructions {
            let destination = if instruction.result == self.output {
                target_register
            } else {
                program.alloc_register()
            };
            match instruction.op {
                ScalarOp::Constant(Value::Null) => program.emit_insn(Insn::Null {
                    dest: destination,
                    dest_end: None,
                }),
                ScalarOp::Constant(Value::Numeric(Numeric::Integer(value))) => {
                    program.emit_insn(Insn::Integer {
                        value,
                        dest: destination,
                    });
                }
                ScalarOp::Constant(Value::Numeric(Numeric::Float(value))) => {
                    program.emit_insn(Insn::Real {
                        value: value.into(),
                        dest: destination,
                    });
                }
                ScalarOp::Constant(Value::Text(value)) => program.emit_insn(Insn::String8 {
                    value: value.into(),
                    dest: destination,
                }),
                ScalarOp::Constant(Value::Blob(value)) => program.emit_insn(Insn::Blob {
                    value,
                    dest: destination,
                }),
                ScalarOp::Add { lhs, rhs } => program.emit_insn(Insn::Add {
                    lhs: registers[lhs.index()],
                    rhs: registers[rhs.index()],
                    dest: destination,
                }),
            }
            registers.push(destination);
        }
        Ok(())
    }
}

impl fmt::Display for ScalarProgram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for instruction in &self.instructions {
            write!(f, "%{} = ", instruction.result.0)?;
            match &instruction.op {
                ScalarOp::Constant(value) => writeln!(f, "constant {value:?}")?,
                ScalarOp::Add { lhs, rhs } => {
                    writeln!(f, "add %{}, %{}", lhs.0, rhs.0)?;
                }
            }
        }
        write!(f, "return %{}", self.output.0)
    }
}

pub(crate) struct Constant(Value);

pub(crate) fn constant(value: Value) -> Constant {
    Constant(value)
}

impl Compile for Constant {
    type Output = ValueId;

    fn compile(self, builder: &mut ScalarIrBuilder) -> Result<Self::Output> {
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

    fn compile(self, builder: &mut ScalarIrBuilder) -> Result<Self::Output> {
        builder.push(ScalarOp::Add {
            lhs: self.lhs,
            rhs: self.rhs,
        })
    }
}

pub(crate) fn compile_scalar<Compiler>(compiler: Compiler) -> Result<ScalarProgram>
where
    Compiler: Compile<Output = ValueId>,
{
    let mut builder = ScalarIrBuilder::default();
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

        let scalar = compile_scalar(compiler).unwrap();

        assert_eq!(
            scalar.to_string(),
            "%0 = constant Numeric(Integer(40))\n\
             %1 = constant Numeric(Integer(2))\n\
             %2 = add %0, %1\n\
             return %2"
        );
    }

    #[test]
    fn map_transforms_compiler_output_without_emitting_an_operation() {
        let compiler = constant(Value::from_i64(1)).map(|value| value);

        let scalar = compile_scalar(compiler).unwrap();

        assert_eq!(scalar.instructions.len(), 1);
        assert_eq!(scalar.output, ValueId(0));
    }

    #[test]
    fn verifier_rejects_use_before_definition() {
        let scalar = ScalarProgram {
            instructions: smallvec::smallvec![ScalarInstruction {
                result: ValueId(0),
                op: ScalarOp::Add {
                    lhs: ValueId(1),
                    rhs: ValueId(1),
                },
            }],
            output: ValueId(0),
        };

        let error = scalar.verify().unwrap_err();
        assert!(error.to_string().contains("used before its definition"));
    }

    #[test]
    fn lowering_assigns_registers_after_composition() {
        let compiler = constant(Value::from_i64(40))
            .then(constant(Value::from_i64(2)))
            .and_then(|(lhs, rhs)| add(lhs, rhs));
        let scalar = compile_scalar(compiler).unwrap();
        let mut program =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 3, 0));

        scalar.lower_into(&mut program, 7).unwrap();

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
            .prepare("SELECT 40 + 2")
            .unwrap()
            .run_collect_rows()
            .unwrap();

        assert_eq!(rows, vec![vec![Value::from_i64(42)]]);
    }
}
