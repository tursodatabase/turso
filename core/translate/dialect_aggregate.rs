//! Aggregates that a dialect runs as functions.

use super::aggregation::{handle_distinct, AggArgumentSource};
use super::emitter::Resolver;
use super::expr::emit_function_call;
use super::expr::expr_type::{aggregate_operand_types, TypeEnv};
use super::plan::{Aggregate, TableReferences};
use crate::dialect::{AggregateChoice, ChosenAggregate, ChosenFunction};
use crate::function::{Func, FuncCtx};
use crate::numeric::Numeric;
use crate::translate::collate::CollationSeq;
use crate::vdbe::{builder::ProgramBuilder, insn::Insn};
use crate::{ast, LimboError, Result, Value};

/// The aggregate the dialect runs as functions for this call, or `None` when
/// the engine runs it.
pub(crate) fn chosen_aggregate(
    tables: &TableReferences,
    aggregate: &Aggregate,
    resolver: &Resolver,
) -> Result<Option<ChosenAggregate>> {
    let ast::Expr::FunctionCall { name, .. } = &aggregate.original_expr else {
        return Ok(None);
    };
    let env = TypeEnv::new(Some(tables), resolver);
    let arguments = || aggregate_operand_types(&aggregate.args, &env);
    match resolver
        .catalog
        .dialect
        .choose_aggregate(name.as_str(), &arguments)?
    {
        AggregateChoice::LeftToTheEngine => Ok(None),
        AggregateChoice::Chosen(chosen) => Ok(Some(chosen)),
    }
}

/// Replaces the state in `accumulator` with the state function of `chosen`
/// called on the state and the arguments of one row.
pub(crate) fn emit_chosen_aggregate_step(
    program: &mut ProgramBuilder,
    tables: &TableReferences,
    source: AggArgumentSource,
    accumulator: usize,
    resolver: &Resolver,
    chosen: &ChosenAggregate,
) -> Result<usize> {
    let argument_count = source.num_args();
    let state_function = &chosen.state_function;
    if state_function.is_strict && chosen.initial_value.is_none() && argument_count == 0 {
        crate::bail_parse_error!(
            "{}() has no argument to start the state with",
            state_function.name
        );
    }
    let function = function_to_call(resolver, state_function, 1 + argument_count)?;

    let call = program.alloc_registers(1 + argument_count);
    for i in 0..argument_count {
        let argument = source.translate(program, tables, resolver, i)?;
        program.emit_insn(Insn::Copy {
            src_reg: argument,
            dst_reg: call + 1 + i,
            extra_amount: 0,
        });
    }
    if argument_count == 1 {
        handle_distinct(program, source.distinctness(), call + 1);
    }

    let done = program.allocate_label();
    if state_function.is_strict {
        for i in 0..argument_count {
            program.emit_insn(Insn::IsNull {
                reg: call + 1 + i,
                target_pc: done,
            });
        }
    }
    match &chosen.initial_value {
        Some(initial_value) => emit_initial_value_if_null(program, accumulator, initial_value)?,
        None if state_function.is_strict => {
            let has_state = program.allocate_label();
            program.emit_insn(Insn::NotNull {
                reg: accumulator,
                target_pc: has_state,
            });
            program.emit_insn(Insn::Copy {
                src_reg: call + 1,
                dst_reg: accumulator,
                extra_amount: 0,
            });
            program.emit_insn(Insn::Goto { target_pc: done });
            program.preassign_label_to_next_insn(has_state);
        }
        None => {}
    }
    program.emit_insn(Insn::Copy {
        src_reg: accumulator,
        dst_reg: call,
        extra_amount: 0,
    });
    emit_function_call(program, function, &[call], accumulator)?;
    program.preassign_label_to_next_insn(done);
    Ok(accumulator)
}

/// Replaces the state in `accumulator` with the result of the aggregate.
pub(crate) fn emit_chosen_aggregate_final(
    program: &mut ProgramBuilder,
    accumulator: usize,
    resolver: &Resolver,
    chosen: &ChosenAggregate,
) -> Result<()> {
    if let Some(initial_value) = &chosen.initial_value {
        emit_initial_value_if_null(program, accumulator, initial_value)?;
    }
    let Some(final_function) = &chosen.final_function else {
        return Ok(());
    };
    let function = function_to_call(resolver, final_function, 1)?;
    let done = program.allocate_label();
    if final_function.is_strict {
        program.emit_insn(Insn::IsNull {
            reg: accumulator,
            target_pc: done,
        });
    }
    emit_function_call(program, function, &[accumulator], accumulator)?;
    program.preassign_label_to_next_insn(done);
    Ok(())
}

fn function_to_call(
    resolver: &Resolver,
    function: &ChosenFunction,
    arg_count: usize,
) -> Result<FuncCtx> {
    let registered = resolver
        .catalog
        .symbol_table
        .resolve_function(&function.name, arg_count)
        .ok_or_else(|| LimboError::ParseError(format!("no such function: {}", function.name)))?;
    Ok(FuncCtx {
        func: Func::External(registered),
        arg_count,
        collation: CollationSeq::BINARY,
    })
}

fn emit_initial_value_if_null(
    program: &mut ProgramBuilder,
    register: usize,
    initial_value: &Value,
) -> Result<()> {
    let has_value = program.allocate_label();
    program.emit_insn(Insn::NotNull {
        reg: register,
        target_pc: has_value,
    });
    match initial_value {
        Value::Numeric(Numeric::Integer(value)) => program.emit_insn(Insn::Integer {
            value: *value,
            dest: register,
        }),
        Value::Numeric(Numeric::Float(value)) => program.emit_insn(Insn::Real {
            value: f64::from(*value),
            dest: register,
        }),
        Value::Text(text) => program.emit_insn(Insn::String8 {
            value: text.as_str().to_string(),
            dest: register,
        }),
        _ => crate::bail_parse_error!("an aggregate cannot start from {initial_value}"),
    }
    program.preassign_label_to_next_insn(has_value);
    Ok(())
}
