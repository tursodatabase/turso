use crate::{
    function::{AccumulatorFunc, AggFunc, Func},
    sync::Arc,
    translate::collate::CollationSeq,
    vdbe::{
        builder::ProgramBuilder,
        insn::{AggStepData, CmpInsFlags, HashDistinctData, Insn},
    },
    LimboError, Result,
};

/// Emit the runtime range check for a percentile fraction argument.
/// NULL is accepted here and propagated by aggregate finalization.
pub(crate) fn emit_percentile_fraction_range_check(program: &mut ProgramBuilder, fraction: usize) {
    // NULL skips the range check and propagates to a NULL result in finalize.
    // Use one scratch register for both bounds: success falls through to `done`
    // via `Le` on the upper bound; failure on either bound jumps to `bad: Halt`.
    let done = program.allocate_label();
    let bad = program.allocate_label();
    let bound = program.alloc_register();
    program.emit_insn(Insn::IsNull {
        reg: fraction,
        target_pc: done,
    });
    program.emit_insn(Insn::Real {
        value: 0.0,
        dest: bound,
    });
    program.emit_insn(Insn::Lt {
        lhs: fraction,
        rhs: bound,
        target_pc: bad,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.emit_insn(Insn::Real {
        value: 1.0,
        dest: bound,
    });
    program.emit_insn(Insn::Le {
        lhs: fraction,
        rhs: bound,
        target_pc: done,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    program.preassign_label_to_next_insn(bad);
    program.emit_insn(Insn::Halt {
        err_code: crate::error::SQLITE_ERROR,
        description: "percentile value is not between 0 and 1".to_string(),
        on_error: None,
        description_reg: None,
    });
    program.preassign_label_to_next_insn(done);
}

use super::semantic::hir::{self, HirDocument};

pub(crate) struct HirAggregateDistinct {
    pub(crate) hash_table: usize,
    pub(crate) collation: CollationSeq,
    pub(crate) on_conflict: crate::vdbe::BranchOffset,
}

pub(crate) struct PreparedAggregateArguments<'a> {
    pub(crate) function: &'a AggFunc,
    pub(crate) registers_start: usize,
    pub(crate) count: usize,
    pub(crate) collations: &'a [CollationSeq],
    pub(crate) comparators: &'a [Option<crate::vdbe::insn::SortComparatorType>],
    pub(crate) custom_types_enabled: bool,
}

enum AggregationArguments<'a> {
    Hir {
        document: &'a HirDocument,
        call: &'a hir::FunctionCall,
        func: AggFunc,
        distinct: Option<&'a HirAggregateDistinct>,
    },
    Prepared(PreparedAggregateArguments<'a>),
}

impl AggregationArguments<'_> {
    fn func(&self) -> &AggFunc {
        match self {
            Self::Hir { func, .. } => func,
            Self::Prepared(arguments) => arguments.function,
        }
    }

    fn num_args(&self) -> usize {
        match self {
            Self::Hir { call, .. } => match &call.arguments {
                hir::FunctionArguments::Star => 0,
                hir::FunctionArguments::Expressions { values, .. } => values.len(),
                hir::FunctionArguments::OrderedSet { direct, .. } => direct.len() + 1,
            },
            Self::Prepared(arguments) => arguments.count,
        }
    }

    fn hir_argument(call: &hir::FunctionCall, index: usize) -> Option<&hir::Expr> {
        match &call.arguments {
            hir::FunctionArguments::Star => None,
            hir::FunctionArguments::Expressions { values, .. } => values.get(index),
            hir::FunctionArguments::OrderedSet { direct, order_by } => match index {
                0 => Some(&order_by.expr),
                _ => direct.get(index - 1),
            },
        }
    }

    fn translate(&self, program: &mut ProgramBuilder, index: usize) -> Result<usize> {
        match self {
            Self::Hir { document, call, .. } => {
                let expression = Self::hir_argument(call, index).ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "HIR aggregate argument {index} is outside its function arguments"
                    ))
                })?;
                let register = program.alloc_register();
                super::semantic_lowering::expr::translate_expr(
                    program, document, expression, register,
                )?;
                Ok(register)
            }
            Self::Prepared(arguments) => {
                if index >= arguments.count {
                    return Err(LimboError::InternalError(format!(
                        "prepared aggregate argument {index} is outside {} arguments",
                        arguments.count
                    )));
                }
                Ok(arguments.registers_start + index)
            }
        }
    }

    fn handle_distinct(&self, program: &mut ProgramBuilder, argument: usize) {
        match self {
            Self::Hir {
                distinct: Some(distinct),
                ..
            } => program.emit_insn(Insn::HashDistinct {
                data: Box::new(HashDistinctData {
                    hash_table_id: distinct.hash_table,
                    key_start_reg: argument,
                    num_keys: 1,
                    collations: vec![distinct.collation],
                    target_pc: distinct.on_conflict,
                }),
            }),
            Self::Hir { distinct: None, .. } => {}
            Self::Prepared(_) => {}
        }
    }

    fn collation(&self, index: usize) -> CollationSeq {
        match self {
            Self::Hir { call, .. } => match &call.arguments {
                hir::FunctionArguments::Expressions { facts, .. } => facts
                    .get(index)
                    .and_then(|facts| facts.collation.as_ref())
                    .map(|collation| *collation.value())
                    .unwrap_or(CollationSeq::Binary),
                hir::FunctionArguments::OrderedSet { order_by, .. } if index == 0 => order_by
                    .collation
                    .as_ref()
                    .map(|collation| *collation.value())
                    .unwrap_or(CollationSeq::Binary),
                _ => CollationSeq::Binary,
            },
            Self::Prepared(arguments) => arguments
                .collations
                .get(index)
                .copied()
                .unwrap_or(CollationSeq::Binary),
        }
    }

    fn comparator(&self, index: usize) -> Option<crate::vdbe::insn::SortComparatorType> {
        match self {
            Self::Hir { call, .. } => match &call.arguments {
                hir::FunctionArguments::Expressions { facts, .. } => {
                    facts.get(index).and_then(|facts| {
                        super::order_by::custom_type_comparator_from_type_fact(&facts.type_fact)
                    })
                }
                hir::FunctionArguments::OrderedSet { order_by, .. } if index == 0 => {
                    super::order_by::custom_type_comparator_from_type_fact(&order_by.type_fact)
                }
                _ => None,
            },
            Self::Prepared(arguments) => arguments.comparators.get(index).copied().flatten(),
        }
    }

    fn translate_integer_one(&self, program: &mut ProgramBuilder) -> Result<usize> {
        match self {
            Self::Hir { .. } => {
                let register = program.alloc_register();
                program.emit_insn(Insn::Integer {
                    value: 1,
                    dest: register,
                });
                Ok(register)
            }
            Self::Prepared(_) => {
                let register = program.alloc_register();
                program.emit_insn(Insn::Integer {
                    value: 1,
                    dest: register,
                });
                Ok(register)
            }
        }
    }

    fn translate_comma(&self, program: &mut ProgramBuilder) -> Result<usize> {
        match self {
            Self::Hir { .. } => {
                let register = program.alloc_register();
                program.emit_insn(Insn::String8 {
                    value: ",".to_string(),
                    dest: register,
                });
                Ok(register)
            }
            Self::Prepared(_) => {
                let register = program.alloc_register();
                program.emit_insn(Insn::String8 {
                    value: ",".to_string(),
                    dest: register,
                });
                Ok(register)
            }
        }
    }

    fn require_custom_types(&self) -> Result<()> {
        match self {
            Self::Hir { .. } => Ok(()),
            Self::Prepared(arguments) => {
                if !arguments.custom_types_enabled {
                    crate::bail_parse_error!(
                        "Array features require --experimental-custom-types flag"
                    );
                }
                Ok(())
            }
        }
    }
}

pub(crate) fn translate_hir_aggregation_step(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    call: &hir::FunctionCall,
    target_register: usize,
    distinct: Option<&HirAggregateDistinct>,
    fraction_reg: Option<usize>,
) -> Result<usize> {
    // Only ordered-set aggregates may carry an ORDER BY on their arguments.
    if !call.arguments.order_terms().is_empty()
        && !matches!(call.arguments, hir::FunctionArguments::OrderedSet { .. })
    {
        crate::bail_parse_error!("ORDER BY clause is not supported yet in aggregate functions");
    }
    let func = hir_aggregate_function(call)?;
    emit_aggregation_step(
        program,
        AggregationArguments::Hir {
            document,
            call,
            func,
            distinct,
        },
        target_register,
        fraction_reg,
    )
}

/// Step an aggregate whose argument ORDER BY its caller applied: the caller
/// feeds it the rows in that order, with the arguments in bound registers.
pub(crate) fn translate_hir_sorted_aggregation_step(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    call: &hir::FunctionCall,
    target_register: usize,
    distinct: Option<&HirAggregateDistinct>,
) -> Result<usize> {
    let func = hir_aggregate_function(call)?;
    emit_aggregation_step(
        program,
        AggregationArguments::Hir {
            document,
            call,
            func,
            distinct,
        },
        target_register,
        None,
    )
}

pub(crate) fn translate_prepared_aggregation_step(
    program: &mut ProgramBuilder,
    arguments: PreparedAggregateArguments<'_>,
    target_register: usize,
) -> Result<usize> {
    emit_aggregation_step(
        program,
        AggregationArguments::Prepared(arguments),
        target_register,
        None,
    )
}

pub(crate) fn hir_aggregate_function(call: &hir::FunctionCall) -> Result<AggFunc> {
    Ok(match call.function.value() {
        Func::Agg(func) => func.clone(),
        Func::External(external) if external.func.is_aggregate() => {
            AggFunc::External(external.func.clone().into())
        }
        function => {
            return Err(LimboError::InternalError(format!(
                "HIR aggregate identity belongs to non-aggregate function {function}"
            )))
        }
    })
}

fn emit_aggregation_step(
    program: &mut ProgramBuilder,
    agg_arg_source: AggregationArguments<'_>,
    target_register: usize,
    fraction_reg: Option<usize>,
) -> Result<usize> {
    let num_args = agg_arg_source.num_args();
    let func = agg_arg_source.func();
    let dest = match func {
        AggFunc::Avg => {
            if num_args != 1 {
                crate::bail_parse_error!("avg bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::Avg),
                    comparator: None,
                    collation: None,
                }),
            });
            target_register
        }
        AggFunc::Count0 => {
            let expr_reg = agg_arg_source.translate_integer_one(program)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::Count0),
                    comparator: None,
                    collation: None,
                }),
            });
            target_register
        }
        AggFunc::Count => {
            if num_args != 1 {
                crate::bail_parse_error!("count bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::Count),
                    comparator: None,
                    collation: None,
                }),
            });
            target_register
        }
        AggFunc::GroupConcat => {
            if num_args != 1 && num_args != 2 {
                crate::bail_parse_error!("group_concat bad number of arguments");
            }

            let delimiter_reg = if num_args == 2 {
                agg_arg_source.translate(program, 1)?
            } else {
                agg_arg_source.translate_comma(program)?
            };

            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);

            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: delimiter_reg,
                    func: AccumulatorFunc::Agg(AggFunc::GroupConcat),
                    comparator: None,
                    collation: None,
                }),
            });

            target_register
        }
        AggFunc::Max => {
            if num_args != 1 {
                crate::bail_parse_error!("max bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            let arg_collation = agg_arg_source.collation(0);
            let comparator = agg_arg_source.comparator(0);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::Max),
                    comparator,
                    collation: Some(arg_collation),
                }),
            });
            target_register
        }
        AggFunc::Min => {
            if num_args != 1 {
                crate::bail_parse_error!("min bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            let arg_collation = agg_arg_source.collation(0);
            let comparator = agg_arg_source.comparator(0);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::Min),
                    comparator,
                    collation: Some(arg_collation),
                }),
            });
            target_register
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject => {
            if num_args != 2 {
                crate::bail_parse_error!("max bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            let value_reg = agg_arg_source.translate(program, 1)?;

            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: value_reg,
                    func: AccumulatorFunc::Agg(AggFunc::JsonGroupObject),
                    comparator: None,
                    collation: None,
                }),
            });
            target_register
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupArray | AggFunc::JsonbGroupArray => {
            if num_args != 1 {
                crate::bail_parse_error!("max bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::JsonGroupArray),
                    comparator: None,
                    collation: None,
                }),
            });
            target_register
        }
        AggFunc::StringAgg => {
            if num_args != 2 {
                crate::bail_parse_error!("string_agg bad number of arguments");
            }

            let expr_reg = agg_arg_source.translate(program, 0)?;
            let delimiter_reg = agg_arg_source.translate(program, 1)?;

            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: delimiter_reg,
                    func: AccumulatorFunc::Agg(AggFunc::StringAgg),
                    comparator: None,
                    collation: None,
                }),
            });

            target_register
        }
        AggFunc::Sum => {
            if num_args != 1 {
                crate::bail_parse_error!("sum bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::Sum),
                    comparator: None,
                    collation: None,
                }),
            });
            target_register
        }
        AggFunc::Total => {
            if num_args != 1 {
                crate::bail_parse_error!("total bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::Total),
                    comparator: None,
                    collation: None,
                }),
            });
            target_register
        }
        AggFunc::ArrayAgg => {
            agg_arg_source.require_custom_types()?;
            if num_args != 1 {
                crate::bail_parse_error!("array_agg bad number of arguments");
            }
            let expr_reg = agg_arg_source.translate(program, 0)?;
            agg_arg_source.handle_distinct(program, expr_reg);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::ArrayAgg),
                    comparator: None,
                    collation: None,
                }),
            });
            target_register
        }
        AggFunc::Mode => {
            // Planner rewrites `mode() WITHIN GROUP (ORDER BY x)` to a single arg `[x]`.
            if num_args != 1 {
                crate::bail_parse_error!("mode bad number of arguments");
            }
            let value_reg = agg_arg_source.translate(program, 0)?;
            // Activate the value's collation so finalize can sort text correctly.
            let arg_collation = agg_arg_source.collation(0);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: value_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::Mode),
                    comparator: None,
                    collation: Some(arg_collation),
                }),
            });
            target_register
        }
        AggFunc::PercentileCont | AggFunc::PercentileDisc => {
            // Planner rewrites `percentile_*(fraction) WITHIN GROUP (ORDER BY x)` to
            // args `[x, fraction]`: the value goes in `col`, the fraction in `delimiter`.
            // The fraction is evaluated and range-checked once before the row loop
            // in `InitLoop::emit` — including the input-column / subquery rejection.
            if num_args != 2 {
                crate::bail_parse_error!("percentile bad number of arguments");
            }
            let value_reg = agg_arg_source.translate(program, 0)?;
            let fraction_reg =
                fraction_reg.expect("percentile fraction register must be set by InitLoop::emit");
            let arg_collation = agg_arg_source.collation(0);
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: value_reg,
                    delimiter: fraction_reg,
                    func: AccumulatorFunc::Agg(func.clone()),
                    comparator: None,
                    collation: Some(arg_collation),
                }),
            });
            target_register
        }
        AggFunc::External(ref func) => {
            let registered_argc = func.agg_args().map_err(|_| {
                LimboError::ExtensionError(
                    "External aggregate function called with wrong number of arguments".to_string(),
                )
            })?;
            if registered_argc >= 0 && registered_argc as usize != num_args {
                crate::bail_parse_error!(
                    "External aggregate function called with wrong number of arguments"
                );
            }
            let argc = num_args;
            let expr_reg = if argc == 0 {
                0
            } else {
                agg_arg_source.translate(program, 0)?
            };
            for i in 0..argc {
                if i != 0 {
                    let _ = agg_arg_source.translate(program, i)?;
                }
                // invariant: distinct aggregates are only supported for single-argument functions
                if argc == 1 {
                    agg_arg_source.handle_distinct(program, expr_reg + i);
                }
            }
            program.emit_insn(Insn::AggStep {
                data: Box::new(AggStepData {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AccumulatorFunc::Agg(AggFunc::External(if registered_argc < 0 {
                        Arc::new(func.with_aggregate_arg_count(num_args))
                    } else {
                        func.clone()
                    })),
                    comparator: None,
                    collation: None,
                }),
            });
            target_register
        }
    };
    // Aggregate arguments can carry column or explicit COLLATE metadata for the
    // aggregate's internal comparator, but that state must not leak to the
    // surrounding expression that consumes the aggregate result.
    program.reset_collation();
    Ok(dest)
}
