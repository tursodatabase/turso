//! The aggregates of PostgreSQL's `pg_aggregate` that the frontend has.

use crate::error::sqlstate_error;
use crate::functions::{format_type, undefined_function};
use crate::operators::checked_operand_types;
use crate::procs::{Proc, PROCS};
use turso_core::dialect::{AggregateChoice, ChosenAggregate, ChosenFunction, OperandType};
use turso_core::Result;
use turso_pg_parser::sqlstate::SQLSTATE_FEATURE_NOT_SUPPORTED;

/// A row of `pg_aggregate`. `oid` is the `pg_proc` oid of the aggregate, a
/// function is the `pg_proc` oid of the function or 0 for none, and a type is
/// the `oid` of its `pg_type` row.
pub(crate) struct Aggregate {
    pub oid: i64,
    pub kind: &'static str,
    pub direct_args: i64,
    pub state_function: i64,
    pub final_function: i64,
    pub combine_function: i64,
    pub serialize_function: i64,
    pub deserialize_function: i64,
    pub moving_state_function: i64,
    pub moving_inverse_function: i64,
    pub moving_final_function: i64,
    pub final_extra: bool,
    pub moving_final_extra: bool,
    pub final_modify: &'static str,
    pub moving_final_modify: &'static str,
    pub sort_operator: i64,
    pub state_type: i64,
    pub state_space: i64,
    pub moving_state_type: i64,
    pub moving_state_space: i64,
    pub initial_value: Option<&'static str>,
    pub moving_initial_value: Option<&'static str>,
}

macro_rules! aggregates {
    ($((
        $oid:literal, $kind:literal, $direct_args:literal, $state_function:literal,
        $final_function:literal, $combine_function:literal, $serialize_function:literal,
        $deserialize_function:literal, $moving_state_function:literal,
        $moving_inverse_function:literal, $moving_final_function:literal,
        $final_extra:literal, $moving_final_extra:literal, $final_modify:literal,
        $moving_final_modify:literal, $sort_operator:literal, $state_type:literal,
        $state_space:literal, $moving_state_type:literal, $moving_state_space:literal,
        $initial_value:expr, $moving_initial_value:expr
    ),)*) => {
        &[$(Aggregate {
            oid: $oid,
            kind: $kind,
            direct_args: $direct_args,
            state_function: $state_function,
            final_function: $final_function,
            combine_function: $combine_function,
            serialize_function: $serialize_function,
            deserialize_function: $deserialize_function,
            moving_state_function: $moving_state_function,
            moving_inverse_function: $moving_inverse_function,
            moving_final_function: $moving_final_function,
            final_extra: $final_extra,
            moving_final_extra: $moving_final_extra,
            final_modify: $final_modify,
            moving_final_modify: $moving_final_modify,
            sort_operator: $sort_operator,
            state_type: $state_type,
            state_space: $state_space,
            moving_state_type: $moving_state_type,
            moving_state_space: $moving_state_space,
            initial_value: $initial_value,
            moving_initial_value: $moving_initial_value,
        }),*]
    };
}

/// The rows as PostgreSQL 18 has them, in the order of its columns.
#[rustfmt::skip]
pub(crate) const AGGREGATES: &[Aggregate] = aggregates![
    (2517, "n", 0, 2515, 0, 2515, 0, 0, 3496, 3497, 3498, false, false, "r", "r", 58, 16, 0, 2281, 16, None, None),
    (2518, "n", 0, 2516, 0, 2516, 0, 0, 3496, 3497, 3499, false, false, "r", "r", 59, 16, 0, 2281, 16, None, None),
    (2519, "n", 0, 2515, 0, 2515, 0, 0, 3496, 3497, 3498, false, false, "r", "r", 58, 16, 0, 2281, 16, None, None),
];

/// The aggregate that PostgreSQL chooses for a call of `name`, from the
/// `AGGREGATES` row of the function whose name and argument types fit. A
/// call with an argument of a type that no such function takes is refused
/// with 42883.
pub(crate) fn choose_aggregate(
    name: &str,
    arguments: &dyn Fn() -> Vec<OperandType>,
) -> Result<AggregateChoice> {
    let candidates: Vec<(&Aggregate, &Proc)> = AGGREGATES
        .iter()
        .filter_map(|aggregate| {
            let proc = proc_with_oid(aggregate.oid)?;
            proc.name
                .eq_ignore_ascii_case(name)
                .then_some((aggregate, proc))
        })
        .collect();
    if candidates.is_empty() {
        return Ok(AggregateChoice::LeftToTheEngine);
    }
    let Some(types) = checked_operand_types(&arguments()) else {
        return Ok(AggregateChoice::LeftToTheEngine);
    };
    // FIXME(registered-types): The engine gives a value that has no registered type one of its own types, which stands for several PostgreSQL types, so only an argument of a registered type is checked. The type of each value replaces this.
    let types: Vec<Option<i64>> = types
        .iter()
        .map(|ty| {
            ty.as_ref()
                .filter(|ty| ty.engine.is_none())
                .map(|ty| ty.oid)
        })
        .collect();
    let takes_arguments = |proc: &Proc| {
        proc.arg_types.len() == types.len()
            && proc
                .arg_types
                .iter()
                .zip(&types)
                .all(|(expected, ty)| ty.is_none_or(|ty| ty == *expected))
    };
    let mut fitting = candidates.iter().filter(|(_, proc)| takes_arguments(proc));
    match (fitting.next(), fitting.next()) {
        (Some((aggregate, _)), None) => chosen_aggregate(aggregate).map(AggregateChoice::Chosen),
        (None, _) if types.iter().any(Option::is_some) => {
            let names: Vec<String> = types
                .iter()
                .map(|ty| ty.map_or("unknown".to_string(), |oid| format_type(oid, -1)))
                .collect();
            let names: Vec<&str> = names.iter().map(String::as_str).collect();
            Err(undefined_function(name, &names))
        }
        _ => Ok(AggregateChoice::LeftToTheEngine),
    }
}

fn chosen_aggregate(aggregate: &Aggregate) -> Result<ChosenAggregate> {
    if aggregate.initial_value.is_some() {
        return Err(sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            "an aggregate with an initial value is not supported",
            None,
        ));
    }
    let function = |oid: i64| {
        proc_with_oid(oid).map(|proc| ChosenFunction {
            name: proc.name.to_string(),
            is_strict: proc.strict,
        })
    };
    Ok(ChosenAggregate {
        state_function: function(aggregate.state_function)
            .expect("the state function of an aggregate is in pg_proc"),
        final_function: function(aggregate.final_function),
        initial_value: None,
    })
}

fn proc_with_oid(oid: i64) -> Option<&'static Proc> {
    PROCS.iter().find(|proc| proc.oid == oid)
}
