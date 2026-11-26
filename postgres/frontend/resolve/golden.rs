use super::catalog::fixtures::FixtureCatalog;
use super::catalog::UNKNOWNOID;
use super::oracle::{oracle_cases, OracleCase};
use super::{
    resolve_function, resolve_operator, ArgCoercion, ArgType, Reason, Resolution, ResolveError,
};
use std::collections::BTreeMap;

const EXPECTED_TABLE: &[(&str, usize)] = &[
    ("total", 24000),
    ("equal to the oracle", 23034),
    ("equal to the oracle: same choice", 10103),
    ("equal to the oracle: error 42704", 10),
    ("equal to the oracle: error 42725", 121),
    ("equal to the oracle: error 42804", 156),
    ("equal to the oracle: error 42883", 12644),
    ("chosen, PostgreSQL fails after choosing", 8),
    ("not supported", 958),
    ("not supported: Aggregate", 413),
    ("not supported: Array", 143),
    ("not supported: Cast", 58),
    ("not supported: Internal", 1),
    ("not supported: Variadic", 343),
    ("differences", 0),
];

#[test]
fn every_case_of_the_oracle_gets_the_answer_of_postgresql() {
    let mut table: BTreeMap<String, usize> = BTreeMap::new();
    let mut differences = Vec::new();
    for case in oracle_cases() {
        let outcome = match compare(case) {
            Ok(outcome) => outcome,
            Err(difference) => {
                differences.push(format!(
                    "{} {} {:?}: {difference}",
                    case.kind, case.name, case.args
                ));
                Outcome::Different
            }
        };
        *table.entry("total".to_string()).or_default() += 1;
        *table.entry(outcome.group().to_string()).or_default() += 1;
        if let Some(row) = outcome.row() {
            *table.entry(row).or_default() += 1;
        }
    }
    table
        .entry(Outcome::Different.group().to_string())
        .or_default();

    for (row, count) in &table {
        println!("{row:<45} {count:>6}");
    }
    assert!(
        differences.is_empty(),
        "{} differences, the first ones: {:#?}",
        differences.len(),
        &differences[..differences.len().min(40)]
    );
    let expected: BTreeMap<String, usize> = EXPECTED_TABLE
        .iter()
        .map(|(row, count)| (row.to_string(), *count))
        .collect();
    assert_eq!(table, expected);
}

enum Outcome {
    SameChoice,
    SameError(&'static str),
    /// PostgreSQL makes the same choice and then fails to read the untyped literal as the parameter type.
    ChoiceThatPostgresCannotRun,
    Unsupported(Reason),
    Different,
}

impl Outcome {
    fn group(&self) -> &'static str {
        match self {
            Outcome::SameChoice | Outcome::SameError(_) => "equal to the oracle",
            Outcome::ChoiceThatPostgresCannotRun => "chosen, PostgreSQL fails after choosing",
            Outcome::Unsupported(_) => "not supported",
            Outcome::Different => "differences",
        }
    }

    fn row(&self) -> Option<String> {
        match self {
            Outcome::SameChoice => Some("equal to the oracle: same choice".to_string()),
            Outcome::SameError(sqlstate) => Some(format!("equal to the oracle: error {sqlstate}")),
            Outcome::Unsupported(reason) => Some(format!("not supported: {reason:?}")),
            Outcome::ChoiceThatPostgresCannotRun | Outcome::Different => None,
        }
    }
}

struct Choice {
    oid: i64,
    function: Option<i64>,
    parameter_types: Vec<i64>,
    coercions: Vec<ArgCoercion>,
    result_type: i64,
}

fn compare(case: &OracleCase) -> Result<Outcome, String> {
    match resolve(case)? {
        Resolution::Resolved(choice) => compare_choice(case, &choice),
        Resolution::Error(error) => compare_error(case, &error),
        Resolution::Unsupported(reason) => compare_unsupported(case, reason),
    }
}

fn resolve(case: &OracleCase) -> Result<Resolution<Choice>, String> {
    let catalog = FixtureCatalog::get();
    let arguments: Vec<ArgType> = case.args.iter().map(|&arg| argument(arg)).collect();
    if case.kind != "operator" {
        return Ok(match resolve_function(catalog, &case.name, &arguments) {
            Resolution::Resolved(call) => Resolution::Resolved(Choice {
                oid: call.oid,
                function: None,
                parameter_types: call.parameter_types,
                coercions: call.coercions,
                result_type: call.result_type,
            }),
            Resolution::Error(error) => Resolution::Error(error),
            Resolution::Unsupported(reason) => Resolution::Unsupported(reason),
        });
    }
    let resolution = match arguments.as_slice() {
        [right] => resolve_operator(catalog, &case.name, None, *right),
        [left, right] => resolve_operator(catalog, &case.name, Some(*left), *right),
        operands => return Err(format!("an operator with {} operands", operands.len())),
    };
    Ok(match resolution {
        Resolution::Resolved(operator) => Resolution::Resolved(Choice {
            oid: operator.oid,
            function: Some(operator.function),
            parameter_types: operator.operand_types,
            coercions: operator.coercions,
            result_type: operator.result_type,
        }),
        Resolution::Error(error) => Resolution::Error(error),
        Resolution::Unsupported(reason) => Resolution::Unsupported(reason),
    })
}

fn argument(type_id: i64) -> ArgType {
    match type_id {
        UNKNOWNOID => ArgType::Unknown,
        known => ArgType::Known(known),
    }
}

fn compare_choice(case: &OracleCase, choice: &Choice) -> Result<Outcome, String> {
    if case.chosen.is_none() && postgres_fails_after_choosing(case) {
        return Ok(Outcome::ChoiceThatPostgresCannotRun);
    }
    let ours = (
        Some(choice.oid),
        choice.function,
        Some(choice.result_type),
        coercions_as_the_oracle_writes_them(case, choice),
    );
    let theirs = (
        case.chosen,
        case.function,
        case.result,
        case.coercions.clone(),
    );
    if ours != theirs || case.kind == "aggregate" {
        return Err(format!("chose {ours:?}, PostgreSQL {}", answer(case)));
    }
    Ok(Outcome::SameChoice)
}

fn postgres_fails_after_choosing(case: &OracleCase) -> bool {
    let message = case.message.as_deref().unwrap_or("");
    match case.sqlstate.as_deref() {
        Some("0A000") => message.starts_with("cannot accept a value of type "),
        Some("XX000") => message.starts_with("failed to find conversion function from unknown to "),
        _ => false,
    }
}

fn coercions_as_the_oracle_writes_them(case: &OracleCase, choice: &Choice) -> Vec<String> {
    case.args
        .iter()
        .zip(&choice.parameter_types)
        .zip(&choice.coercions)
        .map(|((&argument, parameter), coercion)| match coercion {
            ArgCoercion::None if argument == UNKNOWNOID => format!("unknown>{UNKNOWNOID}"),
            ArgCoercion::None => "none".to_string(),
            ArgCoercion::Literal => format!("unknown>{parameter}"),
            ArgCoercion::Relabel => format!("relabel>{parameter}"),
            ArgCoercion::Function(function) => format!("fn:{function}>{parameter}"),
            ArgCoercion::ViaInputOutput => format!("io>{parameter}"),
        })
        .collect()
}

fn compare_error(case: &OracleCase, error: &ResolveError) -> Result<Outcome, String> {
    let ours = (
        Some(error.sqlstate()),
        Some(error.message()),
        error.detail(),
        error.hint(),
    );
    let theirs = (
        case.sqlstate.as_deref(),
        case.message.as_deref(),
        case.detail.as_deref(),
        case.hint.as_deref(),
    );
    if ours != theirs {
        return Err(format!("failed with {ours:?}, PostgreSQL {}", answer(case)));
    }
    Ok(Outcome::SameError(error.sqlstate()))
}

fn compare_unsupported(case: &OracleCase, reason: Reason) -> Result<Outcome, String> {
    let is_a_cast = case.kind == "cast";
    let is_an_aggregate = case.kind == "aggregate";
    let coerces_an_array = case
        .coercions
        .iter()
        .any(|coercion| coercion.starts_with("array>"));
    let fits = match reason {
        Reason::Cast => is_a_cast,
        Reason::Aggregate => is_an_aggregate || case.sqlstate.is_some(),
        Reason::Variadic => !is_an_aggregate,
        Reason::Array => coerces_an_array || case.sqlstate.as_deref() == Some("42809"),
        _ => !is_a_cast && !is_an_aggregate,
    };
    if !fits {
        return Err(format!(
            "not supported: {reason:?}, PostgreSQL {}",
            answer(case)
        ));
    }
    Ok(Outcome::Unsupported(reason))
}

fn answer(case: &OracleCase) -> String {
    match (&case.chosen, &case.sqlstate, &case.node) {
        (Some(chosen), _, _) => format!(
            "chose {:?}",
            (chosen, case.function, case.result, &case.coercions)
        ),
        (None, Some(sqlstate), _) => format!(
            "failed with {:?}",
            (sqlstate, &case.message, &case.detail, &case.hint)
        ),
        (None, None, node) => format!("read it as the cast {node:?}"),
    }
}
