//! The operators of PostgreSQL's `pg_operator` that the frontend has, and
//! the refusal of an operator that has no row for the types of its operands.

use crate::casts::{resolve, wraps_negative_integers, CastType};
use crate::catalog::{pg_type_category, pg_type_name, pg_type_oid};
use crate::error::sqlstate_error;
use crate::functions::{format_type, undefined_operator_of, validate_pg_input};
use crate::pg_type::{IntegerOperator, IntegerType};
use turso_core::ast;
use turso_core::dialect::{
    CastContext, ChosenOperator, EngineType, OperandType, OperatorChoice, OperatorFunction,
};
use turso_core::types::Type as _;
use turso_core::Result;
use turso_pg_parser::sqlstate::{
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INVALID_TEXT_REPRESENTATION,
    SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
};

/// A row of `pg_operator`. A type is the `oid` of its `pg_type` row, and
/// `left` is `NONE` for a prefix operator.
pub(crate) struct Operator {
    pub oid: i64,
    pub name: &'static str,
    pub left: i64,
    pub right: i64,
    pub result: i64,
    pub commutator: i64,
    pub negator: i64,
    pub can_merge: bool,
    pub can_hash: bool,
}

/// PostgreSQL's choice of an operator named `op` for operands of these types
/// (`oper_select` in parse_oper.c), as far as the rows of `OPERATORS` decide
/// it:
///
/// 1. An operand has a type that `CHECKED_TYPES` does not name, or has no
///    type at all: the engine runs its own operator.
/// 2. The operator is not a comparison and an operand is of a type in
///    `OPERATORS_RUN_BY_CORE`: the operator signatures of that type in core
///    decide whether the operator exists. The rows are not asked.
/// 3. No row can take the operands: the operator is refused with 42883.
/// 4. A string literal is next to an operand of a number type, and a row
///    takes two operands of that type: the literal is read as that type.
/// 5. Otherwise the engine runs its own operator.
pub(crate) fn choose_operator(op: &str, operands: &[OperandType]) -> Result<OperatorChoice> {
    let Some(types) = checked_operand_types(operands) else {
        return Ok(OperatorChoice::LeftToTheEngine);
    };
    if !is_run_by_core(op, &types) {
        let oids: Vec<Option<i64>> = types
            .iter()
            .map(|ty| ty.as_ref().map(|ty| ty.oid))
            .collect();
        check_operator(op, &oids)?;
    }
    let (literal, ty) = match (operands, types.as_slice()) {
        ([OperandType::StringLiteral(literal), _], [_, Some(ty)])
        | ([_, OperandType::StringLiteral(literal)], [Some(ty), _]) => (literal, ty),
        _ => {
            let choice = implicit_cast_choice(op, &types);
            return Ok(choice.unwrap_or_else(|| integer_operator_choice(op, &types)));
        }
    };
    // The engine reads a literal next to a value of another type as that
    // type already, with the input function of the type.
    if pg_type_category(ty.oid) != Some("N") {
        return Ok(OperatorChoice::LeftToTheEngine);
    }
    let takes_two = |row: &&Operator| row.name == op && row.left == ty.oid && row.right == ty.oid;
    let Some(row) = OPERATORS.iter().find(takes_two) else {
        return Ok(OperatorChoice::LeftToTheEngine);
    };
    if is_spelling_left_to_the_engine(literal, ty.oid) {
        return Ok(OperatorChoice::LeftToTheEngine);
    }
    check_number_literal(literal, ty)?;
    Ok(OperatorChoice::Chosen(ChosenOperator {
        id: row.oid,
        operands: vec![ty.cast_to.clone(), ty.cast_to.clone()],
        cast_operands: vec![],
        function: integer_operator_function(op, &[ty, ty]),
    }))
}

/// FIXME(registered-types): The engine casts an operand that is not a string literal only when the choice says so (`cast_operands`), and this says so only for a cast that wraps a negative integer, which is the cast of an integer to oid. The cast of any other operand to the type of the row is left to the engine, which compares the stored numbers. Once the operand casts come from the operator that `id` names, the choice of any operator says so.
/// PostgreSQL's choice of an operator for operands that no row takes as they
/// are (`oper_select_candidate` in parse_oper.c): of the rows named `op` that
/// every operand converts to with an implicit cast, the one that most operands
/// have the type of. An operand of unknown type is taken to have the type of
/// the other. A choice is only given when a cast wraps a negative integer,
/// as an integer next to an oid does, and the operands that it casts are those.
fn implicit_cast_choice(op: &str, types: &[Option<Checked>]) -> Option<OperatorChoice> {
    let [left, right] = types else {
        return None;
    };
    let (left_oid, right_oid) = match (left, right) {
        (Some(left), Some(right)) => (left.oid, right.oid),
        (Some(known), None) | (None, Some(known)) => (known.oid, known.oid),
        (None, None) => return None,
    };
    let exact_matches =
        |row: &Operator| usize::from(row.left == left_oid) + usize::from(row.right == right_oid);
    let candidates = || {
        OPERATORS.iter().filter(|row| {
            row.name == op
                && row.left != NONE
                && implicitly_casts(left_oid, row.left)
                && implicitly_casts(right_oid, row.right)
        })
    };
    let most = candidates().map(exact_matches).max()?;
    let mut best = candidates().filter(|row| exact_matches(row) == most);
    let row = best.next().filter(|_| best.next().is_none())?;
    let cast_operands = vec![
        casts_wrapping(left.as_ref(), row.left),
        casts_wrapping(right.as_ref(), row.right),
    ];
    if !cast_operands.contains(&true) {
        return None;
    }
    let wanted = |oid: i64| ast::Type {
        name: pg_type_name(oid)
            .expect("a row names types of pg_type")
            .to_string(),
        size: None,
        array_dimensions: 0,
    };
    Some(OperatorChoice::Chosen(ChosenOperator {
        id: row.oid,
        operands: vec![wanted(row.left), wanted(row.right)],
        cast_operands,
        function: None,
    }))
}

/// FIXME(registered-types): Two integers that have no registered type, such as two integers written in the statement or count(*), are left to the engine, because one engine type stands for integer and bigint (see `engine_type_oid`). So `2147483647 + 1` gives 2147483648, where PostgreSQL fails with 22003. A type for an integer literal by its value, and for a function result from prorettype of its pg_proc row, replace this.
/// The choice of the operator that runs `op` on operands that are all
/// integers, one of them at least of a registered integer type.
fn integer_operator_choice(op: &str, types: &[Option<Checked>]) -> OperatorChoice {
    let Some(types) = types.iter().map(Option::as_ref).collect::<Option<Vec<_>>>() else {
        return OperatorChoice::LeftToTheEngine;
    };
    let Some(function) = integer_operator_function(op, &types) else {
        return OperatorChoice::LeftToTheEngine;
    };
    let oids: Vec<i64> = types.iter().map(|ty| ty.oid).collect();
    let takes_these = |row: &&Operator| match oids[..] {
        [right] => row.name == op && row.left == NONE && row.right == right,
        [left, right] => row.name == op && row.left == left && row.right == right,
        _ => false,
    };
    let row = OPERATORS
        .iter()
        .find(takes_these)
        .expect("an integer operator function is the oprcode of a row");
    OperatorChoice::Chosen(ChosenOperator {
        id: row.oid,
        operands: types.iter().map(|ty| ty.cast_to.clone()).collect(),
        cast_operands: vec![],
        function: Some(function),
    })
}

fn integer_operator_function(op: &str, types: &[&Checked]) -> Option<OperatorFunction> {
    if types.iter().all(|ty| ty.engine.is_some()) {
        return None;
    }
    let integer_types = types
        .iter()
        .map(|ty| integer_type(ty.oid))
        .collect::<Option<Vec<_>>>()?;
    let operator = match integer_types[..] {
        [right] => IntegerOperator::find(op, None, right),
        [left, right] => IntegerOperator::find(op, Some(left), right),
        _ => None,
    }?;
    Some(OperatorFunction {
        name: operator.function_name(),
        result: ast::Type {
            name: operator.result().name().to_string(),
            size: None,
            array_dimensions: 0,
        },
    })
}

fn integer_type(oid: i64) -> Option<IntegerType> {
    match oid {
        INT2 => Some(IntegerType::Int2),
        INT4 => Some(IntegerType::Int4),
        INT8 => Some(IntegerType::Int8),
        _ => None,
    }
}

/// PostgreSQL's choice of the `=` (`<>` for NOT IN) of `x IN (a, b, ...)` for
/// the values that read no column (`transformAExprIn` in parse_expr.c). The
/// operands are `x` and then each of those values:
///
/// 1. An operand has a type that `CHECKED_TYPES` does not name, or has no
///    type at all: the engine runs its own comparisons.
/// 2. There is one such value, or the operands have no common type
///    (`select_common_type`): each value is compared with `x` alone, which
///    `choose_operator` decides.
/// 3. The common type is a number type, and a row takes two operands of that
///    type: each string literal is read as that type.
/// 4. Otherwise the engine runs its own comparisons.
///
/// Every type of `CHECKED_TYPES` has a row for `=` and `<>` with itself, so
/// a common type never makes the operator undefined.
pub(crate) fn choose_list_operator(
    op: &str,
    operands: &[OperandType],
) -> Result<Option<OperatorChoice>> {
    let Some(types) = checked_operand_types(operands) else {
        return Ok(Some(OperatorChoice::LeftToTheEngine));
    };
    let oids: Vec<Option<i64>> = types
        .iter()
        .map(|ty| ty.as_ref().map(|ty| ty.oid))
        .collect();
    let values = &oids[1..];
    let Some(common) = common_type(&oids).filter(|_| values.len() > 1) else {
        return Ok(None);
    };
    let takes_two = |row: &&Operator| row.name == op && row.left == common && row.right == common;
    let row = OPERATORS.iter().find(takes_two);
    let Some(ty) = types.iter().flatten().find(|ty| ty.oid == common) else {
        return Ok(Some(OperatorChoice::LeftToTheEngine));
    };
    let Some(row) = row.filter(|_| pg_type_category(common) == Some("N")) else {
        return Ok(Some(OperatorChoice::LeftToTheEngine));
    };
    let literals: Vec<&String> = operands
        .iter()
        .filter_map(|operand| match operand {
            OperandType::StringLiteral(literal) => Some(literal),
            _ => None,
        })
        .collect();
    let mut left_to_the_engine = literals.is_empty();
    for literal in literals {
        if is_spelling_left_to_the_engine(literal, common) {
            left_to_the_engine = true;
        } else {
            check_number_literal(literal, ty)?;
        }
    }
    if left_to_the_engine {
        return Ok(Some(OperatorChoice::LeftToTheEngine));
    }
    Ok(Some(OperatorChoice::Chosen(ChosenOperator {
        id: row.oid,
        operands: vec![ty.cast_to.clone(); operands.len()],
        cast_operands: types
            .iter()
            .map(|operand| casts_wrapping(operand.as_ref(), common))
            .collect(),
        function: None,
    })))
}

/// The type that `select_common_type` (parse_coerce.c) gives these operands, a
/// string literal or parameter being `None`: the first type, replaced by a
/// later one of its category when the first casts to the later implicitly and
/// not the other way, unless the first is the preferred type of the category.
/// It is `None` when two types are of different categories or a type does not
/// cast implicitly to the one chosen, and text when no operand has a type.
fn common_type(types: &[Option<i64>]) -> Option<i64> {
    let mut chosen: Option<i64> = None;
    for &ty in types.iter().flatten() {
        let Some(current) = chosen else {
            chosen = Some(ty);
            continue;
        };
        if ty == current {
            continue;
        }
        if pg_type_category(ty) != pg_type_category(current) {
            return None;
        }
        if !PREFERRED_TYPES.contains(&current)
            && casts_implicitly(current, ty)
            && !casts_implicitly(ty, current)
        {
            chosen = Some(ty);
        }
    }
    let chosen = chosen.unwrap_or(TEXT);
    let all_cast = types
        .iter()
        .flatten()
        .all(|&ty| ty == chosen || casts_implicitly(ty, chosen));
    all_cast.then_some(chosen)
}

fn casts_implicitly(from: i64, to: i64) -> bool {
    IMPLICIT_CASTS.contains(&(from, to))
}

/// FIXME(pg-catalog): This hand-codes the rows of pg_cast with castcontext 'i' between the types of `CHECKED_TYPES`, and `PREFERRED_TYPES` the pg_type rows with typispreferred. A lookup in pg_cast and pg_type replaces both, and `fits`, which takes the implicit casts to be every cast within a typcategory, uses the same lookup.
const IMPLICIT_CASTS: [(i64, i64); 20] = [
    (DATE, TIMESTAMP),
    (DATE, TIMESTAMPTZ),
    (TIMESTAMP, TIMESTAMPTZ),
    (FLOAT4, FLOAT8),
    (INT2, INT4),
    (INT2, INT8),
    (INT2, FLOAT4),
    (INT2, FLOAT8),
    (INT2, NUMERIC),
    (INT4, INT8),
    (INT4, FLOAT4),
    (INT4, FLOAT8),
    (INT4, NUMERIC),
    (INT8, FLOAT4),
    (INT8, FLOAT8),
    (INT8, NUMERIC),
    (NUMERIC, FLOAT4),
    (NUMERIC, FLOAT8),
    (TEXT, VARCHAR),
    (VARCHAR, TEXT),
];

const PREFERRED_TYPES: [i64; 4] = [BOOL, TEXT, FLOAT8, TIMESTAMPTZ];

/// The operand types whose operators are decided by core's operator signatures
/// for the arithmetic and bit operators, and the prefix `-` and `~`. Core has
/// no signature for a comparison, so `OPERATORS` decides every comparison.
///
/// FIXME(pg-catalog): Core runs these operators with its own functions (`date_plus`, `numeric_add`) and decides which operand types they take. Once `pg_operator` names the function of each row (`oprcode`, with `pg_proc` under PostgreSQL's oids) and `pg_cast` gives the implicit casts, the rows decide these operators too and this list goes away.
const OPERATORS_RUN_BY_CORE: [i64; 2] = [DATE, NUMERIC];

/// Whether core's operator signatures, and not `OPERATORS`, decide whether
/// `op` exists for these operands: an operand is a registered type of
/// `OPERATORS_RUN_BY_CORE`. A decimal written in the statement is not
/// registered, so it does not count.
fn is_run_by_core(op: &str, types: &[Option<Checked>]) -> bool {
    !COMPARISONS.contains(&op)
        && types
            .iter()
            .flatten()
            .any(|ty| ty.engine.is_none() && OPERATORS_RUN_BY_CORE.contains(&ty.oid))
}

/// FIXME(registered-types): The engine stores numeric and double precision both as REAL, and cannot hold NaN or an exponent past a double, so these spellings are compared as text and give false. Once numeric is a registered type with its own storage, the input function of the type reads every spelling and this function goes away.
/// Whether `literal` is a value of the type `oid` that PostgreSQL reads but
/// the engine cannot store or the input check here does not know. The integer
/// types and oid read every spelling themselves. For a real, a double precision or a
/// numeric it is a number in the hex, octal or binary spelling or with
/// underscores between digits, NaN and the infinities, and a number with an
/// exponent past the range of a double, which a numeric holds. The engine
/// names a numeric REAL, like a double precision, so that exponent is not
/// refused for either. The operator is left to the engine for these, as for
/// any literal.
fn is_spelling_left_to_the_engine(literal: &str, oid: i64) -> bool {
    if [INT2, INT4, INT8, OID].contains(&oid) {
        return false;
    }
    let text = literal.trim();
    let digits = text.trim_start_matches(['+', '-']);
    let lower = digits.to_ascii_lowercase();
    let has_prefix = ["0x", "0o", "0b"]
        .iter()
        .any(|prefix| lower.starts_with(prefix));
    if has_prefix || digits.contains('_') {
        return true;
    }
    if ![FLOAT4, FLOAT8, NUMERIC].contains(&oid) {
        return false;
    }
    if ["nan", "inf", "infinity"]
        .iter()
        .any(|special| digits.eq_ignore_ascii_case(special))
    {
        return true;
    }
    [FLOAT8, NUMERIC].contains(&oid) && text.parse::<f64>().is_ok_and(f64::is_infinite)
}

/// FIXME(registered-types): TEMPORARY. This check goes away once numeric and double precision are registered types. The engine stores a numeric and a double precision both as REAL, and cannot tell them apart, so a literal next to one of them is cast to a double. PostgreSQL keeps all the digits of a numeric, so a literal that a double does not keep gives another answer: `n = '9007199254740993'` is true for a numeric column that holds 9007199254740992. A registered numeric stores the value exactly, and a registered double precision rounds it, as PostgreSQL does.
/// Fails with 0A000 when `ty` is a number that the engine stores as a double
/// and a double does not keep `literal` exactly.
fn check_a_double_keeps(literal: &str, ty: &Checked) -> Result<()> {
    let is_stored_as_a_double = ty.engine.is_some() && [FLOAT8, NUMERIC].contains(&ty.oid);
    if !is_stored_as_a_double || a_double_keeps(literal) {
        return Ok(());
    }
    Err(sqlstate_error(
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        format!("the number \"{literal}\" has more digits than a double keeps, and the engine stores a numeric and a double precision as a double"),
        None,
    ))
}

/// Whether a double keeps the number `text` exactly: at most 15 significant
/// digits always do, and more only when the shortest text of the double is the
/// same number.
fn a_double_keeps(text: &str) -> bool {
    let Ok(value) = text.trim().parse::<f64>() else {
        return true;
    };
    let digits_of = |number: &str| -> (String, usize) {
        let number = number.trim_start_matches(['+', '-']);
        let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
        let fraction = fraction.trim_end_matches('0');
        let digits = format!("{whole}{fraction}");
        (digits.trim_start_matches('0').to_string(), fraction.len())
    };
    let (digits, fraction_digits) = digits_of(text.trim());
    if digits.trim_end_matches('0').len() <= 15 {
        return true;
    }
    if text.contains(['e', 'E']) {
        return false;
    }
    digits_of(&format!("{value}")) == (digits, fraction_digits)
}

/// Fails when `literal` is not a value of the number type `ty`.
fn check_number_literal(literal: &str, ty: &Checked) -> Result<()> {
    let type_name = pg_type_name(ty.oid).expect("an operand type is a row of pg_type");
    let Some((message, sqlstate)) = validate_pg_input(literal, type_name) else {
        return check_a_double_keeps(literal, ty);
    };
    let out_of_range = sqlstate == SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE;
    let is_engine_integer = ty.engine == Some(EngineType::Integer);
    if out_of_range && is_engine_integer && validate_pg_input(literal, "int8").is_none() {
        return Ok(());
    }
    let sqlstate = if out_of_range {
        SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE
    } else {
        SQLSTATE_INVALID_TEXT_REPRESENTATION
    };
    Err(sqlstate_error(sqlstate, message, None))
}

/// An operand whose type the rows of `OPERATORS` decide.
struct Checked {
    oid: i64,
    /// The type that a string literal next to the operand is cast to.
    cast_to: ast::Type,
    /// The engine type of the operand, or `None` for a registered type.
    engine: Option<EngineType>,
}

/// Each operand as a `Checked`, `None` for an operand that fits any type: a
/// string literal or a parameter. Gives `None` itself when an operand has no
/// type, or a type that `CHECKED_TYPES` does not name.
fn checked_operand_types(operands: &[OperandType]) -> Option<Vec<Option<Checked>>> {
    operands
        .iter()
        .map(|operand| match operand {
            OperandType::StringLiteral(_) | OperandType::Untyped => Some(None),
            OperandType::Registered(ty) => checked_registered_type(ty).map(Some),
            OperandType::Engine {
                ty,
                array_dimensions,
            } => checked_engine_type(*ty, *array_dimensions).map(Some),
            OperandType::Unnamed => None,
        })
        .collect()
}

fn checked_registered_type(ty: &ast::Type) -> Option<Checked> {
    if ty.array_dimensions > 0 {
        return None;
    }
    let oid = pg_type_oid(&ty.name).filter(|oid| CHECKED_TYPES.contains(oid))?;
    Some(Checked {
        oid,
        cast_to: ty.clone(),
        engine: None,
    })
}

/// FIXME(registered-types): An integer that has no registered type is taken to be an integer (int4), but it may be a bigint, such as count(*) or an integer written with more digits. So a string literal next to it is cast to bigint, and `check_number_literal` only refuses one out of the range of int4 when it is out of the range of int8. A type for each value replaces this.
fn checked_engine_type(ty: EngineType, array_dimensions: u32) -> Option<Checked> {
    if array_dimensions > 0 {
        return None;
    }
    let oid = engine_type_oid(ty)?;
    let cast_to = match ty {
        EngineType::Integer => "bigint",
        _ => ty.sql_name(),
    };
    Some(Checked {
        oid,
        cast_to: ast::Type {
            name: cast_to.to_string(),
            size: None,
            array_dimensions: 0,
        },
        engine: Some(ty),
    })
}

/// FIXME(registered-types): The engine type stands for several PostgreSQL types, and this takes the one that PostgreSQL gives a literal of that kind, or the widest: an integer may also be a bigint (`count(*)`), and a real may also be a real. A type for each value replaces this.
/// The PostgreSQL type that an engine type is taken to be, `None` for one that
/// `CHECKED_TYPES` does not name.
fn engine_type_oid(ty: EngineType) -> Option<i64> {
    match ty {
        EngineType::Integer => Some(INT4),
        EngineType::Real => Some(FLOAT8),
        EngineType::Decimal | EngineType::Number => Some(NUMERIC),
        EngineType::Text => Some(TEXT),
        EngineType::Blob => None,
    }
}

/// Fails with SQLSTATE 42883 when no row of `OPERATORS` named `op` takes
/// operands of these types.
fn check_operator(op: &str, types: &[Option<i64>]) -> Result<()> {
    if types.iter().all(Option::is_none) {
        return Ok(());
    }
    // FIXME(registered-types): See `fits`. A text operand may be a uuid, json or inet value, so only its comparisons are decided.
    if types.contains(&Some(TEXT)) && !COMPARISONS.contains(&op) {
        return Ok(());
    }
    let takes_operands = |row: &Operator| match types {
        [right] => row.left == NONE && fits(*right, row.right),
        [left, right] => row.left != NONE && fits(*left, row.left) && fits(*right, row.right),
        _ => false,
    };
    if OPERATORS
        .iter()
        .any(|row| row.name == op && takes_operands(row))
    {
        return Ok(());
    }
    let names: Vec<String> = types
        .iter()
        .map(|ty| match ty {
            Some(oid) => format_type(*oid, -1),
            None => "unknown".to_string(),
        })
        .collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    Err(undefined_operator_of(op, &names))
}

/// FIXME(pg-catalog): `OPERATORS` has no rows for interval, time, bytea, json, jsonb, inet, arrays and enums, and for a comparison of date with timestamp or timestamptz (which tursopg refuses with 0A000), so an operand of those types is not checked. Once pg_operator has all 799 rows, every operand is checked and a type with no row is an error.
/// The types whose operators are decided by the rows of `OPERATORS`. Varchar
/// has no rows of its own: it takes the operators of text.
const CHECKED_TYPES: [i64; 14] = [
    BOOL,
    OID,
    INT2,
    INT4,
    INT8,
    FLOAT4,
    FLOAT8,
    NUMERIC,
    TEXT,
    VARCHAR,
    DATE,
    TIMESTAMP,
    TIMESTAMPTZ,
    UUID,
];

/// FIXME(pg-catalog): This hand-codes the implicit casts of pg_cast, which is not filled yet: a type is taken to cast implicitly to every type of its typcategory, and so varchar, which has no operators of its own, to text. A lookup in a pg_cast table replaces this.
/// FIXME(pg-catalog): The engine gives the type text to a uuid, json, jsonb, inet, cidr or macaddr value that a cast or a function returns. So a text operand is taken to be of any string, network or user-defined type, and only its comparisons are decided: those types have other operators in pg_operator that are not rows here. A type for each value, from pg_type, replaces this.
/// Whether an operand of type `operand` may be given to an operator that
/// takes `expected`. `None` is an operand that fits any type.
fn fits(operand: Option<i64>, expected: i64) -> bool {
    if operand == Some(OID) || expected == OID {
        return fits_oid(operand, expected);
    }
    let expected = pg_type_category(expected);
    match operand {
        None => true,
        Some(TEXT) => matches!(expected, Some("S" | "I" | "U")),
        Some(operand) => pg_type_category(operand) == expected,
    }
}

/// An oid has the same typcategory as the integers but takes none of their
/// operators, so it fits an operand that casts to oid implicitly.
fn fits_oid(operand: Option<i64>, expected: i64) -> bool {
    expected == OID && operand.is_none_or(|operand| implicitly_casts(operand, OID))
}

/// Whether casting `operand`, an operand of unknown type when it is `None`, to
/// the type `wanted` wraps a negative integer around.
fn casts_wrapping(operand: Option<&Checked>, wanted: i64) -> bool {
    let source = match operand {
        Some(operand) => match cast_type_of(operand.oid) {
            Some(source) => Some(source),
            None => return false,
        },
        None => None,
    };
    cast_type_of(wanted).is_some_and(|target| wraps_negative_integers(source, target))
}

fn cast_type_of(oid: i64) -> Option<CastType> {
    CastType::from_name(pg_type_name(oid)?)
}

/// Whether a value of the type `from` converts to the type `to` with an
/// implicit cast: the rows of `pg_cast` of `casts.rs` for the types that have
/// them, and `IMPLICIT_CASTS` for the others.
fn implicitly_casts(from: i64, to: i64) -> bool {
    from == to
        || casts_implicitly(from, to)
        || matches!(
            (cast_type_of(from), cast_type_of(to)),
            (Some(source), Some(target))
                if resolve(source, target, CastContext::Implicit).is_ok()
        )
}

const COMPARISONS: [&str; 6] = ["=", "<>", "<", "<=", ">", ">="];

const NONE: i64 = 0;
const BOOL: i64 = 16;
const INT8: i64 = 20;
const INT2: i64 = 21;
const INT4: i64 = 23;
const TEXT: i64 = 25;
const OID: i64 = 26;
const FLOAT4: i64 = 700;
const FLOAT8: i64 = 701;
const DATE: i64 = 1082;
const TIMESTAMP: i64 = 1114;
const TIMESTAMPTZ: i64 = 1184;
const NUMERIC: i64 = 1700;
const UUID: i64 = 2950;
const VARCHAR: i64 = 1043;

macro_rules! operators {
    ($((
        $oid:literal, $name:literal, $left:ident, $right:ident, $result:ident,
        $commutator:literal, $negator:literal, $can_merge:literal, $can_hash:literal
    ),)*) => {
        &[$(Operator {
            oid: $oid,
            name: $name,
            left: $left,
            right: $right,
            result: $result,
            commutator: $commutator,
            negator: $negator,
            can_merge: $can_merge,
            can_hash: $can_hash,
        }),*]
    };
}

/// The rows as PostgreSQL 18 has them: oid, oprname, oprleft, oprright,
/// oprresult, oprcom, oprnegate, oprcanmerge and oprcanhash.
#[rustfmt::skip]
pub(crate) const OPERATORS: &[Operator] = operators![
    (15, "=", INT4, INT8, BOOL, 416, 36, true, true),
    (36, "<>", INT4, INT8, BOOL, 417, 15, false, false),
    (37, "<", INT4, INT8, BOOL, 419, 82, false, false),
    (58, "<", BOOL, BOOL, BOOL, 59, 1695, false, false),
    (59, ">", BOOL, BOOL, BOOL, 58, 1694, false, false),
    (76, ">", INT4, INT8, BOOL, 418, 80, false, false),
    (80, "<=", INT4, INT8, BOOL, 430, 76, false, false),
    (82, ">=", INT4, INT8, BOOL, 420, 37, false, false),
    (85, "<>", BOOL, BOOL, BOOL, 85, 91, false, false),
    (91, "=", BOOL, BOOL, BOOL, 91, 85, true, true),
    (94, "=", INT2, INT2, BOOL, 94, 519, true, true),
    (95, "<", INT2, INT2, BOOL, 520, 524, false, false),
    (96, "=", INT4, INT4, BOOL, 96, 518, true, true),
    (97, "<", INT4, INT4, BOOL, 521, 525, false, false),
    (98, "=", TEXT, TEXT, BOOL, 98, 531, true, true),
    (410, "=", INT8, INT8, BOOL, 410, 411, true, true),
    (411, "<>", INT8, INT8, BOOL, 411, 410, false, false),
    (412, "<", INT8, INT8, BOOL, 413, 415, false, false),
    (413, ">", INT8, INT8, BOOL, 412, 414, false, false),
    (414, "<=", INT8, INT8, BOOL, 415, 413, false, false),
    (415, ">=", INT8, INT8, BOOL, 414, 412, false, false),
    (416, "=", INT8, INT4, BOOL, 15, 417, true, true),
    (417, "<>", INT8, INT4, BOOL, 36, 416, false, false),
    (418, "<", INT8, INT4, BOOL, 76, 430, false, false),
    (419, ">", INT8, INT4, BOOL, 37, 420, false, false),
    (420, "<=", INT8, INT4, BOOL, 82, 419, false, false),
    (430, ">=", INT8, INT4, BOOL, 80, 418, false, false),
    (439, "%", INT8, INT8, INT8, 0, 0, false, false),
    (484, "-", NONE, INT8, INT8, 0, 0, false, false),
    (514, "*", INT4, INT4, INT4, 514, 0, false, false),
    (518, "<>", INT4, INT4, BOOL, 518, 96, false, false),
    (519, "<>", INT2, INT2, BOOL, 519, 94, false, false),
    (520, ">", INT2, INT2, BOOL, 95, 522, false, false),
    (521, ">", INT4, INT4, BOOL, 97, 523, false, false),
    (522, "<=", INT2, INT2, BOOL, 524, 520, false, false),
    (523, "<=", INT4, INT4, BOOL, 525, 521, false, false),
    (524, ">=", INT2, INT2, BOOL, 522, 95, false, false),
    (525, ">=", INT4, INT4, BOOL, 523, 97, false, false),
    (526, "*", INT2, INT2, INT2, 526, 0, false, false),
    (527, "/", INT2, INT2, INT2, 0, 0, false, false),
    (528, "/", INT4, INT4, INT4, 0, 0, false, false),
    (529, "%", INT2, INT2, INT2, 0, 0, false, false),
    (530, "%", INT4, INT4, INT4, 0, 0, false, false),
    (531, "<>", TEXT, TEXT, BOOL, 531, 98, false, false),
    (532, "=", INT2, INT4, BOOL, 533, 538, true, true),
    (533, "=", INT4, INT2, BOOL, 532, 539, true, true),
    (534, "<", INT2, INT4, BOOL, 537, 542, false, false),
    (535, "<", INT4, INT2, BOOL, 536, 543, false, false),
    (536, ">", INT2, INT4, BOOL, 535, 540, false, false),
    (537, ">", INT4, INT2, BOOL, 534, 541, false, false),
    (538, "<>", INT2, INT4, BOOL, 539, 532, false, false),
    (539, "<>", INT4, INT2, BOOL, 538, 533, false, false),
    (540, "<=", INT2, INT4, BOOL, 543, 536, false, false),
    (541, "<=", INT4, INT2, BOOL, 542, 537, false, false),
    (542, ">=", INT2, INT4, BOOL, 541, 534, false, false),
    (543, ">=", INT4, INT2, BOOL, 540, 535, false, false),
    (544, "*", INT2, INT4, INT4, 545, 0, false, false),
    (545, "*", INT4, INT2, INT4, 544, 0, false, false),
    (546, "/", INT2, INT4, INT4, 0, 0, false, false),
    (547, "/", INT4, INT2, INT4, 0, 0, false, false),
    (550, "+", INT2, INT2, INT2, 550, 0, false, false),
    (551, "+", INT4, INT4, INT4, 551, 0, false, false),
    (552, "+", INT2, INT4, INT4, 553, 0, false, false),
    (553, "+", INT4, INT2, INT4, 552, 0, false, false),
    (554, "-", INT2, INT2, INT2, 0, 0, false, false),
    (555, "-", INT4, INT4, INT4, 0, 0, false, false),
    (556, "-", INT2, INT4, INT4, 0, 0, false, false),
    (557, "-", INT4, INT2, INT4, 0, 0, false, false),
    (558, "-", NONE, INT4, INT4, 0, 0, false, false),
    (559, "-", NONE, INT2, INT2, 0, 0, false, false),
    (584, "-", NONE, FLOAT4, FLOAT4, 0, 0, false, false),
    (585, "-", NONE, FLOAT8, FLOAT8, 0, 0, false, false),
    (586, "+", FLOAT4, FLOAT4, FLOAT4, 586, 0, false, false),
    (587, "-", FLOAT4, FLOAT4, FLOAT4, 0, 0, false, false),
    (588, "/", FLOAT4, FLOAT4, FLOAT4, 0, 0, false, false),
    (589, "*", FLOAT4, FLOAT4, FLOAT4, 589, 0, false, false),
    (591, "+", FLOAT8, FLOAT8, FLOAT8, 591, 0, false, false),
    (592, "-", FLOAT8, FLOAT8, FLOAT8, 0, 0, false, false),
    (593, "/", FLOAT8, FLOAT8, FLOAT8, 0, 0, false, false),
    (594, "*", FLOAT8, FLOAT8, FLOAT8, 594, 0, false, false),
    (607, "=", OID, OID, BOOL, 607, 608, true, true),
    (608, "<>", OID, OID, BOOL, 608, 607, false, false),
    (609, "<", OID, OID, BOOL, 610, 612, false, false),
    (610, ">", OID, OID, BOOL, 609, 611, false, false),
    (611, "<=", OID, OID, BOOL, 612, 610, false, false),
    (612, ">=", OID, OID, BOOL, 611, 609, false, false),
    (620, "=", FLOAT4, FLOAT4, BOOL, 620, 621, true, true),
    (621, "<>", FLOAT4, FLOAT4, BOOL, 621, 620, false, false),
    (622, "<", FLOAT4, FLOAT4, BOOL, 623, 625, false, false),
    (623, ">", FLOAT4, FLOAT4, BOOL, 622, 624, false, false),
    (624, "<=", FLOAT4, FLOAT4, BOOL, 625, 623, false, false),
    (625, ">=", FLOAT4, FLOAT4, BOOL, 624, 622, false, false),
    (641, "~", TEXT, TEXT, BOOL, 0, 642, false, false),
    (642, "!~", TEXT, TEXT, BOOL, 0, 641, false, false),
    (654, "||", TEXT, TEXT, TEXT, 0, 0, false, false),
    (664, "<", TEXT, TEXT, BOOL, 666, 667, false, false),
    (665, "<=", TEXT, TEXT, BOOL, 667, 666, false, false),
    (666, ">", TEXT, TEXT, BOOL, 664, 665, false, false),
    (667, ">=", TEXT, TEXT, BOOL, 665, 664, false, false),
    (670, "=", FLOAT8, FLOAT8, BOOL, 670, 671, true, true),
    (671, "<>", FLOAT8, FLOAT8, BOOL, 671, 670, false, false),
    (672, "<", FLOAT8, FLOAT8, BOOL, 674, 675, false, false),
    (673, "<=", FLOAT8, FLOAT8, BOOL, 675, 674, false, false),
    (674, ">", FLOAT8, FLOAT8, BOOL, 672, 673, false, false),
    (675, ">=", FLOAT8, FLOAT8, BOOL, 673, 672, false, false),
    (684, "+", INT8, INT8, INT8, 684, 0, false, false),
    (685, "-", INT8, INT8, INT8, 0, 0, false, false),
    (686, "*", INT8, INT8, INT8, 686, 0, false, false),
    (687, "/", INT8, INT8, INT8, 0, 0, false, false),
    (688, "+", INT8, INT4, INT8, 692, 0, false, false),
    (689, "-", INT8, INT4, INT8, 0, 0, false, false),
    (690, "*", INT8, INT4, INT8, 694, 0, false, false),
    (691, "/", INT8, INT4, INT8, 0, 0, false, false),
    (692, "+", INT4, INT8, INT8, 688, 0, false, false),
    (693, "-", INT4, INT8, INT8, 0, 0, false, false),
    (694, "*", INT4, INT8, INT8, 690, 0, false, false),
    (695, "/", INT4, INT8, INT8, 0, 0, false, false),
    (818, "+", INT8, INT2, INT8, 822, 0, false, false),
    (819, "-", INT8, INT2, INT8, 0, 0, false, false),
    (820, "*", INT8, INT2, INT8, 824, 0, false, false),
    (821, "/", INT8, INT2, INT8, 0, 0, false, false),
    (822, "+", INT2, INT8, INT8, 818, 0, false, false),
    (823, "-", INT2, INT8, INT8, 0, 0, false, false),
    (824, "*", INT2, INT8, INT8, 820, 0, false, false),
    (825, "/", INT2, INT8, INT8, 0, 0, false, false),
    (1093, "=", DATE, DATE, BOOL, 1093, 1094, true, true),
    (1094, "<>", DATE, DATE, BOOL, 1094, 1093, false, false),
    (1095, "<", DATE, DATE, BOOL, 1097, 1098, false, false),
    (1096, "<=", DATE, DATE, BOOL, 1098, 1097, false, false),
    (1097, ">", DATE, DATE, BOOL, 1095, 1096, false, false),
    (1098, ">=", DATE, DATE, BOOL, 1096, 1095, false, false),
    (1099, "-", DATE, DATE, INT4, 0, 0, false, false),
    (1100, "+", DATE, INT4, DATE, 2555, 0, false, false),
    (1101, "-", DATE, INT4, DATE, 0, 0, false, false),
    (1116, "+", FLOAT4, FLOAT8, FLOAT8, 1126, 0, false, false),
    (1117, "-", FLOAT4, FLOAT8, FLOAT8, 0, 0, false, false),
    (1118, "/", FLOAT4, FLOAT8, FLOAT8, 0, 0, false, false),
    (1119, "*", FLOAT4, FLOAT8, FLOAT8, 1129, 0, false, false),
    (1120, "=", FLOAT4, FLOAT8, BOOL, 1130, 1121, true, true),
    (1121, "<>", FLOAT4, FLOAT8, BOOL, 1131, 1120, false, false),
    (1122, "<", FLOAT4, FLOAT8, BOOL, 1133, 1125, false, false),
    (1123, ">", FLOAT4, FLOAT8, BOOL, 1132, 1124, false, false),
    (1124, "<=", FLOAT4, FLOAT8, BOOL, 1135, 1123, false, false),
    (1125, ">=", FLOAT4, FLOAT8, BOOL, 1134, 1122, false, false),
    (1126, "+", FLOAT8, FLOAT4, FLOAT8, 1116, 0, false, false),
    (1127, "-", FLOAT8, FLOAT4, FLOAT8, 0, 0, false, false),
    (1128, "/", FLOAT8, FLOAT4, FLOAT8, 0, 0, false, false),
    (1129, "*", FLOAT8, FLOAT4, FLOAT8, 1119, 0, false, false),
    (1130, "=", FLOAT8, FLOAT4, BOOL, 1120, 1131, true, true),
    (1131, "<>", FLOAT8, FLOAT4, BOOL, 1121, 1130, false, false),
    (1132, "<", FLOAT8, FLOAT4, BOOL, 1123, 1135, false, false),
    (1133, ">", FLOAT8, FLOAT4, BOOL, 1122, 1134, false, false),
    (1134, "<=", FLOAT8, FLOAT4, BOOL, 1125, 1133, false, false),
    (1135, ">=", FLOAT8, FLOAT4, BOOL, 1124, 1132, false, false),
    (1228, "~*", TEXT, TEXT, BOOL, 0, 1229, false, false),
    (1229, "!~*", TEXT, TEXT, BOOL, 0, 1228, false, false),
    (1320, "=", TIMESTAMPTZ, TIMESTAMPTZ, BOOL, 1320, 1321, true, true),
    (1321, "<>", TIMESTAMPTZ, TIMESTAMPTZ, BOOL, 1321, 1320, false, false),
    (1322, "<", TIMESTAMPTZ, TIMESTAMPTZ, BOOL, 1324, 1325, false, false),
    (1323, "<=", TIMESTAMPTZ, TIMESTAMPTZ, BOOL, 1325, 1324, false, false),
    (1324, ">", TIMESTAMPTZ, TIMESTAMPTZ, BOOL, 1322, 1323, false, false),
    (1325, ">=", TIMESTAMPTZ, TIMESTAMPTZ, BOOL, 1323, 1322, false, false),
    (1694, "<=", BOOL, BOOL, BOOL, 1695, 59, false, false),
    (1695, ">=", BOOL, BOOL, BOOL, 1694, 58, false, false),
    (1751, "-", NONE, NUMERIC, NUMERIC, 0, 0, false, false),
    (1752, "=", NUMERIC, NUMERIC, BOOL, 1752, 1753, true, true),
    (1753, "<>", NUMERIC, NUMERIC, BOOL, 1753, 1752, false, false),
    (1754, "<", NUMERIC, NUMERIC, BOOL, 1756, 1757, false, false),
    (1755, "<=", NUMERIC, NUMERIC, BOOL, 1757, 1756, false, false),
    (1756, ">", NUMERIC, NUMERIC, BOOL, 1754, 1755, false, false),
    (1757, ">=", NUMERIC, NUMERIC, BOOL, 1755, 1754, false, false),
    (1758, "+", NUMERIC, NUMERIC, NUMERIC, 1758, 0, false, false),
    (1759, "-", NUMERIC, NUMERIC, NUMERIC, 0, 0, false, false),
    (1760, "*", NUMERIC, NUMERIC, NUMERIC, 1760, 0, false, false),
    (1761, "/", NUMERIC, NUMERIC, NUMERIC, 0, 0, false, false),
    (1762, "%", NUMERIC, NUMERIC, NUMERIC, 0, 0, false, false),
    (1862, "=", INT2, INT8, BOOL, 1868, 1863, true, true),
    (1863, "<>", INT2, INT8, BOOL, 1869, 1862, false, false),
    (1864, "<", INT2, INT8, BOOL, 1871, 1867, false, false),
    (1865, ">", INT2, INT8, BOOL, 1870, 1866, false, false),
    (1866, "<=", INT2, INT8, BOOL, 1873, 1865, false, false),
    (1867, ">=", INT2, INT8, BOOL, 1872, 1864, false, false),
    (1868, "=", INT8, INT2, BOOL, 1862, 1869, true, true),
    (1869, "<>", INT8, INT2, BOOL, 1863, 1868, false, false),
    (1870, "<", INT8, INT2, BOOL, 1865, 1873, false, false),
    (1871, ">", INT8, INT2, BOOL, 1864, 1872, false, false),
    (1872, "<=", INT8, INT2, BOOL, 1867, 1871, false, false),
    (1873, ">=", INT8, INT2, BOOL, 1866, 1870, false, false),
    (1874, "&", INT2, INT2, INT2, 1874, 0, false, false),
    (1875, "|", INT2, INT2, INT2, 1875, 0, false, false),
    (1877, "~", NONE, INT2, INT2, 0, 0, false, false),
    (1878, "<<", INT2, INT4, INT2, 0, 0, false, false),
    (1879, ">>", INT2, INT4, INT2, 0, 0, false, false),
    (1880, "&", INT4, INT4, INT4, 1880, 0, false, false),
    (1881, "|", INT4, INT4, INT4, 1881, 0, false, false),
    (1883, "~", NONE, INT4, INT4, 0, 0, false, false),
    (1884, "<<", INT4, INT4, INT4, 0, 0, false, false),
    (1885, ">>", INT4, INT4, INT4, 0, 0, false, false),
    (1886, "&", INT8, INT8, INT8, 1886, 0, false, false),
    (1887, "|", INT8, INT8, INT8, 1887, 0, false, false),
    (1889, "~", NONE, INT8, INT8, 0, 0, false, false),
    (1890, "<<", INT8, INT4, INT8, 0, 0, false, false),
    (1891, ">>", INT8, INT4, INT8, 0, 0, false, false),
    (1916, "+", NONE, INT8, INT8, 0, 0, false, false),
    (1917, "+", NONE, INT2, INT2, 0, 0, false, false),
    (1918, "+", NONE, INT4, INT4, 0, 0, false, false),
    (1919, "+", NONE, FLOAT4, FLOAT4, 0, 0, false, false),
    (1920, "+", NONE, FLOAT8, FLOAT8, 0, 0, false, false),
    (1921, "+", NONE, NUMERIC, NUMERIC, 0, 0, false, false),
    (2060, "=", TIMESTAMP, TIMESTAMP, BOOL, 2060, 2061, true, true),
    (2061, "<>", TIMESTAMP, TIMESTAMP, BOOL, 2061, 2060, false, false),
    (2062, "<", TIMESTAMP, TIMESTAMP, BOOL, 2064, 2065, false, false),
    (2063, "<=", TIMESTAMP, TIMESTAMP, BOOL, 2065, 2064, false, false),
    (2064, ">", TIMESTAMP, TIMESTAMP, BOOL, 2062, 2063, false, false),
    (2065, ">=", TIMESTAMP, TIMESTAMP, BOOL, 2063, 2062, false, false),
    (2555, "+", INT4, DATE, DATE, 1100, 0, false, false),
    (2972, "=", UUID, UUID, BOOL, 2972, 2973, true, true),
    (2973, "<>", UUID, UUID, BOOL, 2973, 2972, false, false),
    (2974, "<", UUID, UUID, BOOL, 2975, 2977, false, false),
    (2975, ">", UUID, UUID, BOOL, 2974, 2976, false, false),
    (2976, "<=", UUID, UUID, BOOL, 2977, 2975, false, false),
    (2977, ">=", UUID, UUID, BOOL, 2976, 2974, false, false),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_operand_and_result_type_is_a_row_of_pg_type() {
        for row in OPERATORS {
            for oid in [row.left, row.right, row.result] {
                assert!(
                    oid == NONE || pg_type_category(oid).is_some(),
                    "operator {} names the type {oid}",
                    row.oid
                );
            }
        }
    }

    fn registered(name: &str) -> OperandType {
        OperandType::Registered(ast::Type {
            name: name.to_string(),
            size: None,
            array_dimensions: 0,
        })
    }

    fn engine(ty: EngineType) -> OperandType {
        OperandType::Engine {
            ty,
            array_dimensions: 0,
        }
    }

    fn chosen_id(op: &str, operands: &[OperandType]) -> Option<i64> {
        match choose_operator(op, operands).unwrap() {
            OperatorChoice::Chosen(chosen) => Some(chosen.id),
            OperatorChoice::LeftToTheEngine => None,
        }
    }

    #[test]
    fn a_literal_next_to_a_number_chooses_the_row_of_that_type() {
        let literal = || OperandType::StringLiteral("1".to_string());
        let integer = || engine(EngineType::Integer);
        assert_eq!(chosen_id("=", &[integer(), literal()]), Some(96));
        assert_eq!(chosen_id("<", &[literal(), integer()]), Some(97));
        assert_eq!(
            chosen_id("+", &[registered("smallint"), literal()]),
            Some(550)
        );
        assert_eq!(
            chosen_id("=", &[registered("numeric"), literal()]),
            Some(1752)
        );
        assert_eq!(
            chosen_id("=", &[engine(EngineType::Decimal), literal()]),
            Some(1752)
        );
        assert_eq!(chosen_id("=", &[integer(), integer()]), None);
    }

    fn list_choice(op: &str, operands: &[OperandType]) -> Option<OperatorChoice> {
        choose_list_operator(op, operands).unwrap()
    }

    #[test]
    fn an_in_list_gives_a_literal_the_common_type_of_its_values() {
        let literal = || OperandType::StringLiteral("1.5".to_string());
        let integer = || engine(EngineType::Integer);
        let decimal = || engine(EngineType::Decimal);
        let Some(OperatorChoice::Chosen(chosen)) =
            list_choice("=", &[integer(), literal(), decimal()])
        else {
            panic!("a row takes numeric and numeric");
        };
        assert_eq!(chosen.id, 1752);
        assert_eq!(chosen.operands.len(), 3);
        assert!(chosen.operands.iter().all(|ty| ty.name == "NUMERIC"));

        let two = || OperandType::StringLiteral("2".to_string());
        let Some(OperatorChoice::Chosen(chosen)) =
            list_choice("<>", &[integer(), two(), integer()])
        else {
            panic!("a row takes int4 and int4");
        };
        assert_eq!(chosen.id, 518);
    }

    #[test]
    fn an_in_list_compares_each_value_when_there_is_no_common_type_or_one_value() {
        let literal = || OperandType::StringLiteral("a".to_string());
        let integer = || engine(EngineType::Integer);
        let text = || engine(EngineType::Text);
        assert!(list_choice("=", &[integer(), literal()]).is_none());
        assert!(list_choice("=", &[text(), integer(), integer()]).is_none());
        let boolean_and_numeric = [
            registered("boolean"),
            literal(),
            engine(EngineType::Decimal),
        ];
        assert!(list_choice("=", &boolean_and_numeric).is_none());
    }

    #[test]
    fn a_double_keeps_the_numbers_up_to_fifteen_digits_and_the_ones_it_reads_back() {
        for kept in [
            "0.1",
            "1.50",
            "-123456789012345",
            "9007199254740992",
            "1e5",
            "100000000000000000000",
        ] {
            assert!(a_double_keeps(kept), "{kept}");
        }
        for lost in [
            "9007199254740993",
            "3.14159265358979323846",
            "1.5000000000000000001",
            "1.2345678901234567e5",
        ] {
            assert!(!a_double_keeps(lost), "{lost}");
        }
    }

    #[test]
    fn the_common_type_follows_the_implicit_casts() {
        assert_eq!(
            common_type(&[Some(INT4), None, Some(NUMERIC)]),
            Some(NUMERIC)
        );
        assert_eq!(common_type(&[Some(NUMERIC), Some(INT4)]), Some(NUMERIC));
        assert_eq!(
            common_type(&[Some(INT2), Some(INT8), Some(INT4)]),
            Some(INT8)
        );
        assert_eq!(
            common_type(&[Some(INT4), Some(FLOAT8), Some(NUMERIC)]),
            Some(FLOAT8)
        );
        assert_eq!(
            common_type(&[Some(DATE), Some(TIMESTAMPTZ)]),
            Some(TIMESTAMPTZ)
        );
        assert_eq!(common_type(&[Some(INT4), Some(TEXT)]), None);
        assert_eq!(common_type(&[None, None]), Some(TEXT));
    }

    #[test]
    fn core_decides_the_arithmetic_of_a_registered_date_or_numeric() {
        let integer = || engine(EngineType::Integer);
        for op in ["+", "*", "&"] {
            for operands in [
                [registered("date"), integer()],
                [integer(), registered("numeric")],
                [registered("date"), engine(EngineType::Decimal)],
            ] {
                assert_eq!(chosen_id(op, &operands), None, "{op}");
            }
        }
    }

    #[test]
    fn the_rows_decide_what_core_does_not() {
        let integer = || engine(EngineType::Integer);
        let refused = |op: &str, operands: &[OperandType]| {
            let error = choose_operator(op, operands).expect_err("a refusal");
            error.to_string().contains("operator does not exist")
        };
        assert!(refused("<", &[registered("date"), integer()]));
        assert!(refused("=", &[registered("numeric"), registered("uuid")]));
        assert!(refused("+", &[integer(), registered("uuid")]));
        assert!(refused(
            "*",
            &[engine(EngineType::Decimal), registered("uuid")]
        ));
    }

    #[test]
    fn every_commutator_and_negator_is_a_row() {
        for row in OPERATORS {
            for oid in [row.commutator, row.negator] {
                assert!(
                    oid == NONE || OPERATORS.iter().any(|other| other.oid == oid),
                    "operator {} names the operator {oid}",
                    row.oid
                );
            }
        }
    }

    #[test]
    fn checked_types_are_the_types_of_the_rows() {
        let in_rows = |oid: i64| {
            OPERATORS
                .iter()
                .any(|row| row.left == oid || row.right == oid)
        };
        for row in OPERATORS {
            for oid in [row.left, row.right] {
                assert!(
                    oid == NONE || CHECKED_TYPES.contains(&oid),
                    "row {} takes a type that CHECKED_TYPES does not name",
                    row.oid
                );
            }
        }
        for oid in CHECKED_TYPES {
            assert!(
                oid == VARCHAR || in_rows(oid),
                "no row takes type {oid}, which CHECKED_TYPES names"
            );
        }
    }
}
