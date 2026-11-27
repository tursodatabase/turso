//! PostgreSQL's choice of the function that runs a call, from the types of its
//! arguments.

use crate::catalog::pg_type_oid;
use crate::procs::{PROCS, SQL_LANGUAGE};
use crate::resolve::{resolve_function, ArgCoercion, ArgType, Resolution, ResolveError};
use crate::resolve_catalog::FrontendCatalog;
use turso_core::ast;
use turso_core::dialect::{ChosenFunction, EngineType, FunctionChoice, OperandType};
use turso_core::{DialectError, LimboError, Result};
use turso_pg_parser::sqlstate::SQLSTATE_INDETERMINATE_DATATYPE;
use turso_pg_parser::translator::cast_type_name;

/// Chooses the `pg_proc` row that runs `name(arguments)`. A name with no row
/// is left to the engine, and so is a row written in SQL or one whose function
/// core runs under the SQL name.
pub(crate) fn choose_function(name: &str, arguments: &[OperandType]) -> Result<FunctionChoice> {
    if !FrontendCatalog::has_function(name) {
        return Ok(FunctionChoice::LeftToTheEngine);
    }
    resolve(name, arguments)
}

fn resolve(name: &str, arguments: &[OperandType]) -> Result<FunctionChoice> {
    let argument_types = arguments
        .iter()
        .enumerate()
        .map(|(index, argument)| argument_type(name, index, argument))
        .collect::<Result<Vec<_>>>()?;
    let Some(argument_types) = argument_types.into_iter().collect::<Option<Vec<_>>>() else {
        return Ok(FunctionChoice::LeftToTheEngine);
    };
    let call = match resolve_function(&FrontendCatalog, name, &argument_types) {
        Resolution::Resolved(call) => call,
        Resolution::Unsupported(_) => return Ok(FunctionChoice::LeftToTheEngine),
        Resolution::Error(error) => return Err(resolve_error(error)),
    };
    let proc = PROCS
        .iter()
        .find(|proc| proc.oid == call.oid)
        .expect("the resolver chooses a row of PROCS");
    if proc.language == SQL_LANGUAGE || LEFT_TO_THE_ENGINE.contains(&proc.source) {
        return Ok(FunctionChoice::LeftToTheEngine);
    }
    let argument_types = call
        .coercions
        .iter()
        .zip(&call.parameter_types)
        .map(|(coercion, &parameter)| {
            let ty = cast_type(parameter)?;
            match coercion {
                ArgCoercion::None => None,
                ArgCoercion::Literal
                    if READ_AS_TEXT_BY_THE_FUNCTION.contains(&ty.name.as_str()) =>
                {
                    None
                }
                _ => Some(ty),
            }
        })
        .collect();
    Ok(FunctionChoice::Chosen(ChosenFunction {
        implementation: proc.source.to_string(),
        argument_types,
        result_type: cast_type(call.result_type),
    }))
}

/// FIXME(pg-catalog): A CAST to these SQLite names does not run the input function of the
/// PostgreSQL type they stand for, so the function reads the literal's text itself, as before.
const READ_AS_TEXT_BY_THE_FUNCTION: &[&str] = &["INTEGER", "REAL", "BLOB"];

/// The type core gives a value of the type `oid`, `None` for a pseudo-type.
pub(crate) fn cast_type(oid: i64) -> Option<ast::Type> {
    let (name, is_array) = FrontendCatalog::type_name(oid)?;
    Some(ast::Type {
        name: cast_type_name(name, is_array),
        size: None,
        array_dimensions: u32::from(is_array),
    })
}

// FIXME(registered-types): A column of a table that is not STRICT and a catalog column whose
// declared type is not PostgreSQL's have no type yet, so a function called on one fails with 42P18. An engine type stands for several PostgreSQL types: the
// function gets the one in `engine_type_oid`. Typing those columns and registering the types
// removes both.
/// The type the resolver is given for the argument at `index`, or `None` for an
/// argument of a type PostgreSQL has no row for here: a custom type such as an
/// enum or a domain, which the engine runs as before.
fn argument_type(name: &str, index: usize, argument: &OperandType) -> Result<Option<ArgType>> {
    let (oid, array_dimensions) = match argument {
        OperandType::StringLiteral(_) | OperandType::Untyped => return Ok(Some(ArgType::Unknown)),
        OperandType::Registered(ty) => match pg_type_oid(&ty.name) {
            Some(oid) => (oid, ty.array_dimensions),
            None => return Ok(None),
        },
        OperandType::Engine {
            ty,
            array_dimensions,
        } => (engine_type_oid(*ty), *array_dimensions),
        OperandType::Unnamed => {
            return Err(LimboError::Dialect(Box::new(DialectError {
                code: SQLSTATE_INDETERMINATE_DATATYPE,
                message: format!(
                    "could not determine data type of argument {} of {name}()",
                    index + 1
                ),
                detail: None,
                hint: None,
            })))
        }
    };
    if array_dimensions == 0 {
        return Ok(Some(ArgType::Known(oid)));
    }
    Ok(FrontendCatalog::array_type(oid).map(ArgType::Known))
}

/// The PostgreSQL type that an engine type is read as: the type it most often
/// stands for.
pub(crate) fn engine_type_oid(ty: EngineType) -> i64 {
    match ty {
        EngineType::Integer => 23,
        EngineType::Real => 701,
        EngineType::Decimal | EngineType::Number => 1700,
        EngineType::Text => 25,
        EngineType::Blob => 17,
    }
}

pub(crate) fn resolve_error(error: ResolveError) -> LimboError {
    LimboError::Dialect(Box::new(DialectError {
        code: error.sqlstate(),
        message: error.message().to_string(),
        detail: error.detail().map(str::to_string),
        hint: error.hint().map(str::to_string),
    }))
}

/// FIXME(pg-catalog): The frontend registers no function under the `prosrc` of
/// these rows, so the engine runs the call by its SQL name, as before.
const LEFT_TO_THE_ENGINE: &[&str] = &[
    "btrim1",
    "byteapos",
    "dceil",
    "dexp",
    "dfloor",
    "dpi",
    "dround",
    "dsign",
    "dtrunc",
    "extract_date",
    "extract_time",
    "extract_timestamp",
    "extract_timestamptz",
    "float4abs",
    "float8abs",
    "int2abs",
    "int4abs",
    "int4random",
    "int8abs",
    "int8random",
    "ltrim1",
    "macaddr_trunc",
    "numeric_abs",
    "numeric_ceil",
    "numeric_exp",
    "numeric_floor",
    "numeric_random",
    "numeric_round",
    "numeric_sign",
    "numeric_trunc",
    "PG_encoding_to_char",
    "pg_get_indexdef_ext",
    "replace_text",
    "rtrim1",
    "text_reverse",
    "textlen",
    "textoctetlen",
    "textpos",
    "textregexsubstr",
    "time_part",
    "timestamp_part",
    "timestamptz_part",
];

#[cfg(test)]
mod tests {
    use super::*;

    fn test_connection() -> crate::Connection {
        let (_io, db) = crate::open_database(
            ":memory:",
            None,
            turso_core::OpenFlags::default(),
            turso_core::DatabaseOpts::new(),
        )
        .unwrap();
        crate::Connection::connect(&db).unwrap()
    }

    #[test]
    fn every_function_row_names_a_registered_function_or_is_left_to_the_engine() {
        let conn = test_connection();
        let registered = conn.inner().get_syms_functions();
        let is_registered = |name: &str, argument_count: usize| {
            registered.iter().any(|(registered, _, count, _)| {
                registered == name && (*count < 0 || *count as usize == argument_count)
            }) || turso_core::dialect::resolve_builtin_function(name, argument_count)
                .is_ok_and(|func| func.is_some())
        };
        let mut missing: Vec<&str> = PROCS
            .iter()
            .filter(|proc| proc.kind == "f" && proc.language != SQL_LANGUAGE)
            .filter(|proc| {
                let runs_somewhere = LEFT_TO_THE_ENGINE.contains(&proc.source)
                    || is_registered(proc.source, proc.arg_types.len());
                !runs_somewhere
            })
            .map(|proc| proc.source)
            .collect();
        missing.dedup();
        assert!(missing.is_empty(), "no function runs {missing:?}");
        for source in LEFT_TO_THE_ENGINE {
            let proc = PROCS.iter().find(|proc| proc.source == *source).unwrap();
            assert!(
                !is_registered(source, proc.arg_types.len()),
                "{source} is registered"
            );
        }
    }

    #[test]
    fn a_call_on_a_value_with_no_type_fails_with_42p18() {
        let conn = test_connection();
        let error = conn
            .execute("SELECT length(typinput) FROM pg_type")
            .unwrap_err();
        assert_eq!(error.sqlstate, "42P18");
        assert_eq!(
            error.message,
            "could not determine data type of argument 1 of length()"
        );
    }
}
