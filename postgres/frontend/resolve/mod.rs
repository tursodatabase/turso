//! Chooses the function or the operator of a call from the types of its
//! arguments, with the rules of PostgreSQL 18.

mod candidates;
mod catalog;
mod coerce;
mod error;
mod function;
#[cfg(test)]
mod golden;
mod operator;
#[cfg(test)]
mod oracle;

pub(crate) use catalog::{
    CastMethod, CastRow, Catalog, OperatorRow, ProcRow, RangeRow, TypeKind, TypeRow,
};
pub(crate) use error::ResolveError;

/// The type of one argument of a call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArgType {
    Known(i64),
    /// An untyped literal or parameter.
    Unknown,
}

/// What PostgreSQL does to an argument to make it the type of its parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArgCoercion {
    None,
    Relabel,
    /// Calls the cast function with this oid.
    Function(i64),
    ViaInputOutput,
    /// Reads the untyped literal or parameter as the type of the parameter.
    Literal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedCall {
    /// The oid of the chosen `pg_proc` row.
    pub oid: i64,
    pub parameter_types: Vec<i64>,
    pub coercions: Vec<ArgCoercion>,
    pub result_type: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedOperator {
    /// The oid of the chosen `pg_operator` row.
    pub oid: i64,
    /// The oid of the `pg_proc` row that runs the operator.
    pub function: i64,
    /// One type for the operand of a prefix operator, two for a binary operator.
    pub operand_types: Vec<i64>,
    pub coercions: Vec<ArgCoercion>,
    pub result_type: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Resolution<Chosen = ResolvedCall> {
    Resolved(Chosen),
    Unsupported(Reason),
    Error(ResolveError),
}

/// Why a call cannot be resolved here and is left to the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reason {
    Variadic,
    Defaults,
    Domain,
    Composite,
    Array,
    /// A range or multirange type without a `pg_range` row in the catalog.
    Range,
    /// The call is a cast written as a function call, such as `text(x)`.
    Cast,
    Aggregate,
    Window,
    Procedure,
    /// A parameter or the result of the chosen function is of type `internal`.
    Internal,
    TooManyArguments,
    /// The catalog has no `pg_type` row for a type of the call.
    MissingType,
}

/// Chooses the function that PostgreSQL runs for `name(arguments)`.
pub(crate) fn resolve_function(
    catalog: &dyn Catalog,
    name: &str,
    arguments: &[ArgType],
) -> Resolution {
    match function::parse_func_or_column(catalog, name, arguments) {
        Ok(Ok(call)) => Resolution::Resolved(call),
        Ok(Err(error)) => Resolution::Error(error),
        Err(reason) => Resolution::Unsupported(reason),
    }
}

/// Chooses the operator that PostgreSQL runs for `left name right`, or for `name right` without a left operand.
pub(crate) fn resolve_operator(
    catalog: &dyn Catalog,
    name: &str,
    left: Option<ArgType>,
    right: ArgType,
) -> Resolution<ResolvedOperator> {
    match operator::make_op(catalog, name, left, right) {
        Ok(Ok(operator)) => Resolution::Resolved(operator),
        Ok(Err(error)) => Resolution::Error(error),
        Err(reason) => Resolution::Unsupported(reason),
    }
}
