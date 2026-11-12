use crate::sqlstate::{
    SQLSTATE_AMBIGUOUS_FUNCTION, SQLSTATE_CANNOT_COERCE, SQLSTATE_DATATYPE_MISMATCH,
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INVALID_TEXT_REPRESENTATION,
    SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE, SQLSTATE_UNDEFINED_FUNCTION,
};
use crate::translator::function_call;
use crate::ParseError;
use std::fmt;
use turso_parser::ast;

/// A PostgreSQL type that the translator knows how to convert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgType {
    Bool,
    /// The two-byte integer, smallint.
    Int2,
    Int4,
    Int8,
    Float8,
    Numeric,
    Text,
    Bytea,
    /// An object id, kept as an integer from 0 to 4294967295.
    Oid,
    /// An oid that names a relation.
    Regclass,
    /// An oid that names a schema.
    Regnamespace,
    /// A reference to a function: the value passes as it is, whether it is an oid or a name.
    Regproc,
    /// The one-byte internal type that PostgreSQL writes as "char".
    Char,
    /// A string literal, NULL or parameter: its type comes from where it is used.
    Unknown,
    /// A type that has a name but no supported casts yet (date, uuid, an enum, ...). Lowercase name.
    Other(String),
}

/// The kind of an operator, which decides the type its operands are converted to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorKind {
    Arithmetic,
    Comparison,
    Concat,
    Pattern,
}

/// Where a conversion happens, from the weakest to the strongest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CastContext {
    Implicit,
    Assignment,
    Explicit,
}

impl PgType {
    /// Finds the type with this name, ignoring case; every name of a type gives the same value.
    pub fn from_name(name: &str) -> PgType {
        let spelled = one_spelling(name);
        match spelled.as_str() {
            "int2" => PgType::Int2,
            "int4" => PgType::Int4,
            "int8" => PgType::Int8,
            "float8" => PgType::Float8,
            "numeric" => PgType::Numeric,
            "text" | "name" => PgType::Text,
            "bool" => PgType::Bool,
            "bytea" => PgType::Bytea,
            "oid" => PgType::Oid,
            "regclass" => PgType::Regclass,
            "regnamespace" => PgType::Regnamespace,
            "regproc" => PgType::Regproc,
            "char" => PgType::Char,
            _ => PgType::Other(spelled),
        }
    }

    /// The type of a column that the engine's catalog stores under this type name and type parameters.
    pub fn from_column_type(declared: &str, array_dimensions: u32, parameters: &[i64]) -> PgType {
        let lowercase = declared.to_lowercase();
        if array_dimensions > 0 {
            let brackets = "[]".repeat(array_dimensions as usize);
            return PgType::from_name(&format!("{lowercase}{brackets}"));
        }
        match (lowercase.as_str(), parameters) {
            ("integer" | "int", _) => PgType::Int4,
            ("bigint", _) => PgType::Int8,
            ("real", _) => PgType::Float8,
            ("text", _) => PgType::Text,
            ("blob" | "bytea", _) => PgType::Bytea,
            ("boolean", _) => PgType::Bool,
            ("numeric", [precision, scale]) => {
                PgType::from_name(&format!("numeric({precision},{scale})"))
            }
            ("numeric", [precision]) => PgType::from_name(&format!("numeric({precision},0)")),
            ("numeric", _) => PgType::from_name("numeric with a precision"),
            _ => PgType::from_name(&lowercase),
        }
    }

    /// The type the engine uses to store values of this type, if there is one.
    pub fn engine_type(&self) -> Option<ast::Type> {
        let name = match self {
            PgType::Int2 => "smallint",
            PgType::Int4 | PgType::Oid | PgType::Regclass | PgType::Regnamespace => "INTEGER",
            PgType::Int8 => "BIGINT",
            PgType::Float8 | PgType::Numeric => "REAL",
            PgType::Text | PgType::Char => "TEXT",
            PgType::Bytea => "BLOB",
            PgType::Bool => "boolean",
            PgType::Regproc | PgType::Unknown | PgType::Other(_) => return None,
        };
        Some(ast::Type {
            name: name.to_string(),
            size: None,
            array_dimensions: 0,
        })
    }

    /// The letter `pg_type.typcategory` has for this type, if this module knows the type.
    pub fn category_letter(&self) -> Option<&'static str> {
        match self.category() {
            TypeCategory::Boolean => Some("B"),
            TypeCategory::Number => Some("N"),
            TypeCategory::String => Some("S"),
            TypeCategory::UserDefined => Some("U"),
            TypeCategory::Other => None,
        }
    }

    fn category(&self) -> TypeCategory {
        match self {
            PgType::Bool => TypeCategory::Boolean,
            PgType::Int2
            | PgType::Int4
            | PgType::Int8
            | PgType::Float8
            | PgType::Numeric
            | PgType::Oid
            | PgType::Regclass
            | PgType::Regnamespace
            | PgType::Regproc => TypeCategory::Number,
            PgType::Text | PgType::Char => TypeCategory::String,
            PgType::Bytea => TypeCategory::UserDefined,
            PgType::Unknown | PgType::Other(_) => TypeCategory::Other,
        }
    }

    fn is_oid_alias(&self) -> bool {
        matches!(
            self,
            PgType::Oid | PgType::Regclass | PgType::Regnamespace | PgType::Regproc
        )
    }

    fn is_preferred(&self) -> bool {
        matches!(
            self,
            PgType::Bool | PgType::Float8 | PgType::Oid | PgType::Text
        )
    }

    fn unknown_reader(&self) -> Option<&'static str> {
        match self {
            PgType::Regclass => Some(UNKNOWN_TO_REGCLASS),
            PgType::Regnamespace => Some(UNKNOWN_TO_REGNAMESPACE),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypeCategory {
    Boolean,
    Number,
    String,
    UserDefined,
    Other,
}

const UNKNOWN_TO_REGCLASS: &str = "unknown_to_regclass";
const UNKNOWN_TO_REGNAMESPACE: &str = "unknown_to_regnamespace";
const TEXT_TO_REGCLASS: &str = "text_to_regclass";
const NUMBER_TO_INT2: &str = "number_to_int2";

impl fmt::Display for PgType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            PgType::Bool => "boolean",
            PgType::Int2 => "smallint",
            PgType::Int4 => "integer",
            PgType::Int8 => "bigint",
            PgType::Float8 => "double precision",
            PgType::Numeric => "numeric",
            PgType::Text => "text",
            PgType::Bytea => "bytea",
            PgType::Oid => "oid",
            PgType::Regclass => "regclass",
            PgType::Regnamespace => "regnamespace",
            PgType::Regproc => "regproc",
            PgType::Char => "\"char\"",
            PgType::Unknown => "unknown",
            PgType::Other(name) => name,
        };
        f.write_str(name)
    }
}

/// Builds the expression that converts `expr` from `source` (None: a name that does not resolve, which the engine reports) to `target`, or the error PostgreSQL gives.
pub fn coerce(
    expr: ast::Expr,
    source: Option<&PgType>,
    target: &PgType,
    context: CastContext,
) -> Result<ast::Expr, ParseError> {
    let Some(source) = source else {
        return coerce_from_unknown_source(expr, target, context);
    };
    match cast_decision(source, target, context)? {
        CastDecision::Keep => Ok(wrap_if_explicit(expr, target, context)),
        CastDecision::FromUnknown => coerce_from_unknown(expr, target, context),
        CastDecision::Convert(method) => {
            let converted = if *target == PgType::Int8 && is_numeric_literal_outside_int8(&expr) {
                function_call("int8_out_of_range", vec![])
            } else {
                match method {
                    CastMethod::Nothing => expr,
                    CastMethod::EngineCast => engine_cast(expr, target),
                    CastMethod::Function(name) => function_call(name, vec![expr]),
                    CastMethod::RoundThenCast => {
                        engine_cast(function_call("round", vec![expr]), target)
                    }
                }
            };
            Ok(wrap_if_explicit(converted, target, context))
        }
    }
}

/// Decides what converting a value from `source` to `target` in `context` takes, or gives the error PostgreSQL gives.
pub fn cast_decision(
    source: &PgType,
    target: &PgType,
    context: CastContext,
) -> Result<CastDecision, ParseError> {
    if *target == PgType::Unknown || source == target {
        return Ok(CastDecision::Keep);
    }
    if *source == PgType::Unknown {
        return Ok(CastDecision::FromUnknown);
    }
    match cast_rule(source, target) {
        CastRule::NoCast if context < CastContext::Explicit => {
            Err(cannot_be_used_without_a_cast(source, target))
        }
        CastRule::NoCast => Err(error(
            SQLSTATE_CANNOT_COERCE,
            format!("cannot cast type {source} to {target}"),
        )),
        CastRule::Unsupported(weakest) if context < weakest => {
            Err(cannot_be_used_without_a_cast(source, target))
        }
        CastRule::Unsupported(_) => Err(cast_not_supported(source, target)),
        CastRule::Allowed(weakest, _) if context < weakest => {
            Err(cannot_be_used_without_a_cast(source, target))
        }
        CastRule::Allowed(_, method) => Ok(CastDecision::Convert(method)),
    }
}

/// Like `cast_decision` for a value stored in the column `column`, with the error PostgreSQL gives for that.
pub fn assignment_decision(
    source: &PgType,
    column: &str,
    target: &PgType,
) -> Result<CastDecision, ParseError> {
    cast_decision(source, target, CastContext::Assignment)
        .map_err(|failure| column_mismatch(failure, column, source, target))
}

/// Converts `expr` to the type of the column `column` it is stored in, or gives the error PostgreSQL gives.
pub fn coerce_assignment(
    expr: ast::Expr,
    source: Option<&PgType>,
    column: &str,
    target: Option<&PgType>,
) -> Result<ast::Expr, ParseError> {
    let (Some(source), Some(target)) = (source, target) else {
        return Ok(expr);
    };
    coerce(expr, Some(source), target, CastContext::Assignment)
        .map_err(|failure| column_mismatch(failure, column, source, target))
}

/// Converts the values a construct returns one of (the results of CASE, the arguments of COALESCE, ...) to their common type.
pub fn coerce_to_common(
    construct: &str,
    values: Vec<(ast::Expr, Option<PgType>)>,
) -> Result<Vec<ast::Expr>, ParseError> {
    let Some(types) = values
        .iter()
        .map(|(_, ty)| ty.clone())
        .collect::<Option<Vec<PgType>>>()
    else {
        return Ok(without_types(values));
    };
    let target = common_type(&types, construct)?;
    values
        .into_iter()
        .zip(&types)
        .map(|((expr, _), ty)| coerce(expr, Some(ty), &target, CastContext::Implicit))
        .collect()
}

fn coerce_from_unknown_source(
    expr: ast::Expr,
    target: &PgType,
    context: CastContext,
) -> Result<ast::Expr, ParseError> {
    match target {
        PgType::Other(_) => Err(cast_not_supported(&PgType::Unknown, target)),
        PgType::Unknown => Ok(expr),
        _ => Ok(wrap_if_explicit(expr, target, context)),
    }
}

fn coerce_from_unknown(
    expr: ast::Expr,
    target: &PgType,
    context: CastContext,
) -> Result<ast::Expr, ParseError> {
    let converted = match expr {
        ast::Expr::Literal(ast::Literal::String(quoted)) => {
            if matches!(target, PgType::Other(_)) {
                return Err(cast_not_supported(&PgType::Unknown, target));
            }
            convert_literal(&quoted, target)?
        }
        _ if context == CastContext::Explicit && matches!(target, PgType::Other(_)) => {
            return Err(cast_not_supported(&PgType::Unknown, target))
        }
        other => match target.unknown_reader() {
            Some(reader) if other != ast::Expr::Literal(ast::Literal::Null) => {
                function_call(reader, vec![other])
            }
            _ => other,
        },
    };
    Ok(wrap_if_explicit(converted, target, context))
}

/// Converts the operands of `operator` to the type the operator runs on, or gives the error PostgreSQL gives.
pub fn coerce_operands(
    operator: &str,
    kind: OperatorKind,
    operands: Vec<(ast::Expr, Option<PgType>)>,
) -> Result<Vec<ast::Expr>, ParseError> {
    let Some(types) = operands
        .iter()
        .map(|(_, ty)| ty.clone())
        .collect::<Option<Vec<PgType>>>()
    else {
        return Ok(without_types(operands));
    };
    let Some(target) = operand_target(operator, kind, &types)? else {
        return Ok(without_types(operands));
    };
    let context = match kind {
        OperatorKind::Concat => CastContext::Assignment,
        _ => CastContext::Implicit,
    };
    operands
        .into_iter()
        .zip(&types)
        .map(|((expr, _), ty)| coerce(expr, Some(ty), &target, context))
        .collect()
}

/// Makes `expr` a boolean for `construct` (WHERE, HAVING, ...), or gives the error PostgreSQL gives.
pub fn coerce_to_boolean(
    expr: ast::Expr,
    source: Option<&PgType>,
    construct: &str,
) -> Result<ast::Expr, ParseError> {
    match source {
        None | Some(PgType::Bool) => Ok(expr),
        Some(PgType::Unknown) => coerce(
            expr,
            Some(&PgType::Unknown),
            &PgType::Bool,
            CastContext::Implicit,
        ),
        Some(other @ PgType::Other(_)) => Err(cast_not_supported(other, &PgType::Bool)),
        Some(other) => Err(error(
            SQLSTATE_DATATYPE_MISMATCH,
            format!("argument of {construct} must be type boolean, not type {other}"),
        )),
    }
}

/// Checks that a unary `+` or `-` can be applied to a value of type `operand`, or gives the error PostgreSQL gives.
pub fn check_unary_operand(operator: &str, operand: Option<&PgType>) -> Result<(), ParseError> {
    match operand {
        None => Ok(()),
        Some(
            ty @ (PgType::Text | PgType::Bool | PgType::Bytea | PgType::Oid | PgType::Regclass),
        ) => Err(error(
            SQLSTATE_UNDEFINED_FUNCTION,
            format!("operator does not exist: {operator} {ty}"),
        )),
        Some(_) => Ok(()),
    }
}

/// Finds the one type that all of `types` can be converted to; `construct` names the SQL construct in error messages.
// FIXME: the engine picks a common type again for the translated expression, in
// core/translate/expr/expr_type.rs, and the two can disagree. Make them one.
pub fn common_type(types: &[PgType], construct: &str) -> Result<PgType, ParseError> {
    let known: Vec<&PgType> = types.iter().filter(|t| **t != PgType::Unknown).collect();
    let Some(first) = known.first() else {
        return Ok(PgType::Text);
    };
    let mut chosen: &PgType = first;
    for next in &known {
        if *next == chosen {
            continue;
        }
        if matches!(chosen, PgType::Other(_)) || matches!(next, PgType::Other(_)) {
            return Err(error(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                format!("{construct} with types {chosen} and {next} is not supported"),
            ));
        }
        if chosen.category() != next.category() {
            return Err(error(
                SQLSTATE_DATATYPE_MISMATCH,
                format!("{construct} types {chosen} and {next} cannot be matched"),
            ));
        }
        if !chosen.is_preferred()
            && converts_implicitly(chosen, next)
            && !converts_implicitly(next, chosen)
        {
            chosen = next;
        }
    }
    if let Some(stuck) = known.iter().find(|t| !converts_implicitly(t, chosen)) {
        return Err(error(
            SQLSTATE_CANNOT_COERCE,
            format!("{construct} could not convert type {stuck} to {chosen}"),
        ));
    }
    Ok(chosen.clone())
}

fn converts_implicitly(source: &PgType, target: &PgType) -> bool {
    source == target || cast_decision(source, target, CastContext::Implicit).is_ok()
}

/// Builds the engine's CAST of `expr` to `type_name`, for the stored SQL that the engine runs as it is.
pub fn engine_cast_as(expr: ast::Expr, type_name: ast::Type) -> ast::Expr {
    ast::Expr::Cast {
        expr: Box::new(expr),
        type_name: Some(type_name),
    }
}

/// A boolean literal.
pub fn bool_literal(value: bool) -> ast::Expr {
    ast::Expr::Cast {
        expr: Box::new(ast::Expr::Literal(ast::Literal::Numeric(
            if value { "1" } else { "0" }.to_string(),
        ))),
        type_name: PgType::Bool.engine_type(),
    }
}

/// What converting a value from one type to another takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastDecision {
    /// The value is already of the target type, or the target is unknown.
    Keep,
    /// The value is a string literal, NULL or parameter, which reads as the target type.
    FromUnknown,
    Convert(CastMethod),
}

/// How the engine expression changes a value that converts to another type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastMethod {
    Nothing,
    EngineCast,
    RoundThenCast,
    Function(&'static str),
}

enum CastRule {
    NoCast,
    Unsupported(CastContext),
    Allowed(CastContext, CastMethod),
}

fn cast_rule(source: &PgType, target: &PgType) -> CastRule {
    use CastContext::{Assignment, Explicit, Implicit};
    use CastMethod::{EngineCast, Function, Nothing, RoundThenCast};
    use CastRule::{Allowed, NoCast, Unsupported};
    use PgType::{
        Bool, Bytea, Char, Float8, Int2, Int4, Int8, Numeric, Oid, Other, Regclass, Regnamespace,
        Regproc, Text,
    };

    match (source, target) {
        (Int2, Int4 | Int8) => Allowed(Implicit, Nothing),
        (Int2, Float8 | Numeric) => Allowed(Implicit, EngineCast),
        (Int2, Text) => Allowed(Assignment, EngineCast),
        (Int4 | Int8 | Float8 | Numeric, Int2) => Allowed(Assignment, Function(NUMBER_TO_INT2)),
        (Int2, alias) if alias.is_oid_alias() => Allowed(Implicit, Nothing),
        (alias, Int2) if alias.is_oid_alias() => NoCast,
        (Bool | Bytea, Int2) | (Int2, Bool | Bytea) => NoCast,
        (Int4, Int8) => Allowed(Implicit, Nothing),
        (Int4, Float8) => Allowed(Implicit, EngineCast),
        (Int4, Numeric) => Allowed(Implicit, EngineCast),
        (Int4, Text) => Allowed(Assignment, EngineCast),
        (Int4, Bool) => Unsupported(Explicit),
        (Int8, Int4) => Allowed(Assignment, Nothing),
        (Int8, Float8) => Allowed(Implicit, EngineCast),
        (Int8, Numeric) => Allowed(Implicit, EngineCast),
        (Int8, Text) => Allowed(Assignment, EngineCast),
        (Float8, Int4) => Allowed(Assignment, RoundThenCast),
        (Float8, Int8) => Allowed(Assignment, RoundThenCast),
        (Float8, Numeric) => Allowed(Assignment, Nothing),
        (Float8, Text) => Unsupported(Assignment),
        (Numeric, Int4) => Allowed(Assignment, RoundThenCast),
        (Numeric, Int8) => Allowed(Assignment, RoundThenCast),
        (Numeric, Float8) => Allowed(Implicit, Nothing),
        (Numeric, Text) => Unsupported(Assignment),
        (Bool, Int4) => Allowed(Explicit, EngineCast),
        (Bool, Text) => Unsupported(Assignment),
        (Bytea, Text) => Unsupported(Assignment),
        (Int4 | Int8, alias) if alias.is_oid_alias() => Allowed(Implicit, Nothing),
        (alias, Int4 | Int8) if alias.is_oid_alias() => Allowed(Assignment, Nothing),
        (Oid, alias) | (alias, Oid) if alias.is_oid_alias() => Allowed(Implicit, Nothing),
        (Oid, Text) => Allowed(Assignment, EngineCast),
        (Regclass | Regnamespace | Regproc, Text) => Unsupported(Assignment),
        (Text, Regclass) => Allowed(Implicit, Function(TEXT_TO_REGCLASS)),
        (Text, Regproc) => Allowed(Explicit, Nothing),
        (Char, Text) => Allowed(Implicit, Nothing),
        (alias, Bool | Float8 | Numeric | Bytea) | (Bool | Float8 | Numeric | Bytea, alias)
            if alias.is_oid_alias() =>
        {
            NoCast
        }
        (first, second) if first.is_oid_alias() && second.is_oid_alias() => NoCast,
        (Other(_), _) | (_, Other(_)) => Unsupported(Implicit),
        (Text, _) => Unsupported(Explicit),
        (Bool, Int8 | Float8 | Numeric) | (Int8 | Float8 | Numeric, Bool) => NoCast,
        (Bytea, Bool | Float8 | Numeric) | (Bool | Float8 | Numeric, Bytea) => NoCast,
        _ => Unsupported(Explicit),
    }
}

fn without_types(operands: Vec<(ast::Expr, Option<PgType>)>) -> Vec<ast::Expr> {
    operands.into_iter().map(|(expr, _)| expr).collect()
}

fn operand_target(
    operator: &str,
    kind: OperatorKind,
    types: &[PgType],
) -> Result<Option<PgType>, ParseError> {
    let known: Vec<&PgType> = types.iter().filter(|t| **t != PgType::Unknown).collect();
    let Some(first) = known.first() else {
        if kind == OperatorKind::Arithmetic {
            return Err(error(
                SQLSTATE_AMBIGUOUS_FUNCTION,
                format!("operator is not unique: unknown {operator} unknown"),
            ));
        }
        return Ok(None);
    };
    let different = known.iter().find(|t| *t != first);
    if known.iter().any(|t| matches!(t, PgType::Other(_))) {
        return match different {
            None => Ok(Some((*first).clone())),
            Some(different) => Err(error(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                format!(
                    "the operator {operator} for types {first} and {different} is not supported"
                ),
            )),
        };
    }
    let target = match kind {
        OperatorKind::Arithmetic => operand_type_among(&known, ARITHMETIC_OPERAND_TYPES),
        OperatorKind::Comparison => match different {
            None if COMPARISON_OPERAND_TYPES.contains(first) => Some((*first).clone()),
            _ => operand_type_among(&known, COMPARISON_OPERAND_TYPES),
        },
        OperatorKind::Concat if known.iter().all(|t| **t == PgType::Bytea) => Some(PgType::Bytea),
        OperatorKind::Concat => Some(PgType::Text),
        OperatorKind::Pattern => known
            .iter()
            .all(|t| converts_implicitly(t, &PgType::Text))
            .then_some(PgType::Text),
    };
    match target {
        Some(target) => Ok(Some(target)),
        None => Err(operator_does_not_exist(operator, types, different)),
    }
}

fn operator_does_not_exist(
    operator: &str,
    types: &[PgType],
    different: Option<&&PgType>,
) -> ParseError {
    let (left, right) = match (types.iter().find(|t| **t != PgType::Unknown), different) {
        (Some(first), Some(different)) => (first, *different),
        _ => (&types[0], types.get(1).unwrap_or(&types[0])),
    };
    error(
        SQLSTATE_UNDEFINED_FUNCTION,
        format!("operator does not exist: {left} {operator} {right}"),
    )
}

const ARITHMETIC_OPERAND_TYPES: &[PgType] = &[
    PgType::Int2,
    PgType::Int4,
    PgType::Int8,
    PgType::Numeric,
    PgType::Float8,
];

const COMPARISON_OPERAND_TYPES: &[PgType] = &[
    PgType::Int2,
    PgType::Int4,
    PgType::Int8,
    PgType::Numeric,
    PgType::Float8,
    PgType::Oid,
    PgType::Text,
    PgType::Bool,
    PgType::Bytea,
];

fn operand_type_among(known: &[&PgType], candidates: &[PgType]) -> Option<PgType> {
    let exact_matches = |candidate: &PgType| known.iter().filter(|t| ***t == *candidate).count();
    candidates
        .iter()
        .filter(|candidate| known.iter().all(|t| converts_implicitly(t, candidate)))
        .max_by_key(|candidate| (exact_matches(candidate), candidate.is_preferred()))
        .cloned()
}

fn convert_literal(quoted: &str, target: &PgType) -> Result<ast::Expr, ParseError> {
    let text = unquote(quoted);
    let trimmed = text.trim();
    let literal = match target {
        PgType::Text | PgType::Regproc => {
            return Ok(ast::Expr::Literal(ast::Literal::String(quoted.to_string())))
        }
        PgType::Int2 => ast::Literal::Numeric(parse_int2(&text)?.to_string()),
        PgType::Int4 => ast::Literal::Numeric(parse_int4(&text)?.to_string()),
        PgType::Int8 => ast::Literal::Numeric(parse_int8(&text)?.to_string()),
        PgType::Float8 => parse_real(&text, trimmed, "double precision")?,
        PgType::Numeric => parse_real(&text, trimmed, "numeric")?,
        PgType::Bool => return parse_bool(&text, trimmed).map(bool_literal),
        PgType::Bytea => parse_bytea(&text, trimmed)?,
        PgType::Oid => ast::Literal::Numeric(parse_oid(&text)?.to_string()),
        PgType::Char => parse_char(&text)?,
        PgType::Regclass | PgType::Regnamespace => {
            let reader = target.unknown_reader().expect("an oid alias has a reader");
            return Ok(function_call(
                reader,
                vec![ast::Expr::Literal(ast::Literal::String(quoted.to_string()))],
            ));
        }
        PgType::Unknown | PgType::Other(_) => {
            return Ok(ast::Expr::Literal(ast::Literal::String(quoted.to_string())))
        }
    };
    Ok(ast::Expr::Literal(literal))
}

fn unquote(quoted: &str) -> String {
    let inner = quoted
        .strip_prefix('\'')
        .and_then(|rest| rest.strip_suffix('\''))
        .unwrap_or(quoted);
    inner.replace("''", "'")
}

/// Reads `text` the way PostgreSQL reads a smallint.
pub fn parse_int2(text: &str) -> Result<i64, ParseError> {
    parse_integer(text, "smallint", 1 << 15)
}

/// Reads `text` the way PostgreSQL reads an integer: spaces around, an optional sign, then decimal digits or a 0x, 0o or 0b prefix, with single underscores between digits.
pub fn parse_int4(text: &str) -> Result<i64, ParseError> {
    parse_integer(text, "integer", 1 << 31)
}

/// Reads `text` the way PostgreSQL reads a bigint, with the rules of `parse_int4`.
pub fn parse_int8(text: &str) -> Result<i64, ParseError> {
    parse_integer(text, "bigint", 1 << 63)
}

fn parse_integer(text: &str, type_name: &str, negative_limit: u64) -> Result<i64, ParseError> {
    let bytes = text.as_bytes();
    let invalid = || invalid_input(type_name, text);
    let out_of_range = || {
        error(
            SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
            format!("value \"{text}\" is out of range for type {type_name}"),
        )
    };
    let is_space = |b: &u8| matches!(*b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r');
    let mut position = bytes.iter().take_while(|b| is_space(b)).count();
    let negative = bytes.get(position) == Some(&b'-');
    if matches!(bytes.get(position), Some(b'-' | b'+')) {
        position += 1;
    }
    let radix: u32 = match (bytes.get(position), bytes.get(position + 1)) {
        (Some(b'0'), Some(b'x' | b'X')) => 16,
        (Some(b'0'), Some(b'o' | b'O')) => 8,
        (Some(b'0'), Some(b'b' | b'B')) => 2,
        _ => 10,
    };
    if radix != 10 {
        position += 2;
    }
    let first_digit = position;
    let mut magnitude: u64 = 0;
    while let Some(byte) = bytes.get(position) {
        if let Some(digit) = char::from(*byte).to_digit(radix) {
            if magnitude > negative_limit / u64::from(radix) {
                return Err(out_of_range());
            }
            magnitude = magnitude * u64::from(radix) + u64::from(digit);
            position += 1;
        } else if *byte == b'_' {
            if radix == 10 && position == first_digit {
                return Err(invalid());
            }
            position += 1;
            let after = bytes
                .get(position)
                .and_then(|b| char::from(*b).to_digit(radix));
            if after.is_none() {
                return Err(invalid());
            }
        } else {
            break;
        }
    }
    if position == first_digit || !bytes[position..].iter().all(is_space) {
        return Err(invalid());
    }
    if negative && magnitude <= negative_limit {
        return Ok((magnitude as i128).wrapping_neg() as i64);
    }
    if !negative && magnitude < negative_limit {
        return Ok(magnitude as i64);
    }
    Err(out_of_range())
}

/// Reads `text` the way PostgreSQL reads an oid: C-style number with an optional sign, from -2147483648 to 4294967295, where a negative number wraps around.
pub fn parse_oid(text: &str) -> Result<i64, ParseError> {
    let bytes = text.as_bytes();
    let is_space = |b: u8| matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r');
    let mut position = bytes.iter().take_while(|b| is_space(**b)).count();
    let negative = bytes.get(position) == Some(&b'-');
    if matches!(bytes.get(position), Some(b'-' | b'+')) {
        position += 1;
    }
    let (radix, digits_start) = match (bytes.get(position), bytes.get(position + 1)) {
        (Some(b'0'), Some(b'x' | b'X'))
            if bytes.get(position + 2).is_some_and(u8::is_ascii_hexdigit) =>
        {
            (16, position + 2)
        }
        (Some(b'0'), _) => (8, position),
        _ => (10, position),
    };
    let digits: Vec<u32> = bytes[digits_start..]
        .iter()
        .map_while(|b| char::from(*b).to_digit(radix))
        .collect();
    if digits.is_empty() {
        return Err(invalid_input("oid", text));
    }
    let magnitude = digits.iter().try_fold(0u64, |total, digit| {
        total
            .checked_mul(u64::from(radix))
            .and_then(|total| total.checked_add(u64::from(*digit)))
    });
    let out_of_range = || {
        error(
            SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
            format!("value \"{text}\" is out of range for type oid"),
        )
    };
    let Some(magnitude) = magnitude else {
        return Err(out_of_range());
    };
    if !bytes[digits_start + digits.len()..]
        .iter()
        .all(|b| is_space(*b))
    {
        return Err(invalid_input("oid", text));
    }
    let wrapped = if negative {
        magnitude.wrapping_neg()
    } else {
        magnitude
    };
    let fits_unsigned = wrapped <= u64::from(u32::MAX);
    let fits_signed = i64::from(wrapped as u32 as i32) as u64 == wrapped;
    if !fits_unsigned && !fits_signed {
        return Err(out_of_range());
    }
    Ok(i64::from(wrapped as u32))
}

fn parse_char(text: &str) -> Result<ast::Literal, ParseError> {
    let bytes = text.as_bytes();
    let is_octal = |b: &u8| (b'0'..=b'7').contains(b);
    let code = if bytes.len() == 4 && bytes[0] == b'\\' && bytes[1..].iter().all(is_octal) {
        bytes[1..]
            .iter()
            .fold(0u32, |total, digit| total * 8 + u32::from(digit - b'0'))
    } else {
        text.chars().next().map_or(0, u32::from)
    };
    let kept = match u8::try_from(code) {
        Ok(byte) if byte.is_ascii() => char::from(byte),
        _ => {
            return Err(error(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                format!("the value \"{text}\" for type \"char\" is not supported"),
            ))
        }
    };
    let shown = if code == 0 {
        String::new()
    } else {
        kept.to_string()
    };
    Ok(ast::Literal::String(format!(
        "'{}'",
        shown.replace('\'', "''")
    )))
}

fn parse_real(text: &str, trimmed: &str, type_name: &str) -> Result<ast::Literal, ParseError> {
    let unsigned = trimmed.strip_prefix(['+', '-']).unwrap_or(trimmed);
    let lowercase = unsigned.to_lowercase();
    if matches!(lowercase.as_str(), "nan" | "infinity" | "inf") {
        return Err(error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!("the value \"{text}\" for type {type_name} is not supported"),
        ));
    }
    if !is_decimal_number(unsigned) {
        return Err(invalid_input(type_name, text));
    }
    let mut number = trimmed.strip_prefix('+').unwrap_or(trimmed).to_string();
    if !number.contains(['.', 'e', 'E']) {
        number.push_str(".0");
    }
    Ok(ast::Literal::Numeric(number))
}

fn is_decimal_number(unsigned: &str) -> bool {
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, Some(exponent)),
        None => (unsigned, None),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let all_digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if !all_digits(whole) || !all_digits(fraction) || (whole.is_empty() && fraction.is_empty()) {
        return false;
    }
    match exponent {
        None => true,
        Some(exponent) => {
            let digits = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
            !digits.is_empty() && all_digits(digits)
        }
    }
}

fn parse_bool(text: &str, trimmed: &str) -> Result<bool, ParseError> {
    match trimmed.to_lowercase().as_str() {
        "t" | "true" | "y" | "yes" | "on" | "1" => Ok(true),
        "f" | "false" | "n" | "no" | "off" | "0" => Ok(false),
        _ => Err(invalid_input("boolean", text)),
    }
}

fn parse_bytea(text: &str, trimmed: &str) -> Result<ast::Literal, ParseError> {
    let Some(hex) = trimmed.strip_prefix("\\x") else {
        return Err(error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            "the bytea escape format is not supported".to_string(),
        ));
    };
    if hex.len() % 2 != 0 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(error(
            SQLSTATE_INVALID_TEXT_REPRESENTATION,
            format!("invalid hexadecimal data: \"{text}\""),
        ));
    }
    Ok(ast::Literal::Blob(format!("X'{hex}'")))
}

/// Builds the engine's CAST for a rewrite the translator itself makes; not a PostgreSQL cast.
pub fn engine_cast(expr: ast::Expr, target: &PgType) -> ast::Expr {
    ast::Expr::Cast {
        expr: Box::new(expr),
        type_name: target.engine_type(),
    }
}

fn is_numeric_literal_outside_int8(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Literal(ast::Literal::Numeric(text)) => numeric_text_outside_int8(text),
        ast::Expr::Unary(ast::UnaryOperator::Positive, inner) => {
            is_numeric_literal_outside_int8(inner)
        }
        _ => false,
    }
}

fn numeric_text_outside_int8(literal: &str) -> bool {
    let literal = literal.replace('_', "");
    let (negative, unsigned) = match literal.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, literal.as_str()),
    };
    let limit = if negative {
        i64::MIN.unsigned_abs() as u128
    } else {
        i64::MAX as u128
    };
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => match exponent.parse::<i64>() {
            Ok(exponent) => (mantissa, exponent),
            Err(_) => return false,
        },
        None => (unsigned, 0),
    };
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if !int_part
        .bytes()
        .chain(frac_part.bytes())
        .all(|b| b.is_ascii_digit())
    {
        return false;
    }
    let all_digits = format!("{int_part}{frac_part}");
    let digits = all_digits.trim_start_matches('0').as_bytes();
    if digits.is_empty() {
        return false;
    }
    let int_len = (digits.len() as i64)
        .saturating_sub(frac_part.len() as i64)
        .saturating_add(exponent);
    if int_len > 19 {
        return true;
    }
    if int_len < 0 {
        return false;
    }
    let int_len = int_len as usize;
    let mut magnitude: u128 = 0;
    for i in 0..int_len {
        let digit = digits.get(i).map_or(0, |d| d - b'0');
        magnitude = magnitude * 10 + digit as u128;
    }
    if digits.get(int_len).is_some_and(|&d| d >= b'5') {
        magnitude += 1;
    }
    magnitude > limit
}

fn wrap_if_explicit(expr: ast::Expr, target: &PgType, context: CastContext) -> ast::Expr {
    if context != CastContext::Explicit || target.engine_type().is_none() {
        return expr;
    }
    match &expr {
        ast::Expr::Cast { type_name, .. } if *type_name == target.engine_type() => expr,
        _ => engine_cast(expr, target),
    }
}

fn invalid_input(type_name: &str, text: &str) -> ParseError {
    error(
        SQLSTATE_INVALID_TEXT_REPRESENTATION,
        format!("invalid input syntax for type {type_name}: \"{text}\""),
    )
}

fn column_mismatch(
    failure: ParseError,
    column: &str,
    source: &PgType,
    target: &PgType,
) -> ParseError {
    match failure {
        ParseError::Sqlstate {
            sqlstate: SQLSTATE_DATATYPE_MISMATCH,
            ..
        } => error(
            SQLSTATE_DATATYPE_MISMATCH,
            format!("column \"{column}\" is of type {target} but expression is of type {source}"),
        ),
        other => other,
    }
}

fn cannot_be_used_without_a_cast(source: &PgType, target: &PgType) -> ParseError {
    error(
        SQLSTATE_DATATYPE_MISMATCH,
        format!("a value of type {source} cannot be used as {target} without a cast"),
    )
}

fn cast_not_supported(source: &PgType, target: &PgType) -> ParseError {
    error(
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        format!("a cast from {source} to {target} is not supported"),
    )
}

fn error(sqlstate: &'static str, message: String) -> ParseError {
    ParseError::Sqlstate { sqlstate, message }
}

fn one_spelling(name: &str) -> String {
    let lowercase = name.trim().to_lowercase();
    let mut base = lowercase.as_str();
    let mut array_suffix = String::new();
    while let Some(rest) = base.strip_suffix("[]") {
        array_suffix.push_str("[]");
        base = rest.trim_end();
    }
    let (word, modifiers) = match base.find('(') {
        Some(position) => base.split_at(position),
        None => (base, ""),
    };
    let word = word.trim_end();
    let short = match word {
        "integer" | "int" => "int4",
        "bigint" => "int8",
        "smallint" => "int2",
        "real" => "float4",
        "double precision" | "float" => "float8",
        "decimal" => "numeric",
        "boolean" => "bool",
        "character varying" => "varchar",
        "character" => "bpchar",
        "bit varying" => "varbit",
        "timestamp with time zone" => "timestamptz",
        "timestamp without time zone" => "timestamp",
        "time with time zone" => "timetz",
        "time without time zone" => "time",
        other => other,
    };
    format!("{short}{modifiers}{array_suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use CastContext::{Assignment, Explicit, Implicit};

    fn number(text: &str) -> ast::Expr {
        ast::Expr::Literal(ast::Literal::Numeric(text.to_string()))
    }

    fn string(text: &str) -> ast::Expr {
        ast::Expr::Literal(ast::Literal::String(format!(
            "'{}'",
            text.replace('\'', "''")
        )))
    }

    fn null() -> ast::Expr {
        ast::Expr::Literal(ast::Literal::Null)
    }

    fn cast_to(expr: ast::Expr, target: &PgType) -> ast::Expr {
        ast::Expr::Cast {
            expr: Box::new(expr),
            type_name: target.engine_type(),
        }
    }

    fn assert_error(result: Result<ast::Expr, ParseError>, code: &str, text: &str) {
        match result {
            Err(ParseError::Sqlstate { sqlstate, message }) => {
                assert_eq!(sqlstate, code);
                assert_eq!(message, text);
            }
            other => panic!("expected error {code} {text:?}, got {other:?}"),
        }
    }

    fn convert(
        source: &PgType,
        target: &PgType,
        context: CastContext,
    ) -> Result<ast::Expr, ParseError> {
        coerce(number("5"), Some(source), target, context)
    }

    fn convert_text(text: &str, target: &PgType) -> Result<ast::Expr, ParseError> {
        coerce(string(text), Some(&PgType::Unknown), target, Implicit)
    }

    fn expect_expr(source: &PgType, target: &PgType, weakest: CastContext, method: &str) {
        let expected = |expr: ast::Expr| match method {
            "nothing" => expr,
            "cast" => cast_to(expr, target),
            "round" => cast_to(function_call("round", vec![expr]), target),
            _ => unreachable!(),
        };
        assert_eq!(
            convert(source, target, weakest).unwrap(),
            expected(number("5")),
            "{source} to {target}"
        );
        assert_eq!(
            convert(source, target, Explicit).unwrap(),
            if method == "nothing" {
                cast_to(number("5"), target)
            } else {
                expected(number("5"))
            },
            "{source} to {target} explicit"
        );
        if weakest > Implicit {
            let weaker = if weakest == Explicit {
                Assignment
            } else {
                Implicit
            };
            assert_error(
                convert(source, target, weaker),
                "42804",
                &format!("a value of type {source} cannot be used as {target} without a cast"),
            );
        }
    }

    #[test]
    fn int4_converts_to_bigint_without_changing_the_value() {
        expect_expr(&PgType::Int4, &PgType::Int8, Implicit, "nothing");
    }

    #[test]
    fn int4_converts_to_float_and_numeric_with_an_engine_cast() {
        expect_expr(&PgType::Int4, &PgType::Float8, Implicit, "cast");
        expect_expr(&PgType::Int4, &PgType::Numeric, Implicit, "cast");
    }

    #[test]
    fn int4_converts_to_text_only_on_assignment() {
        expect_expr(&PgType::Int4, &PgType::Text, Assignment, "cast");
    }

    #[test]
    fn int8_converts_to_int4_only_on_assignment() {
        expect_expr(&PgType::Int8, &PgType::Int4, Assignment, "nothing");
    }

    #[test]
    fn int8_converts_to_float_and_numeric_with_an_engine_cast() {
        expect_expr(&PgType::Int8, &PgType::Float8, Implicit, "cast");
        expect_expr(&PgType::Int8, &PgType::Numeric, Implicit, "cast");
    }

    #[test]
    fn int8_converts_to_text_only_on_assignment() {
        expect_expr(&PgType::Int8, &PgType::Text, Assignment, "cast");
    }

    #[test]
    fn float_and_numeric_round_before_converting_to_integers() {
        for source in [PgType::Float8, PgType::Numeric] {
            expect_expr(&source, &PgType::Int4, Assignment, "round");
            expect_expr(&source, &PgType::Int8, Assignment, "round");
        }
    }

    #[test]
    fn float_converts_to_numeric_only_on_assignment() {
        expect_expr(&PgType::Float8, &PgType::Numeric, Assignment, "nothing");
    }

    #[test]
    fn numeric_converts_to_float_without_changing_the_value() {
        expect_expr(&PgType::Numeric, &PgType::Float8, Implicit, "nothing");
    }

    #[test]
    fn bool_converts_to_int4_only_with_an_explicit_cast() {
        expect_expr(&PgType::Bool, &PgType::Int4, Explicit, "cast");
    }

    #[test]
    fn unsupported_casts_are_rejected_below_the_context_postgresql_needs() {
        let cases = [
            (PgType::Int4, PgType::Bool, Explicit),
            (PgType::Float8, PgType::Text, Assignment),
            (PgType::Numeric, PgType::Text, Assignment),
            (PgType::Bool, PgType::Text, Assignment),
            (PgType::Bytea, PgType::Text, Assignment),
            (PgType::Text, PgType::Bool, Explicit),
            (PgType::Text, PgType::Bytea, Explicit),
            (PgType::Text, PgType::Float8, Explicit),
            (PgType::Int4, PgType::Bytea, Explicit),
        ];
        for (source, target, weakest) in cases {
            for context in [Implicit, Assignment, Explicit] {
                let result = convert(&source, &target, context);
                if context < weakest {
                    assert_error(
                        result,
                        "42804",
                        &format!(
                            "a value of type {source} cannot be used as {target} without a cast"
                        ),
                    );
                } else {
                    assert_error(
                        result,
                        "0A000",
                        &format!("a cast from {source} to {target} is not supported"),
                    );
                }
            }
        }
    }

    #[test]
    fn types_without_a_cast_in_postgresql_fail_below_explicit_as_a_mismatch() {
        let numbers = [PgType::Int8, PgType::Float8, PgType::Numeric];
        let mut pairs = vec![];
        for n in &numbers {
            pairs.push((PgType::Bool, n.clone()));
            pairs.push((n.clone(), PgType::Bool));
        }
        for t in [PgType::Bool, PgType::Float8, PgType::Numeric] {
            pairs.push((PgType::Bytea, t.clone()));
            pairs.push((t, PgType::Bytea));
        }
        for (source, target) in pairs {
            for context in [Implicit, Assignment] {
                assert_error(
                    convert(&source, &target, context),
                    "42804",
                    &format!("a value of type {source} cannot be used as {target} without a cast"),
                );
            }
            assert_error(
                convert(&source, &target, Explicit),
                "42846",
                &format!("cannot cast type {source} to {target}"),
            );
        }
    }

    #[test]
    fn a_value_of_the_same_type_passes_as_it_is() {
        for t in [PgType::Int4, PgType::Text, PgType::Bool, PgType::Bytea] {
            assert_eq!(convert(&t, &t, Implicit).unwrap(), number("5"));
            assert_eq!(convert(&t, &t, Assignment).unwrap(), number("5"));
        }
    }

    #[test]
    fn casting_to_the_same_type_explicitly_adds_one_engine_cast() {
        assert_eq!(
            convert(&PgType::Int4, &PgType::Int4, Explicit).unwrap(),
            cast_to(number("5"), &PgType::Int4)
        );
    }

    #[test]
    fn explicit_cast_does_not_wrap_an_expression_that_is_already_that_cast() {
        let already = cast_to(number("5"), &PgType::Int8);
        let result = coerce(
            already.clone(),
            Some(&PgType::Int4),
            &PgType::Int8,
            Explicit,
        )
        .unwrap();
        assert_eq!(result, cast_to(number("5"), &PgType::Int8));
        let again = coerce(
            already.clone(),
            Some(&PgType::Int8),
            &PgType::Int8,
            Explicit,
        )
        .unwrap();
        assert_eq!(again, already);
    }

    #[test]
    fn explicit_cast_wraps_an_expression_that_is_a_different_cast() {
        let other = cast_to(number("5"), &PgType::Int4);
        let result = coerce(other.clone(), Some(&PgType::Int4), &PgType::Int4, Explicit).unwrap();
        assert_eq!(result, other);
        let result = coerce(other.clone(), Some(&PgType::Int8), &PgType::Int8, Explicit).unwrap();
        assert_eq!(result, cast_to(other, &PgType::Int8));
    }

    #[test]
    fn non_explicit_contexts_never_add_an_extra_cast() {
        assert_eq!(
            convert(&PgType::Int4, &PgType::Int8, Assignment).unwrap(),
            number("5")
        );
    }

    #[test]
    fn a_type_with_no_supported_casts_converts_only_to_itself() {
        let date = PgType::from_name("DATE");
        assert_eq!(convert(&date, &date, Explicit).unwrap(), number("5"));
        assert_error(
            convert(&date, &PgType::Text, Explicit),
            "0A000",
            "a cast from date to text is not supported",
        );
        assert_error(
            convert(&PgType::Int4, &date, Explicit),
            "0A000",
            "a cast from integer to date is not supported",
        );
        assert_error(
            coerce(
                string("2024-01-01"),
                Some(&PgType::Unknown),
                &date,
                Implicit,
            ),
            "0A000",
            "a cast from unknown to date is not supported",
        );
    }

    #[test]
    fn converting_to_unknown_returns_the_expression() {
        for source in [PgType::Int4, PgType::Unknown, PgType::Text] {
            assert_eq!(
                convert(&source, &PgType::Unknown, Explicit).unwrap(),
                number("5")
            );
        }
    }

    #[test]
    fn null_and_parameters_of_unknown_type_pass_as_they_are() {
        assert_eq!(
            coerce(null(), Some(&PgType::Unknown), &PgType::Int4, Implicit).unwrap(),
            null()
        );
        let parameter = ast::Expr::Variable(ast::Variable::indexed(1.try_into().unwrap()));
        assert_eq!(
            coerce(
                parameter.clone(),
                Some(&PgType::Unknown),
                &PgType::Int8,
                Assignment
            )
            .unwrap(),
            parameter
        );
    }

    #[test]
    fn null_and_parameters_of_unknown_type_get_one_cast_when_explicit() {
        assert_eq!(
            coerce(null(), Some(&PgType::Unknown), &PgType::Int4, Explicit).unwrap(),
            cast_to(null(), &PgType::Int4)
        );
        let parameter = ast::Expr::Variable(ast::Variable::indexed(1.try_into().unwrap()));
        assert_eq!(
            coerce(
                parameter.clone(),
                Some(&PgType::Unknown),
                &PgType::Text,
                Explicit
            )
            .unwrap(),
            cast_to(parameter, &PgType::Text)
        );
    }

    #[test]
    fn string_literal_stays_a_string_for_text() {
        assert_eq!(
            convert_text("  it's ", &PgType::Text).unwrap(),
            string("  it's ")
        );
        assert_eq!(
            coerce(string("a"), Some(&PgType::Unknown), &PgType::Text, Explicit).unwrap(),
            cast_to(string("a"), &PgType::Text)
        );
    }

    #[test]
    fn string_literal_becomes_an_integer() {
        assert_eq!(convert_text(" 42 ", &PgType::Int4).unwrap(), number("42"));
        assert_eq!(convert_text("+7", &PgType::Int4).unwrap(), number("7"));
        assert_eq!(convert_text("-7", &PgType::Int8).unwrap(), number("-7"));
        assert_eq!(
            convert_text("9223372036854775807", &PgType::Int8).unwrap(),
            number("9223372036854775807")
        );
        assert_eq!(
            convert_text("-2147483648", &PgType::Int4).unwrap(),
            number("-2147483648")
        );
    }

    #[test]
    fn string_literal_that_is_not_an_integer_is_rejected() {
        for text in ["abc", "", "1.5", "-", "1e3", "0x", "1__0", "_1"] {
            assert_error(
                convert_text(text, &PgType::Int4),
                "22P02",
                &format!("invalid input syntax for type integer: \"{text}\""),
            );
        }
        assert_error(
            convert_text("x'y", &PgType::Int8),
            "22P02",
            "invalid input syntax for type bigint: \"x'y\"",
        );
    }

    #[test]
    fn string_literal_out_of_integer_range_is_rejected() {
        assert_error(
            convert_text("2147483648", &PgType::Int4),
            "22003",
            "value \"2147483648\" is out of range for type integer",
        );
        assert_error(
            convert_text("9223372036854775808", &PgType::Int8),
            "22003",
            "value \"9223372036854775808\" is out of range for type bigint",
        );
        assert_error(
            convert_text("99999999999999999999999999999999999999999999", &PgType::Int8),
            "22003",
            "value \"99999999999999999999999999999999999999999999\" is out of range for type bigint",
        );
    }

    #[test]
    fn string_literal_becomes_a_real_number() {
        for target in [PgType::Float8, PgType::Numeric] {
            assert_eq!(convert_text("5", &target).unwrap(), number("5.0"));
            assert_eq!(convert_text(" +5 ", &target).unwrap(), number("5.0"));
            assert_eq!(convert_text("-1.5", &target).unwrap(), number("-1.5"));
            assert_eq!(convert_text(".5", &target).unwrap(), number(".5"));
            assert_eq!(convert_text("5.", &target).unwrap(), number("5."));
            assert_eq!(convert_text("1e3", &target).unwrap(), number("1e3"));
            assert_eq!(convert_text("2.5E-3", &target).unwrap(), number("2.5E-3"));
        }
    }

    #[test]
    fn string_literal_that_is_not_a_real_number_is_rejected() {
        for text in ["abc", "", ".", "1e", "1e+", "1.2.3", "--1", "1 2"] {
            assert_error(
                convert_text(text, &PgType::Float8),
                "22P02",
                &format!("invalid input syntax for type double precision: \"{text}\""),
            );
            assert_error(
                convert_text(text, &PgType::Numeric),
                "22P02",
                &format!("invalid input syntax for type numeric: \"{text}\""),
            );
        }
    }

    #[test]
    fn string_literal_nan_and_infinity_are_not_supported() {
        for text in ["NaN", "nan", "Infinity", "-Infinity", "inf", "+INFINITY"] {
            assert_error(
                convert_text(text, &PgType::Float8),
                "0A000",
                &format!("the value \"{text}\" for type double precision is not supported"),
            );
            assert_error(
                convert_text(text, &PgType::Numeric),
                "0A000",
                &format!("the value \"{text}\" for type numeric is not supported"),
            );
        }
    }

    #[test]
    fn string_literal_becomes_a_boolean() {
        for text in ["t", "TRUE", "y", "Yes", "on", "1", " true "] {
            assert_eq!(
                convert_text(text, &PgType::Bool).unwrap(),
                bool_literal(true)
            );
        }
        for text in ["f", "False", "n", "NO", "off", "0", " f "] {
            assert_eq!(
                convert_text(text, &PgType::Bool).unwrap(),
                bool_literal(false)
            );
        }
    }

    #[test]
    fn string_literal_that_is_not_a_boolean_is_rejected() {
        for text in ["maybe", "", "2", "tru"] {
            assert_error(
                convert_text(text, &PgType::Bool),
                "22P02",
                &format!("invalid input syntax for type boolean: \"{text}\""),
            );
        }
    }

    #[test]
    fn explicit_cast_of_a_boolean_string_literal_is_not_wrapped_twice() {
        assert_eq!(
            coerce(string("t"), Some(&PgType::Unknown), &PgType::Bool, Explicit).unwrap(),
            bool_literal(true)
        );
    }

    #[test]
    fn string_literal_becomes_a_blob() {
        assert_eq!(
            convert_text("\\x0aFF", &PgType::Bytea).unwrap(),
            ast::Expr::Literal(ast::Literal::Blob("X'0aFF'".to_string()))
        );
        assert_eq!(
            convert_text(" \\x ", &PgType::Bytea).unwrap(),
            ast::Expr::Literal(ast::Literal::Blob("X''".to_string()))
        );
    }

    #[test]
    fn string_literal_with_bad_hex_is_rejected() {
        for text in ["\\xabc", "\\xzz", "\\x0g"] {
            assert_error(
                convert_text(text, &PgType::Bytea),
                "22P02",
                &format!("invalid hexadecimal data: \"{text}\""),
            );
        }
    }

    #[test]
    fn string_literal_in_the_bytea_escape_format_is_not_supported() {
        assert_error(
            convert_text("abc", &PgType::Bytea),
            "0A000",
            "the bytea escape format is not supported",
        );
    }

    #[test]
    fn explicit_cast_of_a_converted_string_literal_adds_one_cast() {
        assert_eq!(
            coerce(string("5"), Some(&PgType::Unknown), &PgType::Int4, Explicit).unwrap(),
            cast_to(number("5"), &PgType::Int4)
        );
    }

    #[test]
    fn a_value_whose_name_does_not_resolve_passes_as_it_is() {
        for target in [PgType::Int4, PgType::Text, PgType::Bool, PgType::Bytea] {
            for context in [Implicit, Assignment] {
                assert_eq!(
                    coerce(number("5"), None, &target, context).unwrap(),
                    number("5")
                );
            }
            assert_eq!(
                coerce(number("5"), None, &target, Explicit).unwrap(),
                cast_to(number("5"), &target)
            );
        }
        assert_eq!(
            coerce(number("5"), None, &PgType::Unknown, Explicit).unwrap(),
            number("5")
        );
    }

    #[test]
    fn a_value_whose_name_does_not_resolve_cannot_become_an_unsupported_type() {
        for context in [Implicit, Assignment, Explicit] {
            assert_error(
                coerce(
                    number("5"),
                    None,
                    &PgType::Other("date".to_string()),
                    context,
                ),
                "0A000",
                "a cast from unknown to date is not supported",
            );
        }
    }

    #[test]
    fn a_numeric_literal_outside_bigint_raises_instead_of_converting() {
        let raises = function_call("int8_out_of_range", vec![]);
        for text in [
            "9223372036854775808",
            "-9223372036854775809",
            "9223372036854775807.5",
            "1e19",
            "9_223_372_036_854_775_808",
        ] {
            assert_eq!(
                coerce(
                    number(text),
                    Some(&PgType::Numeric),
                    &PgType::Int8,
                    Assignment
                )
                .unwrap(),
                raises,
                "{text}"
            );
            assert_eq!(
                coerce(
                    number(text),
                    Some(&PgType::Numeric),
                    &PgType::Int8,
                    Explicit
                )
                .unwrap(),
                cast_to(raises.clone(), &PgType::Int8),
                "{text}"
            );
        }
        let plus = ast::Expr::Unary(
            ast::UnaryOperator::Positive,
            Box::new(number("9223372036854775808")),
        );
        assert_eq!(
            coerce(plus, Some(&PgType::Numeric), &PgType::Int8, Assignment).unwrap(),
            raises
        );
    }

    #[test]
    fn a_numeric_literal_inside_bigint_is_rounded_then_cast() {
        for text in [
            "9223372036854775807.4",
            "-9223372036854775808.4",
            "9.223372036854775807e18",
            "0e100",
            "0.0e25",
        ] {
            assert_eq!(
                coerce(
                    number(text),
                    Some(&PgType::Numeric),
                    &PgType::Int8,
                    Explicit
                )
                .unwrap(),
                cast_to(function_call("round", vec![number(text)]), &PgType::Int8),
                "{text}"
            );
        }
    }

    #[test]
    fn from_name_finds_every_supported_type_ignoring_case() {
        let cases = [
            ("int", PgType::Int4),
            ("INTEGER", PgType::Int4),
            ("int4", PgType::Int4),
            ("bigint", PgType::Int8),
            ("Int8", PgType::Int8),
            ("float8", PgType::Float8),
            ("Double Precision", PgType::Float8),
            ("numeric", PgType::Numeric),
            ("DECIMAL", PgType::Numeric),
            ("text", PgType::Text),
            ("bool", PgType::Bool),
            ("Boolean", PgType::Bool),
            ("bytea", PgType::Bytea),
        ];
        for (name, expected) in cases {
            assert_eq!(PgType::from_name(name), expected, "{name}");
        }
    }

    #[test]
    fn from_name_keeps_other_names_in_lowercase() {
        for name in ["Date", "uuid", "float4", "int8range", "bpchar"] {
            assert_eq!(PgType::from_name(name), PgType::Other(name.to_lowercase()));
        }
    }

    #[test]
    fn from_name_gives_one_value_for_every_name_of_a_type() {
        let groups: [&[&str]; 10] = [
            &[
                "timestamptz",
                "timestamp with time zone",
                "TIMESTAMP WITH TIME ZONE",
            ],
            &["timestamp", "timestamp without time zone"],
            &["timetz", "time with time zone"],
            &["time", "time without time zone"],
            &["int2", "smallint"],
            &["float4", "real"],
            &["bpchar", "character"],
            &["int4[]", "integer[]", "int[]"],
            &["float8", "double precision", "float"],
            &["numeric", "decimal", "Numeric"],
        ];
        for group in groups {
            let first = PgType::from_name(group[0]);
            for name in &group[1..] {
                assert_eq!(PgType::from_name(name), first, "{name}");
            }
        }
        assert_eq!(
            PgType::from_name("timestamp with time zone"),
            PgType::Other("timestamptz".to_string())
        );
        assert_eq!(
            PgType::from_name("numeric(10,2)"),
            PgType::Other("numeric(10,2)".to_string())
        );
        assert_eq!(PgType::from_name("float"), PgType::Float8);
        assert_eq!(PgType::from_name("decimal"), PgType::Numeric);
    }

    #[test]
    fn name_is_text() {
        assert_eq!(PgType::from_name("name"), PgType::Text);
        assert_eq!(PgType::from_name("NAME"), PgType::Text);
    }

    #[test]
    fn display_uses_postgresql_names() {
        let cases = [
            (PgType::Bool, "boolean"),
            (PgType::Int4, "integer"),
            (PgType::Int8, "bigint"),
            (PgType::Float8, "double precision"),
            (PgType::Numeric, "numeric"),
            (PgType::Text, "text"),
            (PgType::Bytea, "bytea"),
            (PgType::Unknown, "unknown"),
            (PgType::Other("uuid".to_string()), "uuid"),
        ];
        for (t, name) in cases {
            assert_eq!(t.to_string(), name);
        }
    }

    #[test]
    fn engine_type_names_the_engine_storage_type() {
        let cases = [
            (PgType::Int4, Some("INTEGER")),
            (PgType::Int8, Some("BIGINT")),
            (PgType::Float8, Some("REAL")),
            (PgType::Numeric, Some("REAL")),
            (PgType::Text, Some("TEXT")),
            (PgType::Bytea, Some("BLOB")),
            (PgType::Bool, Some("boolean")),
            (PgType::Unknown, None),
            (PgType::Other("date".to_string()), None),
        ];
        for (t, name) in cases {
            let expected = name.map(|name| ast::Type {
                name: name.to_string(),
                size: None,
                array_dimensions: 0,
            });
            assert_eq!(t.engine_type(), expected);
        }
    }

    #[test]
    fn bool_literal_is_a_cast_of_one_or_zero() {
        assert_eq!(bool_literal(true), cast_to(number("1"), &PgType::Bool));
        assert_eq!(bool_literal(false), cast_to(number("0"), &PgType::Bool));
    }

    #[test]
    fn column_types_come_from_the_catalog_type_name() {
        for (declared, expected) in [
            ("INTEGER", PgType::Int4),
            ("int", PgType::Int4),
            ("BIGINT", PgType::Int8),
            ("REAL", PgType::Float8),
            ("Text", PgType::Text),
            ("BLOB", PgType::Bytea),
            ("bytea", PgType::Bytea),
            ("BOOLEAN", PgType::Bool),
            ("DATE", PgType::Other("date".to_string())),
            ("smallint", PgType::Int2),
            ("timestamptz", PgType::Other("timestamptz".to_string())),
        ] {
            assert_eq!(
                PgType::from_column_type(declared, 0, &[]),
                expected,
                "{declared}"
            );
        }
    }

    #[test]
    fn a_numeric_column_is_never_the_plain_numeric_type() {
        assert_eq!(
            PgType::from_column_type("numeric", 0, &[10, 2]),
            PgType::Other("numeric(10,2)".to_string())
        );
        assert_eq!(
            PgType::from_column_type("numeric", 0, &[7]),
            PgType::Other("numeric(7,0)".to_string())
        );
        assert_eq!(
            PgType::from_column_type("numeric", 0, &[]),
            PgType::Other("numeric with a precision".to_string())
        );
        assert_ne!(
            PgType::from_column_type("numeric", 0, &[10, 2]),
            PgType::Numeric
        );
    }

    #[test]
    fn array_columns_are_arrays_of_one_dimension_or_other_types() {
        assert_eq!(
            PgType::from_column_type("Text", 2, &[]),
            PgType::Other("text[][]".to_string())
        );
    }

    #[test]
    fn common_type_of_nothing_or_only_unknown_is_text() {
        assert_eq!(common_type(&[], "CASE").unwrap(), PgType::Text);
        assert_eq!(
            common_type(&[PgType::Unknown, PgType::Unknown], "CASE").unwrap(),
            PgType::Text
        );
    }

    #[test]
    fn common_type_of_equal_types_is_that_type() {
        let types = [PgType::Bytea, PgType::Unknown, PgType::Bytea];
        assert_eq!(common_type(&types, "UNION").unwrap(), PgType::Bytea);
        let date = PgType::Other("date".to_string());
        assert_eq!(
            common_type(&[date.clone(), date.clone()], "UNION").unwrap(),
            date
        );
    }

    #[test]
    fn common_type_of_numbers_is_the_widest() {
        let cases = [
            (vec![PgType::Int4, PgType::Int8], PgType::Int8),
            (vec![PgType::Int8, PgType::Numeric], PgType::Numeric),
            (vec![PgType::Numeric, PgType::Float8], PgType::Float8),
            (
                vec![
                    PgType::Float8,
                    PgType::Unknown,
                    PgType::Int4,
                    PgType::Numeric,
                ],
                PgType::Float8,
            ),
            (vec![PgType::Int4, PgType::Numeric], PgType::Numeric),
        ];
        for (types, expected) in cases {
            assert_eq!(common_type(&types, "COALESCE").unwrap(), expected);
        }
    }

    #[test]
    fn common_type_of_types_that_do_not_match_is_an_error() {
        assert_error(
            common_type(&[PgType::Int4, PgType::Text], "CASE").map(|_| null()),
            "42804",
            "CASE types integer and text cannot be matched",
        );
        assert_error(
            common_type(&[PgType::Int4, PgType::Bool, PgType::Int8], "UNION").map(|_| null()),
            "42804",
            "UNION types integer and boolean cannot be matched",
        );
    }

    #[test]
    fn common_type_with_an_unsupported_type_is_not_supported() {
        let date = PgType::Other("date".to_string());
        assert_error(
            common_type(&[PgType::Int4, date.clone()], "COALESCE").map(|_| null()),
            "0A000",
            "COALESCE with types integer and date is not supported",
        );
        assert_error(
            common_type(&[date, PgType::Text], "CASE").map(|_| null()),
            "0A000",
            "CASE with types date and text is not supported",
        );
    }

    fn variable() -> ast::Expr {
        ast::Expr::Variable(ast::Variable::indexed(1.try_into().unwrap()))
    }

    fn date() -> PgType {
        PgType::Other("date".to_string())
    }

    fn operands_of(items: Vec<(ast::Expr, Option<PgType>)>) -> Vec<(ast::Expr, Option<PgType>)> {
        items
    }

    fn assert_operand_error(result: Result<Vec<ast::Expr>, ParseError>, code: &str, text: &str) {
        assert_error(result.map(|_| null()), code, text);
    }

    #[test]
    fn null_literal_of_unknown_type_passes_into_a_type_with_no_supported_casts_unless_explicit() {
        for context in [Implicit, Assignment] {
            assert_eq!(
                coerce(null(), Some(&PgType::Unknown), &date(), context).unwrap(),
                null()
            );
        }
        assert_error(
            coerce(null(), Some(&PgType::Unknown), &date(), Explicit),
            "0A000",
            "a cast from unknown to date is not supported",
        );
    }

    #[test]
    fn explicit_cast_of_a_parameter_to_a_type_with_no_supported_casts_is_not_supported() {
        assert_error(
            coerce(variable(), Some(&PgType::Unknown), &date(), Explicit),
            "0A000",
            "a cast from unknown to date is not supported",
        );
    }

    #[test]
    fn string_literal_of_unknown_type_is_rejected_for_a_type_with_no_supported_casts() {
        for context in [Implicit, Assignment, Explicit] {
            assert_error(
                coerce(
                    string("2024-01-01"),
                    Some(&PgType::Unknown),
                    &date(),
                    context,
                ),
                "0A000",
                "a cast from unknown to date is not supported",
            );
        }
    }

    #[test]
    fn parameter_of_unknown_type_passes_into_any_type() {
        for target in [date(), PgType::Int4, PgType::Text, PgType::Bool] {
            for context in [Implicit, Assignment] {
                assert_eq!(
                    coerce(variable(), Some(&PgType::Unknown), &target, context).unwrap(),
                    variable(),
                    "{target}"
                );
            }
        }
    }

    #[test]
    fn explicit_conversion_of_a_parameter_adds_a_cast() {
        assert_eq!(
            coerce(variable(), Some(&PgType::Unknown), &PgType::Int4, Explicit).unwrap(),
            cast_to(variable(), &PgType::Int4)
        );
    }

    #[test]
    fn operands_with_a_name_that_does_not_resolve_pass_as_they_are() {
        let items = vec![
            (number("1"), None),
            (string("x"), Some(PgType::Text)),
            (number("2"), Some(PgType::Int4)),
        ];
        for kind in [
            OperatorKind::Arithmetic,
            OperatorKind::Comparison,
            OperatorKind::Concat,
            OperatorKind::Pattern,
        ] {
            assert_eq!(
                coerce_operands("+", kind, items.clone()).unwrap(),
                vec![number("1"), string("x"), number("2")]
            );
        }
    }

    #[test]
    fn arithmetic_on_only_unknown_operands_is_ambiguous() {
        let items = operands_of(vec![
            (string("1"), Some(PgType::Unknown)),
            (string("1"), Some(PgType::Unknown)),
        ]);
        assert_operand_error(
            coerce_operands("+", OperatorKind::Arithmetic, items),
            "42725",
            "operator is not unique: unknown + unknown",
        );
    }

    #[test]
    fn other_kinds_of_operator_on_only_unknown_operands_pass_as_they_are() {
        let items = vec![
            (string("1"), Some(PgType::Unknown)),
            (null(), Some(PgType::Unknown)),
        ];
        for kind in [
            OperatorKind::Comparison,
            OperatorKind::Concat,
            OperatorKind::Pattern,
        ] {
            assert_eq!(
                coerce_operands("=", kind, items.clone()).unwrap(),
                vec![string("1"), null()]
            );
        }
    }

    #[test]
    fn operands_of_the_same_unsupported_type_pass_as_they_are() {
        let items = vec![(number("1"), Some(date())), (number("2"), Some(date()))];
        for kind in [OperatorKind::Comparison, OperatorKind::Arithmetic] {
            assert_eq!(
                coerce_operands("=", kind, items.clone()).unwrap(),
                vec![number("1"), number("2")]
            );
        }
    }

    #[test]
    fn operands_of_different_types_where_one_is_unsupported_are_not_supported() {
        let items = vec![
            (number("1"), Some(PgType::Int4)),
            (number("2"), Some(PgType::Int4)),
            (number("3"), Some(date())),
        ];
        assert_operand_error(
            coerce_operands("=", OperatorKind::Comparison, items),
            "0A000",
            "the operator = for types integer and date is not supported",
        );
        let items = vec![
            (number("1"), Some(date())),
            (number("2"), Some(PgType::Text)),
        ];
        assert_operand_error(
            coerce_operands("||", OperatorKind::Concat, items),
            "0A000",
            "the operator || for types date and text is not supported",
        );
    }

    #[test]
    fn string_literal_read_as_an_unsupported_type_is_not_supported() {
        let items = vec![
            (number("1"), Some(date())),
            (string("2024-01-01"), Some(PgType::Unknown)),
        ];
        assert_operand_error(
            coerce_operands("=", OperatorKind::Comparison, items),
            "0A000",
            "a cast from unknown to date is not supported",
        );
    }

    #[test]
    fn null_read_as_an_unsupported_type_passes() {
        let items = vec![(number("1"), Some(date())), (null(), Some(PgType::Unknown))];
        assert_eq!(
            coerce_operands("=", OperatorKind::Comparison, items).unwrap(),
            vec![number("1"), null()]
        );
    }

    #[test]
    fn arithmetic_on_numbers_converts_to_the_widest() {
        let items = vec![
            (number("1"), Some(PgType::Int4)),
            (number("2"), Some(PgType::Int8)),
            (number("3"), Some(PgType::Float8)),
            (number("4"), Some(PgType::Numeric)),
        ];
        assert_eq!(
            coerce_operands("+", OperatorKind::Arithmetic, items).unwrap(),
            vec![
                cast_to(number("1"), &PgType::Float8),
                cast_to(number("2"), &PgType::Float8),
                number("3"),
                number("4"),
            ]
        );
        let items = vec![
            (number("1"), Some(PgType::Int4)),
            (number("2"), Some(PgType::Int8)),
        ];
        assert_eq!(
            coerce_operands("-", OperatorKind::Arithmetic, items).unwrap(),
            vec![number("1"), number("2")]
        );
    }

    #[test]
    fn arithmetic_reads_string_literals_as_the_number_type() {
        let items = vec![
            (number("1"), Some(PgType::Int4)),
            (string("1"), Some(PgType::Unknown)),
        ];
        assert_eq!(
            coerce_operands("+", OperatorKind::Arithmetic, items).unwrap(),
            vec![number("1"), number("1")]
        );
        let items = vec![
            (number("1"), Some(PgType::Int4)),
            (string("abc"), Some(PgType::Unknown)),
        ];
        assert_operand_error(
            coerce_operands("+", OperatorKind::Arithmetic, items),
            "22P02",
            "invalid input syntax for type integer: \"abc\"",
        );
    }

    #[test]
    fn arithmetic_on_a_value_that_is_not_a_number_does_not_exist() {
        let cases = [
            (
                vec![PgType::Text, PgType::Int4],
                "operator does not exist: text + integer",
            ),
            (
                vec![PgType::Int4, PgType::Text],
                "operator does not exist: integer + text",
            ),
            (
                vec![PgType::Text, PgType::Text],
                "operator does not exist: text + text",
            ),
            (
                vec![PgType::Bool, PgType::Unknown],
                "operator does not exist: boolean + unknown",
            ),
            (
                vec![PgType::Unknown, PgType::Bytea],
                "operator does not exist: unknown + bytea",
            ),
        ];
        for (types, message) in cases {
            let items = types.into_iter().map(|ty| (null(), Some(ty))).collect();
            assert_operand_error(
                coerce_operands("+", OperatorKind::Arithmetic, items),
                "42883",
                message,
            );
        }
    }

    #[test]
    fn comparison_of_equal_types_converts_string_literals_to_that_type() {
        let items = vec![
            (number("1"), Some(PgType::Bool)),
            (string("true"), Some(PgType::Unknown)),
        ];
        assert_eq!(
            coerce_operands("=", OperatorKind::Comparison, items).unwrap(),
            vec![number("1"), bool_literal(true)]
        );
        let items = vec![
            (string("x"), Some(PgType::Text)),
            (string("y"), Some(PgType::Unknown)),
        ];
        assert_eq!(
            coerce_operands("=", OperatorKind::Comparison, items).unwrap(),
            vec![string("x"), string("y")]
        );
    }

    #[test]
    fn comparison_of_numbers_converts_to_the_widest() {
        let items = vec![
            (number("1"), Some(PgType::Int8)),
            (number("2"), Some(PgType::Float8)),
            (string("3"), Some(PgType::Unknown)),
        ];
        assert_eq!(
            coerce_operands("<", OperatorKind::Comparison, items).unwrap(),
            vec![
                cast_to(number("1"), &PgType::Float8),
                number("2"),
                number("3.0")
            ]
        );
    }

    #[test]
    fn comparison_of_types_that_do_not_match_does_not_exist() {
        let cases = [
            (
                vec![PgType::Text, PgType::Int4],
                "operator does not exist: text = integer",
            ),
            (
                vec![PgType::Int4, PgType::Bool, PgType::Int4],
                "operator does not exist: integer = boolean",
            ),
            (
                vec![PgType::Bytea, PgType::Unknown, PgType::Text],
                "operator does not exist: bytea = text",
            ),
        ];
        for (types, message) in cases {
            let items = types.into_iter().map(|ty| (null(), Some(ty))).collect();
            assert_operand_error(
                coerce_operands("=", OperatorKind::Comparison, items),
                "42883",
                message,
            );
        }
    }

    #[test]
    fn comparison_of_three_operands_names_the_first_two_types_that_differ() {
        let items = vec![
            (number("1"), Some(PgType::Text)),
            (number("2"), Some(PgType::Int4)),
            (number("3"), Some(PgType::Int4)),
        ];
        assert_operand_error(
            coerce_operands(">=", OperatorKind::Comparison, items),
            "42883",
            "operator does not exist: text >= integer",
        );
    }

    #[test]
    fn concat_converts_every_operand_to_text() {
        let items = vec![
            (number("1"), Some(PgType::Int4)),
            (string("x"), Some(PgType::Unknown)),
            (number("2"), Some(PgType::Int8)),
            (string("y"), Some(PgType::Text)),
        ];
        assert_eq!(
            coerce_operands("||", OperatorKind::Concat, items).unwrap(),
            vec![
                cast_to(number("1"), &PgType::Text),
                string("x"),
                cast_to(number("2"), &PgType::Text),
                string("y"),
            ]
        );
    }

    #[test]
    fn concat_with_a_value_that_has_no_cast_to_text_is_not_supported() {
        let items = vec![
            (string("x"), Some(PgType::Text)),
            (number("1"), Some(PgType::Bool)),
        ];
        assert_operand_error(
            coerce_operands("||", OperatorKind::Concat, items),
            "0A000",
            "a cast from boolean to text is not supported",
        );
    }

    #[test]
    fn pattern_operands_must_be_text_or_unknown() {
        let items = vec![
            (string("x"), Some(PgType::Text)),
            (string("x%"), Some(PgType::Unknown)),
        ];
        assert_eq!(
            coerce_operands("~~", OperatorKind::Pattern, items).unwrap(),
            vec![string("x"), string("x%")]
        );
        let items = vec![
            (number("1"), Some(PgType::Int4)),
            (string("x"), Some(PgType::Unknown)),
        ];
        assert_operand_error(
            coerce_operands("~~", OperatorKind::Pattern, items),
            "42883",
            "operator does not exist: integer ~~ unknown",
        );
        let items = vec![
            (string("x"), Some(PgType::Text)),
            (number("1"), Some(PgType::Int4)),
        ];
        assert_operand_error(
            coerce_operands("!~~", OperatorKind::Pattern, items),
            "42883",
            "operator does not exist: text !~~ integer",
        );
    }

    #[test]
    fn a_value_of_type_boolean_stays_a_condition() {
        assert_eq!(
            coerce_to_boolean(number("1"), Some(&PgType::Bool), "WHERE").unwrap(),
            number("1")
        );
    }

    #[test]
    fn a_value_whose_name_does_not_resolve_stays_a_condition() {
        assert_eq!(
            coerce_to_boolean(number("1"), None, "WHERE").unwrap(),
            number("1")
        );
    }

    #[test]
    fn a_string_literal_becomes_a_boolean_condition() {
        assert_eq!(
            coerce_to_boolean(string("true"), Some(&PgType::Unknown), "WHERE").unwrap(),
            bool_literal(true)
        );
        assert_eq!(
            coerce_to_boolean(variable(), Some(&PgType::Unknown), "WHERE").unwrap(),
            variable()
        );
        assert_error(
            coerce_to_boolean(string("maybe"), Some(&PgType::Unknown), "WHERE"),
            "22P02",
            "invalid input syntax for type boolean: \"maybe\"",
        );
    }

    #[test]
    fn a_condition_of_another_type_is_rejected() {
        for ty in [PgType::Int4, PgType::Text, PgType::Bytea, PgType::Numeric] {
            assert_error(
                coerce_to_boolean(number("1"), Some(&ty), "HAVING"),
                "42804",
                &format!("argument of HAVING must be type boolean, not type {ty}"),
            );
        }
    }

    #[test]
    fn a_condition_of_a_type_with_no_supported_casts_is_not_supported() {
        assert_error(
            coerce_to_boolean(number("1"), Some(&date()), "WHERE"),
            "0A000",
            "a cast from date to boolean is not supported",
        );
    }

    #[test]
    fn a_sign_in_front_of_text_boolean_or_bytea_does_not_exist() {
        for ty in [PgType::Text, PgType::Bool, PgType::Bytea] {
            for operator in ["-", "+"] {
                let result = check_unary_operand(operator, Some(&ty));
                assert_error(
                    result.map(|()| null()),
                    "42883",
                    &format!("operator does not exist: {operator} {ty}"),
                );
            }
        }
    }

    #[test]
    fn a_sign_in_front_of_other_values_is_allowed() {
        for ty in [
            Some(PgType::Int4),
            Some(PgType::Int8),
            Some(PgType::Float8),
            Some(PgType::Numeric),
            Some(PgType::Unknown),
            Some(date()),
            None,
        ] {
            assert!(check_unary_operand("-", ty.as_ref()).is_ok());
        }
    }

    #[test]
    fn engine_cast_as_wraps_the_expression_in_a_cast_to_the_given_type() {
        let type_name = ast::Type {
            name: "jsonb".to_string(),
            size: None,
            array_dimensions: 0,
        };
        assert_eq!(
            engine_cast_as(string("{}"), type_name.clone()),
            ast::Expr::Cast {
                expr: Box::new(string("{}")),
                type_name: Some(type_name),
            }
        );
    }

    fn varchar5() -> PgType {
        PgType::Other("varchar(5)".to_string())
    }

    #[test]
    fn a_pair_with_a_type_that_has_no_supported_casts_is_never_a_mismatch() {
        let timestamp = PgType::Other("timestamp".to_string());
        let pairs = [
            (date(), timestamp),
            (PgType::Int4, date()),
            (date(), PgType::Int4),
            (PgType::Text, date()),
            (date(), PgType::Text),
            (PgType::Numeric, varchar5()),
            (PgType::Text, varchar5()),
            (varchar5(), PgType::Text),
        ];
        for (source, target) in pairs {
            for context in [Implicit, Assignment, Explicit] {
                assert_error(
                    convert(&source, &target, context),
                    "0A000",
                    &format!("a cast from {source} to {target} is not supported"),
                );
            }
        }
    }

    #[test]
    fn concat_of_only_bytea_operands_stays_bytea() {
        let items = vec![
            (number("1"), Some(PgType::Bytea)),
            (string("\\x0102"), Some(PgType::Unknown)),
            (number("2"), Some(PgType::Bytea)),
        ];
        assert_eq!(
            coerce_operands("||", OperatorKind::Concat, items).unwrap(),
            vec![
                number("1"),
                ast::Expr::Literal(ast::Literal::Blob("X'0102'".to_string())),
                number("2"),
            ]
        );
    }

    #[test]
    fn concat_of_bytea_with_text_converts_to_text() {
        let items = vec![
            (number("1"), Some(PgType::Bytea)),
            (string("x"), Some(PgType::Text)),
        ];
        assert_operand_error(
            coerce_operands("||", OperatorKind::Concat, items),
            "0A000",
            "a cast from bytea to text is not supported",
        );
    }

    fn assert_assignment_error(
        source: Option<&PgType>,
        target: Option<&PgType>,
        expr: ast::Expr,
        code: &str,
        text: &str,
    ) {
        assert_error(coerce_assignment(expr, source, "col", target), code, text);
    }

    #[test]
    fn assignment_without_a_known_source_or_target_returns_the_expression() {
        assert_eq!(
            coerce_assignment(string("x"), None, "col", Some(&PgType::Int4)).unwrap(),
            string("x")
        );
        assert_eq!(
            coerce_assignment(number("5"), Some(&PgType::Int4), "col", None).unwrap(),
            number("5")
        );
        assert_eq!(
            coerce_assignment(number("5"), None, "col", None).unwrap(),
            number("5")
        );
    }

    #[test]
    fn assignment_converts_with_the_assignment_context() {
        assert_eq!(
            coerce_assignment(
                string("5"),
                Some(&PgType::Unknown),
                "col",
                Some(&PgType::Int4)
            )
            .unwrap(),
            number("5")
        );
        assert_eq!(
            coerce_assignment(number("5"), Some(&PgType::Int4), "col", Some(&PgType::Text))
                .unwrap(),
            cast_to(number("5"), &PgType::Text)
        );
        assert_eq!(
            coerce_assignment(number("5"), Some(&PgType::Int4), "col", Some(&PgType::Int8))
                .unwrap(),
            number("5")
        );
        assert_eq!(
            coerce_assignment(null(), Some(&PgType::Unknown), "col", Some(&date())).unwrap(),
            null()
        );
        assert_eq!(
            coerce_assignment(variable(), Some(&PgType::Unknown), "col", Some(&date())).unwrap(),
            variable()
        );
    }

    #[test]
    fn assignment_that_needs_a_cast_gives_the_column_message() {
        assert_assignment_error(
            Some(&PgType::Text),
            Some(&PgType::Int4),
            number("5"),
            "42804",
            "column \"col\" is of type integer but expression is of type text",
        );
        assert_assignment_error(
            Some(&PgType::Bool),
            Some(&PgType::Int4),
            number("5"),
            "42804",
            "column \"col\" is of type integer but expression is of type boolean",
        );
        assert_assignment_error(
            Some(&PgType::Int4),
            Some(&PgType::Bool),
            number("5"),
            "42804",
            "column \"col\" is of type boolean but expression is of type integer",
        );
        assert_assignment_error(
            Some(&PgType::Bool),
            Some(&PgType::Float8),
            number("5"),
            "42804",
            "column \"col\" is of type double precision but expression is of type boolean",
        );
    }

    #[test]
    fn assignment_keeps_the_other_errors() {
        assert_assignment_error(
            Some(&PgType::Unknown),
            Some(&PgType::Int4),
            string("x"),
            "22P02",
            "invalid input syntax for type integer: \"x\"",
        );
        assert_assignment_error(
            Some(&PgType::Unknown),
            Some(&date()),
            string("2024-01-01"),
            "0A000",
            "a cast from unknown to date is not supported",
        );
        assert_assignment_error(
            Some(&date()),
            Some(&PgType::Other("timestamp".to_string())),
            number("5"),
            "0A000",
            "a cast from date to timestamp is not supported",
        );
    }

    fn assert_common_error(
        construct: &str,
        values: Vec<(ast::Expr, Option<PgType>)>,
        code: &str,
        text: &str,
    ) {
        assert_operand_error(coerce_to_common(construct, values), code, text);
    }

    #[test]
    fn values_with_a_name_that_does_not_resolve_pass_as_they_are() {
        let values = vec![
            (number("1"), None),
            (string("x"), Some(PgType::Text)),
            (number("2"), Some(PgType::Int4)),
        ];
        assert_eq!(
            coerce_to_common("CASE", values).unwrap(),
            vec![number("1"), string("x"), number("2")]
        );
    }

    #[test]
    fn values_of_numbers_convert_to_the_widest() {
        let values = vec![
            (number("1"), Some(PgType::Int4)),
            (number("2"), Some(PgType::Int8)),
            (number("3"), Some(PgType::Float8)),
        ];
        assert_eq!(
            coerce_to_common("COALESCE", values).unwrap(),
            vec![
                cast_to(number("1"), &PgType::Float8),
                cast_to(number("2"), &PgType::Float8),
                number("3"),
            ]
        );
        let values = vec![
            (number("1"), Some(PgType::Int4)),
            (number("2"), Some(PgType::Int8)),
        ];
        assert_eq!(
            coerce_to_common("GREATEST", values).unwrap(),
            vec![number("1"), number("2")]
        );
    }

    #[test]
    fn string_literals_read_as_the_common_type() {
        let values = vec![
            (number("1"), Some(PgType::Int4)),
            (string("0"), Some(PgType::Unknown)),
            (null(), Some(PgType::Unknown)),
        ];
        assert_eq!(
            coerce_to_common("COALESCE", values).unwrap(),
            vec![number("1"), number("0"), null()]
        );
        assert_common_error(
            "CASE",
            vec![
                (number("1"), Some(PgType::Int4)),
                (string("x"), Some(PgType::Unknown)),
            ],
            "22P02",
            "invalid input syntax for type integer: \"x\"",
        );
    }

    #[test]
    fn values_that_are_all_unknown_stay_as_they_are() {
        let values = vec![
            (string("a"), Some(PgType::Unknown)),
            (null(), Some(PgType::Unknown)),
        ];
        assert_eq!(
            coerce_to_common("CASE", values).unwrap(),
            vec![string("a"), null()]
        );
    }

    #[test]
    fn values_of_types_that_do_not_match_are_an_error() {
        assert_common_error(
            "CASE",
            vec![
                (number("1"), Some(PgType::Int4)),
                (string("x"), Some(PgType::Text)),
            ],
            "42804",
            "CASE types integer and text cannot be matched",
        );
        assert_common_error(
            "LEAST",
            vec![
                (number("1"), Some(PgType::Bool)),
                (number("2"), Some(PgType::Int8)),
            ],
            "42804",
            "LEAST types boolean and bigint cannot be matched",
        );
    }

    #[test]
    fn a_string_literal_read_as_a_type_with_no_supported_casts_is_not_supported() {
        assert_common_error(
            "COALESCE",
            vec![
                (number("1"), Some(date())),
                (string("2024-01-01"), Some(PgType::Unknown)),
            ],
            "0A000",
            "a cast from unknown to date is not supported",
        );
        let values = vec![(number("1"), Some(date())), (null(), Some(PgType::Unknown))];
        assert_eq!(
            coerce_to_common("COALESCE", values).unwrap(),
            vec![number("1"), null()]
        );
    }

    #[test]
    fn values_of_different_types_where_one_has_no_supported_casts_are_not_supported() {
        assert_common_error(
            "GREATEST",
            vec![
                (number("1"), Some(PgType::Int4)),
                (number("2"), Some(date())),
            ],
            "0A000",
            "GREATEST with types integer and date is not supported",
        );
    }

    #[test]
    fn decision_keeps_a_value_of_the_target_type_or_for_an_unknown_target() {
        for context in [Implicit, Assignment, Explicit] {
            assert_eq!(
                cast_decision(&PgType::Int4, &PgType::Int4, context).unwrap(),
                CastDecision::Keep
            );
            assert_eq!(
                cast_decision(&PgType::Int4, &PgType::Unknown, context).unwrap(),
                CastDecision::Keep
            );
            assert_eq!(
                cast_decision(&date(), &date(), context).unwrap(),
                CastDecision::Keep
            );
        }
    }

    #[test]
    fn decision_reads_an_unknown_value_as_the_target_type() {
        for context in [Implicit, Assignment, Explicit] {
            assert_eq!(
                cast_decision(&PgType::Unknown, &PgType::Int4, context).unwrap(),
                CastDecision::FromUnknown
            );
        }
    }

    #[test]
    fn decision_names_the_method_of_a_supported_conversion() {
        assert_eq!(
            cast_decision(&PgType::Int4, &PgType::Int8, Implicit).unwrap(),
            CastDecision::Convert(CastMethod::Nothing)
        );
        assert_eq!(
            cast_decision(&PgType::Int4, &PgType::Float8, Implicit).unwrap(),
            CastDecision::Convert(CastMethod::EngineCast)
        );
        assert_eq!(
            cast_decision(&PgType::Float8, &PgType::Int4, Assignment).unwrap(),
            CastDecision::Convert(CastMethod::RoundThenCast)
        );
    }

    #[test]
    fn decision_gives_the_same_errors_as_converting() {
        let cases = [
            (PgType::Float8, PgType::Int4, Implicit),
            (PgType::Int4, PgType::Bool, Explicit),
            (PgType::Bool, PgType::Int8, Explicit),
            (PgType::Int4, date(), Explicit),
        ];
        for (source, target, context) in cases {
            let decision = cast_decision(&source, &target, context).unwrap_err();
            let converted = convert(&source, &target, context).unwrap_err();
            assert_eq!(format!("{decision:?}"), format!("{converted:?}"));
        }
    }

    #[test]
    fn assignment_decision_gives_the_column_message() {
        match assignment_decision(&PgType::Text, "col", &PgType::Int4) {
            Err(ParseError::Sqlstate { sqlstate, message }) => {
                assert_eq!(sqlstate, "42804");
                assert_eq!(
                    message,
                    "column \"col\" is of type integer but expression is of type text"
                );
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(
            assignment_decision(&PgType::Int8, "col", &PgType::Int4).unwrap(),
            CastDecision::Convert(CastMethod::Nothing)
        );
    }

    #[test]
    fn oid_and_regclass_are_found_by_name_and_shown_by_name() {
        assert_eq!(PgType::from_name("oid"), PgType::Oid);
        assert_eq!(PgType::from_name("OID"), PgType::Oid);
        assert_eq!(PgType::from_name("regclass"), PgType::Regclass);
        assert_eq!(PgType::Oid.to_string(), "oid");
        assert_eq!(PgType::Regclass.to_string(), "regclass");
        assert_eq!(
            PgType::from_name("regtype"),
            PgType::Other("regtype".to_string())
        );
    }

    #[test]
    fn oid_and_regclass_are_stored_as_integers() {
        let integer = PgType::Int4.engine_type();
        assert_eq!(PgType::Oid.engine_type(), integer);
        assert_eq!(PgType::Regclass.engine_type(), integer);
    }

    #[test]
    fn integers_convert_to_oid_and_regclass_without_changing_the_value() {
        for source in [PgType::Int4, PgType::Int8] {
            for target in [PgType::Oid, PgType::Regclass] {
                expect_expr(&source, &target, Implicit, "nothing");
            }
        }
    }

    #[test]
    fn oid_and_regclass_convert_to_integers_only_on_assignment() {
        for source in [PgType::Oid, PgType::Regclass] {
            for target in [PgType::Int4, PgType::Int8] {
                expect_expr(&source, &target, Assignment, "nothing");
            }
        }
    }

    #[test]
    fn oid_and_regclass_convert_to_each_other_implicitly() {
        expect_expr(&PgType::Oid, &PgType::Regclass, Implicit, "nothing");
        expect_expr(&PgType::Regclass, &PgType::Oid, Implicit, "nothing");
    }

    #[test]
    fn oid_converts_to_text_only_on_assignment() {
        expect_expr(&PgType::Oid, &PgType::Text, Assignment, "cast");
    }

    #[test]
    fn regclass_to_text_is_not_supported() {
        assert_error(
            convert(&PgType::Regclass, &PgType::Text, Implicit),
            "42804",
            "a value of type regclass cannot be used as text without a cast",
        );
        for context in [Assignment, Explicit] {
            assert_error(
                convert(&PgType::Regclass, &PgType::Text, context),
                "0A000",
                "a cast from regclass to text is not supported",
            );
        }
    }

    #[test]
    fn text_converts_to_regclass_by_name_even_implicitly() {
        for context in [Implicit, Assignment, Explicit] {
            let converted = convert(&PgType::Text, &PgType::Regclass, context).unwrap();
            let call = function_call("text_to_regclass", vec![number("5")]);
            assert_eq!(
                converted,
                if context == Explicit {
                    cast_to(call, &PgType::Regclass)
                } else {
                    call
                }
            );
        }
    }

    #[test]
    fn text_to_oid_is_not_supported() {
        assert_error(
            convert(&PgType::Text, &PgType::Oid, Explicit),
            "0A000",
            "a cast from text to oid is not supported",
        );
        assert_error(
            convert(&PgType::Text, &PgType::Oid, Assignment),
            "42804",
            "a value of type text cannot be used as oid without a cast",
        );
    }

    #[test]
    fn oid_and_regclass_have_no_cast_to_or_from_boolean_float_numeric_or_bytea() {
        for other in [PgType::Bool, PgType::Float8, PgType::Numeric, PgType::Bytea] {
            for special in [PgType::Oid, PgType::Regclass] {
                for (source, target) in [
                    (special.clone(), other.clone()),
                    (other.clone(), special.clone()),
                ] {
                    assert_error(
                        convert(&source, &target, Implicit),
                        "42804",
                        &format!(
                            "a value of type {source} cannot be used as {target} without a cast"
                        ),
                    );
                    assert_error(
                        convert(&source, &target, Explicit),
                        "42846",
                        &format!("cannot cast type {source} to {target}"),
                    );
                }
            }
        }
    }

    #[test]
    fn string_literal_becomes_an_oid() {
        for (text, expected) in [
            ("0", "0"),
            ("16384", "16384"),
            (" 42 ", "42"),
            ("+7", "7"),
            ("4294967295", "4294967295"),
            ("0x10", "16"),
            ("0X1f", "31"),
            ("010", "8"),
            ("-1", "4294967295"),
            ("-2147483648", "2147483648"),
            ("\t5\n", "5"),
        ] {
            assert_eq!(
                convert_text(text, &PgType::Oid).unwrap(),
                number(expected),
                "{text:?}"
            );
        }
    }

    #[test]
    fn string_literal_outside_the_oid_range_is_rejected() {
        for text in [
            "4294967296",
            "-2147483649",
            "99999999999999999999",
            "99999999999999999999x",
            "0x100000000",
        ] {
            assert_error(
                convert_text(text, &PgType::Oid),
                "22003",
                &format!("value \"{text}\" is out of range for type oid"),
            );
        }
    }

    #[test]
    fn string_literal_that_is_not_an_oid_is_rejected() {
        for text in [
            "", " ", "abc", "-", "+", "1 2", "1.5", "08", "0x", "0xg", "1x", "--1",
        ] {
            assert_error(
                convert_text(text, &PgType::Oid),
                "22P02",
                &format!("invalid input syntax for type oid: \"{text}\""),
            );
        }
    }

    #[test]
    fn explicit_cast_of_an_oid_string_literal_adds_one_cast() {
        assert_eq!(
            coerce(string("5"), Some(&PgType::Unknown), &PgType::Oid, Explicit).unwrap(),
            cast_to(number("5"), &PgType::Oid)
        );
    }

    #[test]
    fn unknown_values_read_as_regclass_through_one_function() {
        let read = |expr: ast::Expr| function_call("unknown_to_regclass", vec![expr]);
        for context in [Implicit, Assignment] {
            assert_eq!(
                coerce(
                    string("pg_class"),
                    Some(&PgType::Unknown),
                    &PgType::Regclass,
                    context
                )
                .unwrap(),
                read(string("pg_class"))
            );
            assert_eq!(
                coerce(
                    variable(),
                    Some(&PgType::Unknown),
                    &PgType::Regclass,
                    context
                )
                .unwrap(),
                read(variable())
            );
            assert_eq!(
                coerce(null(), Some(&PgType::Unknown), &PgType::Regclass, context).unwrap(),
                null()
            );
        }
        assert_eq!(
            coerce(
                string("5"),
                Some(&PgType::Unknown),
                &PgType::Regclass,
                Explicit
            )
            .unwrap(),
            cast_to(read(string("5")), &PgType::Regclass)
        );
        assert_eq!(
            coerce(null(), Some(&PgType::Unknown), &PgType::Regclass, Explicit).unwrap(),
            cast_to(null(), &PgType::Regclass)
        );
    }

    #[test]
    fn a_sign_in_front_of_an_oid_or_regclass_does_not_exist() {
        for ty in [PgType::Oid, PgType::Regclass] {
            assert_error(
                check_unary_operand("-", Some(&ty)).map(|()| null()),
                "42883",
                &format!("operator does not exist: - {ty}"),
            );
        }
    }

    #[test]
    fn common_type_of_oid_and_integers_is_oid() {
        for types in [
            vec![PgType::Int4, PgType::Oid],
            vec![PgType::Oid, PgType::Int4],
            vec![PgType::Int8, PgType::Oid],
            vec![PgType::Oid, PgType::Int8, PgType::Int4],
            vec![PgType::Int4, PgType::Unknown, PgType::Oid, PgType::Int8],
        ] {
            assert_eq!(
                common_type(&types, "CASE").unwrap(),
                PgType::Oid,
                "{types:?}"
            );
        }
    }

    #[test]
    fn common_type_of_oid_and_regclass_is_the_first_of_them() {
        assert_eq!(
            common_type(&[PgType::Oid, PgType::Regclass], "CASE").unwrap(),
            PgType::Oid
        );
        assert_eq!(
            common_type(&[PgType::Regclass, PgType::Oid], "CASE").unwrap(),
            PgType::Regclass
        );
        assert_eq!(
            common_type(&[PgType::Int4, PgType::Regclass], "CASE").unwrap(),
            PgType::Regclass
        );
    }

    #[test]
    fn common_type_of_oid_and_a_float_or_numeric_cannot_convert() {
        assert_error(
            common_type(&[PgType::Oid, PgType::Float8], "COALESCE").map(|_| null()),
            "42846",
            "COALESCE could not convert type double precision to oid",
        );
        assert_error(
            common_type(&[PgType::Float8, PgType::Oid], "COALESCE").map(|_| null()),
            "42846",
            "COALESCE could not convert type oid to double precision",
        );
        assert_error(
            common_type(&[PgType::Int4, PgType::Oid, PgType::Numeric], "CASE").map(|_| null()),
            "42846",
            "CASE could not convert type numeric to oid",
        );
    }

    #[test]
    fn common_type_of_oid_and_another_category_does_not_match() {
        assert_error(
            common_type(&[PgType::Oid, PgType::Text], "CASE").map(|_| null()),
            "42804",
            "CASE types oid and text cannot be matched",
        );
        assert_error(
            common_type(&[PgType::Regclass, PgType::Bool], "UNION").map(|_| null()),
            "42804",
            "UNION types regclass and boolean cannot be matched",
        );
    }

    #[test]
    fn common_type_of_a_later_unsupported_type_is_not_supported() {
        assert_error(
            common_type(&[PgType::Int4, PgType::Int8, date()], "COALESCE").map(|_| null()),
            "0A000",
            "COALESCE with types bigint and date is not supported",
        );
    }

    #[test]
    fn common_type_of_numbers_is_the_widest_in_every_order() {
        let numbers = [
            PgType::Int2,
            PgType::Int4,
            PgType::Int8,
            PgType::Numeric,
            PgType::Float8,
        ];
        let rank = |ty: &PgType| numbers.iter().position(|n| n == ty).unwrap();
        for mask in 1u32..(1 << 6) {
            let mut types: Vec<PgType> = (0..5)
                .filter(|i| mask & (1 << i) != 0)
                .map(|i| numbers[i].clone())
                .collect();
            if mask & 32 != 0 {
                types.insert(0, PgType::Unknown);
            }
            if types.iter().all(|t| *t == PgType::Unknown) {
                continue;
            }
            let mut orders = vec![types.clone()];
            orders.push(types.iter().rev().cloned().collect());
            for order in orders {
                let widest = order
                    .iter()
                    .filter(|t| **t != PgType::Unknown)
                    .max_by_key(|t| rank(t))
                    .unwrap();
                assert_eq!(common_type(&order, "CASE").unwrap(), *widest, "{order:?}");
            }
        }
    }

    #[test]
    fn operands_of_numbers_convert_to_the_widest_in_every_order() {
        let numbers = [
            PgType::Int2,
            PgType::Int4,
            PgType::Int8,
            PgType::Numeric,
            PgType::Float8,
        ];
        let rank = |ty: &PgType| numbers.iter().position(|n| n == ty).unwrap();
        for first in &numbers {
            for second in &numbers {
                for kind in [OperatorKind::Arithmetic, OperatorKind::Comparison] {
                    let widest = if rank(first) >= rank(second) {
                        first
                    } else {
                        second
                    };
                    let items = vec![
                        (number("1"), Some(first.clone())),
                        (number("2"), Some(second.clone())),
                    ];
                    let expected: Vec<ast::Expr> = [("1", first), ("2", second)]
                        .iter()
                        .map(|(text, ty)| coerce(number(text), Some(ty), widest, Implicit).unwrap())
                        .collect();
                    assert_eq!(
                        coerce_operands("+", kind, items).unwrap(),
                        expected,
                        "{first} {second}"
                    );
                }
            }
        }
    }

    #[test]
    fn comparison_of_oid_with_integers_and_regclass_converts_to_oid() {
        for other in [PgType::Int4, PgType::Int8, PgType::Regclass, PgType::Oid] {
            let items = vec![
                (number("1"), Some(PgType::Oid)),
                (number("2"), Some(other.clone())),
                (string("3"), Some(PgType::Unknown)),
            ];
            assert_eq!(
                coerce_operands("=", OperatorKind::Comparison, items).unwrap(),
                vec![number("1"), number("2"), number("3")],
                "{other}"
            );
        }
    }

    #[test]
    fn comparison_of_regclass_reads_a_string_literal_as_an_oid() {
        let items = vec![
            (number("1"), Some(PgType::Regclass)),
            (string("pg_class"), Some(PgType::Unknown)),
        ];
        assert_operand_error(
            coerce_operands("=", OperatorKind::Comparison, items),
            "22P02",
            "invalid input syntax for type oid: \"pg_class\"",
        );
        let items = vec![
            (number("1"), Some(PgType::Regclass)),
            (number("2"), Some(PgType::Regclass)),
        ];
        assert_eq!(
            coerce_operands("=", OperatorKind::Comparison, items).unwrap(),
            vec![number("1"), number("2")]
        );
    }

    #[test]
    fn oid_does_not_compare_with_float_numeric_text_or_boolean() {
        for other in [PgType::Float8, PgType::Numeric, PgType::Text, PgType::Bool] {
            let items = vec![
                (number("1"), Some(PgType::Oid)),
                (number("2"), Some(other.clone())),
            ];
            assert_operand_error(
                coerce_operands("=", OperatorKind::Comparison, items),
                "42883",
                &format!("operator does not exist: oid = {other}"),
            );
        }
    }

    #[test]
    fn oid_has_no_arithmetic() {
        for other in [PgType::Int4, PgType::Oid, PgType::Unknown] {
            let items = vec![
                (number("1"), Some(PgType::Oid)),
                (number("2"), Some(other.clone())),
            ];
            assert_operand_error(
                coerce_operands("+", OperatorKind::Arithmetic, items),
                "42883",
                &format!("operator does not exist: oid + {other}"),
            );
        }
    }

    #[test]
    fn concat_converts_an_oid_to_text() {
        let items = vec![
            (number("1"), Some(PgType::Oid)),
            (string("x"), Some(PgType::Text)),
        ];
        assert_eq!(
            coerce_operands("||", OperatorKind::Concat, items).unwrap(),
            vec![cast_to(number("1"), &PgType::Text), string("x")]
        );
    }

    #[test]
    fn the_internal_char_type_is_not_the_same_as_bpchar() {
        assert_eq!(PgType::from_name("char"), PgType::Char);
        assert_eq!(PgType::from_name("CHAR"), PgType::Char);
        assert_eq!(
            PgType::from_name("bpchar"),
            PgType::Other("bpchar".to_string())
        );
        assert_eq!(PgType::Char.to_string(), "\"char\"");
        assert_eq!(PgType::Char.engine_type(), PgType::Text.engine_type());
    }

    #[test]
    fn string_literal_becomes_a_char() {
        for (text, expected) in [
            ("s", "s"),
            ("r", "r"),
            ("abc", "a"),
            ("", ""),
            ("\\163", "s"),
            ("\\000", ""),
            ("\\1634", "\\"),
            ("'", "'"),
        ] {
            assert_eq!(
                convert_text(text, &PgType::Char).unwrap(),
                string(expected),
                "{text:?}"
            );
        }
    }

    #[test]
    fn string_literal_that_is_not_one_ascii_byte_as_a_char_is_not_supported() {
        for text in ["\\377", "\\200", "\u{e9}", "\u{e9}x"] {
            assert_error(
                convert_text(text, &PgType::Char),
                "0A000",
                &format!("the value \"{text}\" for type \"char\" is not supported"),
            );
        }
    }

    #[test]
    fn explicit_cast_of_a_char_string_literal_adds_one_cast() {
        assert_eq!(
            coerce(string("s"), Some(&PgType::Unknown), &PgType::Char, Explicit).unwrap(),
            cast_to(string("s"), &PgType::Char)
        );
    }

    #[test]
    fn char_converts_to_text_without_changing_the_value() {
        expect_expr(&PgType::Char, &PgType::Text, Implicit, "nothing");
    }

    #[test]
    fn char_has_no_other_supported_cast() {
        for target in [PgType::Int4, PgType::Bool, PgType::Float8, PgType::Bytea] {
            assert_error(
                convert(&PgType::Char, &target, Explicit),
                "0A000",
                &format!("a cast from \"char\" to {target} is not supported"),
            );
        }
        assert_error(
            convert(&PgType::Text, &PgType::Char, Explicit),
            "0A000",
            "a cast from text to \"char\" is not supported",
        );
    }

    #[test]
    fn common_type_of_chars_is_char() {
        assert_eq!(
            common_type(&[PgType::Char, PgType::Unknown, PgType::Char], "CASE").unwrap(),
            PgType::Char
        );
    }

    #[test]
    fn comparison_of_char_with_a_string_literal_runs_on_text() {
        let items = vec![
            (number("1"), Some(PgType::Char)),
            (string("x"), Some(PgType::Unknown)),
        ];
        assert_eq!(
            coerce_operands("=", OperatorKind::Comparison, items).unwrap(),
            vec![number("1"), string("x")]
        );
    }

    #[test]
    fn common_type_of_char_and_text_is_text_in_either_order() {
        for types in [[PgType::Char, PgType::Text], [PgType::Text, PgType::Char]] {
            assert_eq!(common_type(&types, "CASE").unwrap(), PgType::Text);
        }
    }

    #[test]
    fn smallint_is_found_by_name_and_stored_as_a_smallint() {
        for name in ["int2", "smallint", "SMALLINT"] {
            assert_eq!(PgType::from_name(name), PgType::Int2, "{name}");
        }
        assert_eq!(PgType::Int2.to_string(), "smallint");
        assert_eq!(
            PgType::Int2.engine_type().map(|ty| ty.name),
            Some("smallint".to_string())
        );
    }

    #[test]
    fn smallint_converts_up_without_a_check_and_down_with_one() {
        expect_expr(&PgType::Int2, &PgType::Int4, Implicit, "nothing");
        expect_expr(&PgType::Int2, &PgType::Int8, Implicit, "nothing");
        expect_expr(&PgType::Int2, &PgType::Float8, Implicit, "cast");
        expect_expr(&PgType::Int2, &PgType::Numeric, Implicit, "cast");
        expect_expr(&PgType::Int2, &PgType::Text, Assignment, "cast");
        for source in [PgType::Int4, PgType::Int8, PgType::Float8, PgType::Numeric] {
            let checked = function_call("number_to_int2", vec![number("5")]);
            assert_eq!(
                convert(&source, &PgType::Int2, Assignment).unwrap(),
                checked
            );
            assert_eq!(
                convert(&source, &PgType::Int2, Explicit).unwrap(),
                cast_to(checked, &PgType::Int2)
            );
            assert_error(
                convert(&source, &PgType::Int2, Implicit),
                "42804",
                &format!("a value of type {source} cannot be used as smallint without a cast"),
            );
        }
        for target in [PgType::Oid, PgType::Regclass] {
            expect_expr(&PgType::Int2, &target, Implicit, "nothing");
        }
        assert_eq!(
            convert(&PgType::Int2, &PgType::Regproc, Implicit).unwrap(),
            number("5")
        );
    }

    #[test]
    fn smallint_has_no_cast_to_or_from_boolean_bytea_or_an_oid_type() {
        for other in [PgType::Bool, PgType::Bytea, PgType::Oid, PgType::Regclass] {
            for (source, target) in [(PgType::Int2, other.clone()), (other.clone(), PgType::Int2)] {
                if matches!(other, PgType::Oid | PgType::Regclass) && source == PgType::Int2 {
                    continue;
                }
                assert_error(
                    convert(&source, &target, Explicit),
                    "42846",
                    &format!("cannot cast type {source} to {target}"),
                );
            }
        }
    }

    #[test]
    fn string_literal_becomes_a_smallint() {
        assert_eq!(convert_text(" -7 ", &PgType::Int2).unwrap(), number("-7"));
        assert_eq!(
            convert_text("32767", &PgType::Int2).unwrap(),
            number("32767")
        );
        assert_error(
            convert_text("32768", &PgType::Int2),
            "22003",
            "value \"32768\" is out of range for type smallint",
        );
        assert_error(
            convert_text("x", &PgType::Int2),
            "22P02",
            "invalid input syntax for type smallint: \"x\"",
        );
    }

    #[test]
    fn smallint_and_integers_compare_and_combine_as_integers() {
        let items = vec![
            (number("1"), Some(PgType::Int2)),
            (number("2"), Some(PgType::Int4)),
        ];
        for kind in [OperatorKind::Comparison, OperatorKind::Arithmetic] {
            assert_eq!(
                coerce_operands("<", kind, items.clone()).unwrap(),
                vec![number("1"), number("2")]
            );
        }
        assert_eq!(
            common_type(&[PgType::Int2, PgType::Int4], "CASE").unwrap(),
            PgType::Int4
        );
        assert_eq!(
            common_type(&[PgType::Int4, PgType::Int2], "CASE").unwrap(),
            PgType::Int4
        );
    }

    fn expect_expr_without_cast(source: &PgType, target: &PgType, weakest: CastContext) {
        assert_eq!(
            convert(source, target, weakest).unwrap(),
            number("5"),
            "{source} to {target}"
        );
        let explicit = convert(source, target, Explicit).unwrap();
        let wrapped = target.engine_type().is_some();
        assert_eq!(
            explicit,
            if wrapped {
                cast_to(number("5"), target)
            } else {
                number("5")
            },
            "{source} to {target} explicit"
        );
    }

    #[test]
    fn regnamespace_and_regproc_are_oid_aliases() {
        assert_eq!(PgType::from_name("regnamespace"), PgType::Regnamespace);
        assert_eq!(PgType::from_name("regproc"), PgType::Regproc);
        assert_eq!(PgType::Regnamespace.to_string(), "regnamespace");
        assert_eq!(PgType::Regproc.to_string(), "regproc");
        assert_eq!(
            PgType::Regnamespace.engine_type(),
            PgType::Int4.engine_type()
        );
        assert_eq!(PgType::Regproc.engine_type(), None);
        for alias in [PgType::Regnamespace, PgType::Regproc] {
            expect_expr_without_cast(&PgType::Int4, &alias, Implicit);
            expect_expr_without_cast(&alias, &PgType::Int8, Assignment);
            expect_expr_without_cast(&PgType::Oid, &alias, Implicit);
            expect_expr_without_cast(&alias, &PgType::Oid, Implicit);
        }
    }

    #[test]
    fn two_oid_aliases_other_than_oid_have_no_cast_between_them() {
        assert_error(
            convert(&PgType::Regclass, &PgType::Regproc, Explicit),
            "42846",
            "cannot cast type regclass to regproc",
        );
        assert_error(
            convert(&PgType::Regnamespace, &PgType::Regclass, Implicit),
            "42804",
            "a value of type regnamespace cannot be used as regclass without a cast",
        );
    }

    #[test]
    fn unknown_values_read_as_regnamespace_through_one_function() {
        let read = |expr: ast::Expr| function_call("unknown_to_regnamespace", vec![expr]);
        assert_eq!(
            coerce(
                string("pg_catalog"),
                Some(&PgType::Unknown),
                &PgType::Regnamespace,
                Implicit
            )
            .unwrap(),
            read(string("pg_catalog"))
        );
        assert_eq!(
            coerce(
                variable(),
                Some(&PgType::Unknown),
                &PgType::Regnamespace,
                Assignment
            )
            .unwrap(),
            read(variable())
        );
    }

    #[test]
    fn text_converts_to_regproc_only_with_an_explicit_cast() {
        assert_eq!(
            convert(&PgType::Text, &PgType::Regproc, Explicit).unwrap(),
            number("5")
        );
        assert_error(
            convert(&PgType::Text, &PgType::Regproc, Assignment),
            "42804",
            "a value of type text cannot be used as regproc without a cast",
        );
        assert_error(
            convert(&PgType::Regproc, &PgType::Text, Assignment),
            "0A000",
            "a cast from regproc to text is not supported",
        );
    }

    #[test]
    fn a_string_literal_stays_a_string_for_regproc() {
        assert_eq!(
            convert_text("bthandler", &PgType::Regproc).unwrap(),
            string("bthandler")
        );
    }

    const ACCEPTED_INTEGERS: &[(&str, i64)] = &[
        ("42", 42),
        (" 42 ", 42),
        ("+7", 7),
        ("-7", -7),
        ("0", 0),
        ("007", 7),
        ("2147483647", 2147483647),
        ("-2147483648", -2147483648),
        ("0x1F", 31),
        ("0X1f", 31),
        ("0o17", 15),
        ("0O17", 15),
        ("0b101", 5),
        ("0B101", 5),
        ("1_000", 1000),
        ("0x1_F", 31),
        ("0x_1", 1),
        ("-0x10", -16),
        ("  +0b1_1  ", 3),
        ("\t42\n", 42),
        ("\u{b}\u{c}\r7", 7),
        ("0_1", 1),
    ];

    const REJECTED_INTEGERS: &[&str] = &[
        "abc", "", " ", "1.5", "1e3", "+", "-", "0x", "0xg", "0b2", "0o8", "1__0", "_1", "1_",
        "0x1__1", "4 2", "--1", "+-1", "- 1", "0x_", "1\u{a0}", "\u{a0}1",
    ];

    #[test]
    fn integer_text_reads_what_postgresql_reads() {
        for (text, expected) in ACCEPTED_INTEGERS {
            assert_eq!(parse_int4(text).unwrap(), *expected, "{text:?}");
            assert_eq!(parse_int8(text).unwrap(), *expected, "{text:?}");
            assert_eq!(
                convert_text(text, &PgType::Int4).unwrap(),
                number(&expected.to_string()),
                "{text:?}"
            );
        }
    }

    #[test]
    fn integer_text_that_is_not_a_number_is_invalid_for_every_size() {
        for text in REJECTED_INTEGERS {
            for (name, parse) in [
                (
                    "smallint",
                    parse_int2 as fn(&str) -> Result<i64, ParseError>,
                ),
                ("integer", parse_int4),
                ("bigint", parse_int8),
            ] {
                assert_error(
                    parse(text).map(|_| null()),
                    "22P02",
                    &format!("invalid input syntax for type {name}: \"{text}\""),
                );
            }
        }
    }

    #[test]
    fn integer_text_outside_the_range_is_out_of_range() {
        for (text, name, parse) in [
            (
                "32768",
                "smallint",
                parse_int2 as fn(&str) -> Result<i64, ParseError>,
            ),
            ("-32769", "smallint", parse_int2),
            ("2147483648", "integer", parse_int4),
            ("-2147483649", "integer", parse_int4),
            ("0x80000000", "integer", parse_int4),
            ("0b100000000000000000000000000000000", "integer", parse_int4),
            ("99999999999999999999", "integer", parse_int4),
            ("9223372036854775808", "bigint", parse_int8),
            ("-9223372036854775809", "bigint", parse_int8),
            ("0x8000000000000000", "bigint", parse_int8),
            ("99999999999999999999999999", "bigint", parse_int8),
        ] {
            assert_error(
                parse(text).map(|_| null()),
                "22003",
                &format!("value \"{text}\" is out of range for type {name}"),
            );
        }
    }

    #[test]
    fn integer_text_at_the_edge_of_the_range_is_accepted() {
        assert_eq!(parse_int2("-32768").unwrap(), -32768);
        assert_eq!(parse_int2("32767").unwrap(), 32767);
        assert_eq!(parse_int4("-2147483648").unwrap(), i64::from(i32::MIN));
        assert_eq!(parse_int8("9223372036854775807").unwrap(), i64::MAX);
        assert_eq!(parse_int8("-9223372036854775808").unwrap(), i64::MIN);
        assert_eq!(parse_int8("0x7FFFFFFFFFFFFFFF").unwrap(), i64::MAX);
        assert_eq!(parse_int8("-0x8000000000000000").unwrap(), i64::MIN);
    }

    #[test]
    fn integer_text_that_is_too_big_is_out_of_range_even_when_garbage_follows() {
        assert_error(
            parse_int4("99999999999x").map(|_| null()),
            "22003",
            "value \"99999999999x\" is out of range for type integer",
        );
        assert_error(
            parse_int4("2147483649x").map(|_| null()),
            "22P02",
            "invalid input syntax for type integer: \"2147483649x\"",
        );
    }
}
