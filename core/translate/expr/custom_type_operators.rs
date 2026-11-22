//! Operators on custom types, such as `+`, `&` and `-x`: which function runs
//! them.
//!
//! [find_operator_call] decides. Code generation calls it to emit the call and
//! `type_of` calls it to know the type of the result, so the two always agree.
//! Nothing is written into the expression tree.

use super::*;
use crate::dialect::OperatorResolution;
use crate::function::Deterministic;
use crate::schema::{OperatorArg, OperatorSignature, BINARY_OPERATORS, PREFIX_OPERATORS};
use crate::translate::expr::expr_type::{
    dialect_operand_type, same_type_def, sqlite_operand_type, type_of, without_wrappers,
    BuiltinType, ColumnCustomType, ExprType, TypeEnv,
};

/// The function that runs the operator expression `expr`, such as `a + b` or
/// `-a`, for code that has not computed the operands' types yet. See
/// [find_operator_call].
pub(crate) fn custom_operator_call(
    expr: &ast::Expr,
    env: &TypeEnv,
) -> Result<Option<OperatorCall>> {
    let Some(types) = operand_types(expr, env) else {
        return Ok(None);
    };
    find_operator_call(expr, &types, env)
}

/// Fails when no function runs the operator expression `expr`, as in
/// `date * 2`, for an expression that is bound but not yet compiled. An
/// operand whose type is not known yet, such as a subquery that is not planned
/// yet, leaves the decision to a check once it is known.
pub(crate) fn check_custom_operator(expr: &ast::Expr, env: &TypeEnv) -> Result<()> {
    let Some(types) = operand_types(expr, env) else {
        return Ok(());
    };
    if types.iter().any(|ty| matches!(ty, ExprType::NotKnownYet)) {
        return Ok(());
    }
    find_operator_call(expr, &types, env).map(|_| ())
}

/// Fails when the dialect has no comparison for the operands of `expr`, as
/// PostgreSQL has none for `text = integer`. The comparisons of `expr` are
/// its own operator, the two of a BETWEEN, and the `=` of each value of an
/// IN list. An operand whose type is not known yet leaves the decision to a
/// check once it is known.
pub(crate) fn check_comparison(expr: &ast::Expr, env: &TypeEnv) -> Result<()> {
    let Some(resolver) = env.resolver() else {
        return Ok(());
    };
    for (lhs, symbol, rhs) in comparisons_of(expr) {
        if is_hidden_column(lhs, env) || is_hidden_column(rhs, env) {
            continue;
        }
        let (lhs_type, rhs_type) = (type_of(lhs, env), type_of(rhs, env));
        if matches!(lhs_type, ExprType::NotKnownYet) || matches!(rhs_type, ExprType::NotKnownYet) {
            continue;
        }
        let operand_types = || {
            vec![
                dialect_operand_type(&lhs_type),
                dialect_operand_type(&rhs_type),
            ]
        };
        if resolver
            .catalog
            .dialect
            .resolve_operator(symbol, &operand_types)
            == OperatorResolution::Reject
        {
            crate::bail_parse_error!("operator does not exist: {lhs_type} {symbol} {rhs_type}");
        }
    }
    Ok(())
}

/// Casts the string literals that meet an operand of another type to that
/// type when the dialect says so, as PostgreSQL reads `int_column = '5'` as
/// `int_column = '5'::int4`. A literal that is not a value of the type then
/// fails as the cast does. This runs on every expression a statement binds,
/// once its columns are bound and before the optimizer sees the operators.
pub(crate) fn cast_untyped_operands(
    expr: &mut ast::Expr,
    tables: Option<&TableReferences>,
    resolver: &Resolver<'_>,
) -> Result<()> {
    if resolver.schema().type_registry.is_empty() {
        return Ok(());
    }
    let context = ExprContext::new(tables, resolver);
    walk_expr_mut(expr, &mut |expr: &mut ast::Expr| -> Result<WalkControl> {
        let casts = literal_casts(expr, &context.type_env())?;
        for (slot, type_name) in casts {
            let operand = operand_slots(expr).swap_remove(slot);
            if let ast::Expr::Literal(ast::Literal::String(text)) = without_wrappers(operand) {
                resolver
                    .catalog
                    .dialect
                    .check_literal_cast(&sanitize_string(text), &type_name)?;
            }
            let literal = std::mem::replace(operand, ast::Expr::Literal(ast::Literal::Null));
            *operand = ast::Expr::Cast {
                expr: Box::new(literal),
                type_name: Some(ast::Type {
                    name: type_name,
                    size: None,
                    array_dimensions: 0,
                }),
            };
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// The operands of an operator, a BETWEEN, an IN list, a LIKE or a CASE, by
/// slot. A CASE with an operand has it first, then the WHEN values; without
/// one, only the WHEN conditions.
fn operand_slots(expr: &mut ast::Expr) -> Vec<&mut ast::Expr> {
    match expr {
        ast::Expr::Binary(lhs, _, rhs) | ast::Expr::Like { lhs, rhs, .. } => {
            vec![lhs.as_mut(), rhs.as_mut()]
        }
        ast::Expr::Unary(_, operand) => vec![operand.as_mut()],
        ast::Expr::Between {
            lhs, start, end, ..
        } => vec![lhs.as_mut(), start.as_mut(), end.as_mut()],
        ast::Expr::InList { lhs, rhs, .. } => std::iter::once(lhs.as_mut())
            .chain(rhs.iter_mut().map(|value| value.as_mut()))
            .collect(),
        ast::Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            let mut slots: Vec<&mut ast::Expr> =
                base.iter_mut().map(|base| base.as_mut()).collect();
            let (whens, thens): (Vec<_>, Vec<_>) = when_then_pairs
                .iter_mut()
                .map(|(when, then)| (when.as_mut(), then.as_mut()))
                .unzip();
            slots.extend(whens);
            slots.extend(thens);
            slots.extend(else_expr.iter_mut().map(|value| value.as_mut()));
            slots
        }
        ast::Expr::FunctionCall { name, args, .. } if unifies_arguments(name, args.len()) => {
            args.iter_mut().map(|arg| arg.as_mut()).collect()
        }
        _ => Vec::new(),
    }
}

/// The operators that `expr` applies to its operand slots, as pairs of slots
/// with the operator's name, and the slots that must be booleans with the
/// name of what needs the boolean.
struct OperatorUses<'a> {
    slots: Vec<&'a ast::Expr>,
    pairs: Vec<(usize, &'static str, usize)>,
    booleans: Vec<(usize, &'static str)>,
    prefixes: Vec<(usize, &'static str)>,
    unified: Vec<usize>,
}

fn operator_uses<'a>(expr: &'a ast::Expr) -> Option<OperatorUses<'a>> {
    let mut uses = OperatorUses {
        slots: Vec::new(),
        pairs: Vec::new(),
        booleans: Vec::new(),
        prefixes: Vec::new(),
        unified: Vec::new(),
    };
    match expr {
        ast::Expr::Binary(lhs, op, rhs) => {
            uses.slots = vec![lhs.as_ref(), rhs.as_ref()];
            match op {
                ast::Operator::And => uses.booleans = vec![(0, "AND"), (1, "AND")],
                ast::Operator::Or => uses.booleans = vec![(0, "OR"), (1, "OR")],
                ast::Operator::Is | ast::Operator::IsNot => {
                    let tested = match rhs.as_ref() {
                        ast::Expr::Literal(ast::Literal::True) => Some("TRUE"),
                        ast::Expr::Literal(ast::Literal::False) => Some("FALSE"),
                        _ => None,
                    };
                    match (tested, op) {
                        (Some("TRUE"), ast::Operator::Is) => uses.booleans = vec![(0, "IS TRUE")],
                        (Some("TRUE"), _) => uses.booleans = vec![(0, "IS NOT TRUE")],
                        (Some(_), ast::Operator::Is) => uses.booleans = vec![(0, "IS FALSE")],
                        (Some(_), _) => uses.booleans = vec![(0, "IS NOT FALSE")],
                        (None, _) => uses.pairs = vec![(0, "=", 1)],
                    }
                }
                _ => {
                    let symbol = comparison_symbol(op).or_else(|| binary_symbol(op))?;
                    uses.pairs = vec![(0, symbol, 1)];
                }
            }
        }
        ast::Expr::Unary(op, operand) => {
            uses.slots = vec![operand.as_ref()];
            match op {
                ast::UnaryOperator::Not => uses.booleans = vec![(0, "NOT")],
                ast::UnaryOperator::Negative => uses.prefixes = vec![(0, "-")],
                ast::UnaryOperator::Positive => {}
                ast::UnaryOperator::BitwiseNot => uses.prefixes = vec![(0, "~")],
            }
        }
        ast::Expr::Like { lhs, op, rhs, .. } => {
            let symbol = match op {
                ast::LikeOperator::Like | ast::LikeOperator::Glob => "~~",
                ast::LikeOperator::Regexp => "~",
                ast::LikeOperator::Match => return None,
            };
            uses.slots = vec![lhs.as_ref(), rhs.as_ref()];
            uses.pairs = vec![(0, symbol, 1)];
        }
        ast::Expr::Between {
            lhs, start, end, ..
        } => {
            uses.slots = vec![lhs.as_ref(), start.as_ref(), end.as_ref()];
            uses.pairs = vec![(0, ">=", 1), (0, "<=", 2)];
        }
        ast::Expr::InList { lhs, rhs, .. } => {
            uses.slots = std::iter::once(lhs.as_ref())
                .chain(rhs.iter().map(|value| value.as_ref()))
                .collect();
            uses.pairs = (1..=rhs.len()).map(|index| (0, "=", index)).collect();
        }
        ast::Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            let whens = when_then_pairs.iter().map(|(when, _)| when.as_ref());
            let results = when_then_pairs
                .iter()
                .map(|(_, then)| then.as_ref())
                .chain(else_expr.as_deref());
            let conditions: Vec<&ast::Expr> = match base {
                Some(base) => std::iter::once(base.as_ref()).chain(whens).collect(),
                None => whens.collect(),
            };
            match base {
                Some(_) => {
                    uses.pairs = (1..=when_then_pairs.len())
                        .map(|index| (0, "=", index))
                        .collect();
                }
                None => {
                    uses.booleans = (0..when_then_pairs.len())
                        .map(|index| (index, "CASE/WHEN"))
                        .collect();
                }
            }
            let first_result = conditions.len();
            uses.slots = conditions.into_iter().chain(results).collect();
            uses.unified = (first_result..uses.slots.len()).collect();
        }
        ast::Expr::FunctionCall { name, args, .. } if unifies_arguments(name, args.len()) => {
            uses.slots = args.iter().map(|arg| arg.as_ref()).collect();
            uses.unified = (0..args.len()).collect();
        }
        _ => return None,
    }
    Some(uses)
}

/// The slots of `expr` whose string literal the dialect casts, with the type
/// to cast to. Fails when the dialect has no operator for the operands, or an
/// operand that must be a boolean is another type.
fn literal_casts(expr: &ast::Expr, env: &TypeEnv) -> Result<Vec<(usize, String)>> {
    let (Some(resolver), Some(uses)) = (env.resolver(), operator_uses(expr)) else {
        return Ok(Vec::new());
    };
    let OperatorUses {
        slots,
        pairs,
        booleans,
        prefixes,
        unified,
    } = uses;
    if slots.iter().any(|slot| is_hidden_column(slot, env)) {
        return Ok(Vec::new());
    }
    let types: Vec<ExprType> = slots.iter().map(|slot| type_of(slot, env)).collect();
    let mut casts: Vec<(usize, String)> = Vec::new();
    let mut add_cast = |slot: usize, type_name: &str, any_expression: bool| {
        let is_literal = matches!(
            without_wrappers(slots[slot]),
            ast::Expr::Literal(ast::Literal::String(_))
        );
        if (is_literal || any_expression) && !casts.iter().any(|(done, _)| *done == slot) {
            casts.push((slot, type_name.to_string()));
        }
    };
    for (left, symbol, right) in pairs {
        if matches!(types[left], ExprType::NotKnownYet)
            || matches!(types[right], ExprType::NotKnownYet)
        {
            continue;
        }
        let operand_types = || {
            vec![
                dialect_operand_type(&types[left]),
                dialect_operand_type(&types[right]),
            ]
        };
        match resolver
            .catalog
            .dialect
            .resolve_operator(symbol, &operand_types)
        {
            OperatorResolution::Keep => {}
            OperatorResolution::Reject => {
                let (lhs, rhs) = (&types[left], &types[right]);
                crate::bail_parse_error!("operator does not exist: {lhs} {symbol} {rhs}");
            }
            OperatorResolution::Ambiguous => {
                crate::bail_parse_error!(
                    "operator is not unique: {} {symbol} {}",
                    types[left],
                    types[right]
                );
            }
            OperatorResolution::CastUntyped(type_name) => {
                add_cast(left, &type_name, false);
                add_cast(right, &type_name, false);
            }
            OperatorResolution::CastOperands(left_type, right_type) => {
                for (slot, type_name) in [(left, left_type), (right, right_type)] {
                    if let Some(type_name) = type_name {
                        add_cast(slot, &type_name, true);
                    }
                }
            }
        }
    }
    if unified.len() > 1 {
        let unified_types = || {
            unified
                .iter()
                .map(|slot| dialect_operand_type(&types[*slot]))
                .collect()
        };
        if let Some(type_name) = resolver.catalog.dialect.common_untyped_type(&unified_types) {
            for slot in unified {
                add_cast(slot, &type_name, false);
            }
        }
    }
    for (slot, symbol) in prefixes {
        if matches!(types[slot], ExprType::NotKnownYet) {
            continue;
        }
        let operand_types = || vec![dialect_operand_type(&types[slot])];
        match resolver
            .catalog
            .dialect
            .resolve_operator(symbol, &operand_types)
        {
            OperatorResolution::Keep => {}
            OperatorResolution::CastUntyped(type_name) => add_cast(slot, &type_name, false),
            OperatorResolution::Reject => {
                let ty = &types[slot];
                crate::bail_parse_error!("operator does not exist: {symbol} {ty}");
            }
            OperatorResolution::Ambiguous => {
                crate::bail_parse_error!("operator is not unique: {symbol} unknown");
            }
            OperatorResolution::CastOperands(..) => {}
        }
    }
    for (slot, what) in booleans {
        // A column of a SQLite type may hold a boolean, as the columns of the
        // catalogs do, so only a literal or a value of a registered type is
        // refused.
        let is_exact = matches!(types[slot], ExprType::Custom { .. })
            || matches!(without_wrappers(slots[slot]), ast::Expr::Literal(_));
        if !is_exact {
            continue;
        }
        match resolver
            .catalog
            .dialect
            .resolve_boolean_argument(&dialect_operand_type(&types[slot]))
        {
            OperatorResolution::Keep => {}
            OperatorResolution::Reject => {
                let ty = &types[slot];
                crate::bail_parse_error!("argument of {what} must be type boolean, not type {ty}");
            }
            OperatorResolution::CastUntyped(type_name) => add_cast(slot, &type_name, false),
            OperatorResolution::Ambiguous | OperatorResolution::CastOperands(..) => {}
        }
    }
    Ok(casts)
}

/// Whether a call of `name` gives one of its arguments back, so they must
/// have one type: coalesce, nullif and the scalar min and max.
fn unifies_arguments(name: &ast::Name, argument_count: usize) -> bool {
    let name = name.as_str().to_ascii_lowercase();
    match name.as_str() {
        "coalesce" | "nullif" => argument_count > 1,
        "min" | "max" => argument_count > 1,
        _ => false,
    }
}

/// The comparisons that `expr` makes: left operand, operator, right operand.
fn comparisons_of(expr: &ast::Expr) -> Vec<(&ast::Expr, &'static str, &ast::Expr)> {
    match expr {
        ast::Expr::Binary(lhs, op, rhs) => comparison_symbol(op)
            .map(|symbol| (lhs.as_ref(), symbol, rhs.as_ref()))
            .into_iter()
            .collect(),
        ast::Expr::Between {
            lhs, start, end, ..
        } => vec![
            (lhs.as_ref(), ">=", start.as_ref()),
            (lhs.as_ref(), "<=", end.as_ref()),
        ],
        ast::Expr::InList { lhs, rhs, .. } => rhs
            .iter()
            .map(|value| (lhs.as_ref(), "=", value.as_ref()))
            .collect(),
        _ => Vec::new(),
    }
}

/// Whether `expr` is a hidden column, such as the argument of a table
/// function, which the engine compares with the argument itself.
fn is_hidden_column(expr: &ast::Expr, env: &TypeEnv) -> bool {
    let ast::Expr::Column { table, column, .. } = without_wrappers(expr) else {
        return false;
    };
    env.tables()
        .and_then(|tables| tables.find_table_by_internal_id(*table))
        .and_then(|(_, table)| table.get_column_at(*column))
        .is_some_and(|column| column.hidden())
}

fn comparison_symbol(op: &ast::Operator) -> Option<&'static str> {
    use ast::Operator;
    Some(match op {
        Operator::Equals => "=",
        Operator::NotEquals => "<>",
        Operator::Less => "<",
        Operator::LessEquals => "<=",
        Operator::Greater => ">",
        Operator::GreaterEquals => ">=",
        _ => return None,
    })
}

/// The types of the operands of `expr`, in order, when it is an operator that
/// custom types decide (see [decided_operator]).
fn operand_types(expr: &ast::Expr, env: &TypeEnv) -> Option<Vec<ExprType>> {
    let (_, operands) = decided_operator(expr, env)?;
    Some(
        operands
            .iter()
            .map(|operand| type_of(operand, env))
            .collect(),
    )
}

/// The function that runs the operator expression `expr` when an operand is a
/// custom-type value. `None` means SQLite's operator runs: `expr` is not an
/// operator a custom type decides, no operand has a custom type, an operand is
/// NULL, or the signature says so (`smallint`, `bigint`). When no signature
/// fits, as in `date + date`, `date * 2` or `-date`, it is an error: running
/// SQLite's operator on the value would give an answer nobody asked for.
///
/// `types` are the types of the operands, in order, which `type_of` passes in so
/// that it computes each operand's type once.
pub(super) fn find_operator_call(
    expr: &ast::Expr,
    types: &[ExprType],
    env: &TypeEnv,
) -> Result<Option<OperatorCall>> {
    let Some((op, operands)) = decided_operator(expr, env) else {
        return Ok(None);
    };
    assert_eq!(operands.len(), types.len(), "one type per operand");
    // An operator on NULL gives NULL whichever function runs it.
    if types.iter().any(|ty| matches!(ty, ExprType::Null)) {
        return Ok(None);
    }
    if let Some(resolver) = env.resolver() {
        let operand_types = || types.iter().map(dialect_operand_type).collect();
        if resolver
            .catalog
            .dialect
            .resolve_operator(op, &operand_types)
            == OperatorResolution::Reject
        {
            match types {
                [operand] => crate::bail_parse_error!("operator does not exist: {op} {operand}"),
                [lhs, rhs] => {
                    crate::bail_parse_error!("operator does not exist: {lhs} {op} {rhs}")
                }
                _ => unreachable!("an operator has one or two operands"),
            }
        }
    }
    if let Some(resolver) = env.resolver() {
        let operand_types = || types.iter().map(dialect_operand_type).collect();
        if resolver
            .catalog
            .dialect
            .uses_sqlite_arithmetic(op, &operand_types)
        {
            return Ok(None);
        }
    }
    let operands = operands
        .iter()
        .zip(types)
        .map(|(operand, ty)| Operand::new(operand, ty, env))
        .collect::<Result<Vec<_>>>()?;
    let custom_types: Vec<&ColumnCustomType> = operands
        .iter()
        .filter_map(|operand| operand.value_type.as_ref())
        .collect();
    if custom_types.is_empty() {
        return Ok(None);
    }
    let found = custom_types
        .iter()
        .flat_map(|ty| {
            ty.type_def
                .operator_signatures(op, operands.len())
                .map(move |signature| (signature, *ty))
        })
        .find(|(signature, ty)| accepts(signature, ty, &operands));
    let Some((signature, ty)) = found else {
        match operands.as_slice() {
            [operand] => {
                crate::bail_parse_error!("operator does not exist: {op} {}", operand.type_name())
            }
            [lhs, rhs] => crate::bail_parse_error!(
                "operator does not exist: {} {op} {}",
                lhs.type_name(),
                rhs.type_name()
            ),
            _ => unreachable!("an operator has one or two operands"),
        }
    };
    call_for(signature, ty, env)
}

/// The symbol and operands of `expr` when it is an operator that custom types
/// decide: `-x` is `-` with one operand, `a & b` is `&` with two. Comparisons
/// use the order of the operands' type (see `comparison_collation`), and unary
/// `+` gives its operand unchanged.
///
/// Without custom types no operand can have one, so plain SQLite does no more
/// work per operator than before custom types existed.
fn decided_operator<'a>(
    expr: &'a ast::Expr,
    env: &TypeEnv,
) -> Option<(&'static str, Vec<&'a ast::Expr>)> {
    if env.schema().type_registry.is_empty() {
        return None;
    }
    let (op, operands, known) = match expr {
        ast::Expr::Binary(lhs, op, rhs) => (
            binary_symbol(op)?,
            vec![lhs.as_ref(), rhs.as_ref()],
            BINARY_OPERATORS.as_slice(),
        ),
        ast::Expr::Unary(op, operand) => (
            prefix_symbol(op)?,
            vec![operand.as_ref()],
            PREFIX_OPERATORS.as_slice(),
        ),
        _ => return None,
    };
    assert!(known.contains(&op), "custom types do not decide {op}");
    Some((op, operands))
}

fn binary_symbol(op: &ast::Operator) -> Option<&'static str> {
    use ast::Operator;
    Some(match op {
        Operator::Add => "+",
        Operator::Subtract => "-",
        Operator::Multiply => "*",
        Operator::Divide => "/",
        Operator::Modulus => "%",
        Operator::BitwiseAnd => "&",
        Operator::BitwiseOr => "|",
        Operator::LeftShift => "<<",
        Operator::RightShift => ">>",
        _ => return None,
    })
}

fn prefix_symbol(op: &ast::UnaryOperator) -> Option<&'static str> {
    match op {
        ast::UnaryOperator::Negative => Some("-"),
        ast::UnaryOperator::BitwiseNot => Some("~"),
        ast::UnaryOperator::Positive | ast::UnaryOperator::Not => None,
    }
}

/// What a signature needs to know about an operand. A domain is replaced by the
/// custom type it is based on, if any.
struct Operand<'a> {
    /// The custom type of the operand's value: a column, a CAST, the result of
    /// another operator.
    value_type: Option<ColumnCustomType>,
    /// The kind of literal the operand is, for literals: "integer", "text", ...
    literal_kind: Option<&'a str>,
    /// Whether SQLite's arithmetic would see an integer, or a value whose type
    /// is not known before the statement runs.
    may_be_integer: bool,
    /// Whether the operand is a REAL value. A decimal written in the statement
    /// is not one: as in PostgreSQL, it is an exact number.
    is_real: bool,
    /// Whether the operand is a blob, which is not a number.
    is_blob: bool,
    /// The operand's type, as `type_of` gives it.
    expr_type: &'a ExprType,
}

impl<'a> Operand<'a> {
    fn new(expr: &'a ast::Expr, expr_type: &'a ExprType, env: &TypeEnv) -> Result<Self> {
        let expr = without_wrappers(expr);
        // A domain has the operators of the type it is based on. A domain over a
        // plain SQLite type, such as `CREATE DOMAIN pos AS integer`, has SQLite's
        // operators.
        let value_type = match expr_type {
            ExprType::Custom { type_def, params } => {
                env.schema()
                    .without_domains(type_def)?
                    .map(|type_def| ColumnCustomType {
                        type_def,
                        params: params.clone(),
                    })
            }
            _ => None,
        };
        let sqlite_type = sqlite_operand_type(expr_type.clone());
        let may_be_integer = matches!(
            sqlite_type,
            ExprType::Builtin(BuiltinType::Integer | BuiltinType::Numeric | BuiltinType::Any)
        );
        let is_real = matches!(sqlite_type, ExprType::Builtin(BuiltinType::Real))
            && !matches!(expr_type, ExprType::Builtin(BuiltinType::Decimal));
        let is_blob = matches!(sqlite_type, ExprType::Builtin(BuiltinType::Blob));
        Ok(Self {
            value_type,
            literal_kind: literal_type_name(expr),
            may_be_integer,
            is_real,
            is_blob,
            expr_type,
        })
    }

    /// The operand's type for an error message: `date`, `integer`, ...
    fn type_name(&self) -> String {
        match (&self.value_type, self.expr_type) {
            (Some(ty), _) => ty.type_def.name.clone(),
            // A value whose type is only known when the statement runs, such as
            // the result of json_extract().
            (
                None,
                ExprType::Builtin(BuiltinType::Any) | ExprType::Unknown | ExprType::NotKnownYet,
            ) => "value".to_string(),
            (None, ty) => ty.to_string(),
        }
    }
}

/// Whether `signature`, declared by `ty`, accepts every operand.
fn accepts(signature: &OperatorSignature, ty: &ColumnCustomType, operands: &[Operand]) -> bool {
    signature
        .args
        .iter()
        .zip(operands)
        .all(|(arg, operand)| accepts_operand(*arg, signature, ty, operand))
}

fn accepts_operand(
    arg: OperatorArg,
    signature: &OperatorSignature,
    ty: &ColumnCustomType,
    operand: &Operand,
) -> bool {
    let is_this_type =
        |t: Option<&ColumnCustomType>| t.is_some_and(|t| same_type_def(&t.type_def, &ty.type_def));
    match arg {
        OperatorArg::Value => is_this_type(operand.value_type.as_ref()),
        OperatorArg::EncodableLiteral => {
            operand.value_type.is_none()
                && operand
                    .literal_kind
                    .is_some_and(|kind| takes_input_of_type(&ty.type_def, kind))
        }
        OperatorArg::PlainValue => is_plain_value(signature, operand),
        OperatorArg::Integer => is_plain_value(signature, operand) && operand.may_be_integer,
        OperatorArg::Exact => {
            is_plain_value(signature, operand) && !operand.is_real && !operand.is_blob
        }
        OperatorArg::Untyped => operand.value_type.is_none(),
        OperatorArg::UntypedInteger => operand.value_type.is_none() && operand.may_be_integer,
        OperatorArg::Type(name) => operand
            .value_type
            .as_ref()
            .is_some_and(|t| t.type_def.name.eq_ignore_ascii_case(name)),
        OperatorArg::Real => is_plain_value(signature, operand) && operand.is_real,
    }
}

fn is_plain_value(signature: &OperatorSignature, operand: &Operand) -> bool {
    operand.value_type.as_ref().is_none_or(|t| {
        (t.type_def.is_builtin && t.type_def.stores_plain_integers())
            || has_sqlite_operator(&t.type_def, signature)
    })
}

/// A type whose only way to run the operator of `signature` is SQLite's, such
/// as `smallint`. numeric runs SQLite's `+` with a REAL, but its own function
/// otherwise, so `smallint + numeric` must not run SQLite's `+`.
fn has_sqlite_operator(type_def: &TypeDef, signature: &OperatorSignature) -> bool {
    let mut signatures = type_def
        .operator_signatures(signature.op, signature.args.len())
        .peekable();
    signatures.peek().is_some() && signatures.all(|s| s.func.is_none())
}

/// The call that `signature` makes, or `None` for SQLite's operator.
fn call_for(
    signature: &OperatorSignature,
    ty: &ColumnCustomType,
    env: &TypeEnv,
) -> Result<Option<OperatorCall>> {
    let Some(func) = &signature.func else {
        return Ok(None);
    };
    let Some(function) = env.resolve_function(func, signature.args.len())? else {
        return Err(LimboError::InternalError(format!(
            "function {func} for operator {} on type {} does not exist",
            signature.op, ty.type_def.name
        )));
    };
    let encode = signature
        .args
        .iter()
        .map(|arg| match arg {
            OperatorArg::EncodableLiteral if ty.type_def.encode().is_some() => {
                Some(Arc::clone(&ty.type_def))
            }
            _ => None,
        })
        .collect();
    // Checks such as "is this expression constant" treat `a + b` as SQLite's
    // operator, which gives the same result for the same operands. A type's
    // operator must too.
    if !function.is_deterministic() {
        return Err(LimboError::InternalError(format!(
            "operator {} on type {} must be deterministic",
            signature.op, ty.type_def.name
        )));
    }
    let Some(result) = signature.result else {
        return Err(LimboError::InternalError(format!(
            "function {func} for operator {} on type {} has no result type",
            signature.op, ty.type_def.name
        )));
    };
    let result = ast::Type {
        name: result.to_string(),
        size: None,
        array_dimensions: 0,
    };
    Ok(Some(OperatorCall {
        func: function,
        encode,
        result,
    }))
}

pub(crate) struct OperatorCall {
    func: Func,
    /// For each operand, the type to encode it to before the call.
    encode: Vec<Option<Arc<TypeDef>>>,
    /// The type the function returns.
    pub(super) result: ast::Type,
}

/// Emits `call` on the operands of `expr` and returns the register holding its
/// result.
pub(super) fn emit_operator_call(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    call: &OperatorCall,
    resolver: &Resolver,
) -> Result<usize> {
    let operands: Vec<&ast::Expr> = match expr {
        ast::Expr::Binary(lhs, _, rhs) => vec![lhs, rhs],
        ast::Expr::Unary(_, operand) => vec![operand],
        _ => unreachable!("a custom type's function only runs operators"),
    };
    assert_eq!(
        operands.len(),
        call.encode.len(),
        "one argument per operand"
    );
    // An operand is translated into its own register and encoded into the
    // argument register: a literal may be translated into a register that is
    // set once and read on every row, so it must not be encoded in place.
    let start_reg = program.alloc_registers(operands.len());
    for (i, (operand, encode)) in operands.into_iter().zip(&call.encode).enumerate() {
        match encode {
            Some(to) => {
                let value_reg = program.alloc_register();
                translate_expr(program, referenced_tables, operand, value_reg, resolver)?;
                emit_encode(program, to, value_reg, start_reg + i, resolver)?;
            }
            None => {
                translate_expr(program, referenced_tables, operand, start_reg + i, resolver)?;
            }
        }
    }
    let result_reg = program.alloc_register();
    emit_function(program, call, start_reg, result_reg);
    Ok(result_reg)
}

/// Emits `call` on operands that are already in registers, one register per
/// operand, and writes the result to `dest`.
pub(crate) fn emit_operator_call_on_registers(
    program: &mut ProgramBuilder,
    call: &OperatorCall,
    operand_regs: &[usize],
    dest: usize,
    resolver: &Resolver,
) -> Result<()> {
    assert_eq!(
        operand_regs.len(),
        call.encode.len(),
        "one argument per operand"
    );
    let start_reg = program.alloc_registers(operand_regs.len());
    for (i, (&operand_reg, encode)) in operand_regs.iter().zip(&call.encode).enumerate() {
        match encode {
            Some(to) => {
                emit_encode(program, to, operand_reg, start_reg + i, resolver)?;
            }
            None => program.emit_insn(Insn::Copy {
                src_reg: operand_reg,
                dst_reg: start_reg + i,
                extra_amount: 0,
            }),
        }
    }
    emit_function(program, call, start_reg, dest);
    Ok(())
}

fn emit_function(program: &mut ProgramBuilder, call: &OperatorCall, start_reg: usize, dest: usize) {
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg,
        dest,
        func: FuncCtx {
            func: call.func.clone(),
            arg_count: call.encode.len(),
            collation: CollationSeq::BINARY,
        },
    });
}

/// Converts an operand to the type. The column's type parameters are not
/// applied: `n = 3.504` on a numeric(10,2) column compares with 3.504, not
/// with 3.50.
fn emit_encode(
    program: &mut ProgramBuilder,
    to: &Arc<TypeDef>,
    value_reg: usize,
    dest_reg: usize,
    resolver: &Resolver,
) -> Result<()> {
    if to.encode().is_none() {
        return Err(LimboError::InternalError(format!(
            "type {} has no ENCODE",
            to.name
        )));
    }
    let resolved = resolver
        .schema()
        .resolve_type_unchecked(&to.name)?
        .ok_or_else(|| LimboError::InternalError(format!("type {} does not resolve", to.name)))?;
    program.emit_insn(Insn::Copy {
        src_reg: value_reg,
        dst_reg: dest_reg,
        extra_amount: 0,
    });
    emit_encode_for_comparison(
        program,
        std::slice::from_ref(to),
        resolved.storage_class()?,
        dest_reg,
        resolver,
    )
}
