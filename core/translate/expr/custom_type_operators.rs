//! Operators on custom types, such as `+`, `&` and `-x`: which function runs
//! them.
//!
//! [find_operator_call] decides. Code generation calls it to emit the call and
//! `type_of` calls it to know the type of the result, so the two always agree.
//! Nothing is written into the expression tree.

use super::*;
use crate::dialect::{ChosenOperator, Dialect, EngineType, OperandType, OperatorChoice};
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

/// Asks the dialect to choose the operator of each operator in `expr` and
/// wraps each string literal operand in a CAST to the type the chosen
/// operator takes, as PostgreSQL reads `1 = '1'` as `1 = '1'::int4`. The
/// operators are listed in [operator_uses]. This runs once the columns of
/// `expr` are bound and before the optimizer sees the operators.
pub(crate) fn read_literal_operands(
    expr: &mut ast::Expr,
    tables: Option<&TableReferences>,
    resolver: &Resolver<'_>,
) -> Result<()> {
    if resolver.schema().type_registry.is_empty() {
        return Ok(());
    }
    let context = ExprContext::new(tables, resolver);
    let dialect = resolver.catalog.dialect.as_ref();
    walk_expr_mut(expr, &mut |expr: &mut ast::Expr| -> Result<WalkControl> {
        let (mut operands, uses) = operator_uses(expr);
        let mut types = vec![None; operands.len()];
        {
            let shared: Vec<&ast::Expr> = operands.iter().map(|operand| &**operand).collect();
            let env = context.type_env();
            for operator in &uses {
                read_as_chosen_types(operator, &shared, &env, dialect, &mut types)?;
            }
        }
        for (operand, ty) in operands.iter_mut().zip(types) {
            let Some(ty) = ty else {
                continue;
            };
            let literal = std::mem::replace(*operand, ast::Expr::Literal(ast::Literal::Null));
            **operand = ast::Expr::Cast {
                expr: Box::new(literal),
                type_name: Some(ty),
            };
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// Asks the dialect to choose the `=` of `lhs IN (SELECT rhs ...)`, where
/// `rhs` is a column of the subquery and `rhs_type` its type. A string literal
/// selected by the subquery is text, as in PostgreSQL, so it is not read as the
/// type of `lhs`.
pub(crate) fn check_in_subquery_operator(
    lhs: &ast::Expr,
    lhs_env: &TypeEnv,
    rhs: &ast::Expr,
    rhs_env: &TypeEnv,
    rhs_type: ExprType,
    resolver: &Resolver<'_>,
) -> Result<()> {
    if resolver.schema().type_registry.is_empty() {
        return Ok(());
    }
    let lhs_type = type_of(lhs, lhs_env);
    if matches!(lhs_type, ExprType::Null) || matches!(rhs_type, ExprType::Null) {
        return Ok(());
    }
    let operand_types = || {
        let rhs_operand = match dialect_operand_type(rhs, rhs_type.clone(), rhs_env) {
            OperandType::StringLiteral(_) => OperandType::Engine {
                ty: EngineType::Text,
                array_dimensions: 0,
            },
            other => other,
        };
        vec![
            dialect_operand_type(lhs, lhs_type.clone(), lhs_env),
            rhs_operand,
        ]
    };
    let choice = resolver
        .catalog
        .dialect
        .choose_operator("=", &operand_types)?;
    // FIXME(registered-types): Only the comparison of an expression with an
    // expression is cast, so an operand of an IN subquery that the choice says
    // to cast is refused. The comparison of two values has no single place
    // that all comparisons go through yet. Once it has, the subquery's result
    // column is cast there.
    if let OperatorChoice::Chosen(chosen) = &choice {
        if chosen.cast_operands.iter().any(|cast| *cast) {
            return Err(LimboError::ParseError(format!(
                "cannot compare type {lhs_type} with type {rhs_type}"
            )));
        }
    }
    Ok(())
}

/// Asks the dialect to choose `operator` and records, for each string literal
/// among its operands, the type the dialect chose for it. The first type
/// recorded for an operand stays.
fn read_as_chosen_types(
    operator: &OperatorUse,
    operands: &[&ast::Expr],
    env: &TypeEnv,
    dialect: &dyn Dialect,
    types: &mut [Option<ast::Type>],
) -> Result<()> {
    let operand_types = |slots: &[usize], null: OperandType| -> Vec<OperandType> {
        slots
            .iter()
            .map(|&slot| operand_type(operands[slot], env, null.clone()))
            .collect()
    };
    let choose_pair = |slots: &[usize]| {
        dialect.choose_operator(operator.name, &|| {
            operand_types(slots, OperandType::Unnamed)
        })
    };
    match operator.kind {
        UseKind::Each => {
            let choice = choose_pair(&operator.operands)?;
            record_chosen_types(choice, &operator.operands, operands, types);
        }
        UseKind::ListValues => {
            // A NULL fits any type, like a parameter.
            let choice = dialect.choose_list_operator(operator.name, &|| {
                operand_types(&operator.operands, OperandType::Untyped)
            })?;
            match choice {
                Some(choice) => record_chosen_types(choice, &operator.operands, operands, types),
                None => {
                    for &value in &operator.operands[1..] {
                        let slots = [operator.operands[0], value];
                        let choice = choose_pair(&slots)?;
                        record_chosen_types(choice, &slots, operands, types);
                    }
                }
            }
        }
    }
    Ok(())
}

/// What the dialect is told about `expr`: its type, `null` when it is NULL,
/// and no type when it is a hidden column, which the engine compares with the
/// argument itself.
fn operand_type(expr: &ast::Expr, env: &TypeEnv, null: OperandType) -> OperandType {
    if is_hidden_column(expr, env) {
        return OperandType::Unnamed;
    }
    match type_of(expr, env) {
        ExprType::Null => null,
        ty => dialect_operand_type(expr, ty, env),
    }
}

fn record_chosen_types(
    choice: OperatorChoice,
    slots: &[usize],
    operands: &[&ast::Expr],
    types: &mut [Option<ast::Type>],
) {
    let OperatorChoice::Chosen(chosen) = choice else {
        return;
    };
    for (index, (&slot, ty)) in slots.iter().zip(chosen.operands).enumerate() {
        let is_string_literal = matches!(
            without_wrappers(operands[slot]),
            ast::Expr::Literal(ast::Literal::String(_))
        );
        let is_cast = chosen.cast_operands.get(index).copied().unwrap_or(false);
        if (is_string_literal || is_cast) && types[slot].is_none() {
            types[slot] = Some(ty);
        }
    }
}

/// An operator that `expr` applies: its name and which of the operands of
/// [operator_uses] it takes, one for a prefix operator and two otherwise.
struct OperatorUse {
    name: &'static str,
    operands: Vec<usize>,
    kind: UseKind,
}

enum UseKind {
    /// The operator takes exactly the operands.
    Each,
    /// The first operand is compared with all the other operands as the
    /// values of an IN list, which a dialect may read as one type.
    ListValues,
}

/// The operands of `expr` and the operators it applies to them: the
/// arithmetic, bit and comparison operators, the prefix `-` and `~`, the two
/// comparisons of a BETWEEN, the `=` of each value of an IN list and of each
/// WHEN of a CASE with a base, the `=` of NULLIF, a comparison of two row
/// values as one comparison of each pair, and `IS [NOT] DISTINCT FROM` next
/// to a string literal.
///
/// Every kind of expression is named, so a new one must say whether it
/// applies an operator.
fn operator_uses(expr: &mut ast::Expr) -> (Vec<&mut ast::Expr>, Vec<OperatorUse>) {
    match expr {
        ast::Expr::Binary(lhs, op, rhs) => {
            let name = comparison_symbol(op)
                .or_else(|| binary_symbol(op))
                .or_else(|| distinct_from_symbol(op, rhs));
            let Some(name) = name else {
                return (vec![lhs.as_mut(), rhs.as_mut()], Vec::new());
            };
            let row_values = match (lhs.as_ref(), rhs.as_ref()) {
                (ast::Expr::Parenthesized(left), ast::Expr::Parenthesized(right)) => {
                    left.len() > 1 && left.len() == right.len()
                }
                _ => false,
            };
            if !row_values {
                return (
                    vec![lhs.as_mut(), rhs.as_mut()],
                    first_with_each(name, 1..2),
                );
            }
            let (ast::Expr::Parenthesized(left), ast::Expr::Parenthesized(right)) =
                (lhs.as_mut(), rhs.as_mut())
            else {
                unreachable!("both sides are row values")
            };
            let count = left.len();
            let uses = (0..count)
                .map(|i| OperatorUse {
                    name,
                    operands: vec![i, count + i],
                    kind: UseKind::Each,
                })
                .collect();
            let operands = left
                .iter_mut()
                .chain(right.iter_mut())
                .map(|value| value.as_mut())
                .collect();
            (operands, uses)
        }
        ast::Expr::Unary(op, operand) => {
            let uses = prefix_symbol(op)
                .map(|name| {
                    vec![OperatorUse {
                        name,
                        operands: vec![0],
                        kind: UseKind::Each,
                    }]
                })
                .unwrap_or_default();
            (vec![operand.as_mut()], uses)
        }
        ast::Expr::Between {
            lhs, start, end, ..
        } => {
            let mut uses = first_with_each(">=", 1..2);
            uses.extend(first_with_each("<=", 2..3));
            (vec![lhs.as_mut(), start.as_mut(), end.as_mut()], uses)
        }
        ast::Expr::InList { lhs, not, rhs } => {
            let name = if *not { "<>" } else { "=" };
            let row_len = match lhs.as_ref() {
                ast::Expr::Parenthesized(left) if left.len() > 1 => Some(left.len()),
                _ => None,
            };
            let are_rows_of_that_length = |row_len: usize| {
                rhs.iter().all(|value| {
                    matches!(value.as_ref(), ast::Expr::Parenthesized(row) if row.len() == row_len)
                })
            };
            if let Some(row_len) = row_len.filter(|&len| are_rows_of_that_length(len)) {
                let uses = (0..rhs.len())
                    .flat_map(|row| {
                        (0..row_len).map(move |i| OperatorUse {
                            name,
                            operands: vec![i, row_len + row * row_len + i],
                            kind: UseKind::Each,
                        })
                    })
                    .collect();
                let ast::Expr::Parenthesized(left) = lhs.as_mut() else {
                    unreachable!("the row value is parenthesized")
                };
                let mut operands: Vec<&mut ast::Expr> =
                    left.iter_mut().map(|value| value.as_mut()).collect();
                for value in rhs.iter_mut() {
                    let ast::Expr::Parenthesized(row) = value.as_mut() else {
                        unreachable!("every value is a row value")
                    };
                    operands.extend(row.iter_mut().map(|value| value.as_mut()));
                }
                return (operands, uses);
            }
            let (with_columns, without_columns): (Vec<usize>, Vec<usize>) =
                (1..=rhs.len()).partition(|&slot| reads_a_column(&rhs[slot - 1]));
            let mut uses = first_with_each(name, with_columns);
            if !without_columns.is_empty() {
                let mut operands = vec![0];
                operands.extend(&without_columns);
                uses.push(OperatorUse {
                    name,
                    operands,
                    kind: UseKind::ListValues,
                });
            }
            let operands = std::iter::once(lhs.as_mut())
                .chain(rhs.iter_mut().map(|value| value.as_mut()))
                .collect();
            (operands, uses)
        }
        ast::Expr::Case {
            base: Some(base),
            when_then_pairs,
            ..
        } => {
            let uses = first_with_each("=", 1..when_then_pairs.len() + 1);
            let operands = std::iter::once(base.as_mut())
                .chain(when_then_pairs.iter_mut().map(|(when, _)| when.as_mut()))
                .collect();
            (operands, uses)
        }
        ast::Expr::FunctionCall { name, args, .. }
            if name.as_str().eq_ignore_ascii_case("nullif") && args.len() == 2 =>
        {
            let operands = args.iter_mut().map(|arg| arg.as_mut()).collect();
            (operands, first_with_each("=", 1..2))
        }
        // The comparison with the values of a subquery is asked when the
        // subquery is planned: see `in_subquery_collation`.
        ast::Expr::InSelect { .. }
        | ast::Expr::InTable { .. }
        | ast::Expr::SubqueryResult { .. }
        | ast::Expr::Case { base: None, .. }
        | ast::Expr::FunctionCall { .. }
        | ast::Expr::Cast { .. }
        | ast::Expr::Collate(..)
        | ast::Expr::Column { .. }
        | ast::Expr::Default
        | ast::Expr::DoublyQualified(..)
        | ast::Expr::Exists(_)
        | ast::Expr::FieldAccess { .. }
        | ast::Expr::FunctionCallStar { .. }
        | ast::Expr::Id(_)
        | ast::Expr::IsNull(_)
        | ast::Expr::Like { .. }
        | ast::Expr::Literal(_)
        | ast::Expr::MergedColumn(_)
        | ast::Expr::Name(_)
        | ast::Expr::NotNull(_)
        | ast::Expr::Parenthesized(_)
        | ast::Expr::Qualified(..)
        | ast::Expr::Raise(..)
        | ast::Expr::Register { .. }
        | ast::Expr::RowId { .. }
        | ast::Expr::Subquery(_)
        | ast::Expr::Variable(_) => (Vec::new(), Vec::new()),
        ast::Expr::Array { .. } | ast::Expr::Subscript { .. } => {
            unreachable!("Array and Subscript are desugared into function calls by the parser")
        }
    }
}

/// One use of `name` for the first operand and each operand in `others`.
fn first_with_each(
    name: &'static str,
    others: impl IntoIterator<Item = usize>,
) -> Vec<OperatorUse> {
    others
        .into_iter()
        .map(|operand| OperatorUse {
            name,
            operands: vec![0, operand],
            kind: UseKind::Each,
        })
        .collect()
}

/// Whether `expr` reads a column of the query, as an item of an IN list does
/// that PostgreSQL compares with the left side alone.
fn reads_a_column(expr: &ast::Expr) -> bool {
    let mut found = false;
    let _ = walk_expr(expr, &mut |expr: &ast::Expr| -> Result<WalkControl> {
        found |= matches!(
            expr,
            ast::Expr::Column { .. }
                | ast::Expr::RowId { .. }
                | ast::Expr::MergedColumn(_)
                | ast::Expr::Id(_)
                | ast::Expr::Name(_)
                | ast::Expr::Qualified(..)
                | ast::Expr::DoublyQualified(..)
        );
        Ok(WalkControl::Continue)
    });
    found
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

/// `=` for `IS` and `IS NOT`, which is how `IS [NOT] DISTINCT FROM` reads. A
/// truth test such as `x IS TRUE` compares nothing, so no operator is chosen.
fn distinct_from_symbol(op: &ast::Operator, rhs: &ast::Expr) -> Option<&'static str> {
    let is_distinct_from =
        matches!(op, ast::Operator::Is | ast::Operator::IsNot) && truth_test_rhs(rhs).is_none();
    is_distinct_from.then_some("=")
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
    if let Some(call) = call_the_dialect_chose(op, &operands, env)? {
        return Ok(Some(call));
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
        // FIXME(pg-catalog): This is a second table of operators, beside pg_operator, and it fails with a plain parse error where the PostgreSQL frontend must give 42883. For the PostgreSQL frontend, the pg_operator row decides whether the operator exists and which function runs it (its oprcode, once pg_proc has PostgreSQL's function oids), and this lookup is not asked.
        let type_names: Vec<String> = operands.iter().map(Operand::type_name).collect();
        return Err(match env.resolver() {
            Some(resolver) => resolver
                .catalog
                .dialect
                .undefined_operator_error(op, &type_names),
            None => {
                LimboError::ParseError(crate::dialect::undefined_operator_message(op, &type_names))
            }
        });
    };
    call_for(signature, ty, env)
}

/// The call of the function that the dialect names for `op` on `operands`,
/// or `None` when the dialect names none.
fn call_the_dialect_chose(
    op: &'static str,
    operands: &[Operand],
    env: &TypeEnv,
) -> Result<Option<OperatorCall>> {
    let Some(resolver) = env.resolver() else {
        return Ok(None);
    };
    let operand_types = || {
        operands
            .iter()
            .map(|operand| dialect_operand_type(operand.expr, operand.expr_type.clone(), env))
            .collect()
    };
    let choice = resolver
        .catalog
        .dialect
        .choose_operator(op, &operand_types)?;
    let OperatorChoice::Chosen(ChosenOperator {
        function: Some(function),
        ..
    }) = choice
    else {
        return Ok(None);
    };
    let Some(func) = env.resolve_function(&function.name, operands.len())? else {
        return Err(LimboError::InternalError(format!(
            "function {} for operator {op} does not exist",
            function.name
        )));
    };
    if !func.is_deterministic() {
        return Err(LimboError::InternalError(format!(
            "function {} for operator {op} must be deterministic",
            function.name
        )));
    }
    Ok(Some(OperatorCall {
        func,
        encode: vec![None; operands.len()],
        result: function.result,
    }))
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
    expr: &'a ast::Expr,
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
            ExprType::Builtin(BuiltinType::Boolean) => {
                env.schema()
                    .type_of_conditions()
                    .map(|type_def| ColumnCustomType {
                        type_def: Arc::clone(type_def),
                        params: Vec::new(),
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
            expr,
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
        OperatorArg::Real => is_plain_value(signature, operand) && operand.is_real,
        OperatorArg::Registered(name) => operand
            .value_type
            .as_ref()
            .is_some_and(|t| t.type_def.name.eq_ignore_ascii_case(name)),
    }
}

fn is_plain_value(signature: &OperatorSignature, operand: &Operand) -> bool {
    operand
        .value_type
        .as_ref()
        .is_none_or(|t| has_sqlite_operator(&t.type_def, signature))
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
