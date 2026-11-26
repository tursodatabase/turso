//! Function calls that the dialect chooses by the types of the arguments.
//!
//! [choose_function_call] decides. Code generation calls it to emit the call
//! and `type_of` calls it to know the type of the result, so the two always
//! agree.

use super::custom_type_operators::operand_type;
use super::expr_type::TypeEnv;
use super::*;
use crate::dialect::{ChosenFunction, FunctionChoice, OperandType};

#[allow(clippy::vec_box)]
pub(super) struct ChosenCall {
    pub(super) func: Func,
    /// The arguments of the call, each cast to the type the function takes.
    pub(super) arguments: Vec<Box<ast::Expr>>,
    pub(super) result_type: Option<ast::Type>,
}

/// The call of the function that the dialect chose for `name(args)`, or `None`
/// when the engine's own rules run it.
pub(super) fn choose_function_call(
    name: &str,
    args: &[Box<ast::Expr>],
    env: &TypeEnv,
) -> Result<Option<ChosenCall>> {
    let Some(resolver) = env.resolver() else {
        return Ok(None);
    };
    if let Some(func) = env.resolve_function(name, args.len())? {
        if func.is_aggregate() || matches!(func, Func::Window(_)) {
            return Ok(None);
        }
    }
    let argument_types = || {
        args.iter()
            .map(|arg| operand_type(arg, env, OperandType::Untyped))
            .collect()
    };
    let FunctionChoice::Chosen(ChosenFunction {
        implementation,
        argument_types,
        result_type,
    }) = resolver
        .catalog
        .dialect
        .choose_function(name, &argument_types)?
    else {
        return Ok(None);
    };
    if argument_types.len() != args.len() {
        return Err(LimboError::InternalError(format!(
            "the dialect chose {implementation} with {} argument types for a call with {} arguments",
            argument_types.len(),
            args.len()
        )));
    }
    let Some(func) = env.resolve_function(&implementation, args.len())? else {
        crate::bail_parse_error!("no such function: {implementation}");
    };
    let arguments = args
        .iter()
        .zip(argument_types)
        .map(|(arg, ty)| {
            Box::new(ast::Expr::Cast {
                expr: arg.clone(),
                type_name: Some(ty),
            })
        })
        .collect();
    Ok(Some(ChosenCall {
        func,
        arguments,
        result_type,
    }))
}
