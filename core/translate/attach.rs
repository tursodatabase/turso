use crate::function::{Func, ScalarFunc};
use crate::translate::{
    emitter::Resolver,
    expr::{sanitize_string, translate_expr},
    semantic_lowering::PlannedSelect,
    ProgramBuilder, ProgramBuilderOpts,
};
use crate::util::normalize_ident;
use crate::vdbe::insn::Insn;
use crate::{sync::Arc, Connection, Result};
use turso_parser::ast::{Expr, Literal};

/// The HIR settings ATTACH and DETACH evaluate their arguments with, when
/// HIR is on.
use crate::translate::HirContext;

/// How an ATTACH or DETACH argument gets its value.
enum Argument<'a> {
    /// A string literal, a name or a dotted name is the text written.
    Text(String),
    /// Anything else is evaluated.
    Value(&'a Expr),
    /// ATTACH without KEY passes NULL.
    Null,
}

impl<'a> Argument<'a> {
    fn new(expr: &'a Expr) -> Self {
        match expr {
            Expr::Literal(Literal::String(s)) => Self::Text(sanitize_string(s)),
            Expr::Qualified(_, _) => Self::Text(expr.to_string()),
            // normalize_ident strips the quotes of a double-quoted name.
            Expr::Id(id) => Self::Text(normalize_ident(id.as_str())),
            _ => Self::Value(expr),
        }
    }
}

/// Write each argument into consecutive registers from `start`. With HIR,
/// the evaluated arguments run as one `SELECT` planned through HIR.
fn emit_arguments(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    connection: &Arc<Connection>,
    hir: HirContext<'_>,
    arguments: &[Argument<'_>],
    start: usize,
) -> Result<()> {
    let values: Vec<_> = arguments
        .iter()
        .filter_map(|argument| match argument {
            Argument::Value(expr) => Some((*expr).clone()),
            _ => None,
        })
        .collect();
    let mut hir_values = if values.is_empty() {
        None
    } else {
        let (schema, settings) = hir;
        let select = PlannedSelect::values(values, schema, settings, connection)?;
        Some(select.emit_values(program)?)
    };
    for (i, argument) in arguments.iter().enumerate() {
        let dest = start + i;
        match argument {
            Argument::Text(value) => {
                program.emit_insn(Insn::String8 {
                    value: value.clone(),
                    dest,
                });
            }
            Argument::Value(expr) => match &mut hir_values {
                Some(value) => {
                    program.emit_insn(Insn::Copy {
                        src_reg: *value,
                        dst_reg: dest,
                        extra_amount: 0,
                    });
                    *value += 1;
                }
                None => {
                    translate_expr(program, None, expr, dest, resolver)?;
                }
            },
            Argument::Null => {
                program.emit_insn(Insn::Null {
                    dest,
                    dest_end: None,
                });
            }
        }
    }
    Ok(())
}

/// Translate ATTACH statement
pub fn translate_attach(
    expr: &Expr,
    resolver: &Resolver,
    db_name: &Expr,
    key: &Option<Box<Expr>>,
    program: &mut ProgramBuilder,
    connection: Arc<Connection>,
    hir: HirContext<'_>,
) -> Result<()> {
    if !connection.experimental_attach_enabled() {
        return Err(crate::LimboError::ParseError(
            "ATTACH is an experimental feature. Enable with --experimental-attach flag".to_string(),
        ));
    }

    // SQLite treats ATTACH as a function call to sqlite_attach(filename, dbname, key)
    // We'll allocate registers for the arguments and call the function
    program.extend(&ProgramBuilderOpts::new(0, 10, 0));

    let arg_reg = program.alloc_registers(4); // 3 for args + 1 for result

    // Like SQLite, a KEY that is a name is that text, case kept; anything
    // else is evaluated.
    let key = match key.as_deref() {
        Some(Expr::Id(id)) => Argument::Text(id.as_str().to_string()),
        Some(key) => Argument::Value(key),
        None => Argument::Null,
    };
    emit_arguments(
        program,
        resolver,
        &connection,
        hir,
        &[Argument::new(expr), Argument::new(db_name), key],
        arg_reg,
    )?;

    // Call sqlite_attach function
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: arg_reg,
        dest: arg_reg + 3, // Result register (not used but required)
        func: crate::function::FuncCtx {
            func: Func::Scalar(ScalarFunc::Attach),
            arg_count: 3,
        },
    });

    Ok(())
}

/// Translate DETACH statement
pub fn translate_detach(
    expr: &Expr,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: Arc<Connection>,
    hir: HirContext<'_>,
) -> Result<()> {
    if !connection.experimental_attach_enabled() {
        return Err(crate::LimboError::ParseError(
            "DETACH is an experimental feature. Enable with --experimental-attach flag".to_string(),
        ));
    }
    // SQLite treats DETACH as a function call to sqlite_detach(dbname)
    program.extend(&ProgramBuilderOpts::new(0, 5, 0));

    let arg_reg = program.alloc_registers(2); // 1 for arg + 1 for result

    emit_arguments(
        program,
        resolver,
        &connection,
        hir,
        &[Argument::new(expr)],
        arg_reg,
    )?;

    // Call sqlite_detach function
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: arg_reg,
        dest: arg_reg + 1, // Result register (not used but required)
        func: crate::function::FuncCtx {
            func: Func::Scalar(ScalarFunc::Detach),
            arg_count: 1,
        },
    });

    Ok(())
}
