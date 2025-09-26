use crate::function::{Func, ScalarFunc};
use crate::translate::emitter::Resolver;
use crate::translate::expr::{sanitize_string, translate_expr};
use crate::translate::{ProgramBuilder, ProgramBuilderOpts};
use crate::util::normalize_ident;
use crate::vdbe::insn::Insn;
use crate::Result;
use turso_parser::ast::{Expr, Literal};

/// Translate ATTACH statement
/// SQLite implements ATTACH as a function call to sqlite_attach()
pub fn translate_attach(
    expr: &Expr,
    resolver: &Resolver,
    db_name: &Expr,
    key: &Option<Box<Expr>>,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    // SQLite treats ATTACH as a function call to sqlite_attach(filename, dbname, key)
    // We'll allocate registers for the arguments and call the function

    program.extend(&ProgramBuilderOpts {
        num_cursors: 0,
        approx_num_insns: 10,
        approx_num_labels: 0,
    });

    let arg_reg = program.alloc_registers(4); // 3 for args + 1 for result

    // Load filename argument
    // Handle different expression types as string literals for filenames
    match expr {
        Expr::Literal(Literal::String(s)) => {
            // For ATTACH, string literals should be used directly (without quotes)
            program.emit_insn(Insn::String8 {
                value: sanitize_string(s),
                dest: arg_reg,
            });
        }
        Expr::Qualified(_, _) => {
            // For ATTACH, qualified expressions like "foo.db" should be treated as filename strings
            let filename = format!("{expr}");
            program.emit_insn(Insn::String8 {
                value: filename,
                dest: arg_reg,
            });
        }
        Expr::Id(id) => {
            // For ATTACH, identifiers should be treated as filename strings
            // Use normalize_ident to strip quotes from double-quoted identifiers
            program.emit_insn(Insn::String8 {
                value: normalize_ident(id.as_str()),
                dest: arg_reg,
            });
        }
        _ => {
            translate_expr(&mut program, None, expr, arg_reg, resolver)?;
        }
    }

    // Load database name argument
    // Handle different expression types as string literals for database names
    match db_name {
        Expr::Literal(Literal::String(s)) => {
            // For ATTACH, string literals should be used directly (without quotes)
            program.emit_insn(Insn::String8 {
                value: sanitize_string(s),
                dest: arg_reg + 1,
            });
        }
        Expr::Qualified(_, _) => {
            // For ATTACH, qualified expressions should be treated as name strings
            let db_name_str = format!("{db_name}");
            program.emit_insn(Insn::String8 {
                value: db_name_str,
                dest: arg_reg + 1,
            });
        }
        Expr::Id(id) => {
            // For ATTACH, identifiers should be treated as name strings
            // Use normalize_ident to strip quotes from double-quoted identifiers
            program.emit_insn(Insn::String8 {
                value: normalize_ident(id.as_str()),
                dest: arg_reg + 1,
            });
        }
        _ => {
            translate_expr(&mut program, None, db_name, arg_reg + 1, resolver)?;
        }
    }

    // Load key argument (NULL if not provided)
    if let Some(key_expr) = key {
        translate_expr(&mut program, None, key_expr, arg_reg + 2, resolver)?;
    } else {
        program.emit_insn(Insn::Null {
            dest: arg_reg + 2,
            dest_end: None,
        });
    }

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

    Ok(program)
}

/// Translate DETACH statement
/// SQLite implements DETACH as a function call to sqlite_detach()
pub fn translate_detach(
    expr: &Expr,
    resolver: &Resolver,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    // SQLite treats DETACH as a function call to sqlite_detach(dbname)

    program.extend(&ProgramBuilderOpts {
        num_cursors: 0,
        approx_num_insns: 5,
        approx_num_labels: 0,
    });

    let arg_reg = program.alloc_registers(2); // 1 for arg + 1 for result

    // Load database name argument
    // Handle different expression types as string literals for database names
    match expr {
        Expr::Literal(Literal::String(s)) => {
            // For DETACH, string literals should be used directly (without quotes)
            program.emit_insn(Insn::String8 {
                value: sanitize_string(s),
                dest: arg_reg,
            });
        }
        Expr::Qualified(_, _) => {
            // For DETACH, qualified expressions should be treated as name strings
            let db_name_str = format!("{expr}");
            program.emit_insn(Insn::String8 {
                value: db_name_str,
                dest: arg_reg,
            });
        }
        Expr::Id(id) => {
            // For DETACH, identifiers should be treated as name strings
            // Use normalize_ident to strip quotes from double-quoted identifiers
            program.emit_insn(Insn::String8 {
                value: normalize_ident(id.as_str()),
                dest: arg_reg,
            });
        }
        _ => {
            translate_expr(&mut program, None, expr, arg_reg, resolver)?;
        }
    }

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

    Ok(program)
}
