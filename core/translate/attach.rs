use crate::function::{Func, ScalarFunc};
use crate::translate::{
    emitter::DdlContext,
    expr::sanitize_string,
    physical::{
        emit_root_schema_expression_into, PhysicalPlan, RegisterRange, RootRuntimeInputs,
        SourceRuntime,
    },
    semantic::{context::SemanticContext, hir::HirRoot, schema_expr::analyze_scalar_syntax},
    ProgramBuilder, ProgramBuilderOpts,
};
use crate::util::normalize_ident;
use crate::vdbe::insn::Insn;
use crate::{sync::Arc, Connection, Result};
use turso_parser::ast::{Expr, Literal};

fn emit_scalar(
    syntax: &Expr,
    target: usize,
    ddl_context: &DdlContext,
    connection: &Connection,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let context = SemanticContext::new(
        ddl_context.schema(),
        connection.database_schemas(),
        &connection.temp.database,
        connection.attached_databases(),
        ddl_context.symbol_table,
        connection.experimental_custom_types_enabled(),
        connection.get_dqs_dml().into(),
        connection.dialect(),
    );
    let analyzed = analyze_scalar_syntax(&context, crate::MAIN_DB_ID, syntax)?;
    let plan = PhysicalPlan::new(&analyzed.document)
        .map_err(|error| crate::LimboError::InternalError(error.to_string()))?;
    let root = match &analyzed.document.root {
        HirRoot::SchemaExpressions(root) => root,
        _ => unreachable!("scalar analysis returns a schema-expression root"),
    };
    let mut inputs = RootRuntimeInputs::default();
    inputs.bind_source(
        root.source,
        SourceRuntime::Registers {
            columns: RegisterRange::new(0, 0),
            rowid: None,
        },
    );
    emit_root_schema_expression_into(&plan, program, &inputs, 0, target)
        .map_err(|error| crate::LimboError::InternalError(error.to_string()))
}

/// Translate ATTACH statement
pub fn translate_attach(
    expr: &Expr,
    ddl_context: &DdlContext,
    db_name: &Expr,
    key: &Option<Box<Expr>>,
    program: &mut ProgramBuilder,
    connection: Arc<Connection>,
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
            let filename = expr.to_string();
            program.emit_insn(Insn::String8 {
                value: filename,
                dest: arg_reg,
            });
        }
        Expr::Id(id) => {
            // For ATTACH, identifiers should be treated as filename strings
            program.emit_insn(Insn::String8 {
                value: normalize_ident(id.as_str()),
                dest: arg_reg,
            });
        }
        _ => {
            emit_scalar(expr, arg_reg, ddl_context, &connection, program)?;
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
            emit_scalar(db_name, arg_reg + 1, ddl_context, &connection, program)?;
        }
    }

    // Load key argument (NULL if not provided)
    if let Some(key_expr) = key {
        emit_scalar(key_expr, arg_reg + 2, ddl_context, &connection, program)?;
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

    Ok(())
}

/// Translate DETACH statement
pub fn translate_detach(
    expr: &Expr,
    ddl_context: &DdlContext,
    program: &mut ProgramBuilder,
    connection: Arc<Connection>,
) -> Result<()> {
    if !connection.experimental_attach_enabled() {
        return Err(crate::LimboError::ParseError(
            "DETACH is an experimental feature. Enable with --experimental-attach flag".to_string(),
        ));
    }
    // SQLite treats DETACH as a function call to sqlite_detach(dbname)
    program.extend(&ProgramBuilderOpts::new(0, 5, 0));

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
            emit_scalar(expr, arg_reg, ddl_context, &connection, program)?;
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

    Ok(())
}
