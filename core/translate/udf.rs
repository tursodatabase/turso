//! Translation for user-defined functions (`CREATE FUNCTION` / `DROP
//! FUNCTION`) and inline compilation of their Starlark bodies at call sites.
//!
//! Definitions are persisted in the internal `__turso_internal_functions`
//! table (the same model `CREATE TYPE` uses with `__turso_internal_types`),
//! so `sqlite_schema` stays fully SQLite-compatible. The in-memory registry
//! lives on [`crate::schema::Schema::functions`] and is kept in sync with the
//! `AddFunction`/`DropFunction` opcodes.
//!
//! Calls do not go through a runtime dispatch: `emit_udf_call` compiles the
//! parsed body straight into the caller's program. Statements become
//! label-based jumps; expressions are lowered to `ast::Expr` (with parameters
//! and locals bound to registers via [`ast::Expr::Register`]) and reuse the
//! regular expression translator, so runtime semantics for arithmetic,
//! comparisons and truthiness are exactly SQL semantics, and any SQL scalar
//! function (or another UDF) is callable from a function body.

use crate::schema::{BTreeTable, FunctionDef, TURSO_FUNCTIONS_TABLE_NAME};
use crate::storage::pager::CreateBTreeFlags;
use crate::sync::Arc;
use crate::translate::emitter::Resolver;
use crate::translate::expr::{translate_expr_no_constant_opt, NoConstantOptReason};
use crate::translate::plan::TableReferences;
use crate::translate::schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID};
use crate::translate::ProgramBuilder;
use crate::udf::{collect_assigned_names, UdfBinOp, UdfExpr, UdfStmt, UdfUnOp};
use crate::util::normalize_ident;
use crate::vdbe::affinity::Affinity;
use crate::vdbe::builder::CursorType;
use crate::vdbe::insn::{to_u16, CmpInsFlags, Cookie, InsertFlags, Insn, RegisterOrLiteral};
use crate::vdbe::BranchOffset;
use crate::{bail_parse_error, HashMap, LimboError, Result, MAIN_DB_ID};
use turso_parser::ast;

/// Maximum nesting depth for inlined UDF calls (a UDF whose body calls
/// another UDF, and so on). Recursion is rejected outright; this bounds
/// program size for deep non-recursive chains.
const MAX_UDF_INLINE_DEPTH: usize = 32;

#[allow(clippy::too_many_arguments)]
pub fn translate_create_function(
    or_replace: bool,
    func_name: &ast::QualifiedName,
    params: &[ast::FunctionParam],
    returns: Option<&ast::Type>,
    language: &ast::Name,
    body: &str,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    check_main_database(func_name, "created")?;
    let normalized_name = normalize_ident(func_name.name.as_str());

    // Reject names that collide with built-in (or extension) functions: the
    // built-in would always win at resolution time, making the UDF dead.
    let conflicts = match resolver.resolve_function(&normalized_name, params.len()) {
        Ok(Some(_)) => true,
        // A known built-in name with a different arity errors; that is still
        // a collision.
        Err(_) => true,
        Ok(None) => false,
    };
    if conflicts {
        bail_parse_error!(
            "cannot create function {normalized_name}: a built-in function with the same name exists"
        );
    }

    let existing = resolver.schema().get_function(&normalized_name);
    if existing.is_some() && !or_replace {
        bail_parse_error!("function {normalized_name} already exists");
    }

    // Canonical SQL for persistence: no OR REPLACE, no schema qualifier, body
    // as an escaped string literal so it re-parses without dollar-quoting.
    let sql = ast::Stmt::CreateFunction {
        or_replace: false,
        func_name: ast::QualifiedName::single(func_name.name.clone()),
        params: params.to_vec(),
        returns: returns.cloned(),
        language: language.clone(),
        body: body.to_string(),
    }
    .to_string();

    // Validate now (language, unique parameter names, body parses) so errors
    // surface at CREATE FUNCTION time, not at first use.
    FunctionDef::from_create_function(func_name, params, returns, language, body, sql.clone())?;

    persist_function_definition(normalized_name, sql, existing.is_some(), resolver, program)
}

pub fn translate_drop_function(
    if_exists: bool,
    func_name: &ast::QualifiedName,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    check_main_database(func_name, "dropped")?;
    let normalized_name = normalize_ident(func_name.name.as_str());

    if resolver.schema().get_function(&normalized_name).is_none() {
        if if_exists {
            return Ok(());
        }
        bail_parse_error!("no such function: {normalized_name}");
    }

    let functions_table = resolver
        .schema()
        .get_btree_table(TURSO_FUNCTIONS_TABLE_NAME)
        .ok_or_else(|| LimboError::ParseError(format!("no such function: {normalized_name}")))?;
    let functions_cursor_id =
        program.alloc_cursor_id(CursorType::BTreeTable(functions_table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: functions_cursor_id,
        root_page: functions_table.root_page.into(),
        db: MAIN_DB_ID,
    });
    emit_delete_function_row(program, functions_cursor_id, &normalized_name);

    program.emit_insn(Insn::DropFunction {
        db: MAIN_DB_ID,
        func_name: normalized_name,
    });

    program.emit_insn(Insn::SetCookie {
        db: MAIN_DB_ID,
        cookie: Cookie::SchemaVersion,
        value: (resolver.schema().schema_version + 1) as i32,
        p5: 0,
    });

    Ok(())
}

fn check_main_database(func_name: &ast::QualifiedName, action: &str) -> Result<()> {
    if let Some(db_name) = &func_name.db_name {
        if !db_name.as_str().eq_ignore_ascii_case("main") {
            bail_parse_error!("user-defined functions may only be {action} in the main database");
        }
    }
    Ok(())
}

/// Persist `(name, sql)` into `__turso_internal_functions` (creating the
/// table lazily on first use), refresh the in-memory registry, and bump the
/// schema cookie. When `replace_existing` is set, any existing row for the
/// name is deleted first.
fn persist_function_definition(
    normalized_name: String,
    sql: String,
    replace_existing: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let functions_table: Arc<BTreeTable>;
    let functions_root_page: RegisterOrLiteral<i64>;

    if let Some(existing) = resolver
        .schema()
        .get_btree_table(TURSO_FUNCTIONS_TABLE_NAME)
    {
        functions_table = existing.clone();
        functions_root_page = RegisterOrLiteral::Literal(existing.root_page);
    } else {
        let table_root_reg = program.alloc_register();
        program.emit_insn(Insn::CreateBtree {
            db: MAIN_DB_ID,
            root: table_root_reg,
            flags: CreateBTreeFlags::new_table(),
        });
        let create_sql =
            format!("CREATE TABLE {TURSO_FUNCTIONS_TABLE_NAME}(name TEXT PRIMARY KEY, sql TEXT)");
        functions_table = Arc::new(BTreeTable::from_sql(&create_sql, 0)?);
        functions_root_page = RegisterOrLiteral::Register(table_root_reg);

        let schema_table = resolver.schema().get_btree_table(SQLITE_TABLEID).unwrap();
        let schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(schema_table));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: schema_cursor_id,
            root_page: 1i64.into(),
            db: MAIN_DB_ID,
        });
        emit_schema_entry(
            program,
            resolver,
            schema_cursor_id,
            None,
            SchemaEntryType::Table,
            TURSO_FUNCTIONS_TABLE_NAME,
            TURSO_FUNCTIONS_TABLE_NAME,
            table_root_reg,
            Some(create_sql),
        )?;
        program.emit_insn(Insn::ParseSchema {
            db: MAIN_DB_ID,
            where_clause: Some(format!(
                "tbl_name = '{TURSO_FUNCTIONS_TABLE_NAME}' AND type != 'trigger'"
            )),
        });
    }

    let functions_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(functions_table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: functions_cursor_id,
        root_page: functions_root_page,
        db: MAIN_DB_ID,
    });

    if replace_existing {
        emit_delete_function_row(program, functions_cursor_id, &normalized_name);
    }

    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: functions_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });
    let name_reg = program.emit_string8_new_reg(normalized_name);
    program.emit_string8_new_reg(sql.clone());
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u16(name_reg),
        count: to_u16(2),
        dest_reg: to_u16(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: functions_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: TURSO_FUNCTIONS_TABLE_NAME.to_string(),
    });

    program.emit_insn(Insn::AddFunction {
        db: MAIN_DB_ID,
        sql,
    });

    program.emit_insn(Insn::SetCookie {
        db: MAIN_DB_ID,
        cookie: Cookie::SchemaVersion,
        value: (resolver.schema().schema_version + 1) as i32,
        p5: 0,
    });

    Ok(())
}

/// Scan `__turso_internal_functions` through an already-open write cursor and
/// delete every row whose name (column 0) matches.
fn emit_delete_function_row(
    program: &mut ProgramBuilder,
    functions_cursor_id: usize,
    normalized_name: &str,
) {
    let name_reg = program.alloc_register();
    program.emit_insn(Insn::String8 {
        dest: name_reg,
        value: normalized_name.to_owned(),
    });

    let end_loop_label = program.allocate_label();
    let loop_start_label = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: functions_cursor_id,
        pc_if_empty: end_loop_label,
    });
    program.preassign_label_to_next_insn(loop_start_label);

    let col0_reg = program.alloc_register();
    program.emit_column_or_rowid(functions_cursor_id, 0, col0_reg);

    let skip_delete_label = program.allocate_label();
    program.emit_insn(Insn::Ne {
        lhs: col0_reg,
        rhs: name_reg,
        target_pc: skip_delete_label,
        flags: CmpInsFlags::default(),
        collation: program.curr_collation(),
    });
    program.emit_insn(Insn::Delete {
        cursor_id: functions_cursor_id,
        table_name: TURSO_FUNCTIONS_TABLE_NAME.to_string(),
        is_part_of_update: false,
    });
    program.preassign_label_to_next_insn(skip_delete_label);

    program.emit_insn(Insn::Next {
        cursor_id: functions_cursor_id,
        pc_if_next: loop_start_label,
    });
    program.preassign_label_to_next_insn(end_loop_label);
}

/// Compile a call to a user-defined function inline into the caller's
/// program, leaving the result in `target_register`.
pub fn emit_udf_call(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
    func: &FunctionDef,
    args: &[Box<ast::Expr>],
    target_register: usize,
) -> Result<usize> {
    if args.len() != func.params.len() {
        bail_parse_error!("wrong number of arguments to function {}()", func.name);
    }
    if program.udf_inline_stack.contains(&func.name) {
        bail_parse_error!(
            "recursive use of user-defined function {} is not allowed",
            func.name
        );
    }
    if program.udf_inline_stack.len() >= MAX_UDF_INLINE_DEPTH {
        bail_parse_error!(
            "user-defined function calls nested too deeply (max depth {MAX_UDF_INLINE_DEPTH})"
        );
    }

    // Bind parameters: evaluate each argument into a fresh register. This
    // happens in the caller's context, before entering the callee's frame, so
    // a nested call like f(f(x)) is not mistaken for recursion. Hoisting must
    // stay off even for constant arguments — the body may assign to a
    // parameter, so its register has to be re-initialized on every row.
    let mut scope: HashMap<String, usize> = HashMap::default();
    for (param, arg) in func.params.iter().zip(args.iter()) {
        let reg = program.alloc_register();
        translate_expr_no_constant_opt(
            program,
            referenced_tables,
            arg,
            reg,
            resolver,
            NoConstantOptReason::RegisterReuse,
        )?;
        scope.insert(param.clone(), reg);
    }

    program.udf_inline_stack.push(func.name.clone());
    let result = emit_udf_body(
        program,
        referenced_tables,
        resolver,
        func,
        scope,
        target_register,
    );
    program
        .udf_inline_stack
        .pop()
        .expect("stack pushed just above");
    result?;
    Ok(target_register)
}

struct LoopLabels {
    break_label: BranchOffset,
    continue_label: BranchOffset,
}

struct UdfCtx<'a> {
    /// Parameter and local variable registers by name.
    scope: HashMap<String, usize>,
    /// Jump target for `return`; the RETURNS cast (if any) lives there.
    exit_label: BranchOffset,
    /// The caller's target register: `return` writes the result here.
    result_reg: usize,
    /// Shared scratch register for condition results and discarded
    /// expression-statement results.
    scratch_reg: usize,
    /// Enclosing loops, innermost last.
    loops: Vec<LoopLabels>,
    func_name: &'a str,
}

fn emit_udf_body(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
    func: &FunctionDef,
    mut scope: HashMap<String, usize>,
    target_register: usize,
) -> Result<()> {
    // Locals: one register per assigned name, NULL-initialized at body entry
    // so every row starts from a clean slate.
    let mut assigned = Vec::new();
    collect_assigned_names(&func.body.stmts, &mut assigned);
    let mut first_local: Option<usize> = None;
    let mut last_local: Option<usize> = None;
    for name in assigned {
        if scope.contains_key(&name) {
            // Assigning to a parameter reuses its register.
            continue;
        }
        let reg = program.alloc_register();
        first_local.get_or_insert(reg);
        last_local = Some(reg);
        scope.insert(name, reg);
    }
    if let Some(first) = first_local {
        program.emit_insn(Insn::Null {
            dest: first,
            dest_end: last_local.filter(|last| *last != first),
        });
    }

    let mut ctx = UdfCtx {
        scope,
        exit_label: program.allocate_label(),
        result_reg: target_register,
        scratch_reg: program.alloc_register(),
        loops: Vec::new(),
        func_name: &func.name,
    };
    compile_stmts(
        program,
        referenced_tables,
        resolver,
        &func.body.stmts,
        &mut ctx,
    )?;

    // Falling off the end returns NULL.
    program.emit_insn(Insn::Null {
        dest: target_register,
        dest_end: None,
    });
    program.preassign_label_to_next_insn(ctx.exit_label);
    if let Some(returns) = &func.returns {
        program.emit_insn(Insn::Cast {
            reg: target_register,
            affinity: Affinity::affinity(returns),
        });
    }
    Ok(())
}

fn compile_stmts(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
    stmts: &[UdfStmt],
    ctx: &mut UdfCtx,
) -> Result<()> {
    for stmt in stmts {
        compile_stmt(program, referenced_tables, resolver, stmt, ctx)?;
    }
    Ok(())
}

fn compile_stmt(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    resolver: &Resolver,
    stmt: &UdfStmt,
    ctx: &mut UdfCtx,
) -> Result<()> {
    match stmt {
        UdfStmt::Assign { name, value } => {
            let expr = lower_expr(value, ctx)?;
            let reg = *ctx
                .scope
                .get(name)
                .expect("assignment targets are pre-allocated by collect_assigned_names");
            translate_expr_no_constant_opt(
                program,
                referenced_tables,
                &expr,
                reg,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
        }
        UdfStmt::Expr(value) => {
            let expr = lower_expr(value, ctx)?;
            translate_expr_no_constant_opt(
                program,
                referenced_tables,
                &expr,
                ctx.scratch_reg,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
        }
        UdfStmt::Return(value) => {
            match value {
                Some(value) => {
                    let expr = lower_expr(value, ctx)?;
                    translate_expr_no_constant_opt(
                        program,
                        referenced_tables,
                        &expr,
                        ctx.result_reg,
                        resolver,
                        NoConstantOptReason::RegisterReuse,
                    )?;
                }
                None => program.emit_insn(Insn::Null {
                    dest: ctx.result_reg,
                    dest_end: None,
                }),
            }
            program.emit_insn(Insn::Goto {
                target_pc: ctx.exit_label,
            });
        }
        UdfStmt::If { arms, else_body } => {
            let end_label = program.allocate_label();
            for (cond, body) in arms {
                let next_arm_label = program.allocate_label();
                let cond_expr = lower_expr(cond, ctx)?;
                translate_expr_no_constant_opt(
                    program,
                    referenced_tables,
                    &cond_expr,
                    ctx.scratch_reg,
                    resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
                program.emit_insn(Insn::IfNot {
                    reg: ctx.scratch_reg,
                    target_pc: next_arm_label,
                    jump_if_null: true,
                });
                compile_stmts(program, referenced_tables, resolver, body, ctx)?;
                program.emit_insn(Insn::Goto {
                    target_pc: end_label,
                });
                program.preassign_label_to_next_insn(next_arm_label);
            }
            if let Some(body) = else_body {
                compile_stmts(program, referenced_tables, resolver, body, ctx)?;
            }
            program.preassign_label_to_next_insn(end_label);
        }
        UdfStmt::While { cond, body } => {
            let start_label = program.allocate_label();
            let end_label = program.allocate_label();
            program.preassign_label_to_next_insn(start_label);
            let cond_expr = lower_expr(cond, ctx)?;
            translate_expr_no_constant_opt(
                program,
                referenced_tables,
                &cond_expr,
                ctx.scratch_reg,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
            program.emit_insn(Insn::IfNot {
                reg: ctx.scratch_reg,
                target_pc: end_label,
                jump_if_null: true,
            });
            ctx.loops.push(LoopLabels {
                break_label: end_label,
                continue_label: start_label,
            });
            let body_result = compile_stmts(program, referenced_tables, resolver, body, ctx);
            ctx.loops.pop();
            body_result?;
            program.emit_insn(Insn::Goto {
                target_pc: start_label,
            });
            program.preassign_label_to_next_insn(end_label);
        }
        UdfStmt::For {
            var,
            start,
            stop,
            step,
            body,
        } => {
            let var_reg = *ctx
                .scope
                .get(var)
                .expect("loop variables are pre-allocated by collect_assigned_names");
            // The loop is controlled by a hidden counter so that assigning to
            // the loop variable inside the body cannot affect iteration
            // (matching Python/Starlark semantics).
            let iter_reg = program.alloc_register();
            let stop_reg = program.alloc_register();
            match start {
                Some(start) => {
                    let start_expr = lower_expr(start, ctx)?;
                    translate_expr_no_constant_opt(
                        program,
                        referenced_tables,
                        &start_expr,
                        iter_reg,
                        resolver,
                        NoConstantOptReason::RegisterReuse,
                    )?;
                }
                None => program.emit_int(0, iter_reg),
            }
            let stop_expr = lower_expr(stop, ctx)?;
            translate_expr_no_constant_opt(
                program,
                referenced_tables,
                &stop_expr,
                stop_reg,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;

            let top_label = program.allocate_label();
            let incr_label = program.allocate_label();
            let end_label = program.allocate_label();

            // A NULL bound yields zero iterations; non-integer bounds are a
            // runtime error (range() requires integers).
            program.emit_insn(Insn::IsNull {
                reg: iter_reg,
                target_pc: end_label,
            });
            program.emit_insn(Insn::IsNull {
                reg: stop_reg,
                target_pc: end_label,
            });
            program.emit_insn(Insn::MustBeInt {
                reg: iter_reg,
                target_pc: None,
            });
            program.emit_insn(Insn::MustBeInt {
                reg: stop_reg,
                target_pc: None,
            });

            program.preassign_label_to_next_insn(top_label);
            let done_cmp = if *step > 0 {
                Insn::Ge {
                    lhs: iter_reg,
                    rhs: stop_reg,
                    target_pc: end_label,
                    flags: CmpInsFlags::default(),
                    collation: None,
                }
            } else {
                Insn::Le {
                    lhs: iter_reg,
                    rhs: stop_reg,
                    target_pc: end_label,
                    flags: CmpInsFlags::default(),
                    collation: None,
                }
            };
            program.emit_insn(done_cmp);
            program.emit_insn(Insn::Copy {
                src_reg: iter_reg,
                dst_reg: var_reg,
                extra_amount: 0,
            });

            ctx.loops.push(LoopLabels {
                break_label: end_label,
                continue_label: incr_label,
            });
            let body_result = compile_stmts(program, referenced_tables, resolver, body, ctx);
            ctx.loops.pop();
            body_result?;

            program.preassign_label_to_next_insn(incr_label);
            program.emit_insn(Insn::AddImm {
                register: iter_reg,
                value: *step,
            });
            program.emit_insn(Insn::Goto {
                target_pc: top_label,
            });
            program.preassign_label_to_next_insn(end_label);
        }
        UdfStmt::Break => {
            let target_pc = ctx
                .loops
                .last()
                .expect("parser rejects break outside of a loop")
                .break_label;
            program.emit_insn(Insn::Goto { target_pc });
        }
        UdfStmt::Continue => {
            let target_pc = ctx
                .loops
                .last()
                .expect("parser rejects continue outside of a loop")
                .continue_label;
            program.emit_insn(Insn::Goto { target_pc });
        }
        UdfStmt::Pass => {}
    }
    Ok(())
}

/// Lower a Starlark expression to an `ast::Expr` for the regular expression
/// translator. Variables become `Expr::Register`, which is never treated as
/// constant, so nothing from a function body gets hoisted out of per-row
/// execution.
fn lower_expr(expr: &UdfExpr, ctx: &UdfCtx) -> Result<ast::Expr> {
    Ok(match expr {
        UdfExpr::Int(value) => ast::Expr::Literal(ast::Literal::Numeric(value.to_string())),
        UdfExpr::Float(value) => ast::Expr::Literal(ast::Literal::Numeric(format!("{value:?}"))),
        UdfExpr::Str(value) => ast::Expr::Literal(ast::Literal::String(format!(
            "'{}'",
            value.replace('\'', "''")
        ))),
        UdfExpr::Bool(value) => {
            ast::Expr::Literal(ast::Literal::Numeric(if *value { "1" } else { "0" }.into()))
        }
        UdfExpr::None => ast::Expr::Literal(ast::Literal::Null),
        UdfExpr::Var(name) => {
            let reg = ctx.scope.get(name).ok_or_else(|| {
                LimboError::ParseError(format!(
                    "undefined variable {name} in function {}",
                    ctx.func_name
                ))
            })?;
            ast::Expr::Register(*reg)
        }
        UdfExpr::Binary(op, lhs, rhs) => ast::Expr::Binary(
            Box::new(lower_expr(lhs, ctx)?),
            lower_bin_op(*op),
            Box::new(lower_expr(rhs, ctx)?),
        ),
        UdfExpr::Unary(op, inner) => {
            let op = match op {
                UdfUnOp::Neg => ast::UnaryOperator::Negative,
                UdfUnOp::Pos => ast::UnaryOperator::Positive,
                UdfUnOp::BitNot => ast::UnaryOperator::BitwiseNot,
                UdfUnOp::Not => ast::UnaryOperator::Not,
            };
            ast::Expr::Unary(op, Box::new(lower_expr(inner, ctx)?))
        }
        UdfExpr::Ternary { cond, then, else_ } => ast::Expr::Case {
            base: None,
            when_then_pairs: vec![(
                Box::new(lower_expr(cond, ctx)?),
                Box::new(lower_expr(then, ctx)?),
            )],
            else_expr: Some(Box::new(lower_expr(else_, ctx)?)),
        },
        UdfExpr::Call { name, args } => {
            let args = args
                .iter()
                .map(|arg| lower_expr(arg, ctx))
                .collect::<Result<Vec<_>>>()?;
            // A few Starlark built-ins map onto SQL equivalents; everything
            // else resolves against the SQL function namespace.
            match (name.as_str(), args.len()) {
                ("len", 1) => sql_call("length", args),
                ("str", 1) => sql_cast(args, "TEXT"),
                ("int", 1) => sql_cast(args, "INTEGER"),
                ("float", 1) => sql_cast(args, "REAL"),
                _ => sql_call(name, args),
            }
        }
    })
}

fn lower_bin_op(op: UdfBinOp) -> ast::Operator {
    match op {
        UdfBinOp::Add => ast::Operator::Add,
        UdfBinOp::Sub => ast::Operator::Subtract,
        UdfBinOp::Mul => ast::Operator::Multiply,
        // Starlark's `/` and `//` both map to SQL division: division between
        // two integers truncates like SQLite (Starlark `//` floors instead;
        // documented deviation).
        UdfBinOp::Div | UdfBinOp::FloorDiv => ast::Operator::Divide,
        UdfBinOp::Mod => ast::Operator::Modulus,
        UdfBinOp::BitAnd => ast::Operator::BitwiseAnd,
        UdfBinOp::BitOr => ast::Operator::BitwiseOr,
        UdfBinOp::Shl => ast::Operator::LeftShift,
        UdfBinOp::Shr => ast::Operator::RightShift,
        UdfBinOp::Eq => ast::Operator::Equals,
        UdfBinOp::NotEq => ast::Operator::NotEquals,
        UdfBinOp::Lt => ast::Operator::Less,
        UdfBinOp::LtEq => ast::Operator::LessEquals,
        UdfBinOp::Gt => ast::Operator::Greater,
        UdfBinOp::GtEq => ast::Operator::GreaterEquals,
        UdfBinOp::And => ast::Operator::And,
        UdfBinOp::Or => ast::Operator::Or,
    }
}

fn sql_call(name: &str, args: Vec<ast::Expr>) -> ast::Expr {
    ast::Expr::FunctionCall {
        name: ast::Name::exact(name.to_owned()),
        distinctness: None,
        args: args.into_iter().map(Box::new).collect(),
        order_by: Vec::new(),
        within_group: Vec::new(),
        filter_over: ast::FunctionTail {
            filter_clause: None,
            over_clause: None,
        },
    }
}

fn sql_cast(mut args: Vec<ast::Expr>, type_name: &str) -> ast::Expr {
    ast::Expr::Cast {
        expr: Box::new(args.pop().expect("cast helpers take exactly one argument")),
        type_name: Some(ast::Type {
            name: type_name.to_owned(),
            size: None,
            array_dimensions: 0,
        }),
    }
}

#[cfg(test)]
mod tests {
    use crate::io::MemoryIO;
    use crate::sync::Arc;
    use crate::vdbe::insn::Insn;
    use crate::{Database, DatabaseOpts, OpenFlags, SqliteDialect};

    fn udf_test_connection() -> Arc<crate::Connection> {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            ":memory:",
            OpenFlags::default(),
            DatabaseOpts::new().with_udfs(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        db.connect().unwrap()
    }

    /// A user-defined function call must be compiled inline: no Function
    /// opcode (no runtime dispatch), with the body's arithmetic appearing as
    /// plain bytecode in the caller's program.
    #[test]
    fn udf_call_is_inlined_without_function_opcode() {
        let conn = udf_test_connection();
        conn.execute("CREATE FUNCTION double(x INTEGER) LANGUAGE starlark AS 'return x + x'")
            .unwrap();
        conn.execute("CREATE TABLE items(x INTEGER)").unwrap();

        let statement = conn.prepare("SELECT double(x) FROM items").unwrap();
        let instructions = &statement.get_program().insns;
        assert!(
            instructions
                .iter()
                .all(|(insn, _)| !matches!(insn, Insn::Function { .. })),
            "UDF calls must be inlined, not dispatched through Insn::Function"
        );
        assert!(
            instructions
                .iter()
                .any(|(insn, _)| matches!(insn, Insn::Add { .. })),
            "the inlined body's arithmetic must appear in the caller's program"
        );
    }

    /// Loop-carried locals must be re-initialized on every row: nothing from
    /// the function body may be hoisted into the once-per-program constant
    /// section.
    #[test]
    fn udf_body_is_not_constant_hoisted() {
        let conn = udf_test_connection();
        conn.execute(
            "CREATE FUNCTION s(n INTEGER) LANGUAGE starlark AS $$
total = 0
for i in range(1, n + 1):
    total += i
return total
$$",
        )
        .unwrap();
        conn.execute("CREATE TABLE items(x INTEGER)").unwrap();
        conn.execute("INSERT INTO items VALUES (3), (3), (3)")
            .unwrap();

        let mut rows: Vec<i64> = Vec::new();
        conn.prepare("SELECT s(x) FROM items")
            .unwrap()
            .run_with_row_callback(|row| {
                rows.push(row.get::<i64>(0)?);
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, vec![6, 6, 6], "accumulator state leaked across rows");
    }
}
