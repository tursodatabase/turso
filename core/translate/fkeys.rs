use turso_parser::ast::{self, Expr, Literal, Name, QualifiedName, RefAct};

use super::{translate_inner, ProgramBuilder, ProgramBuilderOpts};
use crate::schema::ROWID_STRS;
use crate::{
    error::SQLITE_CONSTRAINT_FOREIGNKEY,
    schema::{BTreeTable, ForeignKey, ResolvedFkRef},
    sync::{Arc, OnceLock, Weak},
    translate::{
        collate::CollationSeq,
        emitter::Resolver,
        physical::{
            emit_root_schema_expression_into, CursorId, PhysicalPlan, RootRuntimeInputs,
            SourceRuntime,
        },
        semantic::{
            context::SemanticContext,
            hir::HirRoot,
            schema_expr::{analyze_table_schema_syntax, SchemaSyntaxInput},
        },
    },
    vdbe::{
        builder::{CursorType, QueryMode},
        insn::{CmpInsFlags, Insn, Subprogram},
        PreparedProgram,
    },
    Connection, LimboError, Result,
};
use std::{cell::RefCell, num::NonZero, rc::Rc};

/// Tracks foreign-key action programs that are currently being compiled.
///
/// This is needed when generated foreign-key action SQL reaches the same
/// foreign-key action again before the first copy has finished compiling.
///
/// Example: in `t(id PRIMARY KEY, parent REFERENCES t(id) ON DELETE CASCADE)`,
/// deleting row `1` runs an action that deletes row `2`. Deleting row `2` must
/// run the same action again to delete row `3`. While compiling that action,
/// this stack lets the nested delete emit a call back to the action program
/// already being built.
///
/// A two-table cycle needs the same mechanism: table `a` cascades to `b`, and
/// `b` cascades back to `a`.
#[derive(Clone, Default)]
pub(super) struct FkActionCompileStack(Rc<RefCell<Vec<FkActionCompileStackEntry>>>);

/// One foreign-key action program that is currently being compiled.
struct FkActionCompileStackEntry {
    /// The foreign key whose action program is being compiled.
    foreign_key: Arc<ForeignKey>,
    /// Whether the action started from a parent delete or a parent key update.
    parent_change: FkActionParentChange,
    /// The place where the finished action program will be stored.
    ///
    /// Recursive calls emitted during compilation hold a clone of this slot.
    slot: Arc<OnceLock<Weak<PreparedProgram>>>,
}

impl FkActionCompileStack {
    /// Find the unfinished action program for this foreign key and parent row change.
    ///
    /// Returning `Some` means the compiler is re-entering the same FK action.
    /// The caller should emit a recursive call to that in-progress program
    /// instead of compiling another copy of the action.
    fn find(
        &self,
        foreign_key: &Arc<ForeignKey>,
        parent_change: FkActionParentChange,
    ) -> Option<Arc<OnceLock<Weak<PreparedProgram>>>> {
        self.0
            .borrow()
            .iter()
            .find(|entry| {
                entry.parent_change == parent_change && Arc::ptr_eq(&entry.foreign_key, foreign_key)
            })
            .map(|entry| entry.slot.clone())
    }

    /// Remember that a foreign-key action program is being compiled.
    ///
    /// The returned guard removes the entry from the stack when compilation
    /// ends, including when compilation returns an error.
    fn push(
        &self,
        foreign_key: Arc<ForeignKey>,
        parent_change: FkActionParentChange,
    ) -> FkActionCompileStackGuard {
        let slot = Arc::new(OnceLock::new());
        self.0.borrow_mut().push(FkActionCompileStackEntry {
            foreign_key,
            parent_change,
            slot: slot.clone(),
        });
        FkActionCompileStackGuard {
            stack: self.clone(),
            slot,
        }
    }
}

/// Removes a foreign-key action program from the compile stack when compilation ends.
struct FkActionCompileStackGuard {
    stack: FkActionCompileStack,
    slot: Arc<OnceLock<Weak<PreparedProgram>>>,
}

impl Drop for FkActionCompileStackGuard {
    fn drop(&mut self) {
        let ended = self
            .stack
            .0
            .borrow_mut()
            .pop()
            .expect("foreign-key action compilation stack underflow");
        debug_assert!(Arc::ptr_eq(&ended.slot, &self.slot));
    }
}

/// The parent-row change that started a foreign-key action.
///
/// Delete and update actions are different generated programs. A recursive
/// delete action must call the in-progress delete action, not an update action
/// for the same foreign key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FkActionParentChange {
    /// The parent row was deleted.
    Delete,
}

#[inline]
pub fn open_read_table(program: &mut ProgramBuilder, tbl: &Arc<BTreeTable>, db: usize) -> usize {
    let tcur = program.alloc_cursor_id(CursorType::BTreeTable(tbl.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: tcur,
        root_page: tbl.root_page,
        db,
    });
    tcur
}

pub fn emit_fk_restrict_halt(program: &mut ProgramBuilder) -> Result<()> {
    program.emit_insn(Insn::Halt {
        err_code: SQLITE_CONSTRAINT_FOREIGNKEY,
        description: "FOREIGN KEY constraint failed".to_string(),
        on_error: None,
        description_reg: None,
    });
    Ok(())
}

fn build_parent_key(
    program: &mut ProgramBuilder,
    parent_bt: &BTreeTable,
    parent_cols: &[String],
    parent_cursor_id: usize,
    parent_rowid_reg: usize,
    dest_start: usize,
    resolver: &Resolver,
    connection: &Arc<Connection>,
    database_id: usize,
) -> Result<()> {
    let mut expressions = Vec::new();
    let expression_indexes = parent_cols
        .iter()
        .map(|parent_column| {
            if parent_bt.get_column(parent_column).is_some() {
                let expression_index = expressions.len();
                expressions.push(ast::Expr::Id(Name::exact(parent_column.clone())));
                Ok(Some(expression_index))
            } else if ROWID_STRS
                .iter()
                .any(|rowid| parent_column.eq_ignore_ascii_case(rowid))
            {
                Ok(None)
            } else {
                Err(LimboError::InternalError(format!(
                    "col {parent_column} missing"
                )))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let syntax = expressions
        .iter()
        .map(|expression| SchemaSyntaxInput {
            syntax: expression,
            profile: crate::schema_expr::SchemaExprProfile::IndexKey,
            owner_column: None,
        })
        .collect::<Vec<_>>();
    let analyzed = if syntax.is_empty() {
        None
    } else {
        let context = SemanticContext::new(
            resolver.schema(),
            connection.database_schemas(),
            &connection.temp.database,
            connection.attached_databases(),
            resolver.symbol_table,
            connection.experimental_custom_types_enabled(),
            connection.get_dqs_dml().into(),
            connection.dialect(),
        );
        Some(analyze_table_schema_syntax(
            &context,
            database_id,
            Arc::new(crate::schema::Table::BTree(Arc::new(parent_bt.clone()))),
            &syntax,
        )?)
    };
    let planned = analyzed
        .as_ref()
        .map(|analyzed| {
            PhysicalPlan::new(&analyzed.document)
                .map_err(|error| LimboError::InternalError(error.to_string()))
        })
        .transpose()?;
    let mut runtime_inputs = RootRuntimeInputs::default();
    if let Some(analyzed) = &analyzed {
        let root = match &analyzed.document.root {
            HirRoot::SchemaExpressions(root) => root,
            _ => unreachable!("parent-key analysis returns a schema-expression root"),
        };
        runtime_inputs.bind_source(
            root.source,
            SourceRuntime::Cursor(CursorId(parent_cursor_id)),
        );
    }

    for (key_position, expression_index) in expression_indexes.into_iter().enumerate() {
        if let Some(expression_index) = expression_index {
            emit_root_schema_expression_into(
                planned
                    .as_ref()
                    .expect("real parent columns create a physical plan"),
                program,
                &runtime_inputs,
                expression_index,
                dest_start + key_position,
            )
            .map_err(|error| LimboError::InternalError(error.to_string()))?;
        } else {
            program.emit_insn(Insn::Copy {
                src_reg: parent_rowid_reg,
                dst_reg: dest_start + key_position,
                extra_amount: 0,
            });
        }
    }
    Ok(())
}

pub struct FkActionContext {
    /// Registers containing the deleted parent key values.
    pub old_key_registers: Vec<usize>,
}

impl FkActionContext {
    pub fn new_for_delete(old_key_registers: Vec<usize>) -> Self {
        Self { old_key_registers }
    }

    /// Return which generated action program this context runs.
    ///
    fn parent_change(&self) -> FkActionParentChange {
        FkActionParentChange::Delete
    }
}

/// Context for compiling FK action subprograms - maps parameter indices to column values
#[derive(Debug)]
struct FkSubprogramContext {
    /// Map from column index to parameter index (1-indexed) for OLD key values
    old_param_start: usize,
}

impl FkSubprogramContext {
    fn new() -> Self {
        Self { old_param_start: 1 }
    }

    fn old_param_index(&self, col_idx: usize) -> NonZero<usize> {
        NonZero::new(self.old_param_start + col_idx).expect("param index should be non-zero")
    }
}

/// Common options for FK action subprogram builders.
const FK_SUBPROGRAM_OPTS: ProgramBuilderOpts = ProgramBuilderOpts::new(2, 32, 4);

fn emit_fk_action_subprogram(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    connection: &Arc<Connection>,
    stmt: ast::Stmt,
    ctx: &FkActionContext,
    foreign_key: Arc<ForeignKey>,
    description: &'static str,
) -> Result<()> {
    let parent_change = ctx.parent_change();
    let compile_stack = resolver.fk_action_compile_stack.clone();

    let subprogram = if let Some(slot) = compile_stack.find(&foreign_key, parent_change) {
        assert!(
            program.flags.is_subprogram(),
            "recursive foreign-key action calls must be emitted from a foreign-key action subprogram"
        );
        Subprogram::Pending(slot)
    } else {
        let mut subprogram_builder = ProgramBuilder::new_for_subprogram(
            QueryMode::Normal,
            program.capture_data_changes_info().clone(),
            FK_SUBPROGRAM_OPTS,
        );
        let entry = compile_stack.push(foreign_key, parent_change);
        subprogram_builder.prologue();
        translate_inner(
            stmt,
            resolver,
            &mut subprogram_builder,
            connection,
            description,
        )?;
        subprogram_builder.epilogue(resolver.schema());
        let built = subprogram_builder.build(connection.clone(), true, description)?;
        let prepared = built.prepared().clone();
        entry
            .slot
            .set(Arc::downgrade(&prepared))
            .expect("foreign-key action subprogram should be set exactly once");
        Subprogram::PreparedProgram(prepared)
    };

    // Foreign-key action subprograms can't contain RAISE(IGNORE), so ignore_jump_target
    // is a no-op that resolves to the next instruction (just falls through).
    let ignore_jump_target = program.allocate_label();
    program.emit_insn(Insn::Program {
        param_registers: ctx.old_key_registers.to_vec(),
        program: subprogram,
        ignore_jump_target,
    });
    program.preassign_label_to_next_insn(ignore_jump_target);

    Ok(())
}

/// Build a QualifiedName with db_name set for non-main databases.
fn qualified_table_name(table_name: &str, db_name: Option<&str>) -> QualifiedName {
    QualifiedName {
        db_name: db_name.map(Name::from_string),
        name: Name::from_string(table_name),
        alias: None,
    }
}

/// Generate a DELETE statement AST for CASCADE DELETE:
/// DELETE FROM child_table WHERE fk_col1 = ?1 AND fk_col2 = ?2 ...
fn generate_cascade_delete_stmt(
    child_table: &str,
    child_cols: &[String],
    ctx: &FkSubprogramContext,
    db_name: Option<&str>,
) -> ast::Stmt {
    ast::Stmt::Delete {
        with: None,
        tbl_name: qualified_table_name(child_table, db_name),
        indexed: None,
        where_clause: Some(Box::new(build_fk_match_where_clause(child_cols, ctx))),
        returning: vec![],
        order_by: vec![],
        limit: None,
    }
}

/// Generate an UPDATE statement AST for SET NULL:
/// UPDATE child_table SET fk_col1 = NULL, fk_col2 = NULL ... WHERE fk_col1 = ?1 AND fk_col2 = ?2 ...
fn generate_set_null_stmt(
    child_table: &str,
    child_cols: &[String],
    ctx: &FkSubprogramContext,
    db_name: Option<&str>,
) -> ast::Stmt {
    // Build SET clause: fk_col1 = NULL, fk_col2 = NULL ...
    let sets: Vec<ast::Set> = child_cols
        .iter()
        .map(|col| ast::Set {
            col_names: vec![Name::from_string(col)],
            expr: Box::new(Expr::Literal(Literal::Null)),
        })
        .collect();
    ast::Stmt::Update(ast::Update {
        with: None,
        or_conflict: None,
        tbl_name: qualified_table_name(child_table, db_name),
        indexed: None,
        sets,
        from: None,
        where_clause: Some(Box::new(build_fk_match_where_clause(child_cols, ctx))),
        returning: vec![],
        order_by: vec![],
        limit: None,
    })
}

/// Generate an UPDATE statement AST for SET DEFAULT:
/// UPDATE child_table SET fk_col1 = default1, fk_col2 = default2 ... WHERE fk_col1 = ?old1 AND fk_col2 = ?old2 ...
fn generate_set_default_stmt(
    child_table: &BTreeTable,
    child_cols: &[String],
    ctx: &FkSubprogramContext,
    db_name: Option<&str>,
) -> ast::Stmt {
    // Build SET clause: if no default is defined for a column, we use NULL
    let sets: Vec<ast::Set> = child_cols
        .iter()
        .map(|col| {
            let default_expr = child_table
                .get_column(col)
                .and_then(|(_, c)| c.default.as_ref())
                .map(|d| (**d).clone())
                .unwrap_or(Expr::Literal(Literal::Null));
            ast::Set {
                col_names: vec![Name::from_string(col)],
                expr: Box::new(default_expr),
            }
        })
        .collect();

    ast::Stmt::Update(ast::Update {
        with: None,
        or_conflict: None,
        tbl_name: qualified_table_name(&child_table.name, db_name),
        indexed: None,
        sets,
        from: None,
        where_clause: Some(Box::new(build_fk_match_where_clause(child_cols, ctx))),
        returning: vec![],
        order_by: vec![],
        limit: None,
    })
}

/// Generate an UPDATE statement AST for CASCADE UPDATE:
/// UPDATE child_table SET fk_col1 = ?new1, fk_col2 = ?new2 ... WHERE fk_col1 = ?old1 AND fk_col2 = ?old2 ...
fn build_fk_match_where_clause(child_cols: &[String], ctx: &FkSubprogramContext) -> Expr {
    let mut conditions: Vec<Expr> = Vec::with_capacity(child_cols.len());

    for (i, col) in child_cols.iter().enumerate() {
        let param_idx = ctx.old_param_index(i);
        let cond = Expr::Binary(
            Box::new(Expr::Id(Name::from_string(col))),
            ast::Operator::Equals,
            Box::new(Expr::Variable(ast::Variable::indexed(
                u32::try_from(param_idx.get())
                    .ok()
                    .and_then(std::num::NonZeroU32::new)
                    .expect("fk parameter index must fit into NonZeroU32"),
            ))),
        );
        conditions.push(cond);
    }

    // Combine the clauses with AND
    if conditions.len() == 1 {
        conditions.remove(0)
    } else {
        conditions
            .into_iter()
            .reduce(|acc, cond| Expr::Binary(Box::new(acc), ast::Operator::And, Box::new(cond)))
            .expect("at least one condition")
    }
}

/// Compile and emit an FK CASCADE DELETE action as a sub-program.
/// This creates a sub-program that deletes all child rows matching the parent key.
fn fire_fk_cascade_delete(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    fk_ref: &ResolvedFkRef,
    connection: &Arc<Connection>,
    ctx: &FkActionContext,
    database_id: usize,
) -> Result<()> {
    let db_name = if database_id != crate::MAIN_DB_ID {
        resolver.get_database_name_by_index(database_id)
    } else {
        None
    };
    let child_cols = &fk_ref.fk.child_columns;
    let subprog_ctx = FkSubprogramContext::new();
    let stmt = generate_cascade_delete_stmt(
        &fk_ref.child_table.name,
        child_cols,
        &subprog_ctx,
        db_name.as_deref(),
    );
    emit_fk_action_subprogram(
        program,
        resolver,
        connection,
        stmt,
        ctx,
        fk_ref.fk.clone(),
        "fk cascade delete",
    )
}

/// Compile and emit an FK SET NULL action as a sub-program.
/// This creates a sub-program that sets FK columns to NULL for all matching child rows.
fn fire_fk_set_null(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    fk_ref: &ResolvedFkRef,
    connection: &Arc<Connection>,
    ctx: &FkActionContext,
    database_id: usize,
) -> Result<()> {
    let db_name = if database_id != crate::MAIN_DB_ID {
        resolver.get_database_name_by_index(database_id)
    } else {
        None
    };
    let child_cols = &fk_ref.fk.child_columns;
    let subprog_ctx = FkSubprogramContext::new();
    let stmt = generate_set_null_stmt(
        &fk_ref.child_table.name,
        child_cols,
        &subprog_ctx,
        db_name.as_deref(),
    );
    emit_fk_action_subprogram(
        program,
        resolver,
        connection,
        stmt,
        ctx,
        fk_ref.fk.clone(),
        "fk set null",
    )
}

/// Compile and emit an FK SET DEFAULT action as a sub-program.
/// This creates a sub-program that sets FK columns to their default values for all matching child rows.
fn fire_fk_set_default(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    fk_ref: &ResolvedFkRef,
    connection: &Arc<Connection>,
    ctx: &FkActionContext,
    database_id: usize,
) -> Result<()> {
    let db_name = if database_id != crate::MAIN_DB_ID {
        resolver.get_database_name_by_index(database_id)
    } else {
        None
    };
    let child_cols = &fk_ref.fk.child_columns;
    let subprog_ctx = FkSubprogramContext::new();
    let stmt = generate_set_default_stmt(
        &fk_ref.child_table,
        child_cols,
        &subprog_ctx,
        db_name.as_deref(),
    );
    emit_fk_action_subprogram(
        program,
        resolver,
        connection,
        stmt,
        ctx,
        fk_ref.fk.clone(),
        "fk set default",
    )
}

pub fn emit_fk_drop_table_check(
    program: &mut ProgramBuilder,
    resolver: &mut Resolver,
    parent_table_name: &str,
    connection: &Arc<Connection>,
    database_id: usize,
) -> Result<()> {
    let parent_tbl = resolver
        .with_schema(database_id, |s| s.get_btree_table(parent_table_name))
        .ok_or_else(|| {
            LimboError::InternalError(format!("parent table {parent_table_name} not found"))
        })?;

    // Get all FK references to this parent table
    let fk_refs = resolver.with_schema(database_id, |s| {
        s.resolved_fks_referencing(parent_table_name)
    })?;

    if fk_refs.is_empty() {
        return Ok(());
    }

    // Separate FK refs by action type:
    // - action_fk_refs: CASCADE, SET NULL, SET DEFAULT - need to fire action subprograms
    // - check_fk_refs: RESTRICT, NO ACTION - need violation counting
    let action_fk_refs: Vec<_> = fk_refs
        .iter()
        .filter(|fk| {
            matches!(
                fk.fk.on_delete,
                RefAct::Cascade | RefAct::SetNull | RefAct::SetDefault
            )
        })
        .collect();
    let check_fk_refs: Vec<_> = fk_refs
        .iter()
        .filter(|fk| matches!(fk.fk.on_delete, RefAct::Restrict | RefAct::NoAction))
        .collect();

    // Collect all parent rowids into a RowSet
    // r[rowset_reg] = NULL (initializes RowSet)
    let rowset_reg = program.alloc_register();
    program.emit_null(rowset_reg, None);

    let parent_cur = open_read_table(program, &parent_tbl, database_id);
    let collect_done = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: parent_cur,
        pc_if_empty: collect_done,
    });

    let collect_loop = program.allocate_label();
    program.preassign_label_to_next_insn(collect_loop);

    // Get parent rowid and add to RowSet
    let parent_rowid_reg = program.alloc_register();
    program.emit_insn(Insn::RowId {
        cursor_id: parent_cur,
        dest: parent_rowid_reg,
    });
    program.emit_insn(Insn::RowSetAdd {
        rowset_reg,
        value_reg: parent_rowid_reg,
    });

    program.emit_insn(Insn::Next {
        cursor_id: parent_cur,
        pc_if_next: collect_loop,
    });

    program.preassign_label_to_next_insn(collect_done);
    program.emit_insn(Insn::Close {
        cursor_id: parent_cur,
    });

    // For each parent rowid, check/execute FK actions
    let parent_write_cur = program.alloc_cursor_id(CursorType::BTreeTable(parent_tbl.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: parent_write_cur,
        root_page: parent_tbl.root_page.into(),
        db: database_id,
    });

    let rowset_done = program.allocate_label();
    let rowset_loop = program.allocate_label();
    program.preassign_label_to_next_insn(rowset_loop);
    // Read next rowid from RowSet
    let current_rowid_reg = program.alloc_register();
    program.emit_insn(Insn::RowSetRead {
        rowset_reg,
        pc_if_empty: rowset_done,
        dest_reg: current_rowid_reg,
    });

    // Verify row still exists, jumps if not found
    let skip_row = program.allocate_label();
    program.emit_insn(Insn::NotExists {
        cursor: parent_write_cur,
        rowid_reg: current_rowid_reg,
        target_pc: skip_row,
    });

    // Fire FK actions for CASCADE, SET NULL, SET DEFAULT
    for fk_ref in &action_fk_refs {
        let parent_cols: &[String] = &fk_ref.parent_cols;
        let ncols = parent_cols.len();
        let key_regs_start = program.alloc_registers(ncols);

        build_parent_key(
            program,
            &parent_tbl,
            parent_cols,
            parent_write_cur,
            current_rowid_reg,
            key_regs_start,
            resolver,
            connection,
            database_id,
        )?;

        let old_key_registers: Vec<usize> = (key_regs_start..key_regs_start + ncols).collect();
        let ctx = FkActionContext::new_for_delete(old_key_registers);

        match fk_ref.fk.on_delete {
            RefAct::Cascade => {
                fire_fk_cascade_delete(program, resolver, fk_ref, connection, &ctx, database_id)?;
            }
            RefAct::SetNull => {
                fire_fk_set_null(program, resolver, fk_ref, connection, &ctx, database_id)?;
            }
            RefAct::SetDefault => {
                fire_fk_set_default(program, resolver, fk_ref, connection, &ctx, database_id)?;
            }
            RefAct::NoAction | RefAct::Restrict => {
                // These are handled below in the check_fk_refs loop
            }
        }
    }

    // For RESTRICT/NO ACTION FKs, scan child table for matching rows and count violations
    for fk_ref in &check_fk_refs {
        let child_tbl = &fk_ref.child_table;
        let child_cols = &fk_ref.fk.child_columns;

        // Determine which parent columns are referenced
        let parent_cols: &[String] = &fk_ref.parent_cols;
        let ncols = parent_cols.len();

        // Build the parent key vector from the current parent row
        let parent_key_start = program.alloc_registers(ncols);
        build_parent_key(
            program,
            &parent_tbl,
            parent_cols,
            parent_write_cur,
            current_rowid_reg,
            parent_key_start,
            resolver,
            connection,
            database_id,
        )?;

        // Scan child table for matching rows
        let child_cur = open_read_table(program, child_tbl, database_id);
        let child_done = program.allocate_label();

        program.emit_insn(Insn::Rewind {
            cursor_id: child_cur,
            pc_if_empty: child_done,
        });

        let child_loop = program.allocate_label();
        program.preassign_label_to_next_insn(child_loop);
        let child_next = program.allocate_label();

        // Compare each FK column to corresponding parent key column
        // All columns must match for a violation
        for (i, cname) in child_cols.iter().enumerate() {
            let (pos, _) = child_tbl
                .get_column(cname)
                .ok_or_else(|| LimboError::InternalError(format!("child col {cname} missing")))?;

            let child_val_reg = program.alloc_register();
            program.emit_insn(Insn::Column {
                cursor_id: child_cur,
                column: pos,
                dest: child_val_reg,
                default: None,
            });
            // If child FK column is NULL, skip (no reference)
            program.emit_insn(Insn::IsNull {
                reg: child_val_reg,
                target_pc: child_next,
            });

            // Compare child FK column to corresponding parent key column
            program.emit_insn(Insn::Ne {
                lhs: child_val_reg,
                rhs: parent_key_start + i,
                target_pc: child_next,
                flags: CmpInsFlags::default().jump_if_null(),
                collation: Some(CollationSeq::Binary),
            });
        }

        // If we reach here, all FK columns match: increment violation counter
        program.emit_insn(Insn::FkCounter {
            increment_value: 1,
            deferred: false,
        });

        program.preassign_label_to_next_insn(child_next);
        program.emit_insn(Insn::Next {
            cursor_id: child_cur,
            pc_if_next: child_loop,
        });

        program.preassign_label_to_next_insn(child_done);
        program.emit_insn(Insn::Close {
            cursor_id: child_cur,
        });
    }

    // Note: SQLite deletes the parent row here, but we skip that since
    // the actual deletion happens later in the DROP TABLE logic
    program.preassign_label_to_next_insn(skip_row);
    program.emit_insn(Insn::Goto {
        target_pc: rowset_loop,
    });

    // After processing all rows, check if there were any violations
    program.preassign_label_to_next_insn(rowset_done);
    program.emit_insn(Insn::Close {
        cursor_id: parent_write_cur,
    });

    // Only check for violations if there are RESTRICT/NO ACTION FKs
    if !check_fk_refs.is_empty() {
        // FkIfZero: if counter == 0, skip the halt
        let no_violations = program.allocate_label();
        program.emit_insn(Insn::FkIfZero {
            deferred: false,
            target_pc: no_violations,
        });

        // There were violations, halt with FK error
        emit_fk_restrict_halt(program)?;
        program.preassign_label_to_next_insn(no_violations);
    }

    Ok(())
}
