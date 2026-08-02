//! Prepare closed semantic roots and trigger subprograms for physical emission.
//!
//! This is the narrow orchestration boundary between semantic analysis and
//! physical lowering. Trigger syntax is analyzed into a separate HIR document
//! before this layer allocates parameters or registers for it.

use std::num::{NonZero, NonZeroU32};

use turso_parser::ast::{self, Expr, Literal, Name, QualifiedName, RefAct, ResolveType};

use crate::{
    schema::{ForeignKey, Table},
    sync::{Arc, OnceLock, Weak},
    translate::{
        physical::{
            self, ForeignKeyParentChange, PhysicalPlan, PreparedForeignKeyAction, PreparedTrigger,
            PreparedTriggers, RegisterId, RegisterRange, RootRuntimeInputs, SourceRuntime,
            TriggerParameter, TriggerRow, TriggerRows,
        },
        semantic::{
            analyze,
            context::SemanticContext,
            hir::{self, HirDocument, ResolvedTrigger, TriggerEnvironment},
            AnalyzeInput, TriggerAnalysisInput,
        },
        set_semantic_statement_journal_flags, set_semantic_transactions,
    },
    vdbe::{
        builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode},
        insn::{Insn, Subprogram},
        PrepareContext,
    },
    Connection, LimboError, PreparedProgram, Result,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct TriggerIdentity {
    owner_database: usize,
    name: String,
}

struct TriggerTarget<'document> {
    database_id: usize,
    table: Arc<Table>,
    new_visible: bool,
    old_visible: bool,
    conflict: Option<ResolveType>,
    triggers: &'document [ResolvedTrigger],
}

struct ForeignKeyActionStackEntry {
    child_table: hir::CatalogObjectId,
    declaration_order: usize,
    parent_change: ForeignKeyParentChange,
    slot: Arc<OnceLock<Weak<PreparedProgram>>>,
}

pub(super) fn prepare_triggers(
    context: &SemanticContext<'_>,
    document: &HirDocument,
    parent: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<PreparedTriggers> {
    let mut trigger_stack = Vec::new();
    let mut foreign_key_stack = Vec::new();
    let prepared = prepare_document_triggers(
        context,
        document,
        parent,
        connection,
        &mut trigger_stack,
        &mut foreign_key_stack,
    )?;
    inherit_trigger_transactions(context, parent, &prepared)?;
    Ok(prepared)
}

fn prepare_document_triggers(
    context: &SemanticContext<'_>,
    document: &HirDocument,
    parent: &ProgramBuilder,
    connection: &Arc<Connection>,
    stack: &mut Vec<TriggerIdentity>,
    foreign_key_stack: &mut Vec<ForeignKeyActionStackEntry>,
) -> Result<PreparedTriggers> {
    let mut prepared = PreparedTriggers::default();
    for target in trigger_targets(document)? {
        for trigger in target.triggers {
            let identity = trigger_identity(trigger)?;
            if stack.contains(&identity) {
                prepared.suppress(trigger.id());
                continue;
            }
            stack.push(identity);
            let compiled = prepare_one_trigger(
                context,
                &target,
                trigger,
                parent,
                connection,
                stack,
                foreign_key_stack,
            );
            stack.pop();
            prepared.push(compiled?);
        }
    }
    prepare_document_foreign_key_actions(
        context,
        document,
        parent,
        connection,
        stack,
        foreign_key_stack,
        &mut prepared,
    )?;
    Ok(prepared)
}

fn prepare_one_trigger(
    context: &SemanticContext<'_>,
    target: &TriggerTarget<'_>,
    trigger: &ResolvedTrigger,
    parent: &ProgramBuilder,
    connection: &Arc<Connection>,
    stack: &mut Vec<TriggerIdentity>,
    foreign_key_stack: &mut Vec<ForeignKeyActionStackEntry>,
) -> Result<PreparedTrigger> {
    let owner_database = trigger
        .database()
        .ok_or_else(|| internal("resolved trigger has no owner database"))?
        .index();
    let definition = trigger.value();
    let trigger_context = context.for_trigger(owner_database, definition.name.clone());
    let analysis_input = TriggerAnalysisInput {
        database_id: target.database_id,
        table: target.table.clone(),
        new_visible: target.new_visible,
        old_visible: target.old_visible,
        override_conflict: target.conflict,
    };
    let mut program = ProgramBuilder::new_for_trigger(
        QueryMode::Normal,
        parent.capture_data_changes_info().clone(),
        ProgramBuilderOpts::new(1, 32, 2),
        trigger.handle(),
    );
    program.set_mvcc_enabled(connection.mvcc_enabled());
    if let Some(conflict) = target.conflict {
        program.set_trigger_conflict_override(conflict);
    }
    program.prologue();

    let (rows, parameters) = allocate_trigger_parameters(
        &mut program,
        target.table.columns().len(),
        target.new_visible,
        target.old_visible,
    )?;
    let skip_trigger = program.allocate_label();

    if let Some(predicate) = &definition.when_clause {
        let document = analyze(
            &trigger_context,
            AnalyzeInput::TriggerPredicate {
                syntax: predicate,
                trigger: &analysis_input,
            },
        )?;
        let inputs = trigger_inputs(&document, rows)?;
        let plan = PhysicalPlan::new(&document).map_err(|error| internal(error.to_string()))?;
        physical::emit_trigger_predicate(&plan, &mut program, &inputs, skip_trigger)
            .map_err(|error| internal(error.to_string()))?;
    }

    for command in &definition.commands {
        let document = analyze(
            &trigger_context,
            AnalyzeInput::TriggerCommand {
                syntax: command,
                trigger: &analysis_input,
            },
        )?;
        let is_write = matches!(
            document.root,
            hir::HirRoot::Insert(_) | hir::HirRoot::Update(_) | hir::HirRoot::Delete(_)
        );
        super::set_semantic_conflict_policy(&mut program, &document);
        set_semantic_statement_journal_flags(&mut program, &document)?;
        set_semantic_transactions(&mut program, &document, is_write)?;
        let nested = prepare_document_triggers(
            &trigger_context,
            &document,
            &program,
            connection,
            stack,
            foreign_key_stack,
        )?;
        inherit_trigger_transactions(&trigger_context, &mut program, &nested)?;
        let inputs = trigger_inputs(&document, rows)?;
        let plan = PhysicalPlan::new(&document).map_err(|error| internal(error.to_string()))?;
        physical::emit_root_with_context(&plan, &mut program, &inputs, &nested)
            .map_err(|error| internal(error.to_string()))?;
        if is_write {
            program.emit_insn(Insn::ResetCount);
        }
    }

    program.preassign_label_to_next_insn(skip_trigger);
    program.epilogue(context.main_schema());
    let prepared = program.build_prepared_program(
        PrepareContext::from_connection(connection),
        true,
        "trigger subprogram",
    )?;
    Ok(PreparedTrigger {
        id: trigger.id(),
        program: Arc::new(prepared),
        parameters,
    })
}

#[allow(clippy::too_many_arguments)]
fn prepare_document_foreign_key_actions(
    context: &SemanticContext<'_>,
    document: &HirDocument,
    parent: &ProgramBuilder,
    connection: &Arc<Connection>,
    trigger_stack: &mut Vec<TriggerIdentity>,
    stack: &mut Vec<ForeignKeyActionStackEntry>,
    prepared: &mut PreparedTriggers,
) -> Result<()> {
    let changes = match &document.root {
        hir::HirRoot::Delete(delete) => vec![(
            delete.foreign_keys.incoming.as_slice(),
            ForeignKeyParentChange::Delete,
        )],
        hir::HirRoot::Update(update) => vec![(
            update.foreign_keys.incoming.as_slice(),
            ForeignKeyParentChange::Update,
        )],
        hir::HirRoot::Insert(insert) => vec![(
            insert.foreign_keys.incoming.as_slice(),
            ForeignKeyParentChange::Update,
        )],
        hir::HirRoot::Query(_)
        | hir::HirRoot::TriggerPredicate(_)
        | hir::HirRoot::SchemaExpressions(_) => return Ok(()),
    };
    for (foreign_keys, parent_change) in changes {
        for foreign_key in foreign_keys {
            let action = match parent_change {
                ForeignKeyParentChange::Delete => foreign_key.declaration.on_delete,
                ForeignKeyParentChange::Update => foreign_key.declaration.on_update,
            };
            if matches!(action, RefAct::NoAction | RefAct::Restrict) {
                continue;
            }
            prepared.push_foreign_key_action(prepare_one_foreign_key_action(
                context,
                foreign_key,
                parent_change,
                action,
                parent,
                connection,
                trigger_stack,
                stack,
            )?);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn prepare_one_foreign_key_action(
    context: &SemanticContext<'_>,
    foreign_key: &hir::ResolvedForeignKey,
    parent_change: ForeignKeyParentChange,
    action: RefAct,
    parent: &ProgramBuilder,
    connection: &Arc<Connection>,
    trigger_stack: &mut Vec<TriggerIdentity>,
    stack: &mut Vec<ForeignKeyActionStackEntry>,
) -> Result<PreparedForeignKeyAction> {
    let child_table = foreign_key.child_table.id();
    let declaration_order = foreign_key.declaration.decl_order;
    if let Some(entry) = stack.iter().find(|entry| {
        entry.child_table == child_table
            && entry.declaration_order == declaration_order
            && entry.parent_change == parent_change
    }) {
        return Ok(PreparedForeignKeyAction {
            child_table,
            declaration_order,
            parent_change,
            program: Subprogram::Pending(entry.slot.clone()),
        });
    }

    let slot = Arc::new(OnceLock::new());
    stack.push(ForeignKeyActionStackEntry {
        child_table,
        declaration_order,
        parent_change,
        slot: slot.clone(),
    });
    let compiled = (|| {
        let statement = foreign_key_action_statement(context, foreign_key, parent_change, action)?;
        let nested_context = context.with_dml_policy(context.dml_policy().as_nested_statement());
        let document = analyze(&nested_context, AnalyzeInput::Statement(&statement))?;
        let mut program = ProgramBuilder::new_for_subprogram(
            QueryMode::Normal,
            parent.capture_data_changes_info().clone(),
            ProgramBuilderOpts::new(2, 32, 4),
        );
        program.set_mvcc_enabled(connection.mvcc_enabled());
        program.prologue();
        super::set_semantic_conflict_policy(&mut program, &document);
        set_semantic_statement_journal_flags(&mut program, &document)?;
        set_semantic_transactions(&mut program, &document, true)?;
        let nested = prepare_document_triggers(
            &nested_context,
            &document,
            &program,
            connection,
            trigger_stack,
            stack,
        )?;
        inherit_trigger_transactions(&nested_context, &mut program, &nested)?;
        let plan = PhysicalPlan::new(&document).map_err(|error| internal(error.to_string()))?;
        physical::emit_root_with_context(
            &plan,
            &mut program,
            &RootRuntimeInputs::default(),
            &nested,
        )
        .map_err(|error| internal(error.to_string()))?;
        program.epilogue(context.main_schema());
        let description = foreign_key_action_description(parent_change, action);
        program.build_prepared_program(
            PrepareContext::from_connection(connection),
            true,
            description,
        )
    })();
    let ended = stack
        .pop()
        .expect("foreign-key action preparation stack underflow");
    debug_assert!(Arc::ptr_eq(&ended.slot, &slot));
    let program = Arc::new(compiled?);
    slot.set(Arc::downgrade(&program))
        .expect("foreign-key action slot is set once");
    Ok(PreparedForeignKeyAction {
        child_table,
        declaration_order,
        parent_change,
        program: Subprogram::PreparedProgram(program),
    })
}

fn foreign_key_action_statement(
    context: &SemanticContext<'_>,
    foreign_key: &hir::ResolvedForeignKey,
    parent_change: ForeignKeyParentChange,
    action: RefAct,
) -> Result<ast::Stmt> {
    let database = foreign_key
        .child_table
        .database()
        .ok_or_else(|| internal("foreign-key child has no database identity"))?
        .index();
    let database_name = (database != crate::MAIN_DB_ID)
        .then(|| context.database_name(database).map(str::to_string))
        .flatten();
    if database != crate::MAIN_DB_ID && database_name.is_none() {
        return Err(internal("foreign-key child database has no name"));
    }
    let Table::BTree(child_table) = foreign_key.child_table.value() else {
        return Err(internal("foreign-key action target is not a B-tree table"));
    };
    let parameters = ForeignKeyActionParameters::new(
        foreign_key.child_positions.len(),
        parent_change == ForeignKeyParentChange::Update,
    );
    let table_name = QualifiedName {
        db_name: database_name.map(Name::from_string),
        name: Name::from_string(&child_table.name),
        alias: None,
    };
    let predicate = foreign_key_action_predicate(&foreign_key.declaration, &parameters);
    if action == RefAct::Cascade && parent_change == ForeignKeyParentChange::Delete {
        return Ok(ast::Stmt::Delete {
            with: None,
            tbl_name: table_name,
            indexed: None,
            where_clause: Some(Box::new(predicate)),
            returning: Vec::new(),
            order_by: Vec::new(),
            limit: None,
        });
    }
    let sets = foreign_key
        .declaration
        .child_columns
        .iter()
        .enumerate()
        .map(|(offset, column)| {
            let expression = match action {
                RefAct::SetNull => Expr::Literal(Literal::Null),
                RefAct::SetDefault => child_table
                    .get_column(column)
                    .and_then(|(_, column)| column.default.as_deref().cloned())
                    .unwrap_or(Expr::Literal(Literal::Null)),
                RefAct::Cascade => parameter_expression(
                    parameters
                        .new_parameter(offset)
                        .expect("UPDATE CASCADE has NEW parameters"),
                ),
                RefAct::NoAction | RefAct::Restrict => unreachable!(),
            };
            ast::Set {
                col_names: vec![Name::from_string(column)],
                expr: Box::new(expression),
            }
        })
        .collect();
    Ok(ast::Stmt::Update(ast::Update {
        with: None,
        or_conflict: None,
        tbl_name: table_name,
        indexed: None,
        sets,
        from: None,
        where_clause: Some(Box::new(predicate)),
        returning: Vec::new(),
        order_by: Vec::new(),
        limit: None,
    }))
}

struct ForeignKeyActionParameters {
    width: usize,
    new_start: Option<usize>,
}

impl ForeignKeyActionParameters {
    fn new(width: usize, has_new: bool) -> Self {
        Self {
            width,
            new_start: has_new.then_some(width + 1),
        }
    }

    fn old(&self, offset: usize) -> NonZero<usize> {
        NonZero::new(offset + 1).expect("foreign-key parameter is one-indexed")
    }

    fn new_parameter(&self, offset: usize) -> Option<NonZero<usize>> {
        self.new_start
            .and_then(|start| NonZero::new(start + offset))
    }
}

fn foreign_key_action_predicate(
    foreign_key: &ForeignKey,
    parameters: &ForeignKeyActionParameters,
) -> Expr {
    debug_assert_eq!(foreign_key.child_columns.len(), parameters.width);
    foreign_key
        .child_columns
        .iter()
        .enumerate()
        .map(|(offset, column)| {
            Expr::Binary(
                Box::new(Expr::Id(Name::from_string(column))),
                ast::Operator::Equals,
                Box::new(parameter_expression(parameters.old(offset))),
            )
        })
        .reduce(|left, right| Expr::Binary(Box::new(left), ast::Operator::And, Box::new(right)))
        .expect("foreign keys have at least one child column")
}

fn parameter_expression(index: NonZero<usize>) -> Expr {
    let index = u32::try_from(index.get())
        .ok()
        .and_then(NonZeroU32::new)
        .expect("foreign-key parameter index fits in u32");
    Expr::Variable(ast::Variable::indexed(index))
}

fn foreign_key_action_description(
    parent_change: ForeignKeyParentChange,
    action: RefAct,
) -> &'static str {
    match (parent_change, action) {
        (ForeignKeyParentChange::Delete, RefAct::Cascade) => "foreign-key cascade delete",
        (ForeignKeyParentChange::Delete, RefAct::SetNull) => "foreign-key set null on delete",
        (ForeignKeyParentChange::Delete, RefAct::SetDefault) => "foreign-key set default on delete",
        (ForeignKeyParentChange::Update, RefAct::Cascade) => "foreign-key cascade update",
        (ForeignKeyParentChange::Update, RefAct::SetNull) => "foreign-key set null on update",
        (ForeignKeyParentChange::Update, RefAct::SetDefault) => "foreign-key set default on update",
        (_, RefAct::NoAction | RefAct::Restrict) => unreachable!(),
    }
}

fn allocate_trigger_parameters(
    program: &mut ProgramBuilder,
    width: usize,
    new_visible: bool,
    old_visible: bool,
) -> Result<(TriggerRows, Vec<TriggerParameter>)> {
    let visible_rows = usize::from(new_visible) + usize::from(old_visible);
    let mut parameters = Vec::with_capacity((width + 1) * visible_rows);
    let new = new_visible
        .then(|| allocate_trigger_row(program, width, true, &mut parameters))
        .transpose()?;
    let old = old_visible
        .then(|| allocate_trigger_row(program, width, false, &mut parameters))
        .transpose()?;
    Ok((TriggerRows { new, old }, parameters))
}

fn allocate_trigger_row(
    program: &mut ProgramBuilder,
    width: usize,
    is_new: bool,
    parameters: &mut Vec<TriggerParameter>,
) -> Result<TriggerRow> {
    let columns = RegisterRange::new(program.alloc_registers(width), width);
    for position in 0..width {
        let recipe = if is_new {
            TriggerParameter::NewColumn(position)
        } else {
            TriggerParameter::OldColumn(position)
        };
        emit_trigger_parameter(program, columns.first.0 + position, parameters, recipe)?;
    }
    let rowid = RegisterId(program.alloc_register());
    let recipe = if is_new {
        TriggerParameter::NewRowId
    } else {
        TriggerParameter::OldRowId
    };
    emit_trigger_parameter(program, rowid.0, parameters, recipe)?;
    Ok(TriggerRow { columns, rowid })
}

fn emit_trigger_parameter(
    program: &mut ProgramBuilder,
    destination: usize,
    parameters: &mut Vec<TriggerParameter>,
    recipe: TriggerParameter,
) -> Result<()> {
    let index = u32::try_from(parameters.len() + 1)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or_else(|| internal("trigger parameter count exceeds the supported range"))?;
    let index = program.register_resolved_parameter(index, None);
    program.emit_insn(Insn::Variable {
        index,
        dest: destination,
    });
    parameters.push(recipe);
    Ok(())
}

fn trigger_inputs(document: &HirDocument, rows: TriggerRows) -> Result<RootRuntimeInputs> {
    let environment = trigger_environment(document)
        .ok_or_else(|| internal("trigger analysis produced a root without its environment"))?;
    let mut inputs = RootRuntimeInputs::default();
    bind_trigger_source(&mut inputs, environment.new_source, rows.new, "NEW")?;
    bind_trigger_source(&mut inputs, environment.old_source, rows.old, "OLD")?;
    Ok(inputs)
}

fn bind_trigger_source(
    inputs: &mut RootRuntimeInputs,
    source: Option<hir::SourceId>,
    row: Option<TriggerRow>,
    name: &'static str,
) -> Result<()> {
    match (source, row) {
        (Some(source), Some(row)) => {
            inputs.bind_source(
                source,
                SourceRuntime::Registers {
                    columns: row.columns,
                    rowid: Some(row.rowid),
                },
            );
            Ok(())
        }
        (None, None) => Ok(()),
        _ => Err(internal(format!(
            "trigger {name} visibility does not match its runtime row"
        ))),
    }
}

fn trigger_environment(document: &HirDocument) -> Option<&TriggerEnvironment> {
    match &document.root {
        hir::HirRoot::Query(root) => root.trigger.as_ref(),
        hir::HirRoot::Insert(root) => root.trigger.as_ref(),
        hir::HirRoot::Update(root) => root.trigger.as_ref(),
        hir::HirRoot::Delete(root) => root.trigger.as_ref(),
        hir::HirRoot::TriggerPredicate(root) => Some(&root.environment),
        hir::HirRoot::SchemaExpressions(_) => None,
    }
}

fn trigger_targets(document: &HirDocument) -> Result<Vec<TriggerTarget<'_>>> {
    let targets = match &document.root {
        hir::HirRoot::Insert(root) => vec![
            (
                root.target,
                true,
                false,
                root.conflict,
                root.triggers.as_slice(),
            ),
            (
                root.target,
                true,
                true,
                Some(ResolveType::Abort),
                root.upsert_triggers.as_slice(),
            ),
        ],
        hir::HirRoot::Update(root) => vec![(
            root.target,
            true,
            true,
            root.conflict,
            root.triggers.as_slice(),
        )],
        hir::HirRoot::Delete(root) => {
            vec![(root.target, false, true, None, root.triggers.as_slice())]
        }
        hir::HirRoot::Query(_)
        | hir::HirRoot::TriggerPredicate(_)
        | hir::HirRoot::SchemaExpressions(_) => return Ok(Vec::new()),
    };
    let mut resolved = Vec::with_capacity(targets.len());
    for (source_id, new_visible, old_visible, conflict, triggers) in targets {
        if triggers.is_empty() {
            continue;
        }
        let source = document
            .source(source_id)
            .ok_or_else(|| internal("trigger target source is missing"))?;
        let hir::SourceKind::Table(table) = &source.kind else {
            return Err(internal("trigger target is not a resolved table"));
        };
        let database_id = table
            .database()
            .ok_or_else(|| internal("trigger target has no database identity"))?
            .index();
        resolved.push(TriggerTarget {
            database_id,
            table: table.handle(),
            new_visible,
            old_visible,
            conflict,
            triggers,
        });
    }
    Ok(resolved)
}

fn trigger_identity(trigger: &ResolvedTrigger) -> Result<TriggerIdentity> {
    Ok(TriggerIdentity {
        owner_database: trigger
            .database()
            .ok_or_else(|| internal("resolved trigger has no owner database"))?
            .index(),
        name: crate::util::normalize_ident(&trigger.value().name),
    })
}

fn inherit_trigger_transactions(
    context: &SemanticContext<'_>,
    parent: &mut ProgramBuilder,
    triggers: &PreparedTriggers,
) -> Result<()> {
    for trigger in triggers.iter() {
        inherit_program_transactions(context, parent, &trigger.program)?;
    }
    for action in triggers.foreign_key_actions() {
        if let Subprogram::PreparedProgram(program) = &action.program {
            inherit_program_transactions(context, parent, program)?;
        }
    }
    Ok(())
}

fn inherit_program_transactions(
    context: &SemanticContext<'_>,
    parent: &mut ProgramBuilder,
    program: &PreparedProgram,
) -> Result<()> {
    for database in &program.write_databases {
        let cookie = schema_cookie(context, database)?;
        parent.begin_write_on_database(database, cookie)?;
    }
    for database in &program.read_databases {
        if program.write_databases.get(database) {
            continue;
        }
        let cookie = schema_cookie(context, database)?;
        parent.begin_read_on_database(database, cookie)?;
    }
    Ok(())
}

fn schema_cookie(context: &SemanticContext<'_>, database: usize) -> Result<u32> {
    context
        .schema(database)
        .map(|schema| schema.schema_version)
        .ok_or_else(|| {
            internal(format!(
                "trigger subprogram database {database} disappeared"
            ))
        })
}

fn internal(message: impl Into<String>) -> LimboError {
    LimboError::InternalError(message.into())
}

#[cfg(test)]
mod properties {
    use hegel::generators;
    use turso_parser::parser::Parser;

    use super::*;
    use crate::{
        dialect::{Dialect, SqliteDialect},
        schema::{BTreeTable, Schema},
        translate::semantic::context::DmlPolicy,
        SymbolTable,
    };

    fn parse_statement(sql: &str) -> ast::Stmt {
        let command = Parser::new(sql.as_bytes())
            .next_cmd()
            .expect("generated SQL parses")
            .expect("generated SQL contains a statement");
        let ast::Cmd::Stmt(statement) = command else {
            panic!("generated SQL contains a statement");
        };
        statement
    }

    // Examples:
    // - deleting `parents.p4` with `ON DELETE CASCADE` becomes a child DELETE
    //   whose `c2 = ?1` predicate binds to frozen child position two;
    // - updating that parent with `ON UPDATE CASCADE` becomes a child UPDATE
    //   whose assignment uses `?2`, while SET NULL and SET DEFAULT freeze their
    //   respective values. Varying positions, action, and parent change proves
    //   the generated action is analyzed into ordinary closed HIR.
    #[hegel::test]
    fn foreign_key_actions_are_closed_hir_child_mutations(tc: hegel::TestCase) {
        let width = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(10)));
        let child_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
        let parent_position = tc.draw(generators::integers::<usize>().max_value(width - 1));
        let parent_change = if tc.draw(generators::booleans()) {
            ForeignKeyParentChange::Delete
        } else {
            ForeignKeyParentChange::Update
        };
        let action = match tc.draw(generators::integers::<u8>().max_value(2)) {
            0 => RefAct::Cascade,
            1 => RefAct::SetNull,
            _ => RefAct::SetDefault,
        };
        let parent_columns = (0..width)
            .map(|position| {
                if position == parent_position {
                    format!("p{position} INTEGER PRIMARY KEY")
                } else {
                    format!("p{position} INTEGER")
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let child_columns = (0..width)
            .map(|position| {
                if position == child_position {
                    format!(
                        "c{position} INTEGER DEFAULT 17 REFERENCES parents(p{parent_position}) ON {} {}",
                        if parent_change == ForeignKeyParentChange::Delete {
                            "DELETE"
                        } else {
                            "UPDATE"
                        },
                        match action {
                            RefAct::Cascade => "CASCADE",
                            RefAct::SetNull => "SET NULL",
                            RefAct::SetDefault => "SET DEFAULT",
                            RefAct::NoAction | RefAct::Restrict => unreachable!(),
                        }
                    )
                } else {
                    format!("c{position} INTEGER")
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let parent = BTreeTable::from_sql(&format!("CREATE TABLE parents({parent_columns})"), 23)
            .expect("parent table SQL is valid");
        let child = BTreeTable::from_sql(&format!("CREATE TABLE children({child_columns})"), 29)
            .expect("child table SQL is valid");
        let mut schema = Schema::new();
        schema
            .add_btree_table(Arc::new(parent))
            .expect("parents is unique");
        schema
            .add_btree_table(Arc::new(child))
            .expect("children is unique");
        let symbols = SymbolTable::new();
        let dialect: Arc<dyn Dialect> = Arc::new(SqliteDialect);
        let context = SemanticContext::for_main_schema_object(&schema, &symbols, true, dialect)
            .with_dml_policy(DmlPolicy::new(false, false, false, false, true));
        let root_statement = parse_statement(match parent_change {
            ForeignKeyParentChange::Delete => "DELETE FROM parents",
            ForeignKeyParentChange::Update => "UPDATE parents SET p0 = p0",
        });
        let root = analyze(&context, AnalyzeInput::Statement(&root_statement))
            .expect("parent mutation has valid SQL meaning");
        let foreign_key = match &root.root {
            hir::HirRoot::Delete(delete) => &delete.foreign_keys.incoming[0],
            hir::HirRoot::Update(update) => &update.foreign_keys.incoming[0],
            _ => panic!("fixture produces parent DML"),
        };
        let action_statement =
            foreign_key_action_statement(&context, foreign_key, parent_change, action)
                .expect("action syntax is generated from frozen FK facts");
        let action_document = analyze(
            &context.with_dml_policy(context.dml_policy().as_nested_statement()),
            AnalyzeInput::Statement(&action_statement),
        )
        .expect("generated action analyzes into closed HIR");
        action_document
            .validate()
            .expect("generated action HIR is closed");

        let (target, predicate, assignment) = match &action_document.root {
            hir::HirRoot::Delete(delete) => (delete.target, delete.predicate.as_ref(), None),
            hir::HirRoot::Update(update) => (
                update.target,
                update.predicate.as_ref(),
                Some(&update.assignments[0]),
            ),
            _ => panic!("an FK action is child DML"),
        };
        let Some(hir::Expr::Binary { lhs, rhs, .. }) = predicate else {
            panic!("one-column FK action has one equality predicate");
        };
        let hir::Expr::Column(column) = lhs.as_ref() else {
            panic!("action predicate reads the frozen child column");
        };
        let hir::Expr::Parameter(parameter) = rhs.as_ref() else {
            panic!("action predicate compares against the OLD parent parameter");
        };
        assert_eq!(column.source, target);
        assert_eq!(column.column, child_position);
        assert_eq!(parameter.index.get(), 1);
        if let Some(assignment) = assignment {
            assert!(matches!(
                assignment.columns.as_slice(),
                [hir::TargetColumn::Column(position)] if *position == child_position
            ));
            if action == RefAct::Cascade {
                let hir::Expr::Parameter(parameter) = &assignment.value else {
                    panic!("UPDATE CASCADE assigns the NEW parent parameter");
                };
                assert_eq!(parameter.index.get(), 2);
            }
        } else {
            assert_eq!(parent_change, ForeignKeyParentChange::Delete);
            assert_eq!(action, RefAct::Cascade);
        }
    }

    // Example: UPDATE of a three-column table allocates
    // `NEW.c0..NEW.c2, NEW.rowid, OLD.c0..OLD.c2, OLD.rowid` as parameters 1..8.
    #[hegel::test]
    fn prepared_trigger_parameters_match_visible_rows_and_table_width(tc: hegel::TestCase) {
        let width = usize::from(tc.draw(generators::integers::<u8>().min_value(1).max_value(16)));
        let new_visible = tc.draw(generators::booleans());
        let old_visible = tc.draw(generators::booleans()) || !new_visible;
        let mut program = ProgramBuilder::new_for_subprogram(
            QueryMode::Normal,
            None,
            ProgramBuilderOpts::new(1, 32, 2),
        );

        let (rows, recipes) =
            allocate_trigger_parameters(&mut program, width, new_visible, old_visible)
                .expect("the generated trigger width fits in parameter indices");
        let variables = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Variable { index, dest } => Some((index.get(), *dest)),
                _ => None,
            })
            .collect::<Vec<_>>();
        let expected_rows = usize::from(new_visible) + usize::from(old_visible);

        assert_eq!(recipes.len(), (width + 1) * expected_rows);
        assert_eq!(variables.len(), recipes.len());
        assert!(variables
            .iter()
            .enumerate()
            .all(|(position, (index, _))| *index == position + 1));
        if let Some(new) = rows.new {
            assert_eq!(recipes[0], TriggerParameter::NewColumn(0));
            assert_eq!(recipes[width], TriggerParameter::NewRowId);
            assert_eq!(variables[0].1, new.columns.first.0);
            assert_eq!(variables[width].1, new.rowid.0);
        }
        if let Some(old) = rows.old {
            let start = usize::from(new_visible) * (width + 1);
            assert_eq!(recipes[start], TriggerParameter::OldColumn(0));
            assert_eq!(recipes[start + width], TriggerParameter::OldRowId);
            assert_eq!(variables[start].1, old.columns.first.0);
            assert_eq!(variables[start + width].1, old.rowid.0);
        }
    }
}
