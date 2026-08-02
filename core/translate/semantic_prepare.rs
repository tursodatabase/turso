//! Prepare closed semantic roots and trigger subprograms for physical emission.
//!
//! This is the narrow orchestration boundary between semantic analysis and
//! physical lowering. Trigger syntax is analyzed into a separate HIR document
//! before this layer allocates parameters or registers for it.

use std::num::NonZeroU32;

use turso_parser::ast::ResolveType;

use crate::{
    schema::Table,
    sync::Arc,
    translate::{
        physical::{
            self, PhysicalPlan, PreparedTrigger, PreparedTriggers, RegisterId, RegisterRange,
            RootRuntimeInputs, SourceRuntime, TriggerParameter, TriggerRow, TriggerRows,
        },
        semantic::{
            analyze,
            context::SemanticContext,
            hir::{self, HirDocument, ResolvedTrigger, TriggerEnvironment},
            AnalyzeInput, TriggerAnalysisInput,
        },
        set_semantic_transactions,
    },
    vdbe::{
        builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode},
        insn::Insn,
        PrepareContext,
    },
    Connection, LimboError, Result,
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

pub(super) fn prepare_triggers(
    context: &SemanticContext<'_>,
    document: &HirDocument,
    parent: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<PreparedTriggers> {
    let mut stack = Vec::new();
    let prepared = prepare_document_triggers(context, document, parent, connection, &mut stack)?;
    inherit_trigger_transactions(context, parent, &prepared)?;
    Ok(prepared)
}

fn prepare_document_triggers(
    context: &SemanticContext<'_>,
    document: &HirDocument,
    parent: &ProgramBuilder,
    connection: &Arc<Connection>,
    stack: &mut Vec<TriggerIdentity>,
) -> Result<PreparedTriggers> {
    let Some(target) = trigger_target(document)? else {
        return Ok(PreparedTriggers::default());
    };
    let mut prepared = PreparedTriggers::default();
    for trigger in target.triggers {
        let identity = trigger_identity(trigger)?;
        if stack.contains(&identity) {
            prepared.suppress(trigger.id());
            continue;
        }
        stack.push(identity);
        let compiled = prepare_one_trigger(context, &target, trigger, parent, connection, stack);
        stack.pop();
        prepared.push(compiled?);
    }
    Ok(prepared)
}

fn prepare_one_trigger(
    context: &SemanticContext<'_>,
    target: &TriggerTarget<'_>,
    trigger: &ResolvedTrigger,
    parent: &ProgramBuilder,
    connection: &Arc<Connection>,
    stack: &mut Vec<TriggerIdentity>,
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
        set_semantic_transactions(&mut program, &document, is_write)?;
        let nested =
            prepare_document_triggers(&trigger_context, &document, &program, connection, stack)?;
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
    }
}

fn trigger_target(document: &HirDocument) -> Result<Option<TriggerTarget<'_>>> {
    let (source_id, new_visible, old_visible, conflict, triggers) = match &document.root {
        hir::HirRoot::Insert(root) => (
            root.target,
            true,
            false,
            root.conflict,
            root.triggers.as_slice(),
        ),
        hir::HirRoot::Update(root) => (
            root.target,
            true,
            true,
            root.conflict,
            root.triggers.as_slice(),
        ),
        hir::HirRoot::Delete(root) => (root.target, false, true, None, root.triggers.as_slice()),
        hir::HirRoot::Query(_) | hir::HirRoot::TriggerPredicate(_) => return Ok(None),
    };
    if triggers.is_empty() {
        return Ok(None);
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
    Ok(Some(TriggerTarget {
        database_id,
        table: table.handle(),
        new_visible,
        old_visible,
        conflict,
        triggers,
    }))
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
        for database in &trigger.program.write_databases {
            let cookie = schema_cookie(context, database)?;
            parent.begin_write_on_database(database, cookie)?;
        }
        for database in &trigger.program.read_databases {
            if trigger.program.write_databases.get(database) {
                continue;
            }
            let cookie = schema_cookie(context, database)?;
            parent.begin_read_on_database(database, cookie)?;
        }
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

    use super::*;

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
