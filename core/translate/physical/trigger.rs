//! Trigger-specific lowering for explicit OLD and NEW HIR sources.

use std::fmt;

use crate::{
    schema::ForeignKey,
    sync::Arc,
    translate::semantic::hir::{CatalogObjectId, ResolvedTrigger},
    vdbe::{
        builder::ProgramBuilder,
        insn::{Insn, Subprogram},
        BranchOffset, PreparedProgram,
    },
};

use super::{
    ExpressionEmitter, PhysicalExpressionError, PhysicalPlan, PhysicalRoot, RootRuntimeInputs,
    RuntimeBindingError, RuntimeBindings,
};

#[derive(Debug)]
pub(crate) enum PhysicalTriggerError {
    Runtime(RuntimeBindingError),
    Expression(PhysicalExpressionError),
    Invalid(&'static str),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TriggerParameter {
    NewColumn(usize),
    NewRowId,
    OldColumn(usize),
    OldRowId,
}

#[derive(Clone)]
pub(crate) struct PreparedTrigger {
    pub(crate) id: CatalogObjectId,
    pub(crate) program: Arc<PreparedProgram>,
    pub(crate) parameters: Vec<TriggerParameter>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ForeignKeyParentChange {
    Delete,
    Update,
}

#[derive(Clone)]
pub(crate) struct PreparedForeignKeyAction {
    pub(crate) child_table: CatalogObjectId,
    pub(crate) declaration_order: usize,
    pub(crate) parent_change: ForeignKeyParentChange,
    pub(crate) program: Subprogram,
}

#[derive(Default)]
pub(crate) struct PreparedTriggers {
    programs: Vec<PreparedTrigger>,
    suppressed: Vec<CatalogObjectId>,
    foreign_key_actions: Vec<PreparedForeignKeyAction>,
}

impl PreparedTriggers {
    pub(crate) fn push(&mut self, trigger: PreparedTrigger) {
        self.programs.push(trigger);
    }

    pub(crate) fn suppress(&mut self, trigger: CatalogObjectId) {
        self.suppressed.push(trigger);
    }

    pub(crate) fn push_foreign_key_action(&mut self, action: PreparedForeignKeyAction) {
        self.foreign_key_actions.push(action);
    }

    pub(crate) fn foreign_key_action(
        &self,
        child_table: CatalogObjectId,
        foreign_key: &ForeignKey,
        parent_change: ForeignKeyParentChange,
    ) -> Option<&PreparedForeignKeyAction> {
        self.foreign_key_actions.iter().find(|action| {
            action.child_table == child_table
                && action.declaration_order == foreign_key.decl_order
                && action.parent_change == parent_change
        })
    }

    fn find(&self, trigger: CatalogObjectId) -> Option<&PreparedTrigger> {
        self.programs.iter().find(|program| program.id == trigger)
    }

    pub(crate) fn covers<'a>(
        &self,
        triggers: impl IntoIterator<Item = &'a ResolvedTrigger>,
    ) -> bool {
        triggers.into_iter().all(|trigger| {
            self.find(trigger.id()).is_some() || self.suppressed.contains(&trigger.id())
        })
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &PreparedTrigger> {
        self.programs.iter()
    }

    pub(crate) fn foreign_key_actions(&self) -> impl Iterator<Item = &PreparedForeignKeyAction> {
        self.foreign_key_actions.iter()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct TriggerRow {
    pub(crate) columns: super::RegisterRange,
    pub(crate) rowid: super::RegisterId,
}

#[derive(Clone, Copy)]
pub(crate) struct TriggerRows {
    pub(crate) new: Option<TriggerRow>,
    pub(crate) old: Option<TriggerRow>,
}

pub(super) fn resolve_trigger_parameters(
    parameters: &[TriggerParameter],
    rows: TriggerRows,
) -> Result<Vec<usize>, PhysicalTriggerError> {
    parameters
        .iter()
        .map(|parameter| {
            match *parameter {
                TriggerParameter::NewColumn(position) => rows
                    .new
                    .and_then(|row| row.columns.register(position))
                    .map(|register| register.0),
                TriggerParameter::NewRowId => rows.new.map(|row| row.rowid.0),
                TriggerParameter::OldColumn(position) => rows
                    .old
                    .and_then(|row| row.columns.register(position))
                    .map(|register| register.0),
                TriggerParameter::OldRowId => rows.old.map(|row| row.rowid.0),
            }
            .ok_or(PhysicalTriggerError::Invalid(
                "trigger parameter requests an unavailable row image",
            ))
        })
        .collect()
}

/// Emit calls for the selected resolved triggers in their frozen order.
pub(crate) fn emit_trigger_programs<'a>(
    program: &mut ProgramBuilder,
    prepared: &PreparedTriggers,
    triggers: impl IntoIterator<Item = &'a ResolvedTrigger>,
    rows: TriggerRows,
    ignore_jump_target: BranchOffset,
) -> Result<(), PhysicalTriggerError> {
    for trigger in triggers {
        let Some(compiled) = prepared.find(trigger.id()) else {
            if prepared.suppressed.contains(&trigger.id()) {
                continue;
            }
            return Err(PhysicalTriggerError::Invalid(
                "resolved trigger has no prepared program",
            ));
        };
        let param_registers = resolve_trigger_parameters(&compiled.parameters, rows)?;
        program.emit_insn(Insn::Program {
            param_registers,
            program: Subprogram::PreparedProgram(compiled.program.clone()),
            ignore_jump_target,
        });
    }
    Ok(())
}

impl fmt::Display for PhysicalTriggerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => error.fmt(formatter),
            Self::Expression(error) => error.fmt(formatter),
            Self::Invalid(message) => write!(formatter, "invalid physical trigger: {message}"),
        }
    }
}

impl std::error::Error for PhysicalTriggerError {}

impl From<RuntimeBindingError> for PhysicalTriggerError {
    fn from(error: RuntimeBindingError) -> Self {
        Self::Runtime(error)
    }
}

impl From<PhysicalExpressionError> for PhysicalTriggerError {
    fn from(error: PhysicalExpressionError) -> Self {
        Self::Expression(error)
    }
}

/// Emit a trigger WHEN predicate and branch when it is false or NULL.
///
/// OLD and NEW are ordinary root inputs here. The caller decides whether they
/// came from parent registers, subprogram parameters, or a test fixture.
pub(crate) fn emit_trigger_predicate(
    plan: &PhysicalPlan<'_>,
    program: &mut ProgramBuilder,
    inputs: &RootRuntimeInputs,
    false_target: BranchOffset,
) -> Result<(), PhysicalTriggerError> {
    let predicate = match &plan.root {
        PhysicalRoot::TriggerPredicate(predicate) => *predicate,
        _ => return Err(PhysicalTriggerError::Invalid("non-predicate HIR root")),
    };
    let mut bindings = RuntimeBindings::new(plan.document, plan.document.snapshot)?;
    inputs.apply(&mut bindings)?;
    let result = ExpressionEmitter::new(program, &mut bindings).emit_new(&predicate.expression)?;
    if result.width != 1 {
        return Err(PhysicalTriggerError::Invalid(
            "WHEN predicate result is not scalar",
        ));
    }
    program.emit_insn(Insn::IfNot {
        reg: result.first.0,
        target_pc: false_target,
        jump_if_null: true,
    });
    Ok(())
}
