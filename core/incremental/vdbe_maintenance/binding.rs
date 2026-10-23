//! Binding the values of a delta stream to the expressions that read them.
//!
//! An operator's expressions are the analyzed expressions of the view's
//! query. They read the view's sources as [`hir::Expr::Column`] and
//! [`hir::Expr::RowId`], and the values earlier operators publish as the
//! expressions those operators were built from. Before an expression is
//! lowered, every one of its nodes that a stream value stands for is bound
//! to the register that holds the value, so the shared expression lowering
//! never reads a cursor of its own.

use super::stream::EphemeralDelta;
use crate::translate::semantic::hir::{self, HirDocument, SourceId};
use crate::vdbe::builder::{ProgramBuilder, SourceBinding};
use crate::vdbe::insn::Insn;

/// The registers one row of a stream was loaded into.
#[derive(Default)]
pub(super) struct StreamRow<'a> {
    /// Each published value with its register.
    columns: Vec<(&'a hir::Expr, usize)>,
    /// The rowid register of each source whose rowid the stream carries.
    rowids: Vec<(SourceId, usize)>,
}

impl<'a> StreamRow<'a> {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn add_column(&mut self, expression: Option<&'a hir::Expr>, register: usize) {
        if let Some(expression) = expression {
            self.columns.push((expression, register));
        }
    }

    pub(super) fn add_rowid(&mut self, source: SourceId, register: usize) {
        self.rowids.push((source, register));
    }

    /// Read the current row of `channel` into fresh registers.
    pub(super) fn load(program: &mut ProgramBuilder, channel: &'a EphemeralDelta) -> Self {
        let mut row = Self::new();
        for (column, expression) in channel.schema.columns.iter().enumerate() {
            let Some(expression) = expression else {
                continue;
            };
            let register = program.alloc_register();
            program.emit_insn(Insn::Column {
                cursor_id: channel.cursor_id,
                column: channel.value_start + column,
                dest: register,
                default: None,
            });
            row.add_column(Some(expression), register);
        }
        for (rowid_column, binding) in channel
            .binding_rowid_columns
            .iter()
            .zip(channel.schema.bindings.iter())
        {
            let Some(rowid_column) = rowid_column else {
                continue;
            };
            let register = program.alloc_register();
            program.emit_insn(Insn::Column {
                cursor_id: channel.cursor_id,
                column: *rowid_column,
                dest: register,
                default: None,
            });
            row.add_rowid(binding.source, register);
        }
        row
    }

    fn register_of(&self, node: &hir::Expr) -> Option<usize> {
        if let hir::Expr::RowId(source) = node {
            if let Some((_, register)) = self.rowids.iter().find(|(bound, _)| bound == source) {
                return Some(*register);
            }
        }
        self.columns
            .iter()
            .find(|(published, _)| published.equivalent(node))
            .map(|(_, register)| *register)
    }

    /// Make every read of a stream value in `expression` read its register.
    /// The bindings are keyed by the expression's own nodes, so they hold
    /// for exactly this expression until the program drops them.
    pub(super) fn bind(
        &self,
        program: &mut ProgramBuilder,
        document: &HirDocument,
        expression: &hir::Expr,
    ) {
        bind_expression(program, document, expression, &mut |node| {
            self.register_of(node)
        });
        // A source whose columns the stream carries whole, in order, is also
        // bound as a register row, so a read the published values do not
        // name one by one, such as a merged USING column, finds it.
        for binding in self.whole_sources(document) {
            program.bind_source(binding.0, binding.1);
        }
    }

    /// The sources every column of which this row holds, in column order
    /// and in consecutive registers.
    fn whole_sources(&self, document: &HirDocument) -> Vec<(SourceId, SourceBinding)> {
        let mut bindings = Vec::new();
        let mut sources: Vec<SourceId> = Vec::new();
        for (expression, _) in &self.columns {
            if let hir::Expr::Column(column) = expression {
                if !sources.contains(&column.source) {
                    sources.push(column.source);
                }
            }
        }
        for source in sources {
            let Some(definition) = document.source(source) else {
                continue;
            };
            let column_count = definition.columns.len();
            let first = self.columns.iter().position(|(expression, _)| {
                matches!(expression, hir::Expr::Column(column)
                    if column.source == source && column.column == 0)
            });
            let Some(first) = first else {
                continue;
            };
            let whole = (0..column_count).all(|index| {
                self.columns
                    .get(first + index)
                    .is_some_and(|(expression, register)| {
                        matches!(expression, hir::Expr::Column(column)
                        if column.source == source && column.column == index)
                            && *register == self.columns[first].1 + index
                    })
            });
            if !whole {
                continue;
            }
            let rowid = self
                .rowids
                .iter()
                .find(|(bound, _)| *bound == source)
                .map(|(_, register)| *register);
            bindings.push((
                source,
                SourceBinding::Registers {
                    start: self.columns[first].1,
                    rowid,
                },
            ));
        }
        bindings
    }
}

/// Bind every node of `expression` for which `lookup` names a register, not
/// descending into a bound node. A reference to an output of the query is
/// followed into the output's expression, which the lowering evaluates in
/// its place.
pub(super) fn bind_expression(
    program: &mut ProgramBuilder,
    document: &HirDocument,
    expression: &hir::Expr,
    lookup: &mut impl FnMut(&hir::Expr) -> Option<usize>,
) {
    if let Some(register) = lookup(expression) {
        program.bind_expression_register(expression, register);
        return;
    }
    if let hir::Expr::Output(output) = expression {
        if let Some(output) = document.output(*output) {
            bind_expression(program, document, &output.expr, lookup);
        }
        return;
    }
    let mut index = 0;
    while let Some(child) = expression.child(index) {
        bind_expression(program, document, child, lookup);
        index += 1;
    }
}

/// Visit every node of `expression`, following references to outputs of
/// the query into their expressions. `visit` returns whether to descend
/// into the node's children.
pub(super) fn visit_expression(
    document: &HirDocument,
    expression: &hir::Expr,
    visit: &mut impl FnMut(&hir::Expr) -> crate::Result<bool>,
) -> crate::Result<()> {
    if !visit(expression)? {
        return Ok(());
    }
    if let hir::Expr::Output(output) = expression {
        if let Some(output) = document.output(*output) {
            visit_expression(document, &output.expr, visit)?;
        }
        return Ok(());
    }
    let mut index = 0;
    while let Some(child) = expression.child(index) {
        visit_expression(document, child, visit)?;
        index += 1;
    }
    Ok(())
}

/// Replace every node of `expression` for which `replace` gives a
/// replacement, not descending into a replaced node. Returns whether any
/// node was replaced.
pub(super) fn replace_expression(
    expression: &mut hir::Expr,
    replace: &impl Fn(&hir::Expr) -> Option<hir::Expr>,
) -> bool {
    if let Some(replacement) = replace(expression) {
        *expression = replacement;
        return true;
    }
    let mut replaced = false;
    let mut visit = |child: &mut hir::Expr| replaced |= replace_expression(child, replace);
    match expression {
        hir::Expr::Literal(_)
        | hir::Expr::Parameter(_)
        | hir::Expr::Column(_)
        | hir::Expr::RowId(_)
        | hir::Expr::Output(_)
        | hir::Expr::Subquery(
            hir::SubqueryExpr::Scalar { .. }
            | hir::SubqueryExpr::Row { .. }
            | hir::SubqueryExpr::Exists(_),
        ) => {}
        hir::Expr::MergedColumn(column) => {
            visit(&mut column.left);
            visit(&mut column.right);
        }
        hir::Expr::Unary { expr, .. }
        | hir::Expr::IsNull(expr)
        | hir::Expr::NotNull(expr)
        | hir::Expr::TruthTest { expr, .. }
        | hir::Expr::Collate { expr, .. }
        | hir::Expr::Cast { expr, .. } => visit(expr),
        hir::Expr::Binary { lhs, rhs, .. } => {
            visit(lhs);
            visit(rhs);
        }
        hir::Expr::Between {
            expr, start, end, ..
        } => {
            visit(expr);
            visit(start);
            visit(end);
        }
        hir::Expr::Case {
            base,
            when_then,
            else_expr,
            ..
        } => {
            if let Some(base) = base {
                visit(base);
            }
            for (when, then) in when_then {
                visit(when);
                visit(then);
            }
            if let Some(else_expr) = else_expr {
                visit(else_expr);
            }
        }
        hir::Expr::Function(call) => {
            match &mut call.arguments {
                hir::FunctionArguments::Star => {}
                hir::FunctionArguments::Expressions {
                    values, order_by, ..
                } => {
                    for value in values {
                        visit(value);
                    }
                    for term in order_by {
                        visit(&mut term.expr);
                    }
                }
                hir::FunctionArguments::OrderedSet { direct, order_by } => {
                    for value in direct {
                        visit(value);
                    }
                    visit(&mut order_by.expr);
                }
            }
            match &mut call.evaluation {
                hir::FunctionEvaluation::Scalar => {}
                hir::FunctionEvaluation::Aggregate { filter, .. }
                | hir::FunctionEvaluation::Window { filter, .. } => {
                    if let Some(filter) = filter {
                        visit(filter);
                    }
                }
            }
        }
        hir::Expr::InList { lhs, values, .. } => {
            visit(lhs);
            for value in values {
                visit(value);
            }
        }
        hir::Expr::Subquery(hir::SubqueryExpr::In { lhs, .. }) => visit(lhs),
        hir::Expr::Like {
            lhs, rhs, escape, ..
        } => {
            visit(lhs);
            visit(rhs);
            if let Some(escape) = escape {
                visit(escape);
            }
        }
        hir::Expr::Row(expressions) | hir::Expr::Array(expressions) => {
            for expression in expressions {
                visit(expression);
            }
        }
        hir::Expr::Subscript { base, index } => {
            visit(base);
            visit(index);
        }
        hir::Expr::FieldAccess(access) => visit(&mut access.base),
        hir::Expr::Raise { message, .. } => {
            if let Some(message) = message {
                visit(message);
            }
        }
    }
    replaced
}
