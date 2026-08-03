//! Direct lowering from resolved HIR expressions to VDBE instructions.
//!
//! This module deliberately has no schema, resolver, symbol-table, or parser
//! expression input. Runtime locations come only from [`RuntimeBindings`], and
//! comparison behavior comes only from facts frozen into HIR.

use std::fmt;

use turso_parser::ast::{LikeOperator, Literal, Operator, ResolveType, UnaryOperator};

use crate::{
    error::{
        SQLITE_CONSTRAINT_CHECK, SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_TRIGGER, SQLITE_ERROR,
    },
    function::{Func, FuncCtx, ScalarFunc},
    schema::Table,
    translate::{
        expr::emit_literal,
        semantic::hir::{
            self, BoundSchemaCall, ComparisonComponent, ComparisonSemantics, Expr, FieldAccessKind,
            HirDocument, MergedColumnValue, SubqueryExpr,
        },
    },
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{to_u32, CmpInsFlags, InsertFlags, Insn, RegisterOrLiteral},
    },
};

use super::{
    CursorId, QueryRuntime, RegisterRange, RuntimeBindingError, RuntimeBindings, SourceRuntime,
};

#[derive(Debug)]
pub(crate) enum PhysicalExpressionError {
    Runtime(RuntimeBindingError),
    Invalid(&'static str),
    Unsupported(&'static str),
    Emission(String),
    Subquery(String),
}

impl fmt::Display for PhysicalExpressionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => error.fmt(formatter),
            Self::Invalid(message) => write!(formatter, "invalid HIR expression: {message}"),
            Self::Unsupported(message) => {
                write!(formatter, "HIR expression is not lowered yet: {message}")
            }
            Self::Emission(message) => {
                write!(formatter, "could not emit HIR expression: {message}")
            }
            Self::Subquery(message) => write!(formatter, "could not emit HIR subquery: {message}"),
        }
    }
}

impl std::error::Error for PhysicalExpressionError {}

impl From<RuntimeBindingError> for PhysicalExpressionError {
    fn from(error: RuntimeBindingError) -> Self {
        Self::Runtime(error)
    }
}

pub(crate) type ExpressionResult<T> = std::result::Result<T, PhysicalExpressionError>;

/// Query lowering supplied by the physical query layer.
///
/// The expression layer only knows that one resolved subquery produces a
/// runtime destination. It does not receive a schema, resolver, or parser AST.
pub(crate) trait PhysicalSubqueryEmitter<'document> {
    fn emit_subquery(
        &mut self,
        program: &mut ProgramBuilder,
        bindings: &mut RuntimeBindings<'document>,
        subquery: &SubqueryExpr,
    ) -> ExpressionResult<QueryRuntime>;
}

/// Emits one resolved expression using document-local runtime identities.
pub(crate) struct ExpressionEmitter<'program, 'bindings, 'document> {
    program: &'program mut ProgramBuilder,
    bindings: &'bindings mut RuntimeBindings<'document>,
    document: &'document HirDocument,
    subqueries: Option<&'bindings mut dyn PhysicalSubqueryEmitter<'document>>,
}

impl<'program, 'bindings, 'document> ExpressionEmitter<'program, 'bindings, 'document> {
    pub(crate) const fn new(
        program: &'program mut ProgramBuilder,
        bindings: &'bindings mut RuntimeBindings<'document>,
    ) -> Self {
        let document = bindings.document();
        Self {
            program,
            bindings,
            document,
            subqueries: None,
        }
    }

    pub(crate) fn with_subqueries(
        program: &'program mut ProgramBuilder,
        bindings: &'bindings mut RuntimeBindings<'document>,
        subqueries: &'bindings mut dyn PhysicalSubqueryEmitter<'document>,
    ) -> Self {
        let document = bindings.document();
        Self {
            program,
            bindings,
            document,
            subqueries: Some(subqueries),
        }
    }

    pub(crate) fn emit_new(&mut self, expression: &Expr) -> ExpressionResult<RegisterRange> {
        let width = expression_width(expression);
        if width == 0 {
            return Err(PhysicalExpressionError::Invalid(
                "an expression cannot produce an empty row",
            ));
        }
        let first = self.program.alloc_registers(width);
        let target = RegisterRange::new(first, width);
        self.emit_into(expression, target)?;
        Ok(target)
    }

    pub(crate) fn emit_into(
        &mut self,
        expression: &Expr,
        target: RegisterRange,
    ) -> ExpressionResult<()> {
        let expected = expression_width(expression);
        if expected == 0 || target.width != expected {
            return Err(PhysicalExpressionError::Invalid(
                "target register width does not match expression width",
            ));
        }

        if let Expr::Row(values) = expression {
            for (position, value) in values.iter().enumerate() {
                let register = target
                    .register(position)
                    .expect("target width was checked against row width");
                self.emit_into(value, RegisterRange::new(register.0, 1))?;
            }
            return Ok(());
        }

        let target = scalar_register(target)?;
        match expression {
            Expr::Literal(literal) => emit_literal(self.program, literal, target)
                .map(|_| ())
                .map_err(|error| PhysicalExpressionError::Emission(error.to_string())),
            Expr::Parameter(parameter) => {
                let index = self
                    .program
                    .register_resolved_parameter(parameter.index, parameter.name.as_deref());
                self.program.emit_insn(Insn::Variable {
                    index,
                    dest: target,
                });
                Ok(())
            }
            Expr::Column(column) => self.emit_column(*column, target),
            Expr::MergedColumn(column) => self.emit_merged_column(column, target),
            Expr::RowId(source) => self.emit_rowid(*source, target),
            Expr::Output(output) => {
                let source = self.bindings.output(*output)?.register.0;
                emit_copy(self.program, source, target, 1);
                Ok(())
            }
            Expr::Unary { operator, expr } => self.emit_unary(*operator, expr, target),
            Expr::Binary {
                lhs,
                operator,
                rhs,
                array_concat,
                custom,
                comparison,
            } => {
                if let Some(custom) = custom {
                    return self.emit_custom_binary(lhs, rhs, custom, target);
                }
                self.emit_binary(
                    lhs,
                    *operator,
                    rhs,
                    *array_concat,
                    comparison.as_ref(),
                    target,
                )
            }
            Expr::Cast { expr, target: cast } => {
                self.emit_into(expr, RegisterRange::new(target, 1))?;
                for call in &cast.programs.encode {
                    self.emit_schema_call(call, target, target)?;
                }
                if cast.programs.apply_builtin_affinity {
                    self.program.emit_insn(Insn::Cast {
                        reg: target,
                        affinity: cast.affinity,
                    });
                }
                if let Some(domain) = &cast.programs.domain {
                    if let Some(description) = &domain.not_null_description {
                        self.program.emit_insn(Insn::HaltIfNull {
                            target_reg: target,
                            err_code: SQLITE_CONSTRAINT_NOTNULL,
                            description: description.clone(),
                        });
                    }
                    for check in &domain.checks {
                        let result = self.program.alloc_register();
                        self.emit_schema_call(&check.call, target, result)?;
                        let passed = self.program.allocate_label();
                        self.program.emit_insn(Insn::IsNull {
                            reg: result,
                            target_pc: passed,
                        });
                        self.program.emit_insn(Insn::If {
                            reg: result,
                            target_pc: passed,
                            jump_if_null: false,
                        });
                        self.program.emit_insn(Insn::Halt {
                            err_code: SQLITE_CONSTRAINT_CHECK,
                            description: check.failure_description.clone(),
                            on_error: None,
                            description_reg: None,
                        });
                        self.program.preassign_label_to_next_insn(passed);
                    }
                }
                Ok(())
            }
            Expr::Collate { expr, .. } => {
                // The selected collation is carried by each comparison that
                // consumes this value. COLLATE does not change the value.
                self.emit_into(expr, RegisterRange::new(target, 1))
            }
            Expr::IsNull(expr) => self.emit_null_test(expr, target, true),
            Expr::NotNull(expr) => self.emit_null_test(expr, target, false),
            Expr::Subquery(subquery) => self.emit_subquery(subquery, target),
            Expr::Array(values) => self.emit_array(values, target),
            Expr::Subscript { base, index } => {
                let base = scalar_register(self.emit_new(base)?)?;
                let index = scalar_register(self.emit_new(index)?)?;
                self.program.emit_insn(Insn::ArrayElement {
                    array_reg: base,
                    index_reg: index,
                    dest: target,
                });
                Ok(())
            }
            Expr::FieldAccess(access) => {
                let source = scalar_register(self.emit_new(&access.base)?)?;
                match access.kind {
                    FieldAccessKind::Struct { field_index } => {
                        self.program.emit_insn(Insn::StructField {
                            src_reg: source,
                            field_index,
                            dest: target,
                        });
                    }
                    FieldAccessKind::Union { tag_index } => {
                        self.program.emit_insn(Insn::UnionExtract {
                            src_reg: source,
                            expected_tag: tag_index,
                            dest: target,
                        });
                    }
                }
                Ok(())
            }
            Expr::Between {
                expr,
                negated,
                start,
                end,
                start_comparison,
                end_comparison,
            } => self.emit_between(
                expr,
                *negated,
                start,
                end,
                start_comparison,
                end_comparison,
                target,
            ),
            Expr::Case {
                base,
                when_then,
                else_expr,
                base_comparisons,
            } => self.emit_case(
                base.as_deref(),
                when_then,
                else_expr.as_deref(),
                base_comparisons,
                target,
            ),
            Expr::Function(function) => self.emit_function(function, target),
            Expr::InList {
                lhs,
                negated,
                values,
                comparisons,
            } => self.emit_in_list(lhs, *negated, values, comparisons, target),
            Expr::Like {
                lhs,
                negated,
                operator,
                function,
                argument_count,
                rhs,
                escape,
            } => self.emit_like(
                lhs,
                *negated,
                *operator,
                function,
                *argument_count,
                rhs,
                escape.as_deref(),
                target,
            ),
            Expr::Raise { action, message } => self.emit_raise(*action, message.as_deref(), target),
            Expr::Row(_) => unreachable!("rows are handled before scalar emission"),
        }
    }

    fn emit_raise(
        &mut self,
        action: ResolveType,
        message: Option<&Expr>,
        target: usize,
    ) -> ExpressionResult<()> {
        let in_trigger = self.program.trigger.is_some();
        match action {
            ResolveType::Ignore => {
                if !in_trigger {
                    return Err(PhysicalExpressionError::Invalid(
                        "RAISE(IGNORE) is outside a trigger program",
                    ));
                }
                self.program.emit_insn(Insn::Halt {
                    err_code: 0,
                    description: String::new(),
                    on_error: Some(ResolveType::Ignore),
                    description_reg: None,
                });
            }
            ResolveType::Fail | ResolveType::Abort | ResolveType::Rollback => {
                if !in_trigger && action != ResolveType::Abort {
                    return Err(PhysicalExpressionError::Invalid(
                        "RAISE action is outside a trigger program",
                    ));
                }
                let message = message.ok_or(PhysicalExpressionError::Invalid(
                    "RAISE action has no error message",
                ))?;
                let err_code = if in_trigger {
                    SQLITE_CONSTRAINT_TRIGGER
                } else {
                    SQLITE_ERROR
                };
                match message {
                    Expr::Literal(Literal::String(message)) => {
                        self.program.emit_insn(Insn::Halt {
                            err_code,
                            description: sanitize_sql_string(message),
                            on_error: Some(action),
                            description_reg: None,
                        });
                    }
                    message => {
                        let value = scalar_register(self.emit_new(message)?)?;
                        self.program.emit_insn(Insn::Halt {
                            err_code,
                            description: String::new(),
                            on_error: Some(action),
                            description_reg: Some(value),
                        });
                    }
                }
            }
            ResolveType::Replace => {
                return Err(PhysicalExpressionError::Invalid(
                    "RAISE(REPLACE) is not valid",
                ));
            }
        }
        let _ = target;
        Ok(())
    }

    /// Emit the equality predicate carried by one resolved USING/NATURAL
    /// column. The right side stays a `ColumnRef`; physical planning does not
    /// manufacture a second expression tree to reuse scalar comparison code.
    pub(crate) fn emit_using_equality(
        &mut self,
        column: &hir::UsingColumn,
    ) -> ExpressionResult<RegisterRange> {
        let lhs = scalar_register(self.emit_new(&column.left)?)?;
        let rhs = self.program.alloc_register();
        self.emit_column(column.right, rhs)?;
        let target = self.program.alloc_register();
        let comparison = only_comparison_component(&column.comparison)?;
        self.emit_comparison(Operator::Equals, lhs, rhs, target, comparison)?;
        Ok(RegisterRange::new(target, 1))
    }

    /// Apply the frozen storage rules for one logical table field. DML calls
    /// this only after SQL expressions for the row have finished evaluating.
    pub(crate) fn emit_column_storage_value(
        &mut self,
        source: hir::SourceId,
        column: usize,
        target: usize,
    ) -> ExpressionResult<()> {
        let definition = self
            .document
            .source(source)
            .ok_or(RuntimeBindingError::UnknownSource(source))?;
        let metadata = definition
            .columns
            .get(column)
            .ok_or(PhysicalExpressionError::Invalid(
                "column position is outside its source",
            ))?;
        self.program.emit_column_affinity(target, metadata.affinity);
        if let Some(programs) = &definition.column_type_programs[column] {
            self.emit_column_storage_encode(programs, target)?;
        }
        Ok(())
    }

    fn emit_column(&mut self, column: hir::ColumnRef, target: usize) -> ExpressionResult<()> {
        let source = self
            .bindings
            .document()
            .source(column.source)
            .ok_or(RuntimeBindingError::UnknownSource(column.source))?;
        if column.column >= source.columns.len() {
            return Err(PhysicalExpressionError::Invalid(
                "column position is outside its source",
            ));
        }
        match self.bindings.source(column.source)? {
            SourceRuntime::Cursor(cursor) => {
                self.emit_cursor_column(source, column.column, cursor, target)
            }
            SourceRuntime::Registers { columns, .. } => {
                let source =
                    columns
                        .register(column.column)
                        .ok_or(PhysicalExpressionError::Invalid(
                            "column position is outside runtime row",
                        ))?;
                emit_copy(self.program, source.0, target, 1);
                Ok(())
            }
        }
    }

    fn emit_cursor_column(
        &mut self,
        source: &hir::Source,
        column: usize,
        cursor: CursorId,
        target: usize,
    ) -> ExpressionResult<()> {
        match &source.generated_expressions[column] {
            hir::ColumnReadExpression::Planned(expression) => {
                self.emit_into(expression, RegisterRange::new(target, 1))?;
                self.program
                    .emit_column_affinity(target, source.columns[column].affinity);
                return Ok(());
            }
            hir::ColumnReadExpression::NotRequired => {
                return Err(PhysicalExpressionError::Invalid(
                    "referenced generated column has no closed read expression",
                ));
            }
            hir::ColumnReadExpression::Absent => {}
        }
        if matches!(source.kind, hir::SourceKind::SchemaExpression) {
            self.program.emit_column_or_rowid(cursor.0, column, target);
            return Ok(());
        }
        let table = match &source.kind {
            hir::SourceKind::Table(table)
            | hir::SourceKind::TableFunction { table, .. }
            | hir::SourceKind::Pseudo { table, .. } => Some(table),
            hir::SourceKind::SchemaExpression => unreachable!("handled above"),
            hir::SourceKind::Cte(_)
            | hir::SourceKind::Derived(_)
            | hir::SourceKind::RecursiveInput(_) => None,
        };
        let Some(table) = table else {
            self.program.emit_insn(Insn::Column {
                cursor_id: cursor.0,
                column,
                dest: target,
                default: None,
            });
            return Ok(());
        };
        let Table::BTree(_) = table.value() else {
            self.program.emit_insn(Insn::VColumn {
                cursor_id: cursor.0,
                column,
                dest: target,
            });
            return Ok(());
        };
        if source.columns[column].rowid_alias {
            self.program.emit_insn(Insn::RowId {
                cursor_id: cursor.0,
                dest: target,
            });
            return Ok(());
        }

        let programs = source.column_type_programs[column].as_ref();
        match &source.default_expressions[column] {
            hir::ColumnReadExpression::Planned(default) => {
                let stored = self.program.allocate_label();
                let merged = self.program.allocate_label();
                self.program.emit_column_has_field(cursor.0, column, stored);
                self.emit_into(default, RegisterRange::new(target, 1))?;
                self.program
                    .emit_column_affinity(target, source.columns[column].affinity);
                if let Some(programs) = programs {
                    self.emit_column_storage_encode(programs, target)?;
                }
                self.program.emit_insn(Insn::Goto { target_pc: merged });
                self.program.preassign_label_to_next_insn(stored);
                self.program.flags.set_suppress_column_default(true);
                self.program.emit_column_or_rowid(cursor.0, column, target);
                self.program.preassign_label_to_next_insn(merged);
            }
            hir::ColumnReadExpression::Absent => {
                self.program.emit_column_or_rowid(cursor.0, column, target);
            }
            hir::ColumnReadExpression::NotRequired => {
                return Err(PhysicalExpressionError::Invalid(
                    "referenced column has an unplanned default expression",
                ));
            }
        }

        if let Some(programs) = programs {
            self.emit_column_storage_decode(programs, target)?;
        } else {
            self.program
                .emit_column_affinity(target, source.columns[column].affinity);
        }
        Ok(())
    }

    fn emit_column_storage_encode(
        &mut self,
        programs: &hir::BoundColumnTypePrograms,
        target: usize,
    ) -> ExpressionResult<()> {
        if let Some(array) = &programs.array {
            if !programs.encode.is_empty() {
                return Err(PhysicalExpressionError::Unsupported(
                    "custom array element encoding",
                ));
            }
            let skip = self.program.allocate_label();
            self.program.emit_insn(Insn::IsNull {
                reg: target,
                target_pc: skip,
            });
            self.program.emit_insn(Insn::ArrayEncode {
                reg: target,
                element_affinity: array.element_affinity,
                element_type: array.element_type.clone().into(),
                table_name: array.table_name.clone().into(),
                col_name: array.column_name.clone().into(),
            });
            self.program.preassign_label_to_next_insn(skip);
            return Ok(());
        }

        let skip = (!programs.encode_nulls && !programs.encode.is_empty())
            .then(|| self.program.allocate_label());
        if let Some(skip) = skip {
            self.program.emit_insn(Insn::IsNull {
                reg: target,
                target_pc: skip,
            });
        }
        for call in &programs.encode {
            self.emit_schema_call(call, target, target)?;
        }
        if let Some(skip) = skip {
            self.program.preassign_label_to_next_insn(skip);
        }
        Ok(())
    }

    fn emit_column_storage_decode(
        &mut self,
        programs: &hir::BoundColumnTypePrograms,
        target: usize,
    ) -> ExpressionResult<()> {
        if programs.array.is_some() || programs.decode.is_empty() {
            return Ok(());
        }
        let skip = self.program.allocate_label();
        self.program.emit_insn(Insn::IsNull {
            reg: target,
            target_pc: skip,
        });
        for call in &programs.decode {
            self.emit_schema_call(call, target, target)?;
        }
        self.program.preassign_label_to_next_insn(skip);
        Ok(())
    }

    fn emit_rowid(&mut self, source: hir::SourceId, target: usize) -> ExpressionResult<()> {
        match self.bindings.source(source)? {
            SourceRuntime::Cursor(cursor) => {
                self.program.emit_insn(Insn::RowId {
                    cursor_id: cursor.0,
                    dest: target,
                });
                Ok(())
            }
            SourceRuntime::Registers {
                rowid: Some(source),
                ..
            } => {
                emit_copy(self.program, source.0, target, 1);
                Ok(())
            }
            SourceRuntime::Registers { rowid: None, .. } => Err(PhysicalExpressionError::Invalid(
                "runtime source has no rowid register",
            )),
        }
    }

    fn emit_merged_column(
        &mut self,
        column: &hir::MergedColumn,
        target: usize,
    ) -> ExpressionResult<()> {
        match column.value {
            MergedColumnValue::Left => self.emit_into(&column.left, RegisterRange::new(target, 1)),
            MergedColumnValue::Right => self.emit_column(column.right, target),
            MergedColumnValue::Coalesce => {
                self.emit_into(&column.left, RegisterRange::new(target, 1))?;
                let done = self.program.allocate_label();
                self.program.emit_insn(Insn::NotNull {
                    reg: target,
                    target_pc: done,
                });
                self.emit_column(column.right, target)?;
                self.program.preassign_label_to_next_insn(done);
                Ok(())
            }
        }
    }

    fn emit_unary(
        &mut self,
        operator: UnaryOperator,
        expression: &Expr,
        target: usize,
    ) -> ExpressionResult<()> {
        let source = scalar_register(self.emit_new(expression)?)?;
        match operator {
            UnaryOperator::Positive => emit_copy(self.program, source, target, 1),
            UnaryOperator::Negative => {
                let zero = self.program.alloc_register();
                self.program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: zero,
                });
                self.program.emit_insn(Insn::Subtract {
                    lhs: zero,
                    rhs: source,
                    dest: target,
                });
            }
            UnaryOperator::BitwiseNot => self.program.emit_insn(Insn::BitNot {
                reg: source,
                dest: target,
            }),
            UnaryOperator::Not => self.program.emit_insn(Insn::Not {
                reg: source,
                dest: target,
            }),
        }
        Ok(())
    }

    fn emit_binary(
        &mut self,
        lhs: &Expr,
        operator: Operator,
        rhs: &Expr,
        array_concat: bool,
        comparison: Option<&ComparisonSemantics>,
        target: usize,
    ) -> ExpressionResult<()> {
        if matches!(
            operator,
            Operator::ArrowRight
                | Operator::ArrowRightShift
                | Operator::ArrayContains
                | Operator::ArrayOverlap
        ) {
            return self.emit_binary_function(lhs, operator, rhs, target);
        }
        let lhs = scalar_register(self.emit_new(lhs)?)?;
        let rhs = scalar_register(self.emit_new(rhs)?)?;

        if operator.is_comparison() {
            let comparison = comparison.ok_or(PhysicalExpressionError::Invalid(
                "comparison operator has no comparison facts",
            ))?;
            let component = only_comparison_component(comparison)?;
            return self.emit_comparison(operator, lhs, rhs, target, component);
        }
        if comparison.is_some() {
            return Err(PhysicalExpressionError::Invalid(
                "non-comparison operator has comparison facts",
            ));
        }

        let instruction = match operator {
            Operator::Add => Insn::Add {
                lhs,
                rhs,
                dest: target,
            },
            Operator::Subtract => Insn::Subtract {
                lhs,
                rhs,
                dest: target,
            },
            Operator::Multiply => Insn::Multiply {
                lhs,
                rhs,
                dest: target,
            },
            Operator::Divide => Insn::Divide {
                lhs,
                rhs,
                dest: target,
            },
            Operator::Modulus => Insn::Remainder {
                lhs,
                rhs,
                dest: target,
            },
            Operator::And => Insn::And {
                lhs,
                rhs,
                dest: target,
            },
            Operator::Or => Insn::Or {
                lhs,
                rhs,
                dest: target,
            },
            Operator::BitwiseAnd => Insn::BitAnd {
                lhs,
                rhs,
                dest: target,
            },
            Operator::BitwiseOr => Insn::BitOr {
                lhs,
                rhs,
                dest: target,
            },
            Operator::RightShift => Insn::ShiftRight {
                lhs,
                rhs,
                dest: target,
            },
            Operator::LeftShift => Insn::ShiftLeft {
                lhs,
                rhs,
                dest: target,
            },
            Operator::BitwiseNot => Insn::BitNot {
                reg: rhs,
                dest: target,
            },
            Operator::Concat if array_concat => Insn::ArrayConcat {
                lhs,
                rhs,
                dest: target,
            },
            Operator::Concat => Insn::Concat {
                lhs,
                rhs,
                dest: target,
            },
            Operator::ArrowRight
            | Operator::ArrowRightShift
            | Operator::ArrayContains
            | Operator::ArrayOverlap => unreachable!("function operators were handled above"),
            Operator::Equals
            | Operator::NotEquals
            | Operator::Less
            | Operator::LessEquals
            | Operator::Greater
            | Operator::GreaterEquals
            | Operator::Is
            | Operator::IsNot => unreachable!("comparisons were handled above"),
        };
        self.program.emit_insn(instruction);
        Ok(())
    }

    fn emit_binary_function(
        &mut self,
        lhs: &Expr,
        operator: Operator,
        rhs: &Expr,
        target: usize,
    ) -> ExpressionResult<()> {
        let arguments = self.program.alloc_registers(2);
        self.emit_into(lhs, RegisterRange::new(arguments, 1))?;
        self.emit_into(rhs, RegisterRange::new(arguments + 1, 1))?;

        let func = match operator {
            #[cfg(feature = "json")]
            Operator::ArrowRight => Func::Json(crate::function::JsonFunc::JsonArrowExtract),
            #[cfg(feature = "json")]
            Operator::ArrowRightShift => {
                Func::Json(crate::function::JsonFunc::JsonArrowShiftExtract)
            }
            #[cfg(not(feature = "json"))]
            Operator::ArrowRight | Operator::ArrowRightShift => {
                return Err(PhysicalExpressionError::Unsupported("JSON operator"));
            }
            Operator::ArrayContains => Func::Scalar(ScalarFunc::ArrayContainsAll),
            Operator::ArrayOverlap => Func::Scalar(ScalarFunc::ArrayOverlap),
            _ => {
                return Err(PhysicalExpressionError::Invalid(
                    "non-function binary operator",
                ));
            }
        };
        self.program.emit_insn(Insn::Function {
            constant_mask: 0,
            start_reg: arguments,
            dest: target,
            func: FuncCtx { func, arg_count: 2 },
        });
        Ok(())
    }

    fn emit_custom_binary(
        &mut self,
        lhs: &Expr,
        rhs: &Expr,
        operation: &hir::CustomBinaryOperator,
        target: usize,
    ) -> ExpressionResult<()> {
        let original = self.program.alloc_registers(2);
        self.emit_into(lhs, RegisterRange::new(original, 1))?;
        self.emit_into(rhs, RegisterRange::new(original + 1, 1))?;
        if let Some(encoding) = &operation.literal_encoding {
            if let Some(call) = &encoding.encoder {
                let value = match encoding.operand {
                    hir::BinaryOperand::Left => original,
                    hir::BinaryOperand::Right => original + 1,
                };
                self.emit_schema_call(call, value, value)?;
            }
        }

        // Keep the function arguments contiguous even when a stored encoder
        // allocated temporary registers between the two original operands.
        let arguments = self.program.alloc_registers(2);
        let (first, second) = if operation.swap_args {
            (original + 1, original)
        } else {
            (original, original + 1)
        };
        emit_copy(self.program, first, arguments, 1);
        emit_copy(self.program, second, arguments + 1, 1);
        self.program.emit_insn(Insn::Function {
            constant_mask: 0,
            start_reg: arguments,
            dest: target,
            func: FuncCtx {
                func: operation.function.value().clone(),
                arg_count: 2,
            },
        });
        if operation.negate {
            self.program.emit_insn(Insn::Not {
                reg: target,
                dest: target,
            });
        }
        Ok(())
    }

    fn emit_function(&mut self, call: &hir::FunctionCall, target: usize) -> ExpressionResult<()> {
        match call.evaluation {
            hir::FunctionEvaluation::Aggregate(id) => {
                let source = self.bindings.aggregate(id)?.register;
                emit_copy(self.program, source.0, target, 1);
                return Ok(());
            }
            hir::FunctionEvaluation::Window(id) => {
                let source = self.bindings.window_function(id)?.register;
                emit_copy(self.program, source.0, target, 1);
                return Ok(());
            }
            hir::FunctionEvaluation::Scalar => {}
        }
        if call.star
            || call.distinctness.is_some()
            || !call.argument_order.is_empty()
            || !call.within_group.is_empty()
            || call.filter.is_some()
            || call.window.is_some()
        {
            return Err(PhysicalExpressionError::Unsupported(
                "aggregate or window function form",
            ));
        }
        if let Some(operation) = &call.sequence_operation {
            return self.emit_sequence_function(call, operation, target);
        }
        if let Some(operation) = &call.custom_type_operation {
            return self.emit_custom_type_function(operation, &call.arguments, target);
        }

        match call.function.value() {
            Func::Agg(_) => Err(PhysicalExpressionError::Unsupported("aggregate function")),
            Func::Window(_) => Err(PhysicalExpressionError::Unsupported("window function")),
            Func::Scalar(ScalarFunc::Coalesce | ScalarFunc::IfNull) => {
                self.emit_coalesce(&call.arguments, target)
            }
            Func::Scalar(ScalarFunc::Iif) => self.emit_iif(&call.arguments, target),
            Func::Scalar(ScalarFunc::Likely | ScalarFunc::Unlikely) => {
                let [value] = call.arguments.as_slice() else {
                    return Err(PhysicalExpressionError::Invalid(
                        "likelihood hint has the wrong argument count",
                    ));
                };
                self.emit_into(value, RegisterRange::new(target, 1))
            }
            Func::Scalar(ScalarFunc::Likelihood) => {
                let [value, _probability] = call.arguments.as_slice() else {
                    return Err(PhysicalExpressionError::Invalid(
                        "likelihood has the wrong argument count",
                    ));
                };
                // Semantic analysis already validated the literal probability.
                // SQLite uses it only as a planning hint, never as a runtime value.
                self.emit_into(value, RegisterRange::new(target, 1))
            }
            Func::Scalar(ScalarFunc::Array | ScalarFunc::StructPack) => {
                self.emit_array(&call.arguments, target)
            }
            Func::Scalar(ScalarFunc::ArrayElement) => {
                let [array, index] = call.arguments.as_slice() else {
                    return Err(PhysicalExpressionError::Invalid(
                        "array_element has the wrong argument count",
                    ));
                };
                let array = scalar_register(self.emit_new(array)?)?;
                let index = scalar_register(self.emit_new(index)?)?;
                self.program.emit_insn(Insn::ArrayElement {
                    array_reg: array,
                    index_reg: index,
                    dest: target,
                });
                Ok(())
            }
            Func::Scalar(ScalarFunc::ArraySetElement) => {
                let [array, index, value] = call.arguments.as_slice() else {
                    return Err(PhysicalExpressionError::Invalid(
                        "array_set_element has the wrong argument count",
                    ));
                };
                let array = scalar_register(self.emit_new(array)?)?;
                let index = scalar_register(self.emit_new(index)?)?;
                let value = scalar_register(self.emit_new(value)?)?;
                self.program.emit_insn(Insn::ArraySetElement {
                    array_reg: array,
                    index_reg: index,
                    value_reg: value,
                    dest: target,
                });
                Ok(())
            }
            Func::Scalar(
                ScalarFunc::Cast
                | ScalarFunc::Attach
                | ScalarFunc::Detach
                | ScalarFunc::StatInit
                | ScalarFunc::StatPush
                | ScalarFunc::StatGet,
            )
            | Func::AlterTable(_) => Err(PhysicalExpressionError::Unsupported(
                "statement-internal function",
            )),
            _ => self.emit_direct_function(call, target),
        }
    }

    fn emit_sequence_function(
        &mut self,
        call: &hir::FunctionCall,
        operation: &hir::SequenceOperation,
        target: usize,
    ) -> ExpressionResult<()> {
        let argument_count = call.arguments.len();
        let arguments = self.program.alloc_registers(argument_count);
        for (position, argument) in call.arguments.iter().enumerate() {
            self.emit_into(argument, RegisterRange::new(arguments + position, 1))?;
        }

        self.program
            .begin_write_on_database(operation.database.index(), operation.schema_cookie)
            .map_err(|error| PhysicalExpressionError::Emission(error.to_string()))?;

        let Table::BTree(backing_table) = operation.backing_table.value() else {
            return Err(PhysicalExpressionError::Invalid(
                "sequence backing object is not a B-tree table",
            ));
        };
        let sqlite_sequence = operation
            .sqlite_sequence
            .as_ref()
            .map(|table| match table.value() {
                Table::BTree(table) => Ok(table.clone()),
                _ => Err(PhysicalExpressionError::Invalid(
                    "sqlite_sequence object is not a B-tree table",
                )),
            })
            .transpose()?;

        match operation.kind {
            hir::SequenceOperationKind::NextValue => {
                if argument_count != 1 {
                    return Err(PhysicalExpressionError::Invalid(
                        "NEXTVAL has the wrong argument count",
                    ));
                }
                crate::translate::sequence::emit_disk_read_nextval_from_resolved(
                    self.program,
                    operation.database.index(),
                    &operation.normalized_name,
                    &operation.sequence,
                    backing_table.clone(),
                    sqlite_sequence,
                    target,
                    Some(arguments),
                )
                .map_err(|error| PhysicalExpressionError::Emission(error.to_string()))?;
            }
            hir::SequenceOperationKind::SetValue => {
                if !(2..=3).contains(&argument_count) {
                    return Err(PhysicalExpressionError::Invalid(
                        "SETVAL has the wrong argument count",
                    ));
                }
                let cursor = self
                    .program
                    .alloc_cursor_id(CursorType::BTreeTable(backing_table.clone()));
                self.program.emit_insn(Insn::OpenWrite {
                    cursor_id: cursor,
                    root_page: RegisterOrLiteral::Literal(backing_table.root_page),
                    db: operation.database.index(),
                });
                self.program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg: arguments,
                    dest: target,
                    func: FuncCtx {
                        func: call.function.value().clone(),
                        arg_count: argument_count,
                    },
                });

                let empty = self.program.allocate_label();
                let delete = self.program.allocate_label();
                self.program.emit_insn(Insn::Rewind {
                    cursor_id: cursor,
                    pc_if_empty: empty,
                });
                self.program.preassign_label_to_next_insn(delete);
                self.program.emit_insn(Insn::Delete {
                    cursor_id: cursor,
                    table_name: operation.normalized_name.clone(),
                    is_part_of_update: true,
                });
                self.program.emit_insn(Insn::Next {
                    cursor_id: cursor,
                    pc_if_next: delete,
                });
                self.program.preassign_label_to_next_insn(empty);

                let columns = self.program.alloc_registers(7);
                emit_copy(self.program, arguments + 1, columns, 1);
                if argument_count == 3 {
                    emit_copy(self.program, arguments + 2, columns + 1, 1);
                } else {
                    self.program.emit_insn(Insn::Integer {
                        dest: columns + 1,
                        value: 1,
                    });
                }
                crate::translate::sequence::emit_sequence_descriptor_literals(
                    self.program,
                    &operation.sequence,
                    columns + 2,
                );
                let record = self.program.alloc_register();
                self.program.emit_insn(Insn::MakeRecord {
                    start_reg: to_u32(columns),
                    count: 7,
                    dest_reg: to_u32(record),
                    index_name: None,
                    affinity_str: None,
                });
                self.program.emit_insn(Insn::Insert {
                    cursor,
                    key_reg: arguments + 1,
                    record_reg: record,
                    flag: InsertFlags::new().require_seek().skip_all_change_counts(),
                    table_name: operation.normalized_name.clone(),
                });
                self.program.emit_insn(Insn::SetSequenceCurrval {
                    seq_name_reg: arguments,
                    value_reg: arguments + 1,
                });
                self.program.emit_insn(Insn::Close { cursor_id: cursor });
                crate::translate::sequence::emit_autoincrement_sqlite_sequence_sync_from_resolved(
                    self.program,
                    operation.database.index(),
                    &operation.normalized_name,
                    arguments + 1,
                    sqlite_sequence,
                )
                .map_err(|error| PhysicalExpressionError::Emission(error.to_string()))?;
            }
        }
        Ok(())
    }

    fn emit_direct_function(
        &mut self,
        call: &hir::FunctionCall,
        target: usize,
    ) -> ExpressionResult<()> {
        let allocation_width = call.arguments.len().max(1);
        let start = self.program.alloc_registers(allocation_width);
        for (position, argument) in call.arguments.iter().enumerate() {
            self.emit_into(argument, RegisterRange::new(start + position, 1))?;
        }
        self.program.emit_insn(Insn::Function {
            constant_mask: 0,
            start_reg: start,
            dest: target,
            func: FuncCtx {
                func: call.function.value().clone(),
                arg_count: call.arguments.len(),
            },
        });
        Ok(())
    }

    fn emit_coalesce(&mut self, arguments: &[Expr], target: usize) -> ExpressionResult<()> {
        if arguments.len() < 2 {
            return Err(PhysicalExpressionError::Invalid(
                "coalesce has fewer than two arguments",
            ));
        }
        let done = self.program.allocate_label();
        for (position, argument) in arguments.iter().enumerate() {
            self.emit_into(argument, RegisterRange::new(target, 1))?;
            if position + 1 < arguments.len() {
                self.program.emit_insn(Insn::NotNull {
                    reg: target,
                    target_pc: done,
                });
            }
        }
        self.program.preassign_label_to_next_insn(done);
        Ok(())
    }

    fn emit_iif(&mut self, arguments: &[Expr], target: usize) -> ExpressionResult<()> {
        if arguments.len() < 2 {
            return Err(PhysicalExpressionError::Invalid(
                "iif has fewer than two arguments",
            ));
        }
        let done = self.program.allocate_label();
        let mut pairs = arguments.chunks_exact(2);
        for pair in &mut pairs {
            let condition = scalar_register(self.emit_new(&pair[0])?)?;
            let next = self.program.allocate_label();
            self.program.emit_insn(Insn::IfNot {
                reg: condition,
                target_pc: next,
                jump_if_null: true,
            });
            self.emit_into(&pair[1], RegisterRange::new(target, 1))?;
            self.program.emit_insn(Insn::Goto { target_pc: done });
            self.program.preassign_label_to_next_insn(next);
        }
        if let Some(otherwise) = pairs.remainder().first() {
            self.emit_into(otherwise, RegisterRange::new(target, 1))?;
        } else {
            self.program.emit_insn(Insn::Null {
                dest: target,
                dest_end: None,
            });
        }
        self.program.preassign_label_to_next_insn(done);
        Ok(())
    }

    fn emit_custom_type_function(
        &mut self,
        operation: &hir::CustomTypeOperation,
        arguments: &[Expr],
        target: usize,
    ) -> ExpressionResult<()> {
        match operation {
            hir::CustomTypeOperation::UnionValue { tag_index, .. } => {
                let [_, value] = arguments else {
                    return Err(PhysicalExpressionError::Invalid(
                        "union_value has the wrong argument count",
                    ));
                };
                let value = scalar_register(self.emit_new(value)?)?;
                self.program.emit_insn(Insn::UnionPack {
                    tag_index: *tag_index,
                    value_reg: value,
                    dest: target,
                });
            }
            hir::CustomTypeOperation::UnionTag { tag_names, .. } => {
                let [value] = arguments else {
                    return Err(PhysicalExpressionError::Invalid(
                        "union_tag has the wrong argument count",
                    ));
                };
                let value = scalar_register(self.emit_new(value)?)?;
                self.program.emit_insn(Insn::UnionTag {
                    src_reg: value,
                    dest: target,
                    tag_names: tag_names.clone(),
                });
            }
            hir::CustomTypeOperation::UnionExtract { tag_index, .. } => {
                let [value, _] = arguments else {
                    return Err(PhysicalExpressionError::Invalid(
                        "union_extract has the wrong argument count",
                    ));
                };
                let value = scalar_register(self.emit_new(value)?)?;
                self.program.emit_insn(Insn::UnionExtract {
                    src_reg: value,
                    expected_tag: *tag_index,
                    dest: target,
                });
            }
            hir::CustomTypeOperation::StructExtract { field_index, .. } => {
                let [value, _] = arguments else {
                    return Err(PhysicalExpressionError::Invalid(
                        "struct_extract has the wrong argument count",
                    ));
                };
                let value = scalar_register(self.emit_new(value)?)?;
                self.program.emit_insn(Insn::StructField {
                    src_reg: value,
                    field_index: *field_index,
                    dest: target,
                });
            }
        }
        Ok(())
    }

    fn emit_comparison(
        &mut self,
        operator: Operator,
        lhs: usize,
        rhs: usize,
        target: usize,
        comparison: &ComparisonComponent,
    ) -> ExpressionResult<()> {
        let label = self.program.allocate_label();
        let mut flags = CmpInsFlags::default().with_affinity(comparison.affinity);
        if comparison.array {
            flags = flags.array_cmp();
        }
        let collation = comparison.collation.as_ref().map(|value| *value.value());
        let null_equal = matches!(operator, Operator::Is | Operator::IsNot);
        if null_equal {
            flags = flags.null_eq();
        }
        let instruction = match operator {
            Operator::Equals | Operator::Is => Insn::Eq {
                lhs,
                rhs,
                target_pc: label,
                flags,
                collation,
            },
            Operator::NotEquals | Operator::IsNot => Insn::Ne {
                lhs,
                rhs,
                target_pc: label,
                flags,
                collation,
            },
            Operator::Less => Insn::Lt {
                lhs,
                rhs,
                target_pc: label,
                flags,
                collation,
            },
            Operator::LessEquals => Insn::Le {
                lhs,
                rhs,
                target_pc: label,
                flags,
                collation,
            },
            Operator::Greater => Insn::Gt {
                lhs,
                rhs,
                target_pc: label,
                flags,
                collation,
            },
            Operator::GreaterEquals => Insn::Ge {
                lhs,
                rhs,
                target_pc: label,
                flags,
                collation,
            },
            _ => {
                return Err(PhysicalExpressionError::Invalid(
                    "comparison facts attached to a non-comparison operator",
                ));
            }
        };

        self.program.emit_insn(Insn::Integer {
            value: 1,
            dest: target,
        });
        self.program.emit_insn(instruction);
        if null_equal {
            self.program.emit_insn(Insn::Integer {
                value: 0,
                dest: target,
            });
        } else {
            self.program.emit_insn(Insn::ZeroOrNull {
                rg1: lhs,
                rg2: rhs,
                dest: target,
            });
        }
        self.program.preassign_label_to_next_insn(label);
        Ok(())
    }

    fn emit_null_test(
        &mut self,
        expression: &Expr,
        target: usize,
        is_null: bool,
    ) -> ExpressionResult<()> {
        let source = scalar_register(self.emit_new(expression)?)?;
        let matched = self.program.allocate_label();
        self.program.emit_insn(Insn::Integer {
            value: 1,
            dest: target,
        });
        if is_null {
            self.program.emit_insn(Insn::IsNull {
                reg: source,
                target_pc: matched,
            });
        } else {
            self.program.emit_insn(Insn::NotNull {
                reg: source,
                target_pc: matched,
            });
        }
        self.program.emit_insn(Insn::Integer {
            value: 0,
            dest: target,
        });
        self.program.preassign_label_to_next_insn(matched);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_between(
        &mut self,
        expression: &Expr,
        negated: bool,
        start: &Expr,
        end: &Expr,
        start_comparison: &ComparisonSemantics,
        end_comparison: &ComparisonSemantics,
        target: usize,
    ) -> ExpressionResult<()> {
        let value = scalar_register(self.emit_new(expression)?)?;
        let start = scalar_register(self.emit_new(start)?)?;
        let end = scalar_register(self.emit_new(end)?)?;
        let lower_result = self.program.alloc_register();
        let upper_result = self.program.alloc_register();
        self.emit_comparison(
            Operator::GreaterEquals,
            value,
            start,
            lower_result,
            only_comparison_component(start_comparison)?,
        )?;
        self.emit_comparison(
            Operator::LessEquals,
            value,
            end,
            upper_result,
            only_comparison_component(end_comparison)?,
        )?;
        self.program.emit_insn(Insn::And {
            lhs: lower_result,
            rhs: upper_result,
            dest: target,
        });
        if negated {
            self.program.emit_insn(Insn::Not {
                reg: target,
                dest: target,
            });
        }
        Ok(())
    }

    fn emit_case(
        &mut self,
        base: Option<&Expr>,
        when_then: &[(Expr, Expr)],
        else_expression: Option<&Expr>,
        base_comparisons: &[ComparisonSemantics],
        target: usize,
    ) -> ExpressionResult<()> {
        if base.is_some() != !base_comparisons.is_empty()
            || base.is_some_and(|_| base_comparisons.len() != when_then.len())
        {
            return Err(PhysicalExpressionError::Invalid(
                "CASE comparison facts do not match its WHEN arms",
            ));
        }
        let base = base.map(|base| self.emit_new(base)).transpose()?;
        let base = base.map(scalar_register).transpose()?;
        let done = self.program.allocate_label();

        for (position, (when, then)) in when_then.iter().enumerate() {
            let next = self.program.allocate_label();
            if let Some(base) = base {
                let when = scalar_register(self.emit_new(when)?)?;
                let comparison_result = self.program.alloc_register();
                self.emit_comparison(
                    Operator::Equals,
                    base,
                    when,
                    comparison_result,
                    only_comparison_component(&base_comparisons[position])?,
                )?;
                self.program.emit_insn(Insn::IfNot {
                    reg: comparison_result,
                    target_pc: next,
                    jump_if_null: true,
                });
            } else {
                let condition = scalar_register(self.emit_new(when)?)?;
                self.program.emit_insn(Insn::IfNot {
                    reg: condition,
                    target_pc: next,
                    jump_if_null: true,
                });
            }
            self.emit_into(then, RegisterRange::new(target, 1))?;
            self.program.emit_insn(Insn::Goto { target_pc: done });
            self.program.preassign_label_to_next_insn(next);
        }

        if let Some(expression) = else_expression {
            self.emit_into(expression, RegisterRange::new(target, 1))?;
        } else {
            self.program.emit_insn(Insn::Null {
                dest: target,
                dest_end: None,
            });
        }
        self.program.preassign_label_to_next_insn(done);
        Ok(())
    }

    fn emit_in_list(
        &mut self,
        lhs: &Expr,
        negated: bool,
        values: &[Expr],
        comparisons: &[ComparisonSemantics],
        target: usize,
    ) -> ExpressionResult<()> {
        if values.len() != comparisons.len() {
            return Err(PhysicalExpressionError::Invalid(
                "IN values and comparison facts have different lengths",
            ));
        }
        if values.is_empty() {
            self.program.emit_insn(Insn::Integer {
                value: i64::from(negated),
                dest: target,
            });
            return Ok(());
        }

        let lhs = scalar_register(self.emit_new(lhs)?)?;
        let matched = self.program.allocate_label();
        let finish = self.program.allocate_label();
        self.program.emit_insn(Insn::Integer {
            value: 0,
            dest: target,
        });

        for (value, comparison) in values.iter().zip(comparisons) {
            let rhs = scalar_register(self.emit_new(value)?)?;
            let result = self.program.alloc_register();
            self.emit_comparison(
                Operator::Equals,
                lhs,
                rhs,
                result,
                only_comparison_component(comparison)?,
            )?;
            self.program.emit_insn(Insn::If {
                reg: result,
                target_pc: matched,
                jump_if_null: false,
            });
            let not_null = self.program.allocate_label();
            self.program.emit_insn(Insn::NotNull {
                reg: result,
                target_pc: not_null,
            });
            self.program.emit_insn(Insn::Null {
                dest: target,
                dest_end: None,
            });
            self.program.preassign_label_to_next_insn(not_null);
        }

        self.program.emit_insn(Insn::Goto { target_pc: finish });
        self.program.preassign_label_to_next_insn(matched);
        self.program.emit_insn(Insn::Integer {
            value: 1,
            dest: target,
        });
        self.program.preassign_label_to_next_insn(finish);
        if negated {
            self.program.emit_insn(Insn::Not {
                reg: target,
                dest: target,
            });
        }
        Ok(())
    }

    fn emit_subquery(&mut self, subquery: &SubqueryExpr, target: usize) -> ExpressionResult<()> {
        match subquery {
            SubqueryExpr::Scalar { query, output } => {
                let QueryRuntime::Registers(registers) = self.query_runtime(subquery, *query)?
                else {
                    return Err(PhysicalExpressionError::Invalid(
                        "scalar subquery has a non-register runtime destination",
                    ));
                };
                let source =
                    registers
                        .register(*output)
                        .ok_or(PhysicalExpressionError::Invalid(
                            "scalar subquery output is out of bounds",
                        ))?;
                emit_copy(self.program, source.0, target, 1);
                Ok(())
            }
            SubqueryExpr::Exists(query) => {
                let QueryRuntime::Exists(source) = self.query_runtime(subquery, *query)? else {
                    return Err(PhysicalExpressionError::Invalid(
                        "EXISTS subquery has the wrong runtime destination",
                    ));
                };
                emit_copy(self.program, source.0, target, 1);
                Ok(())
            }
            SubqueryExpr::In {
                lhs,
                query,
                negated,
                comparison,
            } => {
                let QueryRuntime::RowSet(cursor) = self.query_runtime(subquery, *query)? else {
                    return Err(PhysicalExpressionError::Invalid(
                        "IN subquery has the wrong runtime destination",
                    ));
                };
                self.emit_in_subquery(lhs, *negated, comparison, cursor, target)
            }
        }
    }

    fn emit_in_subquery(
        &mut self,
        lhs: &Expr,
        negated: bool,
        comparison: &ComparisonSemantics,
        cursor: CursorId,
        target: usize,
    ) -> ExpressionResult<()> {
        if comparison.components.is_empty() {
            return Err(PhysicalExpressionError::Invalid(
                "IN subquery has no comparison components",
            ));
        }
        let lhs = self.emit_new(lhs)?;
        if lhs.width != comparison.components.len() {
            return Err(PhysicalExpressionError::Invalid(
                "IN left row width does not match its comparison facts",
            ));
        }

        let loop_start = self.program.allocate_label();
        let next = self.program.allocate_label();
        let matched = self.program.allocate_label();
        let finish = self.program.allocate_label();
        let row_has_null = self.program.alloc_register();
        self.program.emit_insn(Insn::Integer {
            value: 0,
            dest: target,
        });
        self.program.emit_insn(Insn::Rewind {
            cursor_id: cursor.0,
            pc_if_empty: finish,
        });
        self.program.preassign_label_to_next_insn(loop_start);
        self.program.emit_insn(Insn::Integer {
            value: 0,
            dest: row_has_null,
        });

        for (position, component) in comparison.components.iter().enumerate() {
            let rhs = self.program.alloc_register();
            self.program.emit_insn(Insn::Column {
                cursor_id: cursor.0,
                column: position,
                dest: rhs,
                default: None,
            });
            let component_result = self.program.alloc_register();
            self.emit_comparison(
                Operator::Equals,
                lhs.first.0 + position,
                rhs,
                component_result,
                component,
            )?;
            self.program.emit_insn(Insn::IfNot {
                reg: component_result,
                target_pc: next,
                jump_if_null: false,
            });
            let component_known = self.program.allocate_label();
            self.program.emit_insn(Insn::NotNull {
                reg: component_result,
                target_pc: component_known,
            });
            self.program.emit_insn(Insn::Integer {
                value: 1,
                dest: row_has_null,
            });
            self.program.preassign_label_to_next_insn(component_known);
        }

        self.program.emit_insn(Insn::IfNot {
            reg: row_has_null,
            target_pc: matched,
            jump_if_null: false,
        });
        self.program.emit_insn(Insn::Null {
            dest: target,
            dest_end: None,
        });
        self.program.emit_insn(Insn::Goto { target_pc: next });
        self.program.preassign_label_to_next_insn(next);
        self.program.emit_insn(Insn::Next {
            cursor_id: cursor.0,
            pc_if_next: loop_start,
        });
        self.program.emit_insn(Insn::Goto { target_pc: finish });
        self.program.preassign_label_to_next_insn(matched);
        self.program.emit_insn(Insn::Integer {
            value: 1,
            dest: target,
        });
        self.program.preassign_label_to_next_insn(finish);
        if negated {
            self.program.emit_insn(Insn::Not {
                reg: target,
                dest: target,
            });
        }
        Ok(())
    }

    fn query_runtime(
        &mut self,
        subquery: &SubqueryExpr,
        query: hir::QueryId,
    ) -> ExpressionResult<QueryRuntime> {
        match self.bindings.query(query) {
            Ok(runtime) => Ok(runtime),
            Err(RuntimeBindingError::WrongScope("unbound query")) => self
                .subqueries
                .as_deref_mut()
                .ok_or(PhysicalExpressionError::Unsupported(
                    "subquery has no physical query emitter",
                ))?
                .emit_subquery(self.program, self.bindings, subquery),
            Err(error) => Err(error.into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_like(
        &mut self,
        lhs: &Expr,
        negated: bool,
        operator: LikeOperator,
        function: &hir::ResolvedFunction,
        argument_count: usize,
        rhs: &Expr,
        escape: Option<&Expr>,
        target: usize,
    ) -> ExpressionResult<()> {
        let start = self.program.alloc_registers(argument_count);
        match operator {
            LikeOperator::Like | LikeOperator::Glob | LikeOperator::Regexp => {
                let expected = if escape.is_some() { 3 } else { 2 };
                if argument_count != expected {
                    return Err(PhysicalExpressionError::Invalid(
                        "LIKE-family argument count does not match its operands",
                    ));
                }
                self.emit_into(rhs, RegisterRange::new(start, 1))?;
                self.emit_into(lhs, RegisterRange::new(start + 1, 1))?;
                if let Some(escape) = escape {
                    self.emit_into(escape, RegisterRange::new(start + 2, 1))?;
                }
            }
            LikeOperator::Match => {
                if escape.is_some() {
                    return Err(PhysicalExpressionError::Invalid(
                        "MATCH cannot have an ESCAPE operand",
                    ));
                }
                let columns: &[Expr] = match lhs {
                    Expr::Row(columns) => columns,
                    expression => std::slice::from_ref(expression),
                };
                if argument_count != columns.len() + 1 {
                    return Err(PhysicalExpressionError::Invalid(
                        "MATCH argument count does not match its columns",
                    ));
                }
                for (position, column) in columns.iter().enumerate() {
                    self.emit_into(column, RegisterRange::new(start + position, 1))?;
                }
                self.emit_into(rhs, RegisterRange::new(start + columns.len(), 1))?;
            }
        }
        self.program.emit_insn(Insn::Function {
            constant_mask: 0,
            start_reg: start,
            dest: target,
            func: FuncCtx {
                func: function.value().clone(),
                arg_count: argument_count,
            },
        });
        if negated {
            self.program.emit_insn(Insn::Not {
                reg: target,
                dest: target,
            });
        }
        Ok(())
    }

    fn emit_array(&mut self, values: &[Expr], target: usize) -> ExpressionResult<()> {
        let first = self.program.alloc_registers(values.len());
        for (position, value) in values.iter().enumerate() {
            self.emit_into(value, RegisterRange::new(first + position, 1))?;
        }
        self.program.emit_insn(Insn::MakeArray {
            start_reg: first,
            count: values.len(),
            dest: target,
        });
        Ok(())
    }

    /// Evaluate one stored expression against `[value, user arguments...]`.
    /// The program body is already bound to its synthetic HIR source.
    fn emit_schema_call(
        &mut self,
        call: &BoundSchemaCall,
        value: usize,
        target: usize,
    ) -> ExpressionResult<()> {
        let first = self.program.alloc_registers(call.arguments.len() + 1);
        emit_copy(self.program, value, first, 1);
        for (position, argument) in call.arguments.iter().enumerate() {
            self.emit_into(argument, RegisterRange::new(first + position + 1, 1))?;
        }
        let inputs = RegisterRange::new(first, call.arguments.len() + 1);
        self.bindings.enter_schema_program(call.program, inputs)?;

        // Split the emitter into disjoint fields so the body can stay borrowed
        // from the document. Stored expressions are not cloned for lowering.
        let document = self.document;
        let body = &document
            .schema_program(call.program)
            .ok_or(RuntimeBindingError::UnknownSchemaProgram(call.program))?
            .body;
        let result = self.emit_into(body, RegisterRange::new(target, 1));
        let leave_result = self.bindings.leave_schema_program();
        result?;
        leave_result?;
        Ok(())
    }
}

fn expression_width(expression: &Expr) -> usize {
    match expression {
        Expr::Row(values) => values.len(),
        _ => 1,
    }
}

fn scalar_register(range: RegisterRange) -> ExpressionResult<usize> {
    if range.width != 1 {
        return Err(PhysicalExpressionError::Invalid(
            "scalar expression produced a row value",
        ));
    }
    Ok(range.first.0)
}

fn sanitize_sql_string(input: &str) -> String {
    let inner = input
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .unwrap_or(input);
    inner.replace("''", "'")
}

fn only_comparison_component(
    comparison: &ComparisonSemantics,
) -> ExpressionResult<&ComparisonComponent> {
    if comparison.components.len() != 1 {
        return Err(PhysicalExpressionError::Unsupported(
            "row-valued comparison",
        ));
    }
    Ok(&comparison.components[0])
}

fn emit_copy(program: &mut ProgramBuilder, source: usize, target: usize, width: usize) {
    if source == target || width == 0 {
        return;
    }
    program.emit_insn(Insn::Copy {
        src_reg: source,
        dst_reg: target,
        extra_amount: width - 1,
    });
}
