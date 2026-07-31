// Expression compilation for incremental operators
// This module provides utilities to compile SQL expressions into VDBE subprograms
// that can be executed efficiently in the incremental computation context.

use crate::numeric::Numeric;
use crate::schema::Schema;
use crate::storage::pager::Pager;
use crate::sync::Arc;
use crate::translate::emitter::{DoubleQuotedDml, Resolver};
use crate::translate::expr::translate_plan_expr;
use crate::translate::logical::{LogicalExpr, LogicalSchema};
use crate::translate::plan::{
    PlanOutputFact, PlanRuntimeBindings, RuntimeOutputBinding, RuntimeOutputDefinition,
    RuntimeValueBinding,
};
use crate::translate::plan_expr::{
    PlanCastPrograms, PlanExpr, PlanExprAffinity, PlanFunctionCall, PlanOutputId, PlanTypeName,
};
#[cfg(test)]
use crate::types::Text;
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::insn::Insn;
use crate::vdbe::{Program, ProgramState, Register};
use crate::{Connection, QueryMode, Result, Value};
use crate::{DatabaseCatalog, RwLock, SymbolTable};
use rustc_hash::FxHashMap as HashMap;
use turso_parser::ast::{Literal, Operator};

/// Enum to represent either a trivial or compiled expression
#[derive(Clone)]
pub enum ExpressionExecutor {
    /// Trivial expression that can be evaluated inline
    Trivial(TrivialExpression),
    /// Compiled VDBE program for complex expressions
    Compiled(Arc<Program>),
}

/// Trivial expression that can be evaluated inline without VDBE
/// Supports arithmetic operations with automatic type promotion (integer to float)
#[derive(Clone, Debug)]
pub enum TrivialExpression {
    /// Direct column reference
    Column(usize),
    /// Immediate value
    Immediate(Value),
    /// Binary operation on trivial expressions (supports type promotion)
    Binary {
        left: Box<TrivialExpression>,
        op: Operator,
        right: Box<TrivialExpression>,
    },
}

impl TrivialExpression {
    /// Evaluate the trivial expression with the given input values
    /// Automatically promotes integers to floats when mixing types in arithmetic
    pub fn evaluate(&self, values: &[Value]) -> Value {
        match self {
            TrivialExpression::Column(idx) => values.get(*idx).cloned().unwrap_or(Value::Null),
            TrivialExpression::Immediate(val) => val.clone(),
            TrivialExpression::Binary { left, op, right } => {
                let left_val = left.evaluate(values);
                let right_val = right.evaluate(values);

                // Use Value's exec_* methods which handle all type coercion
                // (including Text → Numeric) consistently with SQLite semantics
                match op {
                    Operator::Add => left_val.exec_add(&right_val),
                    Operator::Subtract => left_val.exec_subtract(&right_val),
                    Operator::Multiply => left_val.exec_multiply(&right_val),
                    Operator::Divide => left_val.exec_divide(&right_val),
                    _ => panic!("Unsupported operator in trivial expression: {op:?}"),
                }
            }
        }
    }
}

/// Compiled expression that can be executed on row values
#[derive(Clone)]
pub struct CompiledExpression {
    /// The expression executor (trivial or compiled)
    pub executor: ExpressionExecutor,
    /// Number of input values expected (columns from the row)
    pub input_count: usize,
}

impl std::fmt::Debug for CompiledExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("CompiledExpression");
        s.field("input_count", &self.input_count);
        match &self.executor {
            ExpressionExecutor::Trivial(t) => s.field("executor", &format!("Trivial({t:?})")),
            ExpressionExecutor::Compiled(p) => {
                s.field("executor", &format!("Compiled({} insns)", p.insns.len()))
            }
        };
        s.finish()
    }
}

#[derive(PartialEq)]
enum TrivialType {
    Integer,
    Float,
    Text,
    Null,
}

impl CompiledExpression {
    /// Get the "type" of a trivial expression for type checking
    /// Returns None if type can't be determined statically
    fn get_trivial_type(expr: &TrivialExpression) -> Option<TrivialType> {
        match expr {
            TrivialExpression::Column(_) => None, // Can't know column type statically
            TrivialExpression::Immediate(val) => match val {
                Value::Numeric(Numeric::Integer(_)) => Some(TrivialType::Integer),
                Value::Numeric(Numeric::Float(_)) => Some(TrivialType::Float),
                Value::Text(_) => Some(TrivialType::Text),
                Value::Null => Some(TrivialType::Null),
                _ => None,
            },
            TrivialExpression::Binary { left, right, .. } => {
                // For binary ops, both sides must have the same type
                let left_type = Self::get_trivial_type(left)?;
                let right_type = Self::get_trivial_type(right)?;
                if left_type == right_type {
                    Some(left_type)
                } else {
                    None // Type mismatch
                }
            }
        }
    }

    // Validates if an expression is trivial (columns, immediates, and simple arithmetic)
    // Only considers expressions trivial if they don't require type coercion
    fn try_get_trivial_expr(
        expr: &LogicalExpr,
        input_schema: &LogicalSchema,
    ) -> Option<TrivialExpression> {
        match expr {
            LogicalExpr::Column(column) => input_schema
                .find_column_id(column.id)
                .map(|(index, _)| TrivialExpression::Column(index)),
            LogicalExpr::Literal(value) => Some(TrivialExpression::Immediate(value.clone())),

            // Binary operations with simple operators
            LogicalExpr::BinaryExpr { left, op, right } => {
                // Only support simple arithmetic operators
                match op {
                    Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide => {
                        // Both operands must be trivial
                        let left_trivial = Self::try_get_trivial_expr(left, input_schema)?;
                        let right_trivial = Self::try_get_trivial_expr(right, input_schema)?;

                        // Check if we can determine types statically
                        // For arithmetic operations, we allow mixing integers and floats
                        // since we promote integers to floats as needed
                        if let (Some(left_type), Some(right_type)) = (
                            Self::get_trivial_type(&left_trivial),
                            Self::get_trivial_type(&right_trivial),
                        ) {
                            // Both types are known - check if they're numeric or null
                            let numeric_types = matches!(
                                left_type,
                                TrivialType::Integer | TrivialType::Float | TrivialType::Null
                            ) && matches!(
                                right_type,
                                TrivialType::Integer | TrivialType::Float | TrivialType::Null
                            );

                            if !numeric_types {
                                return None; // Non-numeric types - not trivial
                            }
                        }
                        // If we can't determine types (columns involved), we optimistically
                        // assume they'll be compatible at runtime

                        Some(TrivialExpression::Binary {
                            left: Box::new(left_trivial),
                            op: *op,
                            right: Box::new(right_trivial),
                        })
                    }
                    _ => None,
                }
            }

            LogicalExpr::Alias { expr, .. } => Self::try_get_trivial_expr(expr, input_schema),

            _ => None,
        }
    }

    /// Compile a SQL expression into either a trivial executor or VDBE program
    ///
    /// For trivial expressions (columns, immediates, simple same-type arithmetic), uses inline evaluation.
    /// For complex expressions or those requiring type coercion, compiles to VDBE bytecode.
    pub fn compile(
        expr: &LogicalExpr,
        input_schema: &LogicalSchema,
        schema: &Schema,
        syms: &SymbolTable,
        connection: Arc<Connection>,
    ) -> Result<Self> {
        let input_count = input_schema.column_count();

        // First, check if this is a trivial expression
        if let Some(trivial) = Self::try_get_trivial_expr(expr, input_schema) {
            return Ok(CompiledExpression {
                executor: ExpressionExecutor::Trivial(trivial),
                input_count,
            });
        }

        // Fall back to VDBE compilation for complex expressions
        // Create a minimal program builder for expression compilation
        let mut builder =
            ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 5, 0));

        // Allocate registers for input values
        // Allocate input registers
        for _ in 0..input_count {
            builder.alloc_register();
        }

        // Allocate a temp register for computation
        let temp_result_register = builder.alloc_register();

        let plan_expr = Self::logical_to_plan_expr(expr, input_schema)?;
        let mut bindings = PlanRuntimeBindings::default();
        for (index, column) in input_schema.columns.iter().enumerate() {
            bindings.bind_output(
                PlanOutputId::new(index),
                RuntimeOutputBinding {
                    value: RuntimeValueBinding::Register {
                        register: index,
                        needs_decode: false,
                    },
                    fact: PlanOutputFact {
                        type_fact: crate::translate::semantic::hir::TypeFact::known(column.ty),
                        affinity: PlanExprAffinity {
                            affinity: column.affinity,
                            has_affinity: column.has_affinity,
                        },
                        collation: column.collation.clone(),
                        array_dimensions: 0,
                    },
                    definition: RuntimeOutputDefinition::ExternalInput,
                },
            );
        }

        // Create a resolver for translate_expr
        let database_schemas = RwLock::new(HashMap::default());
        let temp_database = RwLock::new(None);
        let attached_databases = RwLock::new(DatabaseCatalog::new());
        let resolver = Resolver::new(
            schema,
            &database_schemas,
            &temp_database,
            &attached_databases,
            syms,
            true,
            DoubleQuotedDml::Enabled,
            std::sync::Arc::new(crate::dialect::SqliteDialect),
        );

        resolver.with_plan_runtime_bindings(bindings, |resolver| {
            translate_plan_expr(
                &mut builder,
                None,
                &plan_expr,
                temp_result_register,
                resolver,
            )
        })?;

        // Copy the result to register 0 for return
        builder.emit_insn(Insn::Copy {
            src_reg: temp_result_register,
            dst_reg: 0,
            extra_amount: 0,
        });

        // Add a Halt instruction to complete the subprogram
        builder.emit_insn(Insn::Halt {
            err_code: 0,
            description: String::new(),
            on_error: None,
            description_reg: None,
        });

        // Build the program from the compiled expression bytecode
        let program = Arc::new(builder.build(connection, false, "")?);

        Ok(CompiledExpression {
            executor: ExpressionExecutor::Compiled(program),
            input_count,
        })
    }

    fn logical_to_plan_expr(expr: &LogicalExpr, schema: &LogicalSchema) -> Result<PlanExpr> {
        match expr {
            LogicalExpr::Column(column) => {
                let (index, _) = schema.find_column_id(column.id).ok_or_else(|| {
                    crate::LimboError::InternalError(format!(
                        "logical column {:?} is absent from the expression input",
                        column.id
                    ))
                })?;
                Ok(PlanExpr::Output(PlanOutputId::new(index)))
            }
            LogicalExpr::Literal(value) => Ok(PlanExpr::Literal(Self::value_literal(value))),
            LogicalExpr::BinaryExpr { left, op, right } => Ok(PlanExpr::Binary {
                lhs: Box::new(Self::logical_to_plan_expr(left, schema)?),
                operator: *op,
                rhs: Box::new(Self::logical_to_plan_expr(right, schema)?),
                custom: None,
            }),
            LogicalExpr::UnaryExpr { op, expr } => Ok(PlanExpr::Unary {
                operator: *op,
                expr: Box::new(Self::logical_to_plan_expr(expr, schema)?),
            }),
            LogicalExpr::ScalarFunction {
                function,
                args,
                result_type,
            } => Ok(PlanExpr::Function(PlanFunctionCall {
                function: function.clone(),
                arguments: args
                    .iter()
                    .map(|arg| Self::logical_to_plan_expr(arg, schema))
                    .collect::<Result<Vec<_>>>()?,
                star: false,
                distinctness: None,
                argument_order: Vec::new(),
                within_group: Vec::new(),
                filter: None,
                window: None,
                custom_type_operation: None,
                sequence_operation: None,
                result_type: crate::translate::semantic::hir::TypeFact::known(*result_type),
            })),
            LogicalExpr::Case {
                expr,
                when_then,
                else_expr,
            } => Ok(PlanExpr::Case {
                base: expr
                    .as_ref()
                    .map(|expr| Self::logical_to_plan_expr(expr, schema).map(Box::new))
                    .transpose()?,
                when_then: when_then
                    .iter()
                    .map(|(when, then)| {
                        Ok((
                            Self::logical_to_plan_expr(when, schema)?,
                            Self::logical_to_plan_expr(then, schema)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?,
                else_expr: else_expr
                    .as_ref()
                    .map(|expr| Self::logical_to_plan_expr(expr, schema).map(Box::new))
                    .transpose()?,
            }),
            LogicalExpr::InList {
                expr,
                list,
                negated,
            } => Ok(PlanExpr::InList {
                lhs: Box::new(Self::logical_to_plan_expr(expr, schema)?),
                negated: *negated,
                values: list
                    .iter()
                    .map(|value| Self::logical_to_plan_expr(value, schema))
                    .collect::<Result<Vec<_>>>()?,
            }),
            LogicalExpr::Alias { expr, .. } => Self::logical_to_plan_expr(expr, schema),
            LogicalExpr::IsNull { expr, negated } => {
                let expr = Box::new(Self::logical_to_plan_expr(expr, schema)?);
                if *negated {
                    Ok(PlanExpr::NotNull(expr))
                } else {
                    Ok(PlanExpr::IsNull(expr))
                }
            }
            LogicalExpr::Between {
                expr,
                low,
                high,
                negated,
            } => Ok(PlanExpr::Between {
                expr: Box::new(Self::logical_to_plan_expr(expr, schema)?),
                negated: *negated,
                start: Box::new(Self::logical_to_plan_expr(low, schema)?),
                end: Box::new(Self::logical_to_plan_expr(high, schema)?),
            }),
            LogicalExpr::Like {
                expr,
                pattern,
                escape,
                negated,
                operator,
                function,
                argument_count,
            } => Ok(PlanExpr::Like {
                lhs: Box::new(Self::logical_to_plan_expr(expr, schema)?),
                negated: *negated,
                operator: *operator,
                function: function.clone(),
                argument_count: *argument_count,
                rhs: Box::new(Self::logical_to_plan_expr(pattern, schema)?),
                escape: escape
                    .as_ref()
                    .map(|escape| Self::logical_to_plan_expr(escape, schema).map(Box::new))
                    .transpose()?,
            }),
            LogicalExpr::Cast {
                expr,
                name,
                parameters,
                ty,
            } => Ok(PlanExpr::Cast {
                expr: Box::new(Self::logical_to_plan_expr(expr, schema)?),
                target: PlanTypeName {
                    name: name.clone(),
                    parameters: parameters
                        .iter()
                        .map(|parameter| Self::logical_to_plan_expr(parameter, schema))
                        .collect::<Result<Vec<_>>>()?,
                    array_dimensions: 0,
                    type_fact: crate::translate::semantic::hir::TypeFact::known(*ty),
                    programs: PlanCastPrograms {
                        encode: Vec::new(),
                        domain: None,
                        apply_builtin_affinity: true,
                    },
                },
            }),
            LogicalExpr::Collate { expr, collation } => Ok(PlanExpr::Collate {
                expr: Box::new(Self::logical_to_plan_expr(expr, schema)?),
                collation: collation.clone(),
            }),
            LogicalExpr::AggregateFunction { .. } => Err(crate::LimboError::InternalError(
                "aggregate expression reached scalar expression compilation".to_string(),
            )),
            LogicalExpr::InSubquery { .. }
            | LogicalExpr::Exists { .. }
            | LogicalExpr::ScalarSubquery { .. } => Err(crate::LimboError::ParseError(
                "subqueries are not supported in incremental expressions".to_string(),
            )),
        }
    }

    fn value_literal(value: &Value) -> Literal {
        match value {
            Value::Numeric(Numeric::Integer(value)) => Literal::Numeric(value.to_string()),
            Value::Numeric(Numeric::Float(value)) => {
                Literal::Numeric(f64::from(*value).to_string())
            }
            Value::Text(value) => {
                Literal::String(format!("'{}'", value.as_str().replace('\'', "''")))
            }
            Value::Blob(value) => Literal::Blob(format!("X'{}'", hex::encode(value))),
            Value::Null => Literal::Null,
        }
    }

    /// Execute the compiled expression with the given input values
    pub fn execute(&self, values: &[Value], pager: Arc<Pager>) -> Result<Value> {
        match &self.executor {
            ExpressionExecutor::Trivial(trivial) => {
                // Fast path: evaluate trivial expression inline
                Ok(trivial.evaluate(values))
            }
            ExpressionExecutor::Compiled(program) => {
                // Slow path: execute VDBE program
                // Create a state with the input values loaded into registers
                let mut state = ProgramState::new(program.max_registers, 0);

                // Load input values into registers
                assert_eq!(
                    values.len(),
                    self.input_count,
                    "Mismatch in number of registers! Got {}, expected {}",
                    values.len(),
                    self.input_count
                );
                for (idx, value) in values.iter().enumerate() {
                    state.set_register(idx, Register::Value(value.clone()));
                }

                // Execute the program
                let mut pc = 0usize;
                while pc < program.insns.len() {
                    let (insn, _) = &program.insns[pc];
                    let insn_fn = insn.to_function();
                    state.pc = pc as u32;

                    // Execute the instruction
                    match insn_fn(program, &mut state, insn, &pager)? {
                        crate::vdbe::execute::InsnFunctionStepResult::IO(_) => {
                            return Err(crate::LimboError::InternalError(
                                "Expression evaluation encountered unexpected I/O".to_string(),
                            ));
                        }
                        crate::vdbe::execute::InsnFunctionStepResult::Done => {
                            break;
                        }
                        crate::vdbe::execute::InsnFunctionStepResult::Row => {
                            return Err(crate::LimboError::InternalError(
                                "Expression evaluation produced unexpected row".to_string(),
                            ));
                        }
                        crate::vdbe::execute::InsnFunctionStepResult::Step => {
                            pc = state.pc as usize;
                        }
                    }
                }

                // The compiled expression puts the result in register 0
                match state.get_register(0) {
                    Register::Value(v) => Ok(v.clone()),
                    _ => Ok(Value::Null),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mixed_type_arithmetic() {
        // Test integer - float
        let expr = TrivialExpression::Binary {
            left: Box::new(TrivialExpression::Immediate(Value::from_i64(1))),
            op: Operator::Subtract,
            right: Box::new(TrivialExpression::Immediate(Value::from_f64(0.5))),
        };
        let result = expr.evaluate(&[]);
        assert_eq!(result, Value::from_f64(0.5));

        // Test float - integer
        let expr = TrivialExpression::Binary {
            left: Box::new(TrivialExpression::Immediate(Value::from_f64(2.5))),
            op: Operator::Subtract,
            right: Box::new(TrivialExpression::Immediate(Value::from_i64(1))),
        };
        let result = expr.evaluate(&[]);
        assert_eq!(result, Value::from_f64(1.5));

        // Test integer * float
        let expr = TrivialExpression::Binary {
            left: Box::new(TrivialExpression::Immediate(Value::from_i64(10))),
            op: Operator::Multiply,
            right: Box::new(TrivialExpression::Immediate(Value::from_f64(0.1))),
        };
        let result = expr.evaluate(&[]);
        assert_eq!(result, Value::from_f64(1.0));

        // Test integer / float
        let expr = TrivialExpression::Binary {
            left: Box::new(TrivialExpression::Immediate(Value::from_i64(1))),
            op: Operator::Divide,
            right: Box::new(TrivialExpression::Immediate(Value::from_f64(2.0))),
        };
        let result = expr.evaluate(&[]);
        assert_eq!(result, Value::from_f64(0.5));

        // Test integer + float
        let expr = TrivialExpression::Binary {
            left: Box::new(TrivialExpression::Immediate(Value::from_i64(1))),
            op: Operator::Add,
            right: Box::new(TrivialExpression::Immediate(Value::from_f64(0.5))),
        };
        let result = expr.evaluate(&[]);
        assert_eq!(result, Value::from_f64(1.5));
    }

    #[test]
    fn test_nested_mixed_type_expressions() {
        // Test nested expressions with mixed types: (1 - 0.04)
        let one_minus_float = TrivialExpression::Binary {
            left: Box::new(TrivialExpression::Immediate(Value::from_i64(1))),
            op: Operator::Subtract,
            right: Box::new(TrivialExpression::Immediate(Value::from_f64(0.04))),
        };
        let result = one_minus_float.evaluate(&[]);
        assert_eq!(result, Value::from_f64(0.96));

        // Test multiplication with nested mixed-type expression: 100.0 * (1 - 0.04)
        let nested_expr = TrivialExpression::Binary {
            left: Box::new(TrivialExpression::Immediate(Value::from_f64(100.0))),
            op: Operator::Multiply,
            right: Box::new(one_minus_float),
        };
        let result = nested_expr.evaluate(&[]);
        assert_eq!(result, Value::from_f64(96.0));
    }

    #[test]
    fn test_text_to_numeric_coercion_in_arithmetic() {
        // Non-numeric text should coerce to 0 (SQLite behavior)
        let values = vec![Value::Text(Text::new("hello".to_string()))];

        // text - 1 => 0 - 1 = -1
        let expr = TrivialExpression::Binary {
            left: Box::new(TrivialExpression::Column(0)),
            op: Operator::Subtract,
            right: Box::new(TrivialExpression::Immediate(Value::from_i64(1))),
        };
        assert_eq!(expr.evaluate(&values), Value::from_i64(-1));

        // text + 1 => 0 + 1 = 1
        let expr = TrivialExpression::Binary {
            left: Box::new(TrivialExpression::Column(0)),
            op: Operator::Add,
            right: Box::new(TrivialExpression::Immediate(Value::from_i64(1))),
        };
        assert_eq!(expr.evaluate(&values), Value::from_i64(1));

        // text * 2 => 0 * 2 = 0
        let expr = TrivialExpression::Binary {
            left: Box::new(TrivialExpression::Column(0)),
            op: Operator::Multiply,
            right: Box::new(TrivialExpression::Immediate(Value::from_i64(2))),
        };
        assert_eq!(expr.evaluate(&values), Value::from_i64(0));

        // text / 2 => 0 / 2 = 0
        let expr = TrivialExpression::Binary {
            left: Box::new(TrivialExpression::Column(0)),
            op: Operator::Divide,
            right: Box::new(TrivialExpression::Immediate(Value::from_i64(2))),
        };
        assert_eq!(expr.evaluate(&values), Value::from_i64(0));

        // Numeric text "42" - 1 => 41
        let numeric_text_values = vec![Value::Text(Text::new("42".to_string()))];
        let expr = TrivialExpression::Binary {
            left: Box::new(TrivialExpression::Column(0)),
            op: Operator::Subtract,
            right: Box::new(TrivialExpression::Immediate(Value::from_i64(1))),
        };
        assert_eq!(expr.evaluate(&numeric_text_values), Value::from_i64(41));
    }
}
