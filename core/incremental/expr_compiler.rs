// Expression compilation for incremental operators
// This module provides utilities to compile SQL expressions into VDBE subprograms
// that can be executed efficiently in the incremental computation context.

use crate::schema::Schema;
use crate::storage::pager::Pager;
use crate::translate::emitter::Resolver;
use crate::translate::expr::translate_expr;
use crate::types::Text;
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::insn::Insn;
use crate::vdbe::{Program, ProgramState, Register};
use crate::SymbolTable;
use crate::{CaptureDataChangesMode, Connection, QueryMode, Result, Value};
use std::rc::Rc;
use std::sync::Arc;
use turso_sqlite3_parser::ast::{Expr, Literal, Operator};

// Transform an expression to replace column references with Register expressions Why do we want to
// do this?
//
// Imagine you have a view like:
//
// create materialized view hex(count(*) + 2). translate_expr will usually try to find match names
// to either literals or columns. But "count(*)" is not a column in any sqlite table.
//
// We *could* theoretically have a table-representation of every DBSP-step, but it is a lot simpler
// to just pass registers as parameters to the VDBE expression, and teach translate_expr to
// recognize those.
//
// But because the expression compiler will not generate those register inputs, we have to
// transform the expression.
fn transform_expr_for_dbsp(expr: &Expr, input_column_names: &[String]) -> Expr {
    match expr {
        // Transform column references (represented as Id) to Register expressions
        Expr::Id(name) => {
            // Check if this is a column name from our input
            if let Some(idx) = input_column_names
                .iter()
                .position(|col| col == name.as_str())
            {
                // Replace with a Register expression
                Expr::Register(idx)
            } else {
                // Not a column reference, keep as is
                expr.clone()
            }
        }
        // Recursively transform nested expressions
        Expr::Binary(lhs, op, rhs) => Expr::Binary(
            Box::new(transform_expr_for_dbsp(lhs, input_column_names)),
            *op,
            Box::new(transform_expr_for_dbsp(rhs, input_column_names)),
        ),
        Expr::Unary(op, operand) => Expr::Unary(
            *op,
            Box::new(transform_expr_for_dbsp(operand, input_column_names)),
        ),
        Expr::FunctionCall {
            name,
            distinctness,
            args,
            order_by,
            filter_over,
        } => Expr::FunctionCall {
            name: name.clone(),
            distinctness: *distinctness,
            args: args.as_ref().map(|args_vec| {
                args_vec
                    .iter()
                    .map(|arg| transform_expr_for_dbsp(arg, input_column_names))
                    .collect()
            }),
            order_by: order_by.clone(),
            filter_over: filter_over.clone(),
        },
        Expr::Parenthesized(exprs) => Expr::Parenthesized(
            exprs
                .iter()
                .map(|e| transform_expr_for_dbsp(e, input_column_names))
                .collect(),
        ),
        // For other expression types, keep as is
        _ => expr.clone(),
    }
}

/// Enum to represent either a trivial or compiled expression
#[derive(Clone)]
pub enum ExpressionExecutor {
    /// Trivial expression that can be evaluated inline
    Trivial(TrivialExpression),
    /// Compiled VDBE program for complex expressions
    Compiled(Arc<Program>),
}

/// Trivial expression that can be evaluated inline without VDBE
/// Only supports operations where operands have the same type (no coercion)
#[derive(Clone, Debug)]
pub enum TrivialExpression {
    /// Direct column reference
    Column(usize),
    /// Immediate value
    Immediate(Value),
    /// Binary operation on trivial expressions (same-type operands only)
    Binary {
        left: Box<TrivialExpression>,
        op: Operator,
        right: Box<TrivialExpression>,
    },
}

impl TrivialExpression {
    /// Evaluate the trivial expression with the given input values
    /// Panics if type mismatch occurs (this indicates a bug in validation)
    pub fn evaluate(&self, values: &[Value]) -> Value {
        match self {
            TrivialExpression::Column(idx) => values.get(*idx).cloned().unwrap_or(Value::Null),
            TrivialExpression::Immediate(val) => val.clone(),
            TrivialExpression::Binary { left, op, right } => {
                let left_val = left.evaluate(values);
                let right_val = right.evaluate(values);

                // Only perform operations on same-type operands
                match op {
                    Operator::Add => match (&left_val, &right_val) {
                        (Value::Integer(a), Value::Integer(b)) => Value::Integer(a + b),
                        (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
                        (Value::Null, _) | (_, Value::Null) => Value::Null,
                        _ => panic!("Type mismatch in trivial expression: {left_val:?} + {right_val:?}. This is a bug in trivial expression validation."),
                    },
                    Operator::Subtract => match (&left_val, &right_val) {
                        (Value::Integer(a), Value::Integer(b)) => Value::Integer(a - b),
                        (Value::Float(a), Value::Float(b)) => Value::Float(a - b),
                        (Value::Null, _) | (_, Value::Null) => Value::Null,
                        _ => panic!("Type mismatch in trivial expression: {left_val:?} - {right_val:?}. This is a bug in trivial expression validation."),
                    },
                    Operator::Multiply => match (&left_val, &right_val) {
                        (Value::Integer(a), Value::Integer(b)) => Value::Integer(a * b),
                        (Value::Float(a), Value::Float(b)) => Value::Float(a * b),
                        (Value::Null, _) | (_, Value::Null) => Value::Null,
                        _ => panic!("Type mismatch in trivial expression: {left_val:?} * {right_val:?}. This is a bug in trivial expression validation."),
                    },
                    Operator::Divide => match (&left_val, &right_val) {
                        (Value::Integer(a), Value::Integer(b)) => {
                            if *b != 0 {
                                Value::Integer(a / b)
                            } else {
                                Value::Null
                            }
                        }
                        (Value::Float(a), Value::Float(b)) => {
                            if *b != 0.0 {
                                Value::Float(a / b)
                            } else {
                                Value::Null
                            }
                        }
                        (Value::Null, _) | (_, Value::Null) => Value::Null,
                        _ => panic!("Type mismatch in trivial expression: {left_val:?} / {right_val:?}. This is a bug in trivial expression validation."),
                    },
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

impl CompiledExpression {
    /// Get the "type" of a trivial expression for type checking
    /// Returns None if type can't be determined statically
    fn get_trivial_type(expr: &TrivialExpression) -> Option<&'static str> {
        match expr {
            TrivialExpression::Column(_) => None, // Can't know column type statically
            TrivialExpression::Immediate(val) => match val {
                Value::Integer(_) => Some("integer"),
                Value::Float(_) => Some("float"),
                Value::Text(_) => Some("text"),
                Value::Null => Some("null"),
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

    /// Check if an expression is trivial (columns, immediates, and simple arithmetic)
    /// Only considers expressions trivial if they don't require type coercion
    fn is_trivial_expr(expr: &Expr, input_column_names: &[String]) -> Option<TrivialExpression> {
        match expr {
            // Column reference or register
            Expr::Id(name) => input_column_names
                .iter()
                .position(|col| col == name.as_str())
                .map(TrivialExpression::Column),
            Expr::Register(idx) => Some(TrivialExpression::Column(*idx)),

            // Immediate values
            Expr::Literal(lit) => {
                let value = match lit {
                    Literal::Numeric(n) => {
                        if let Ok(i) = n.parse::<i64>() {
                            Value::Integer(i)
                        } else if let Ok(f) = n.parse::<f64>() {
                            Value::Float(f)
                        } else {
                            return None;
                        }
                    }
                    Literal::String(s) => {
                        let cleaned = s.trim_matches('\'').trim_matches('"');
                        Value::Text(Text::new(cleaned))
                    }
                    Literal::Null => Value::Null,
                    _ => return None,
                };
                Some(TrivialExpression::Immediate(value))
            }

            // Binary operations with simple operators
            Expr::Binary(left, op, right) => {
                // Only support simple arithmetic operators
                match op {
                    Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide => {
                        // Both operands must be trivial
                        let left_trivial = Self::is_trivial_expr(left, input_column_names)?;
                        let right_trivial = Self::is_trivial_expr(right, input_column_names)?;

                        // Check if we can determine types statically
                        // If both are immediates, they must have the same type
                        // If either is a column, we can't validate at compile time,
                        // but we'll assert at runtime if there's a mismatch
                        if let (Some(left_type), Some(right_type)) = (
                            Self::get_trivial_type(&left_trivial),
                            Self::get_trivial_type(&right_trivial),
                        ) {
                            // Both types are known - they must match (or one is null)
                            if left_type != right_type
                                && left_type != "null"
                                && right_type != "null"
                            {
                                return None; // Type mismatch - not trivial
                            }
                        }
                        // If we can't determine types (columns involved), we optimistically
                        // assume they'll match at runtime (and assert if they don't)

                        Some(TrivialExpression::Binary {
                            left: Box::new(left_trivial),
                            op: *op,
                            right: Box::new(right_trivial),
                        })
                    }
                    _ => None,
                }
            }

            // Parenthesized expressions with single element
            Expr::Parenthesized(exprs) if exprs.len() == 1 => {
                Self::is_trivial_expr(&exprs[0], input_column_names)
            }

            _ => None,
        }
    }

    /// Compile a SQL expression into either a trivial executor or VDBE program
    ///
    /// For trivial expressions (columns, immediates, simple same-type arithmetic), uses inline evaluation.
    /// For complex expressions or those requiring type coercion, compiles to VDBE bytecode.
    pub fn compile(
        expr: &Expr,
        input_column_names: &[String],
        schema: &Schema,
        syms: &SymbolTable,
        connection: Arc<Connection>,
    ) -> Result<Self> {
        let input_count = input_column_names.len();

        // First, check if this is a trivial expression
        if let Some(trivial) = Self::is_trivial_expr(expr, input_column_names) {
            return Ok(CompiledExpression {
                executor: ExpressionExecutor::Trivial(trivial),
                input_count,
            });
        }

        // Fall back to VDBE compilation for complex expressions
        // Create a minimal program builder for expression compilation
        let mut builder = ProgramBuilder::new(
            QueryMode::Normal,
            CaptureDataChangesMode::Off,
            ProgramBuilderOpts {
                num_cursors: 0,
                approx_num_insns: 5,  // Most expressions are simple
                approx_num_labels: 0, // Expressions don't need labels
            },
        );

        // Allocate registers for input values
        let input_count = input_column_names.len();

        // Allocate input registers
        for _ in 0..input_count {
            builder.alloc_register();
        }

        // Allocate a temp register for computation
        let temp_result_register = builder.alloc_register();

        // Transform the expression to replace column references with Register expressions
        let transformed_expr = transform_expr_for_dbsp(expr, input_column_names);

        // Create a resolver for translate_expr
        let resolver = Resolver::new(schema, syms);

        // Translate the transformed expression to bytecode
        translate_expr(
            &mut builder,
            None, // No table references needed for pure expressions
            &transformed_expr,
            temp_result_register,
            &resolver,
        )?;

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
        });

        // Build the program from the compiled expression bytecode
        let program = Arc::new(builder.build(connection, false, ""));

        Ok(CompiledExpression {
            executor: ExpressionExecutor::Compiled(program),
            input_count,
        })
    }

    /// Execute the compiled expression with the given input values
    pub fn execute(&self, values: &[Value], pager: Rc<Pager>) -> Result<Value> {
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
                for (idx, value) in values.iter().take(self.input_count).enumerate() {
                    state.set_register(idx, Register::Value(value.clone()));
                }

                // Execute the program
                let mut pc = 0usize;
                while pc < program.insns.len() {
                    let (insn, insn_fn) = &program.insns[pc];
                    state.pc = pc as u32;

                    // Execute the instruction
                    match insn_fn(program, &mut state, insn, &pager, None)? {
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
                        crate::vdbe::execute::InsnFunctionStepResult::Interrupt => {
                            return Err(crate::LimboError::InternalError(
                                "Expression evaluation was interrupted".to_string(),
                            ));
                        }
                        crate::vdbe::execute::InsnFunctionStepResult::Busy => {
                            return Err(crate::LimboError::InternalError(
                                "Expression evaluation encountered busy state".to_string(),
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
