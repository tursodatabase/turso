use crate::schema::Column;
use crate::translate::emitter::Resolver;
use crate::translate::expr::translate_expr;
use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::insn::CmpInsFlags;
use crate::vdbe::{insn::Insn, BranchOffset};
use limbo_sqlite3_parser::ast;
use limbo_sqlite3_parser::ast::Expr;

use crate::schema::BTreeTable;

pub fn new_translate_check_constraint(
    program: &mut ProgramBuilder,
    table: &BTreeTable,
    column_registers_start: usize,
    resolver: &Resolver,
) {
    let check_constraints = table
        .table_check_constraints
        .iter()
        .chain(table.column_check_constraints.iter());
    // Run all constraint checks
    for check_constraint in check_constraints {
        let jump_if_true = program.allocate_label();
        translate_check_constraint(
            program,
            &check_constraint.expr,
            table
                .columns
                .iter()
                .enumerate()
                .map(|(i, column)| (column_registers_start + i, column))
                .collect::<Vec<_>>()
                .as_ref(),
            Some(jump_if_true),
            &resolver,
        );

        use crate::error::SQLITE_CONSTRAINT_CHECK;
        program.emit_insn(Insn::Halt {
            err_code: SQLITE_CONSTRAINT_CHECK,
            description: check_constraint.description(),
        });

        program.preassign_label_to_next_insn(jump_if_true);
    }
}

fn translate_check_constraint(
    program: &mut ProgramBuilder,
    check_constraint_expr: &Expr,
    registers_by_columns: &[(usize, &Column)],
    jump_if_true: Option<BranchOffset>,
    resolver: &Resolver,
) -> usize {
    match check_constraint_expr {
        ast::Expr::Literal(_) => {
            let reg = program.alloc_register();
            let _ = translate_expr(program, None, &check_constraint_expr, reg, resolver);
            return reg;
        }
        ast::Expr::Id(id) => {
            for (reg, col) in registers_by_columns.iter() {
                if col.name.as_ref().map_or(false, |name| *name == id.0) {
                    return *reg;
                }
            }
            unreachable!();
        }
        ast::Expr::Binary(lhs, ast::Operator::Equals, rhs) => {
            let lhs_reg =
                translate_check_constraint(program, lhs, registers_by_columns, None, resolver);
            let rhs_reg =
                translate_check_constraint(program, rhs, registers_by_columns, None, resolver);
            if let Some(label) = jump_if_true {
                program.emit_insn(Insn::Eq {
                    lhs: lhs_reg,
                    rhs: rhs_reg,
                    target_pc: label,
                    flags: CmpInsFlags::default().jump_if_null(),
                    collation: program.curr_collation(),
                });
            }
            return rhs_reg;
        }
        ast::Expr::Binary(lhs, ast::Operator::Add, rhs) => {
            let lhs_reg =
                translate_check_constraint(program, lhs, registers_by_columns, None, resolver);
            let rhs_reg =
                translate_check_constraint(program, rhs, registers_by_columns, None, resolver);
            let dest_reg = program.alloc_register();
            program.emit_insn(Insn::Add {
                lhs: lhs_reg,
                rhs: rhs_reg,
                dest: dest_reg,
            });
            if let Some(label) = jump_if_true {
                program.emit_insn(Insn::If {
                    reg: dest_reg,
                    target_pc: label,
                    jump_if_null: true,
                });
            }
            return dest_reg;
        }
        ast::Expr::Binary(lhs, ast::Operator::Subtract, rhs) => {
            let lhs_reg =
                translate_check_constraint(program, lhs, registers_by_columns, None, resolver);
            let rhs_reg =
                translate_check_constraint(program, rhs, registers_by_columns, None, resolver);
            let dest_reg = program.alloc_register();
            program.emit_insn(Insn::Subtract {
                lhs: lhs_reg,
                rhs: rhs_reg,
                dest: dest_reg,
            });
            if let Some(label) = jump_if_true {
                program.emit_insn(Insn::If {
                    reg: dest_reg,
                    target_pc: label,
                    jump_if_null: true,
                });
            }
            return dest_reg;
        }
        ast::Expr::Binary(lhs, ast::Operator::Multiply, rhs) => {
            let lhs_reg =
                translate_check_constraint(program, lhs, registers_by_columns, None, resolver);
            let rhs_reg =
                translate_check_constraint(program, rhs, registers_by_columns, None, resolver);
            let dest_reg = program.alloc_register();
            program.emit_insn(Insn::Multiply {
                lhs: lhs_reg,
                rhs: rhs_reg,
                dest: dest_reg,
            });
            if let Some(label) = jump_if_true {
                program.emit_insn(Insn::If {
                    reg: dest_reg,
                    target_pc: label,
                    jump_if_null: true,
                });
            }
            return dest_reg;
        }
        ast::Expr::Binary(lhs, ast::Operator::Divide, rhs) => {
            let lhs_reg =
                translate_check_constraint(program, lhs, registers_by_columns, None, resolver);
            let rhs_reg =
                translate_check_constraint(program, rhs, registers_by_columns, None, resolver);
            let dest_reg = program.alloc_register();
            program.emit_insn(Insn::Divide {
                lhs: lhs_reg,
                rhs: rhs_reg,
                dest: dest_reg,
            });
            if let Some(label) = jump_if_true {
                program.emit_insn(Insn::If {
                    reg: dest_reg,
                    target_pc: label,
                    jump_if_null: true,
                });
            }
            return dest_reg;
        }
        ast::Expr::Binary(lhs, ast::Operator::Modulus, rhs) => {
            let lhs_reg =
                translate_check_constraint(program, lhs, registers_by_columns, None, resolver);
            let rhs_reg =
                translate_check_constraint(program, rhs, registers_by_columns, None, resolver);
            let dest_reg = program.alloc_register();
            program.emit_insn(Insn::Remainder {
                lhs: lhs_reg,
                rhs: rhs_reg,
                dest: dest_reg,
            });
            if let Some(label) = jump_if_true {
                program.emit_insn(Insn::If {
                    reg: dest_reg,
                    target_pc: label,
                    jump_if_null: true,
                });
            }
            return dest_reg;
        }
        ast::Expr::Parenthesized(expr) => {
            let dest_reg = translate_check_constraint(
                program,
                expr.get(0).expect("Expr should exist"),
                registers_by_columns,
                None,
                resolver,
            );
            if let Some(label) = jump_if_true {
                program.emit_insn(Insn::If {
                    reg: dest_reg,
                    target_pc: label,
                    jump_if_null: true,
                });
            }
            return dest_reg;
        }
        e => todo!("{}", &e),
    }
}
