use crate::{
    translate::expr::translate_expr,
    translate::emitter::Resolver,
    vdbe::{builder::ProgramBuilder, insn::Insn, BranchOffset},
    LimboError, Result, SymbolTable,
};
use limbo_sqlite3_parser::ast;

pub fn emit_values(program: &mut ProgramBuilder, values: &[Vec<ast::Expr>]) -> Result<()> {
    let goto_target = program.allocate_label();
    program.emit_insn(Insn::Init {
        target_pc: goto_target,
    });

    let start_offset = program.offset();

    let start_reg = 1;
    let first_row_len = values[0].len();
    let num_regs = start_reg + first_row_len;

    for _ in 0..num_regs {
        program.alloc_register();
    }

    let symbol_table = SymbolTable::new();
    let resolver = Resolver::new(&symbol_table);

    for row in values {
        if row.len() != first_row_len {
            return Err(LimboError::ParseError(
                "all VALUES rows must have the same number of values".into(),  
            ));
        }

        for (i, expr) in row.iter().enumerate() {
            let reg = start_reg + i;
            translate_expr(program, None, expr, reg, &resolver)?;
        }

        program.emit_insn(Insn::ResultRow {
            start_reg,
            count: first_row_len,
        });
    }

    program.emit_insn(Insn::Halt {
        err_code: 0,
        description: String::new(), 
    });

    program.resolve_label(goto_target, program.offset());
    program.emit_insn(Insn::Transaction { write: false });

    program.emit_constant_insns();

    program.emit_insn(Insn::Goto {
        target_pc: start_offset,
    });

    Ok(())
}