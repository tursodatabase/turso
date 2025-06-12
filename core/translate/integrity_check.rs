use crate::{
    schema::Schema,
    vdbe::{
        builder::ProgramBuilder,
        insn::{CmpInsFlags, Insn},
    },
};

/// Maximum number of errors to report with integrity check. If we exceed this number we will short
/// circuit the procedure and return early to not waste time.
const MAX_INTEGRITY_CHECK_ERRORS: usize = 10;

pub fn translate_integrity_check(
    schema: &Schema,
    program: &mut ProgramBuilder,
) -> crate::Result<()> {
    let mut root_pages = Vec::with_capacity(schema.tables.len() + schema.indexes.len());
    // Collect root pages to run integrity check on
    let mut number_of_tables = 0;
    for (table_name, table) in &schema.tables {
        if let crate::schema::Table::BTree(table) = table.as_ref() {
            root_pages.push(table.root_page);
            number_of_tables += 1;
            if let Some(indexes) = schema.indexes.get(table_name) {
                for index in indexes {
                    root_pages.push(index.root_page);
                    number_of_tables += 1;
                }
            }
        };
    }
    let message_register = program.alloc_register();
    let max_error_updated_register = program.alloc_register();
    let start_count_register = program.alloc_registers(number_of_tables);
    program.emit_insn(Insn::IntegrityCk {
        max_errors: MAX_INTEGRITY_CHECK_ERRORS,
        roots: root_pages,
        message_register,
        start_count_register,
        max_error_updated_register,
    });

    // Check index number of rows is correct
    let mut root_index = 0;
    let error_msg_wrong_index_count_string =
        program.emit_string8_new_reg("wrong # of entries in index ".to_string());
    for (table_name, table) in &schema.tables {
        let table_index = root_index;
        root_index += 1;
        if let crate::schema::Table::BTree(_) = table.as_ref() {
            if let Some(indexes) = schema.indexes.get(table_name) {
                for index in indexes {
                    // FIXME: check these are not partial indexes
                    let jump = program.allocate_label();
                    program.emit_insn(Insn::Eq {
                        lhs: start_count_register + root_index,
                        rhs: start_count_register + table_index,
                        target_pc: jump,
                        flags: CmpInsFlags::default(),
                        collation: None,
                    });
                    let table_name_register =
                        program.emit_string8_new_reg(index.table_name.to_string());
                    let res = program.alloc_register();
                    program.emit_insn(Insn::Concat {
                        lhs: error_msg_wrong_index_count_string,
                        rhs: table_name_register,
                        dest: res,
                    });
                    program.emit_insn(Insn::ResultRow {
                        start_reg: res,
                        count: 1,
                    });
                    program.preassign_label_to_next_insn(jump);
                    root_index += 1;
                }
            }
        };
    }

    program.emit_insn(Insn::ResultRow {
        start_reg: message_register,
        count: 1,
    });
    Ok(())
}
