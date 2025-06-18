use crate::{
    schema::Schema,
    vdbe::{builder::ProgramBuilder, insn::Insn},
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
    for table in schema.tables.values() {
        if let crate::schema::Table::BTree(table) = table.as_ref() {
            root_pages.push(table.root_page);
        };
    }
    let message_register = program.alloc_register();
    program.emit_insn(Insn::IntegrityCk {
        max_errors: MAX_INTEGRITY_CHECK_ERRORS,
        roots: root_pages,
        message_register,
    });
    program.emit_insn(Insn::ResultRow {
        start_reg: message_register,
        count: 1,
    });
    Ok(())
}
