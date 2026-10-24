use crate::vdbe::{
    builder::ProgramBuilder,
    insn::{to_u32, Insn},
};

use crate::vdbe::insn::SortComparatorType;

/// Maps a custom type `<` operator function name to a SortComparatorType.
/// Returns None if the function name is not recognized.
fn sort_comparator_from_func_name(func_name: &str) -> Option<SortComparatorType> {
    match func_name {
        "numeric_lt" => Some(SortComparatorType::NumericLt),
        "test_uint_lt" => Some(SortComparatorType::TestUintLt),
        "string_reverse" => Some(SortComparatorType::StringReverse),
        "array_lt" => Some(SortComparatorType::ArrayLt),
        _ => None,
    }
}

pub(crate) fn custom_type_comparator_from_type_fact(
    type_fact: &crate::translate::semantic::hir::TypeFact,
) -> Option<SortComparatorType> {
    if type_fact.is_array() {
        return Some(SortComparatorType::ArrayLt);
    }
    type_fact
        .declared
        .as_ref()?
        .custom()?
        .value()
        .operators()
        .iter()
        .find(|operator| operator.op == "<")
        .and_then(|operator| operator.func_name.as_deref())
        .and_then(sort_comparator_from_func_name)
}

// Metadata for handling ORDER BY operations

/// Emits the bytecode for inserting a row into a sorter.
/// This can be either a GROUP BY sorter or an ORDER BY sorter.
pub fn sorter_insert(
    program: &mut ProgramBuilder,
    start_reg: usize,
    column_count: usize,
    cursor_id: usize,
    record_reg: usize,
) {
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(start_reg),
        count: to_u32(column_count),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::SorterInsert {
        cursor_id,
        record_reg,
    });
}
