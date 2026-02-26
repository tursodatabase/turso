use super::*;

/// State used to implement LEFT JOIN row production.
///
/// We track whether the right side matched so we can emit NULL-filled rows
/// only when there was no match.
pub struct LeftJoinMetadata {
    // integer register that holds a flag that is set to true if the current row has a match for the left join
    pub reg_match_flag: usize,
    // label for the instruction that sets the match flag to true
    pub label_match_flag_set_true: BranchOffset,
    // label for the instruction that checks if the match flag is true
    pub label_match_flag_check_value: BranchOffset,
}

/// Metadata for handling SEMI/ANTI JOIN operations (EXISTS/NOT EXISTS unnesting)
#[derive(Debug)]
pub struct SemiAntiJoinMetadata {
    /// Label pointing to the body (for anti-join: jump back to body when inner exhausts).
    /// Also used by anti-join to resolve the body start address.
    pub label_body: BranchOffset,
    /// Label of the outer loop's Next instruction (to skip outer row or skip inner loop).
    pub label_next_outer: BranchOffset,
    /// The original table index of the outer (non-semi/anti) table whose Next
    /// instruction `label_next_outer` should resolve to.
    pub outer_table_idx: usize,
}

/// Jump labels for each loop in the query's main execution loop
#[derive(Debug, Clone, Copy)]
pub struct LoopLabels {
    /// jump to the start of the loop body
    pub loop_start: BranchOffset,
    /// jump to the Next instruction (or equivalent)
    pub next: BranchOffset,
    /// jump to the end of the loop, exiting it
    pub loop_end: BranchOffset,
}

impl LoopLabels {
    /// Allocates the three jump labels used by one loop level.
    pub fn new(program: &mut ProgramBuilder) -> Self {
        Self {
            loop_start: program.allocate_label(),
            next: program.allocate_label(),
            loop_end: program.allocate_label(),
        }
    }
}

/// Walk backwards through the join order from `join_idx` to find the nearest
/// ancestor that is not a semi/anti-join table. Returns its `original_idx`.
/// When multiple semi/anti-joins chain (e.g. NOT EXISTS t2 AND NOT EXISTS t3),
/// this finds the non-semi/anti table whose Next instruction is the correct
/// "skip outer row" target.
pub(super) fn find_previous_non_semi_anti_table_idx(
    join_order: &[JoinOrderMember],
    tables: &[JoinedTable],
    join_idx: usize,
) -> usize {
    assert!(join_idx > 0, "semi/anti-join cannot be the first table");
    let mut idx = join_idx - 1;
    while idx > 0 {
        let prev = &tables[join_order[idx].original_idx];
        if !prev
            .join_info
            .as_ref()
            .is_some_and(|ji| ji.is_semi_or_anti())
        {
            break;
        }
        idx -= 1;
    }
    join_order[idx].original_idx
}

/// Emits cursor advancement for the chosen scan direction.
pub(super) fn emit_cursor_advance(
    program: &mut ProgramBuilder,
    cursor_id: CursorID,
    loop_target: BranchOffset,
    iter_dir: IterationDirection,
) {
    match iter_dir {
        IterationDirection::Forwards => program.emit_insn(Insn::Next {
            cursor_id,
            pc_if_next: loop_target,
        }),
        IterationDirection::Backwards => program.emit_insn(Insn::Prev {
            cursor_id,
            pc_if_prev: loop_target,
        }),
    }
}
