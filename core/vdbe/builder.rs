use std::{
    cmp::Ordering,
    sync::{atomic::AtomicI64, Arc},
};

use tracing::{instrument, Level};
use turso_parser::ast::{self, TableInternalId};

use crate::{
    numeric::Numeric,
    parameters::Parameters,
    schema::{BTreeTable, Index, PseudoCursorType, Schema, Table},
    translate::{
        collate::CollationSeq,
        emitter::TransactionMode,
        expr::ParamState,
        plan::{ResultSetColumn, TableReferences},
    },
    CaptureDataChangesMode, Connection, Value, VirtualTable,
};

#[derive(Default)]
pub struct TableRefIdCounter {
    next_free: ast::TableInternalId,
}

impl TableRefIdCounter {
    pub fn new() -> Self {
        Self {
            next_free: TableInternalId::default(),
        }
    }

    pub fn next(&mut self) -> ast::TableInternalId {
        let id = self.next_free;
        self.next_free += 1;
        id
    }
}

use super::{BranchOffset, CursorID, Insn, InsnReference, JumpTarget, Program};

/// A key that uniquely identifies a cursor.
/// The key is a pair of table reference id and index.
/// The index is only provided when the cursor is an index cursor.
#[derive(Debug, Clone)]
pub struct CursorKey {
    /// The table reference that the cursor is associated with.
    /// We cannot use e.g. the table query identifier (e.g. 'users' or 'u')
    /// because it might be ambiguous, e.g. this silly example:
    /// `SELECT * FROM t WHERE EXISTS (SELECT * from t)` <-- two different cursors, which 't' should we use as key?
    ///  TableInternalIds are unique within a program, since there is one id per table reference.
    pub table_reference_id: TableInternalId,
    /// The index, in case of an index cursor.
    /// The combination of table internal id and index is enough to disambiguate.
    pub index: Option<Arc<Index>>,
}

impl CursorKey {
    pub fn table(table_reference_id: TableInternalId) -> Self {
        Self {
            table_reference_id,
            index: None,
        }
    }

    pub fn index(table_reference_id: TableInternalId, index: Arc<Index>) -> Self {
        Self {
            table_reference_id,
            index: Some(index),
        }
    }

    pub fn equals(&self, other: &CursorKey) -> bool {
        if self.table_reference_id != other.table_reference_id {
            return false;
        }
        match (self.index.as_ref(), other.index.as_ref()) {
            (Some(self_index), Some(other_index)) => self_index.name == other_index.name,
            (None, None) => true,
            _ => false,
        }
    }
}

#[allow(dead_code)]
pub struct ProgramBuilder {
    pub table_reference_counter: TableRefIdCounter,
    next_free_register: usize,
    next_free_cursor_id: usize,
    /// Instruction, the function to execute it with, and its original index in the vector.
    insns: Vec<(Insn, usize)>,
    /// A span of instructions from (offset_start_inclusive, offset_end_exclusive),
    /// that are deemed to be compile-time constant and can be hoisted out of loops
    /// so that they get evaluated only once at the start of the program.
    pub constant_spans: Vec<(usize, usize)>,
    /// Cursors that are referenced by the program. Indexed by [CursorKey].
    /// Certain types of cursors do not need a [CursorKey] (e.g. temp tables, sorter),
    /// because they never need to use [ProgramBuilder::resolve_cursor_id] to find it
    /// again. Hence, the key is optional.
    pub cursor_ref: Vec<(Option<CursorKey>, CursorType)>,
    /// A vector where index=label number, value=resolved offset. Resolved in build().
    label_to_resolved_offset: Vec<Option<(InsnReference, JumpTarget)>>,
    // Bitmask of cursors that have emitted a SeekRowid instruction.
    seekrowid_emitted_bitmask: u64,
    // map of instruction index to manual comment (used in EXPLAIN only)
    comments: Vec<(InsnReference, &'static str)>,
    pub parameters: Parameters,
    pub result_columns: Vec<ResultSetColumn>,
    pub table_references: TableReferences,
    /// Curr collation sequence. Bool indicates whether it was set by a COLLATE expr
    collation: Option<(CollationSeq, bool)>,
    /// Current parsing nesting level
    nested_level: usize,
    init_label: BranchOffset,
    start_offset: BranchOffset,
    capture_data_changes_mode: CaptureDataChangesMode,
    // TODO: when we support multiple dbs, this should be a write mask to track which DBs need to be written
    txn_mode: TransactionMode,
    rollback: bool,
    /// The mode in which the query is being executed.
    query_mode: QueryMode,
    /// Current parent explain address, if any.
    current_parent_explain_idx: Option<usize>,
    pub param_ctx: ParamState,
}

#[derive(Debug, Clone)]
pub enum CursorType {
    BTreeTable(Arc<BTreeTable>),
    BTreeIndex(Arc<Index>),
    Pseudo(PseudoCursorType),
    Sorter,
    VirtualTable(Arc<VirtualTable>),
    MaterializedView(
        Arc<BTreeTable>,
        Arc<std::sync::Mutex<crate::incremental::view::IncrementalView>>,
    ),
}

impl CursorType {
    pub fn is_index(&self) -> bool {
        matches!(self, CursorType::BTreeIndex(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum QueryMode {
    Normal,
    Explain,
    ExplainQueryPlan,
}

impl QueryMode {
    pub fn new(cmd: &ast::Cmd) -> Self {
        match cmd {
            ast::Cmd::ExplainQueryPlan(_) => QueryMode::ExplainQueryPlan,
            ast::Cmd::Explain(_) => QueryMode::Explain,
            ast::Cmd::Stmt(_) => QueryMode::Normal,
        }
    }
}

pub struct ProgramBuilderOpts {
    pub num_cursors: usize,
    pub approx_num_insns: usize,
    pub approx_num_labels: usize,
}

/// Use this macro to emit an OP_Explain instruction.
/// Please use this macro instead of calling emit_explain() directly,
/// because we want to avoid allocating a String if we are not in explain mode.
#[macro_export]
macro_rules! emit_explain {
    ($builder:expr, $push:expr, $detail:expr) => {
        if let QueryMode::ExplainQueryPlan = $builder.get_query_mode() {
            $builder.emit_explain($push, $detail);
        }
    };
}

impl ProgramBuilder {
    pub fn new(
        query_mode: QueryMode,
        capture_data_changes_mode: CaptureDataChangesMode,
        opts: ProgramBuilderOpts,
    ) -> Self {
        Self {
            table_reference_counter: TableRefIdCounter::new(),
            next_free_register: 1,
            next_free_cursor_id: 0,
            insns: Vec::with_capacity(opts.approx_num_insns),
            cursor_ref: Vec::with_capacity(opts.num_cursors),
            constant_spans: Vec::new(),
            label_to_resolved_offset: Vec::with_capacity(opts.approx_num_labels),
            seekrowid_emitted_bitmask: 0,
            comments: Vec::new(),
            parameters: Parameters::new(),
            result_columns: Vec::new(),
            table_references: TableReferences::new(vec![], vec![]),
            collation: None,
            nested_level: 0,
            // These labels will be filled when `prologue()` is called
            init_label: BranchOffset::Placeholder,
            start_offset: BranchOffset::Placeholder,
            capture_data_changes_mode,
            txn_mode: TransactionMode::None,
            rollback: false,
            query_mode,
            current_parent_explain_idx: None,
            param_ctx: ParamState::default(),
        }
    }

    pub fn capture_data_changes_mode(&self) -> &CaptureDataChangesMode {
        &self.capture_data_changes_mode
    }

    pub fn extend(&mut self, opts: &ProgramBuilderOpts) {
        self.insns.reserve(opts.approx_num_insns);
        self.cursor_ref.reserve(opts.num_cursors);
        self.label_to_resolved_offset
            .reserve(opts.approx_num_labels);
    }

    /// Start a new constant span. The next instruction to be emitted will be the first
    /// instruction in the span.
    pub fn constant_span_start(&mut self) -> usize {
        let span = self.constant_spans.len();
        let start = self.insns.len();
        self.constant_spans.push((start, usize::MAX));
        span
    }

    /// End the current constant span. The last instruction that was emitted is the last
    /// instruction in the span.
    pub fn constant_span_end(&mut self, span_idx: usize) {
        let span = &mut self.constant_spans[span_idx];
        if span.1 == usize::MAX {
            span.1 = self.insns.len().saturating_sub(1);
        }
    }

    /// End all constant spans that are currently open. This is used to handle edge cases
    /// where we think a parent expression is constant, but we decide during the evaluation
    /// of one of its children that it is not.
    pub fn constant_span_end_all(&mut self) {
        for span in self.constant_spans.iter_mut() {
            if span.1 == usize::MAX {
                span.1 = self.insns.len().saturating_sub(1);
            }
        }
    }

    /// Check if there is a constant span that is currently open.
    pub fn constant_span_is_open(&self) -> bool {
        self.constant_spans
            .last()
            .is_some_and(|(_, end)| *end == usize::MAX)
    }

    /// Get the index of the next constant span.
    /// Used in [crate::translate::expr::translate_expr_no_constant_opt()] to invalidate
    /// all constant spans after the given index.
    pub fn constant_spans_next_idx(&self) -> usize {
        self.constant_spans.len()
    }

    /// Invalidate all constant spans after the given index. This is used when we want to
    /// be sure that constant optimization is never used for translating a given expression.
    /// See [crate::translate::expr::translate_expr_no_constant_opt()] for more details.
    pub fn constant_spans_invalidate_after(&mut self, idx: usize) {
        self.constant_spans.truncate(idx);
    }

    pub fn alloc_register(&mut self) -> usize {
        let reg = self.next_free_register;
        self.next_free_register += 1;
        reg
    }

    pub fn alloc_registers(&mut self, amount: usize) -> usize {
        let reg = self.next_free_register;
        self.next_free_register += amount;
        reg
    }

    pub fn alloc_registers_and_init_w_null(&mut self, amount: usize) -> usize {
        let reg = self.alloc_registers(amount);
        self.emit_insn(Insn::Null {
            dest: reg,
            dest_end: if amount == 1 {
                None
            } else {
                Some(reg + amount - 1)
            },
        });
        reg
    }

    pub fn alloc_cursor_id_keyed(&mut self, key: CursorKey, cursor_type: CursorType) -> usize {
        assert!(
            !self
                .cursor_ref
                .iter()
                .any(|(k, _)| k.as_ref().is_some_and(|k| k.equals(&key))),
            "duplicate cursor key"
        );
        self._alloc_cursor_id(Some(key), cursor_type)
    }

    pub fn alloc_cursor_id(&mut self, cursor_type: CursorType) -> usize {
        self._alloc_cursor_id(None, cursor_type)
    }

    fn _alloc_cursor_id(&mut self, key: Option<CursorKey>, cursor_type: CursorType) -> usize {
        let cursor = self.next_free_cursor_id;
        self.next_free_cursor_id += 1;
        self.cursor_ref.push((key, cursor_type));
        assert_eq!(self.cursor_ref.len(), self.next_free_cursor_id);
        cursor
    }

    pub fn add_pragma_result_column(&mut self, col_name: String) {
        // TODO figure out a better type definition for ResultSetColumn
        // or invent another way to set pragma result columns
        let expr = ast::Expr::Id(ast::Name::exact("".to_string()));
        self.result_columns.push(ResultSetColumn {
            expr,
            alias: Some(col_name),
            contains_aggregates: false,
        });
    }

    #[instrument(skip(self), level = Level::DEBUG)]
    pub fn emit_insn(&mut self, insn: Insn) {
        // This seemingly empty trace here is needed so that a function span is emmited with it
        tracing::trace!("");
        self.insns.push((insn, self.insns.len()));
    }

    pub fn close_cursors(&mut self, cursors: &[CursorID]) {
        for cursor in cursors {
            self.emit_insn(Insn::Close { cursor_id: *cursor });
        }
    }

    pub fn emit_string8(&mut self, value: String, dest: usize) {
        self.emit_insn(Insn::String8 { value, dest });
    }

    pub fn emit_string8_new_reg(&mut self, value: String) -> usize {
        let dest = self.alloc_register();
        self.emit_insn(Insn::String8 { value, dest });
        dest
    }

    pub fn emit_int(&mut self, value: i64, dest: usize) {
        self.emit_insn(Insn::Integer { value, dest });
    }

    pub fn emit_bool(&mut self, value: bool, dest: usize) {
        self.emit_insn(Insn::Integer {
            value: if value { 1 } else { 0 },
            dest,
        });
    }

    pub fn emit_null(&mut self, dest: usize, dest_end: Option<usize>) {
        self.emit_insn(Insn::Null { dest, dest_end });
    }

    pub fn emit_result_row(&mut self, start_reg: usize, count: usize) {
        self.emit_insn(Insn::ResultRow { start_reg, count });
    }

    fn emit_halt(&mut self, rollback: bool) {
        self.emit_insn(Insn::Halt {
            err_code: 0,
            description: if rollback {
                "rollback".to_string()
            } else {
                String::new()
            },
        });
    }

    // no users yet, but I want to avoid someone else in the future
    // just adding parameters to emit_halt! If you use this, remove the
    // clippy warning please.
    #[allow(dead_code)]
    pub fn emit_halt_err(&mut self, err_code: usize, description: String) {
        self.emit_insn(Insn::Halt {
            err_code,
            description,
        });
    }

    pub fn add_comment(&mut self, insn_index: BranchOffset, comment: &'static str) {
        if let QueryMode::Explain | QueryMode::ExplainQueryPlan = self.query_mode {
            self.comments.push((insn_index.as_offset_int(), comment));
        }
    }

    pub fn get_query_mode(&self) -> QueryMode {
        self.query_mode
    }

    /// use emit_explain macro instead, because we don't want to allocate
    /// String if we are not in explain mode
    pub fn emit_explain(&mut self, push: bool, detail: String) {
        if let QueryMode::ExplainQueryPlan = self.query_mode {
            self.emit_insn(Insn::Explain {
                p1: self.insns.len(),
                p2: self.current_parent_explain_idx,
                detail,
            });
            if push {
                self.current_parent_explain_idx = Some(self.insns.len() - 1);
            }
        }
    }

    pub fn pop_current_parent_explain(&mut self) {
        if let QueryMode::ExplainQueryPlan = self.query_mode {
            if let Some(current) = self.current_parent_explain_idx {
                let (Insn::Explain { p2, .. }, _) = &self.insns[current] else {
                    unreachable!("current_parent_explain_idx must point to an Explain insn");
                };
                self.current_parent_explain_idx = *p2;
            }
        } else {
            debug_assert!(self.current_parent_explain_idx.is_none())
        }
    }

    pub fn mark_last_insn_constant(&mut self) {
        if self.constant_span_is_open() {
            // no need to mark this insn as constant as the surrounding parent expression is already constant
            return;
        }

        let prev = self.insns.len().saturating_sub(1);
        self.constant_spans.push((prev, prev));
    }

    fn emit_constant_insns(&mut self) {
        // move compile-time constant instructions to the end of the program, where they are executed once after Init jumps to it.
        // any label_to_resolved_offset that points to an instruction within any moved constant span should be updated to point to the new location.

        // the instruction reordering can be done by sorting the insns, so that the ordering is:
        // 1. if insn not in any constant span, it stays where it is
        // 2. if insn is in a constant span, it is after other insns, except those that are in a later constant span
        // 3. within a single constant span the order is preserver
        self.insns.sort_by(|(_, index_a), (_, index_b)| {
            let a_span = self
                .constant_spans
                .iter()
                .find(|span| span.0 <= *index_a && span.1 >= *index_a);
            let b_span = self
                .constant_spans
                .iter()
                .find(|span| span.0 <= *index_b && span.1 >= *index_b);
            if let (Some(a_span), Some(b_span)) = (a_span, b_span) {
                a_span.0.cmp(&b_span.0)
            } else if a_span.is_some() {
                Ordering::Greater
            } else if b_span.is_some() {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        });
        for resolved_offset in self.label_to_resolved_offset.iter_mut() {
            if let Some((old_offset, target)) = resolved_offset {
                let new_offset = self
                    .insns
                    .iter()
                    .position(|(_, index)| *old_offset == *index as u32)
                    .unwrap() as u32;
                *resolved_offset = Some((new_offset, *target));
            }
        }

        // Fix comments to refer to new locations
        for (old_offset, _) in self.comments.iter_mut() {
            let new_offset = self
                .insns
                .iter()
                .position(|(_, index)| *old_offset == *index as u32)
                .expect("comment must exist") as u32;
            *old_offset = new_offset;
        }

        if let QueryMode::ExplainQueryPlan = self.query_mode {
            self.current_parent_explain_idx =
                if let Some(old_parent) = self.current_parent_explain_idx {
                    self.insns
                        .iter()
                        .position(|(_, index)| old_parent == *index)
                } else {
                    None
                };

            for i in 0..self.insns.len() {
                let (Insn::Explain { p2, .. }, _) = &self.insns[i] else {
                    continue;
                };

                let new_p2 = if p2.is_some() {
                    self.insns.iter().position(|(_, index)| *p2 == Some(*index))
                } else {
                    None
                };

                let (Insn::Explain { p1, p2, .. }, _) = &mut self.insns[i] else {
                    unreachable!();
                };

                *p1 = i;
                *p2 = new_p2;
            }
        }
    }

    pub fn offset(&self) -> BranchOffset {
        BranchOffset::Offset(self.insns.len() as InsnReference)
    }

    pub fn allocate_label(&mut self) -> BranchOffset {
        let label_n = self.label_to_resolved_offset.len();
        self.label_to_resolved_offset.push(None);
        BranchOffset::Label(label_n as u32)
    }

    /// Resolve a label to whatever instruction follows the one that was
    /// last emitted.
    ///
    /// Use this when your use case is: "the program should jump to whatever instruction
    /// follows the one that was previously emitted", and you don't care exactly
    /// which instruction that is. Examples include "the start of a loop", or
    /// "after the loop ends".
    ///
    /// It is important to handle those cases this way, because the precise
    /// instruction that follows any given instruction might change due to
    /// reordering the emitted instructions.
    #[inline]
    pub fn preassign_label_to_next_insn(&mut self, label: BranchOffset) {
        assert!(label.is_label(), "BranchOffset {label:?} is not a label");
        self._resolve_label(label, self.offset().sub(1u32), JumpTarget::AfterThisInsn);
    }

    /// Resolve a label to exactly the instruction that was last emitted.
    ///
    /// Use this when your use case is: "the program should jump to the exact instruction
    /// that was last emitted", and you don't care WHERE exactly that ends up being
    /// once the order of the bytecode of the program is finalized. Examples include
    /// "jump to the Halt instruction", or "jump to the Next instruction of a loop".
    #[inline]
    pub fn resolve_label(&mut self, label: BranchOffset, to_offset: BranchOffset) {
        self._resolve_label(label, to_offset, JumpTarget::ExactlyThisInsn);
    }

    fn _resolve_label(&mut self, label: BranchOffset, to_offset: BranchOffset, target: JumpTarget) {
        assert!(matches!(label, BranchOffset::Label(_)));
        assert!(matches!(to_offset, BranchOffset::Offset(_)));
        let BranchOffset::Label(label_number) = label else {
            unreachable!("Label is not a label");
        };
        self.label_to_resolved_offset[label_number as usize] =
            Some((to_offset.as_offset_int(), target));
    }

    /// Resolve unresolved labels to a specific offset in the instruction list.
    ///
    /// This function scans all instructions and resolves any labels to their corresponding offsets.
    /// It ensures that all labels are resolved correctly and updates the target program counter (PC)
    /// of each instruction that references a label.
    pub fn resolve_labels(&mut self) {
        let resolve = |pc: &mut BranchOffset, insn_name: &str| {
            if let BranchOffset::Label(label) = pc {
                let Some(Some((to_offset, target))) =
                    self.label_to_resolved_offset.get(*label as usize)
                else {
                    panic!("Reference to undefined or unresolved label in {insn_name}: {label}");
                };
                *pc = BranchOffset::Offset(
                    to_offset
                        + if *target == JumpTarget::ExactlyThisInsn {
                            0
                        } else {
                            1
                        },
                );
            }
        };
        for (insn, _) in self.insns.iter_mut() {
            match insn {
                Insn::Init { target_pc } => {
                    resolve(target_pc, "Init");
                }
                Insn::Eq {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Eq");
                }
                Insn::Ne {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Ne");
                }
                Insn::Lt {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Lt");
                }
                Insn::Le {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Le");
                }
                Insn::Gt {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Gt");
                }
                Insn::Ge {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Ge");
                }
                Insn::If {
                    reg: _reg,
                    target_pc,
                    jump_if_null: _,
                } => {
                    resolve(target_pc, "If");
                }
                Insn::IfNot {
                    reg: _reg,
                    target_pc,
                    jump_if_null: _,
                } => {
                    resolve(target_pc, "IfNot");
                }
                Insn::Rewind { pc_if_empty, .. } => {
                    resolve(pc_if_empty, "Rewind");
                }
                Insn::Last { pc_if_empty, .. } => {
                    resolve(pc_if_empty, "Last");
                }
                Insn::Goto { target_pc } => {
                    resolve(target_pc, "Goto");
                }
                Insn::DecrJumpZero {
                    reg: _reg,
                    target_pc,
                } => {
                    resolve(target_pc, "DecrJumpZero");
                }
                Insn::SorterNext {
                    cursor_id: _cursor_id,
                    pc_if_next,
                } => {
                    resolve(pc_if_next, "SorterNext");
                }
                Insn::SorterSort { pc_if_empty, .. } => {
                    resolve(pc_if_empty, "SorterSort");
                }
                Insn::NotNull {
                    reg: _reg,
                    target_pc,
                } => {
                    resolve(target_pc, "NotNull");
                }
                Insn::IfPos { target_pc, .. } => {
                    resolve(target_pc, "IfPos");
                }
                Insn::Next { pc_if_next, .. } => {
                    resolve(pc_if_next, "Next");
                }
                Insn::Once {
                    target_pc_when_reentered,
                    ..
                } => {
                    resolve(target_pc_when_reentered, "Once");
                }
                Insn::Prev { pc_if_prev, .. } => {
                    resolve(pc_if_prev, "Prev");
                }
                Insn::InitCoroutine {
                    yield_reg: _,
                    jump_on_definition,
                    start_offset,
                } => {
                    resolve(jump_on_definition, "InitCoroutine");
                    resolve(start_offset, "InitCoroutine");
                }
                Insn::NotExists {
                    cursor: _,
                    rowid_reg: _,
                    target_pc,
                } => {
                    resolve(target_pc, "NotExists");
                }
                Insn::Yield {
                    yield_reg: _,
                    end_offset,
                } => {
                    resolve(end_offset, "Yield");
                }
                Insn::SeekRowid { target_pc, .. } => {
                    resolve(target_pc, "SeekRowid");
                }
                Insn::Gosub { target_pc, .. } => {
                    resolve(target_pc, "Gosub");
                }
                Insn::Jump {
                    target_pc_eq,
                    target_pc_lt,
                    target_pc_gt,
                } => {
                    resolve(target_pc_eq, "Jump");
                    resolve(target_pc_lt, "Jump");
                    resolve(target_pc_gt, "Jump");
                }
                Insn::SeekGE { target_pc, .. } => {
                    resolve(target_pc, "SeekGE");
                }
                Insn::SeekGT { target_pc, .. } => {
                    resolve(target_pc, "SeekGT");
                }
                Insn::SeekLE { target_pc, .. } => {
                    resolve(target_pc, "SeekLE");
                }
                Insn::SeekLT { target_pc, .. } => {
                    resolve(target_pc, "SeekLT");
                }
                Insn::IdxGE { target_pc, .. } => {
                    resolve(target_pc, "IdxGE");
                }
                Insn::IdxLE { target_pc, .. } => {
                    resolve(target_pc, "IdxLE");
                }
                Insn::IdxGT { target_pc, .. } => {
                    resolve(target_pc, "IdxGT");
                }
                Insn::IdxLT { target_pc, .. } => {
                    resolve(target_pc, "IdxLT");
                }
                Insn::IsNull { reg: _, target_pc } => {
                    resolve(target_pc, "IsNull");
                }
                Insn::VNext { pc_if_next, .. } => {
                    resolve(pc_if_next, "VNext");
                }
                Insn::VFilter { pc_if_empty, .. } => {
                    resolve(pc_if_empty, "VFilter");
                }
                Insn::NoConflict { target_pc, .. } => {
                    resolve(target_pc, "NoConflict");
                }
                Insn::Found { target_pc, .. } => {
                    resolve(target_pc, "Found");
                }
                Insn::NotFound { target_pc, .. } => {
                    resolve(target_pc, "NotFound");
                }
                _ => {}
            }
        }
        self.label_to_resolved_offset.clear();
    }

    // translate [CursorKey] to cursor id
    pub fn resolve_cursor_id_safe(&self, key: &CursorKey) -> Option<CursorID> {
        self.cursor_ref
            .iter()
            .position(|(k, _)| k.as_ref().is_some_and(|k| k.equals(key)))
    }

    pub fn resolve_cursor_id(&self, key: &CursorKey) -> CursorID {
        self.resolve_cursor_id_safe(key)
            .unwrap_or_else(|| panic!("Cursor not found: {key:?}"))
    }

    pub fn set_collation(&mut self, c: Option<(CollationSeq, bool)>) {
        self.collation = c
    }

    pub fn curr_collation_ctx(&self) -> Option<(CollationSeq, bool)> {
        self.collation
    }

    pub fn curr_collation(&self) -> Option<CollationSeq> {
        self.collation.map(|c| c.0)
    }

    pub fn reset_collation(&mut self) {
        self.collation = None;
    }

    #[inline]
    pub fn incr_nesting(&mut self) {
        self.nested_level += 1;
    }

    #[inline]
    pub fn decr_nesting(&mut self) {
        self.nested_level -= 1;
    }

    /// Initialize the program with basic setup and return initial metadata and labels
    pub fn prologue(&mut self) {
        if self.nested_level == 0 {
            self.init_label = self.allocate_label();

            self.emit_insn(Insn::Init {
                target_pc: self.init_label,
            });

            self.start_offset = self.offset();
        }
    }

    /// Tries to mirror: https://github.com/sqlite/sqlite/blob/e77e589a35862f6ac9c4141cfd1beb2844b84c61/src/build.c#L5379
    /// TODO: as we currently do not support multiple dbs
    /// this function just sets a write operation/transaction for the current db
    pub fn begin_write_operation(&mut self) {
        self.txn_mode = TransactionMode::Write;
    }

    pub fn begin_read_operation(&mut self) {
        // Just override the transaction mode when it is None
        if matches!(self.txn_mode, TransactionMode::None) {
            self.txn_mode = TransactionMode::Read;
        }
    }

    pub fn begin_concurrent_operation(&mut self) {
        self.txn_mode = TransactionMode::Concurrent;
    }

    /// Indicates the rollback behvaiour for the halt instruction in epilogue
    pub fn rollback(&mut self) {
        self.rollback = true;
    }

    /// Clean up and finalize the program, resolving any remaining labels
    /// Note that although these are the final instructions, typically an SQLite
    /// query will jump to the Transaction instruction via init_label.
    pub fn epilogue(&mut self, schema: &Schema) {
        if self.nested_level == 0 {
            // "rollback" flag is used to determine if halt should rollback the transaction.
            self.emit_halt(self.rollback);
            self.preassign_label_to_next_insn(self.init_label);

            if !matches!(self.txn_mode, TransactionMode::None) {
                self.emit_insn(Insn::Transaction {
                    db: 0,
                    tx_mode: self.txn_mode,
                    schema_cookie: schema.schema_version,
                });
            }

            self.emit_constant_insns();
            self.emit_insn(Insn::Goto {
                target_pc: self.start_offset,
            });
        }
    }

    /// Checks whether `table` or any of its indices has been opened in the program
    pub fn is_table_open(&self, table: &Table) -> bool {
        self.table_references.contains_table(table)
    }

    #[inline]
    pub fn cursor_loop(&mut self, cursor_id: CursorID, f: impl Fn(&mut ProgramBuilder, usize)) {
        let loop_start = self.allocate_label();
        let loop_end = self.allocate_label();

        self.emit_insn(Insn::Rewind {
            cursor_id,
            pc_if_empty: loop_end,
        });
        self.preassign_label_to_next_insn(loop_start);

        let rowid = self.alloc_register();

        self.emit_insn(Insn::RowId {
            cursor_id,
            dest: rowid,
        });

        self.emit_insn(Insn::IsNull {
            reg: rowid,
            target_pc: loop_end,
        });

        f(self, rowid);

        self.emit_insn(Insn::Next {
            cursor_id,
            pc_if_next: loop_start,
        });
        self.preassign_label_to_next_insn(loop_end);
    }

    pub fn emit_column_or_rowid(&mut self, cursor_id: CursorID, column: usize, out: usize) {
        let (_, cursor_type) = self.cursor_ref.get(cursor_id).unwrap();
        if let CursorType::BTreeTable(btree) = cursor_type {
            let column_def = btree
                .columns
                .get(column)
                .expect("column index out of bounds");
            if column_def.is_rowid_alias {
                self.emit_insn(Insn::RowId {
                    cursor_id,
                    dest: out,
                });
            } else {
                self.emit_column(cursor_id, column, out);
            }
        } else {
            self.emit_column(cursor_id, column, out);
        }
    }

    fn emit_column(&mut self, cursor_id: CursorID, column: usize, out: usize) {
        let (_, cursor_type) = self.cursor_ref.get(cursor_id).unwrap();

        use crate::translate::expr::sanitize_string;

        let default = 'value: {
            let default = match cursor_type {
                CursorType::BTreeTable(btree) => &btree.columns[column].default,
                CursorType::BTreeIndex(index) => &index.columns[column].default,
                CursorType::MaterializedView(btree, _) => &btree.columns[column].default,
                _ => break 'value None,
            };

            let Some(ast::Expr::Literal(ref literal)) = default.as_ref().map(|v| v.as_ref()) else {
                break 'value None;
            };

            Some(match literal {
                ast::Literal::Numeric(s) => match Numeric::from(s) {
                    Numeric::Null => Value::Null,
                    Numeric::Integer(v) => Value::Integer(v),
                    Numeric::Float(v) => Value::Float(v.into()),
                },
                ast::Literal::Null => Value::Null,
                ast::Literal::String(s) => Value::Text(sanitize_string(s).into()),
                ast::Literal::Blob(s) => Value::Blob(
                    // Taken from `translate_expr`
                    s.as_bytes()
                        .chunks_exact(2)
                        .map(|pair| {
                            // We assume that sqlite3-parser has already validated that
                            // the input is valid hex string, thus unwrap is safe.
                            let hex_byte = std::str::from_utf8(pair).unwrap();
                            u8::from_str_radix(hex_byte, 16).unwrap()
                        })
                        .collect(),
                ),
                _ => break 'value None,
            })
        };

        self.emit_insn(Insn::Column {
            cursor_id,
            column,
            dest: out,
            default,
        });
    }

    pub fn build(mut self, connection: Arc<Connection>, change_cnt_on: bool, sql: &str) -> Program {
        self.resolve_labels();

        self.parameters.list.dedup();
        Program {
            max_registers: self.next_free_register,
            insns: self.insns,
            cursor_ref: self.cursor_ref,
            comments: self.comments,
            connection,
            parameters: self.parameters,
            n_change: AtomicI64::new(0),
            change_cnt_on,
            result_columns: self.result_columns,
            table_references: self.table_references,
            sql: sql.to_string(),
            accesses_db: !matches!(self.txn_mode, TransactionMode::None),
        }
    }
}
