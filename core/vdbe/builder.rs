use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tracing::{instrument, Level};
use turso_parser::ast::{self, ResolveType, SortOrder, TableInternalId};

use crate::{
    index_method::IndexMethodAttachment,
    numeric::Numeric,
    parameters::Parameters,
    schema::{BTreeTable, Index, PseudoCursorType, Schema, Table, Trigger},
    translate::{
        collate::CollationSeq,
        emitter::{MaterializedColumnRef, TransactionMode},
        plan::{ResultSetColumn, TableReferences},
    },
    CaptureDataChangesMode, Connection, Value, VirtualTable,
};

// Keep distinct hash-table ids far from table internal ids to avoid collisions.
const HASH_TABLE_ID_BASE: usize = 1 << 30;

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

    #[expect(clippy::should_implement_trait)]
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
    /// Whether this cursor is an special case build cursor.
    pub is_build: bool,
}

impl CursorKey {
    pub fn table(table_reference_id: TableInternalId) -> Self {
        Self {
            table_reference_id,
            index: None,
            is_build: false,
        }
    }

    pub fn index(table_reference_id: TableInternalId, index: Arc<Index>) -> Self {
        Self {
            table_reference_id,
            index: Some(index),
            is_build: false,
        }
    }

    /// Create a cursor key for hash join build operations.
    /// This creates a separate cursor from the regular table cursor.
    pub fn hash_build(table_reference_id: TableInternalId) -> Self {
        Self {
            table_reference_id,
            index: None,
            is_build: true,
        }
    }

    pub fn equals(&self, other: &CursorKey) -> bool {
        if self.table_reference_id != other.table_reference_id {
            return false;
        }
        if self.is_build != other.is_build {
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
    next_hash_table_id: usize,
    /// Instruction, the function to execute it with, and its original index in the vector.
    pub insns: Vec<(Insn, usize)>,
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
    pub(crate) reg_result_cols_start: Option<usize>,
    /// Whether the program needs to use statement subtransactions,
    /// i.e. the individual statement may need to be aborted due to a constraint conflict, etc.
    /// instead of the entire transaction.
    needs_stmt_subtransactions: bool,
    /// If this ProgramBuilder is building trigger subprogram, a ref to the trigger is stored here.
    pub trigger: Option<Arc<Trigger>>,
    /// Whether this is a subprogram (trigger or FK action). Subprograms skip Transaction instructions.
    pub is_subprogram: bool,
    pub resolve_type: ResolveType,
    /// When set, all triggers fired from this program should use this conflict resolution.
    /// This is used in UPSERT DO UPDATE context to ensure nested trigger's OR IGNORE/REPLACE
    /// clauses don't suppress errors.
    pub trigger_conflict_override: Option<ResolveType>,
    /// Temporary cursor overrides maps table internal IDs to cursor IDs that should be used instead of the normal resolution.
    /// This allows for things like hash build to use a separate cursor for iterating the same table.
    cursor_overrides: HashMap<usize, CursorID>,
    /// Hash join build signatures keyed by hash table id.
    hash_build_signatures: HashMap<usize, HashBuildSignature>,
    /// Hash tables to keep open across subplans (e.g. materialization).
    hash_tables_to_keep_open: HashSet<usize>,
    /// Maps table internal_id to result_columns_start_reg for FROM clause subqueries.
    /// Used when nested subqueries need to reference columns from outer query subqueries.
    subquery_result_regs: HashMap<TableInternalId, usize>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MaterializedBuildInputModeTag {
    RowidOnly,
    Payload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Signature of a hash build to allow reuse when inputs are unchanged.
/// TODO: this is very heavy... we might consider hashing instead of storing full data.
pub struct HashBuildSignature {
    /// WHERE term indices used as hash join keys.
    pub join_key_indices: Vec<usize>,
    /// Build-table columns stored as payload.
    pub payload_refs: Vec<MaterializedColumnRef>,
    /// Affinity string applied to join keys.
    pub key_affinities: String,
    /// Whether a bloom filter is enabled for this build.
    pub use_bloom_filter: bool,
    /// Rowid input cursor when the build side is materialized.
    pub materialized_input_cursor: Option<CursorID>,
    /// RowidOnly vs KeyPayload
    pub materialized_mode: Option<MaterializedBuildInputModeTag>,
}

#[derive(Debug, Clone)]
pub enum CursorType {
    BTreeTable(Arc<BTreeTable>),
    BTreeIndex(Arc<Index>),
    IndexMethod(Arc<dyn IndexMethodAttachment>),
    Pseudo(PseudoCursorType),
    Sorter,
    VirtualTable(Arc<VirtualTable>),
    MaterializedView(
        Arc<BTreeTable>,
        Arc<crate::sync::Mutex<crate::incremental::view::IncrementalView>>,
    ),
}

impl CursorType {
    pub fn is_index(&self) -> bool {
        matches!(self, CursorType::BTreeIndex(_))
    }

    pub fn get_explain_description(&self) -> String {
        let out = match self {
            CursorType::BTreeTable(btree_table) => {
                let mut col_count = btree_table.columns.len();
                if btree_table.get_rowid_alias_column().is_none() {
                    col_count += 1;
                }
                Some((
                    col_count,
                    btree_table
                        .columns
                        .iter()
                        .map(|col| {
                            if let Some(coll) = col.collation_opt() {
                                format!("{coll}")
                            } else {
                                "B".to_string()
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(","),
                ))
            }
            CursorType::BTreeIndex(index) => {
                let mut col_count = index.columns.len();
                if index.has_rowid {
                    col_count += 1;
                }
                Some((
                    col_count,
                    index
                        .columns
                        .iter()
                        .map(|col| {
                            let sign = match col.order {
                                SortOrder::Asc => "",
                                SortOrder::Desc => "-",
                            };
                            if let Some(coll) = col.collation {
                                format!("{sign}{coll}")
                            } else {
                                format!("{sign}B")
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(","),
                ))
            }
            _ => None,
        };

        out.map_or(String::new(), |(col_count, collations)| {
            format!("k({col_count},{collations})")
        })
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
        ProgramBuilder::_new(query_mode, capture_data_changes_mode, opts, None, false)
    }
    pub fn new_for_trigger(
        query_mode: QueryMode,
        capture_data_changes_mode: CaptureDataChangesMode,
        opts: ProgramBuilderOpts,
        trigger: Arc<Trigger>,
    ) -> Self {
        ProgramBuilder::_new(
            query_mode,
            capture_data_changes_mode,
            opts,
            Some(trigger),
            true,
        )
    }
    /// Create a ProgramBuilder for a subprogram (FK actions, etc.) that runs within
    /// an existing transaction and doesn't emit Transaction instructions.
    pub fn new_for_subprogram(
        query_mode: QueryMode,
        capture_data_changes_mode: CaptureDataChangesMode,
        opts: ProgramBuilderOpts,
    ) -> Self {
        ProgramBuilder::_new(query_mode, capture_data_changes_mode, opts, None, true)
    }
    fn _new(
        query_mode: QueryMode,
        capture_data_changes_mode: CaptureDataChangesMode,
        opts: ProgramBuilderOpts,
        trigger: Option<Arc<Trigger>>,
        is_subprogram: bool,
    ) -> Self {
        Self {
            table_reference_counter: TableRefIdCounter::new(),
            next_free_register: 1,
            next_free_cursor_id: 0,
            next_hash_table_id: HASH_TABLE_ID_BASE,
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
            reg_result_cols_start: None,
            needs_stmt_subtransactions: false,
            trigger,
            is_subprogram,
            resolve_type: ResolveType::Abort,
            trigger_conflict_override: None,
            cursor_overrides: HashMap::new(),
            hash_build_signatures: HashMap::new(),
            hash_tables_to_keep_open: HashSet::new(),
            subquery_result_regs: HashMap::new(),
        }
    }

    pub fn alloc_hash_table_id(&mut self) -> usize {
        let id = self.next_hash_table_id;
        self.next_hash_table_id = self
            .next_hash_table_id
            .checked_add(1)
            .expect("hash table id overflow");
        id
    }

    pub fn set_resolve_type(&mut self, resolve_type: ResolveType) {
        self.resolve_type = resolve_type;
    }

    /// Set the trigger conflict override. When set, all triggers fired from this program
    /// should use this conflict resolution instead of their own OR clauses.
    pub fn set_trigger_conflict_override(&mut self, resolve_type: ResolveType) {
        self.trigger_conflict_override = Some(resolve_type);
    }

    /// Returns true if the given hash table id should be kept open across subplans.
    pub fn should_keep_hash_table_open(&self, hash_table_id: usize) -> bool {
        self.hash_tables_to_keep_open.contains(&hash_table_id)
    }

    /// Set the set of hash tables to keep open across subplans.
    pub fn set_hash_tables_to_keep_open(&mut self, tables: &HashSet<usize>) {
        self.hash_tables_to_keep_open = tables.clone();
    }

    /// Reset the set of hash tables to keep open.
    pub fn clear_hash_tables_to_keep_open(&mut self) {
        self.hash_tables_to_keep_open.clear();
    }

    /// Returns true if the given hash build signature matches the recorded one for the given hash table id.
    pub fn hash_build_signature_matches(
        &self,
        hash_table_id: usize,
        signature: &HashBuildSignature,
    ) -> bool {
        self.hash_build_signatures
            .get(&hash_table_id)
            .is_some_and(|existing| existing == signature)
    }

    /// Returns true if there is a recorded hash build signature for the given hash table id.
    pub fn has_hash_build_signature(&self, hash_table_id: usize) -> bool {
        self.hash_build_signatures.contains_key(&hash_table_id)
    }

    /// Insert or update the hash build signature for the given hash table id.
    pub fn record_hash_build_signature(
        &mut self,
        hash_table_id: usize,
        signature: HashBuildSignature,
    ) {
        self.hash_build_signatures.insert(hash_table_id, signature);
    }

    /// Clear the hash build signature for the given hash table id.
    pub fn clear_hash_build_signature(&mut self, hash_table_id: usize) {
        self.hash_build_signatures.remove(&hash_table_id);
    }

    /// Store the result_columns_start_reg for a FROM clause subquery by its internal_id.
    /// Used so nested subqueries can access columns from outer query subqueries.
    pub fn set_subquery_result_reg(&mut self, internal_id: TableInternalId, result_reg: usize) {
        self.subquery_result_regs.insert(internal_id, result_reg);
    }

    /// Look up the result_columns_start_reg for a FROM clause subquery by its internal_id.
    /// Returns None if the subquery hasn't been emitted yet.
    pub fn get_subquery_result_reg(&self, internal_id: TableInternalId) -> Option<usize> {
        self.subquery_result_regs.get(&internal_id).copied()
    }

    pub fn set_needs_stmt_subtransactions(&mut self, needs_stmt_subtransactions: bool) {
        self.needs_stmt_subtransactions = needs_stmt_subtransactions;
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

    /// Returns the next register that will be allocated by alloc_register/alloc_registers.
    pub fn peek_next_register(&self) -> usize {
        self.next_free_register
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

    pub fn alloc_cursor_id_keyed_if_not_exists(
        &mut self,
        key: CursorKey,
        cursor_type: CursorType,
    ) -> usize {
        if let Some(cursor_id) = self.resolve_cursor_id_safe(&key) {
            cursor_id
        } else {
            self._alloc_cursor_id(Some(key), cursor_type)
        }
    }

    /// allocate proper cursor for the given index (either [CursorType::BTreeIndex] or [CursorType::IndexMethod])
    pub fn alloc_cursor_index(
        &mut self,
        key: Option<CursorKey>,
        index: &Arc<Index>,
    ) -> crate::Result<usize> {
        tracing::debug!("alloc cursor: {:?} {:?}", key, index.index_method.is_some());
        let module = index.index_method.as_ref();
        if let Some(m) = module {
            if !m.definition().backing_btree {
                return Ok(self._alloc_cursor_id(key, CursorType::IndexMethod(m.clone())));
            }
        }
        Ok(self._alloc_cursor_id(key, CursorType::BTreeIndex(index.clone())))
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

    /// Emit an instruction that is guaranteed not to be in any constant span.
    /// This ensures the instruction won't be hoisted when emit_constant_insns is called.
    #[instrument(skip(self), level = Level::DEBUG)]
    pub fn emit_no_constant_insn(&mut self, insn: Insn) {
        self.constant_span_end_all();
        self.emit_insn(insn);
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
        // Move compile-time constant instructions to the end of the program,
        // where they are executed once after Init jumps to it.

        // Stable partition: non-constant instructions first, then constant.
        // Since spans are sorted and non-overlapping, we track our position
        // in the span list and never look back - O(n + m) total, where
        // n = number of instructions, m = number of constant spans.
        let mut non_constant = Vec::with_capacity(self.insns.len());
        let mut constant = Vec::new();
        let mut span_idx = 0;

        for item in self.insns.drain(..) {
            let idx = item.1;

            // Advance past spans we've completely passed
            while span_idx < self.constant_spans.len() && self.constant_spans[span_idx].1 < idx {
                span_idx += 1;
            }

            // Check if current span contains this index
            let is_constant =
                span_idx < self.constant_spans.len() && self.constant_spans[span_idx].0 <= idx;

            if is_constant {
                constant.push(item);
            } else {
                non_constant.push(item);
            }
        }

        self.insns = non_constant;
        self.insns.extend(constant);

        // Build old index -> new position mapping
        let mut old_to_new = vec![0usize; self.insns.len()];
        for (new_pos, (_, old_idx)) in self.insns.iter().enumerate() {
            old_to_new[*old_idx] = new_pos;
        }

        for resolved_offset in self.label_to_resolved_offset.iter_mut() {
            if let Some((old_offset, target)) = resolved_offset {
                *resolved_offset = Some((old_to_new[*old_offset as usize] as u32, *target));
            }
        }

        for (offset, _) in self.comments.iter_mut() {
            *offset = old_to_new[*offset as usize] as u32;
        }

        if let QueryMode::ExplainQueryPlan = self.query_mode {
            self.current_parent_explain_idx =
                self.current_parent_explain_idx.map(|old| old_to_new[old]);

            for i in 0..self.insns.len() {
                let (Insn::Explain { p2, .. }, _) = &self.insns[i] else {
                    continue;
                };

                let new_p2 = p2.map(|old| old_to_new[old]);

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
    pub fn resolve_labels(&mut self) -> crate::Result<()> {
        let resolve = |pc: &mut BranchOffset, insn_name: &str| -> crate::Result<()> {
            if let BranchOffset::Label(label) = pc {
                let Some(Some((to_offset, target))) =
                    self.label_to_resolved_offset.get(*label as usize)
                else {
                    crate::bail_corrupt_error!(
                        "Reference to undefined or unresolved label in {insn_name}: {label}"
                    );
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
            Ok(())
        };
        for (insn, _) in self.insns.iter_mut() {
            match insn {
                Insn::Init { target_pc } => {
                    resolve(target_pc, "Init")?;
                }
                Insn::Eq {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Eq")?;
                }
                Insn::Ne {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Ne")?;
                }
                Insn::Lt {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Lt")?;
                }
                Insn::Le {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Le")?;
                }
                Insn::Gt {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Gt")?;
                }
                Insn::Ge {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Ge")?;
                }
                Insn::If {
                    reg: _reg,
                    target_pc,
                    jump_if_null: _,
                } => {
                    resolve(target_pc, "If")?;
                }
                Insn::IfNot {
                    reg: _reg,
                    target_pc,
                    jump_if_null: _,
                } => {
                    resolve(target_pc, "IfNot")?;
                }
                Insn::Rewind { pc_if_empty, .. } => {
                    resolve(pc_if_empty, "Rewind")?;
                }
                Insn::Last { pc_if_empty, .. } => {
                    resolve(pc_if_empty, "Last")?;
                }
                Insn::Goto { target_pc } => {
                    resolve(target_pc, "Goto")?;
                }
                Insn::DecrJumpZero {
                    reg: _reg,
                    target_pc,
                } => {
                    resolve(target_pc, "DecrJumpZero")?;
                }
                Insn::SorterNext {
                    cursor_id: _cursor_id,
                    pc_if_next,
                } => {
                    resolve(pc_if_next, "SorterNext")?;
                }
                Insn::SorterSort { pc_if_empty, .. } => {
                    resolve(pc_if_empty, "SorterSort")?;
                }
                Insn::SorterCompare {
                    pc_when_nonequal: target_pc,
                    ..
                } => {
                    resolve(target_pc, "SorterCompare")?;
                }
                Insn::NotNull {
                    reg: _reg,
                    target_pc,
                } => {
                    resolve(target_pc, "NotNull")?;
                }
                Insn::IfPos { target_pc, .. } => {
                    resolve(target_pc, "IfPos")?;
                }
                Insn::Next { pc_if_next, .. } => {
                    resolve(pc_if_next, "Next")?;
                }
                Insn::Once {
                    target_pc_when_reentered,
                    ..
                } => {
                    resolve(target_pc_when_reentered, "Once")?;
                }
                Insn::Prev { pc_if_prev, .. } => {
                    resolve(pc_if_prev, "Prev")?;
                }
                Insn::InitCoroutine {
                    yield_reg: _,
                    jump_on_definition,
                    start_offset,
                } => {
                    resolve(jump_on_definition, "InitCoroutine")?;
                    resolve(start_offset, "InitCoroutine")?;
                }
                Insn::NotExists {
                    cursor: _,
                    rowid_reg: _,
                    target_pc,
                } => {
                    resolve(target_pc, "NotExists")?;
                }
                Insn::Yield {
                    yield_reg: _,
                    end_offset,
                } => {
                    resolve(end_offset, "Yield")?;
                }
                Insn::SeekRowid { target_pc, .. } => {
                    resolve(target_pc, "SeekRowid")?;
                }
                Insn::Gosub { target_pc, .. } => {
                    resolve(target_pc, "Gosub")?;
                }
                Insn::Jump {
                    target_pc_eq,
                    target_pc_lt,
                    target_pc_gt,
                } => {
                    resolve(target_pc_eq, "Jump")?;
                    resolve(target_pc_lt, "Jump")?;
                    resolve(target_pc_gt, "Jump")?;
                }
                Insn::SeekGE { target_pc, .. } => resolve(target_pc, "SeekGE")?,
                Insn::SeekGT { target_pc, .. } => resolve(target_pc, "SeekGT")?,
                Insn::SeekLE { target_pc, .. } => resolve(target_pc, "SeekLE")?,
                Insn::SeekLT { target_pc, .. } => resolve(target_pc, "SeekLT")?,
                Insn::IdxGE { target_pc, .. } => resolve(target_pc, "IdxGE")?,
                Insn::IdxLE { target_pc, .. } => resolve(target_pc, "IdxLE")?,
                Insn::IdxGT { target_pc, .. } => resolve(target_pc, "IdxGT")?,
                Insn::IdxLT { target_pc, .. } => resolve(target_pc, "IdxLT")?,
                Insn::IndexMethodQuery { pc_if_empty, .. } => {
                    resolve(pc_if_empty, "IndexMethodQuery")?;
                }
                Insn::IsNull { reg: _, target_pc } => resolve(target_pc, "IsNull")?,
                Insn::VNext { pc_if_next, .. } => resolve(pc_if_next, "VNext")?,
                Insn::VFilter { pc_if_empty, .. } => resolve(pc_if_empty, "VFilter")?,
                Insn::RowSetRead { pc_if_empty, .. } => resolve(pc_if_empty, "RowSetRead")?,
                Insn::NoConflict { target_pc, .. } => resolve(target_pc, "NoConflict")?,
                Insn::Found { target_pc, .. } => resolve(target_pc, "Found")?,
                Insn::NotFound { target_pc, .. } => resolve(target_pc, "NotFound")?,
                Insn::FkIfZero { target_pc, .. } => resolve(target_pc, "FkIfZero")?,
                Insn::Filter { target_pc, .. } => resolve(target_pc, "Filter")?,
                Insn::HashProbe { target_pc, .. } => resolve(target_pc, "HashProbe")?,
                Insn::HashNext { target_pc, .. } => resolve(target_pc, "HashNext")?,
                Insn::HashDistinct { data } => resolve(&mut data.target_pc, "HashDistinct")?,
                _ => {}
            }
        }
        self.label_to_resolved_offset.clear();
        Ok(())
    }

    /// Set a cursor override for a table. When resolving a table cursor for this table,
    /// the override cursor will be used instead of the normal resolution.
    pub fn set_cursor_override(&mut self, table_ref_id: TableInternalId, cursor_id: CursorID) {
        self.cursor_overrides.insert(table_ref_id.into(), cursor_id);
    }

    /// Clear the cursor override for a table.
    pub fn clear_cursor_override(&mut self, table_ref_id: TableInternalId) {
        self.cursor_overrides.remove(&table_ref_id.into());
    }

    /// Clear all cursor overrides.
    pub fn clear_all_cursor_overrides(&mut self) {
        self.cursor_overrides.clear();
    }

    /// Check if a cursor override is active for a given table.
    pub fn has_cursor_override(&self, table_ref_id: TableInternalId) -> bool {
        self.cursor_overrides.contains_key(&table_ref_id.into())
    }

    // translate [CursorKey] to cursor id
    pub fn resolve_cursor_id_safe(&self, key: &CursorKey) -> Option<CursorID> {
        // Check cursor overrides first, only apply override for table cursors.
        // Index cursor lookups are not overridden because when a cursor override is active,
        // the calling code (translate_expr) should skip index logic entirely.
        if key.index.is_none() && !key.is_build {
            let table_id: usize = key.table_reference_id.into();
            if let Some(&cursor_id) = self.cursor_overrides.get(&table_id) {
                return Some(cursor_id);
            }
        }
        self.cursor_ref
            .iter()
            .position(|(k, _)| k.as_ref().is_some_and(|k| k.equals(key)))
    }

    pub fn resolve_cursor_id(&self, key: &CursorKey) -> CursorID {
        self.resolve_cursor_id_safe(key)
            .unwrap_or_else(|| panic!("Cursor not found: {key:?}"))
    }

    /// Resolve the first allocated index cursor for a given table reference.
    /// This method exists due to a limitation of our translation system where
    /// a subquery that references an outer query table cannot know whether a
    /// table cursor, index cursor, or both were opened for that table reference.
    /// Hence: currently we first try to resolve a table cursor, and if that fails,
    /// we resolve an index cursor via this method.
    pub fn resolve_any_index_cursor_id_for_table(&self, table_ref_id: TableInternalId) -> CursorID {
        self.cursor_ref
            .iter()
            .position(|(k, _)| {
                k.as_ref()
                    .is_some_and(|k| k.table_reference_id == table_ref_id && k.index.is_some())
            })
            .unwrap_or_else(|| panic!("No index cursor found for table {table_ref_id}"))
    }

    /// Resolve the [Index] that a given cursor is associated with.
    pub fn resolve_index_for_cursor_id(&self, cursor_id: CursorID) -> Arc<Index> {
        let cursor_ref = &self
            .cursor_ref
            .get(cursor_id)
            .unwrap_or_else(|| panic!("Cursor not found: {cursor_id}"))
            .1;
        let CursorType::BTreeIndex(index) = cursor_ref else {
            panic!("Cursor is not an index: {cursor_id}");
        };
        index.clone()
    }

    /// Get the [CursorType] of a given cursor.
    pub fn get_cursor_type(&self, cursor_id: CursorID) -> Option<&CursorType> {
        self.cursor_ref
            .get(cursor_id)
            .map(|(_, cursor_type)| cursor_type)
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
        if self.is_subprogram {
            // Subprograms (triggers, FK actions) don't need Transaction - they run within parent's tx
            self.init_label = self.allocate_label();
            self.emit_insn(Insn::Init {
                target_pc: self.init_label,
            });
            self.preassign_label_to_next_insn(self.init_label);
            self.start_offset = self.offset();
            return;
        }
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
        if self.is_subprogram {
            // Subprograms (triggers, FK actions) just emit Halt without Transaction
            let description = if self.trigger.is_some() {
                "trigger"
            } else {
                "fk action"
            };
            self.emit_insn(Insn::Halt {
                err_code: 0,
                description: description.to_string(),
            });
            return;
        }
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

            if !self.constant_spans.is_empty() {
                self.emit_constant_insns();
            }
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
        let (_, cursor_type) = self.cursor_ref.get(cursor_id).expect("cursor_id is valid");
        if let CursorType::BTreeTable(btree) = cursor_type {
            let column_def = btree
                .columns
                .get(column)
                .expect("column index out of bounds");
            if column_def.is_rowid_alias() {
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
        let (_, cursor_type) = self.cursor_ref.get(cursor_id).expect("cursor_id is valid");

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
                            // the input is valid hex string, thus expect is safe.
                            let hex_byte =
                                std::str::from_utf8(pair).expect("parser validated hex string");
                            u8::from_str_radix(hex_byte, 16).expect("parser validated hex digit")
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

    pub fn build(
        mut self,
        connection: Arc<Connection>,
        change_cnt_on: bool,
        sql: &str,
    ) -> crate::Result<Program> {
        self.resolve_labels()?;

        self.parameters.list.dedup();

        // before, we required stmt subtransaction only if !self.table_references.is_empty()
        // this is wrong in case of CREATE UNIQUE INDEX statements - as they can fail with CONSTRAINT error and we need to properly behave in this case
        if matches!(self.txn_mode, TransactionMode::Write) {
            self.needs_stmt_subtransactions = true;
        }

        let contains_trigger_subprograms = self
            .insns
            .iter()
            .any(|(insn, _)| matches!(insn, Insn::Program { .. }));

        // Validate bytecode in debug builds
        super::validator::debug_assert_valid_program(&self.insns, sql);

        Ok(Program {
            max_registers: self.next_free_register,
            insns: self.insns,
            cursor_ref: self.cursor_ref,
            comments: self.comments,
            connection,
            parameters: self.parameters,
            change_cnt_on,
            result_columns: self.result_columns,
            table_references: self.table_references,
            sql: sql.to_string(),
            accesses_db: !matches!(self.txn_mode, TransactionMode::None),
            needs_stmt_subtransactions: self.needs_stmt_subtransactions,
            trigger: self.trigger.take(),
            is_subprogram: self.is_subprogram,
            contains_trigger_subprograms,
            resolve_type: self.resolve_type,
        })
    }
}
