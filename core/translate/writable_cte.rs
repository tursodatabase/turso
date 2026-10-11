//! INSERT, UPDATE and DELETE inside WITH ("writable CTEs"). PostgreSQL
//! semantics.
//!
//! ```sql
//! WITH moved AS (DELETE FROM queue WHERE done RETURNING *)
//! INSERT INTO archive SELECT * FROM moved;
//! ```
//!
//! PostgreSQL rules. "Part" = one CTE or the main statement.
//!
//! 1. Reads see the database as of statement start.
//! 2. Parts share rows only through RETURNING.
//! 3. Constraints see the current rows, including other parts' writes.
//! 4. A row is changed at most once. A second UPDATE or DELETE skips it; a
//!    second ON CONFLICT DO UPDATE fails.
//! 5. Every part runs to the end, even if nothing reads it.
//!
//! How, without per-statement snapshots:
//!
//! - Parts run one after another, each to the end. Order: CTEs the main
//!   statement reads, then the main statement, then the other CTEs, last one
//!   first. A CTE runs before the parts that read it.
//! - PostgreSQL instead runs a CTE that another part reads row by row, next to
//!   its reader. Its order depends on the plan; ours is one it can use. Differs
//!   only when both write the same table. Example:
//!   `WITH d AS (DELETE FROM t WHERE id = 1 RETURNING id)
//!   UPDATE t SET v = 0 WHERE id = 1 OR id IN (SELECT id FROM d)`.
//!   PostgreSQL 17 updates row 1; here the DELETE wins.
//! - Each CTE stores its rows in an ephemeral table. Later parts read that.
//! - Reads of a table that this part or an earlier one writes go to a copy,
//!   taken at statement start. Same rowids, same record bytes. Exception: the
//!   scan that finds the rows an UPDATE or DELETE changes.
//! - Rule 4: the statement tracks the rowids it writes, for tables that need
//!   it ([tracked_tables]). A row an earlier part deleted is simply gone.
//! - The main statement's rows are returned after every part has finished.
//!
//! Foreign keys are checked after all parts, like PostgreSQL. RESTRICT fails
//! at once. Actions (CASCADE, SET NULL, SET DEFAULT) run inside the part, not
//! at the end, so actions that change a table another part writes are
//! rejected ([check_tables_written_by_fk_actions]).

use crate::schema::{BTreeTable, Column};
use crate::sync::Arc;
use crate::translate::delete::translate_delete;
use crate::translate::emitter::{emit_cdc_autocommit_commit, prepare_cdc_if_necessary, Resolver};
use crate::translate::expr::{
    emit_returning_scan_back, walk_expr, ReturningBufferCtx, WalkControl,
};
use crate::translate::insert::translate_insert;
use crate::translate::plan::{
    returning_output_columns, subquery_output_columns, EphemeralRowidMode, JoinedTable,
    QueryDestination, ResultSetColumn, TableReferences,
};
use crate::translate::select::{emit_select_plan, prepare_select_plan};
use crate::translate::stmt_journal::any_effective_replace;
use crate::translate::trigger_exec::has_triggers_including_temp;
use crate::translate::update::translate_update;
use crate::util::normalize_ident;
use crate::vdbe::builder::{CursorKey, CursorType, MaterializedCteInfo, ProgramBuilder};
use crate::vdbe::insn::{InsertFlags, Insn, RegisterOrLiteral};
use crate::vdbe::{BranchOffset, CursorID};
use crate::{bail_parse_error, turso_assert, Connection, HashSet, LimboError, Result};
use turso_parser::ast::{self, RefAct, ResolveType, TriggerEvent, UpsertDo};

/// Root page of a table that a part reads as of the start of the statement.
/// The program opens such a table as a copy of the real table instead; see
/// [WritableCteState::open_table_copies_instead_of_tables].
pub(crate) const STATEMENT_START_COPY_ROOT_PAGE: i64 = i64::MIN;

/// Whether `stmt` has INSERT, UPDATE or DELETE inside its own WITH clause.
pub(crate) fn has_writable_cte(stmt: &ast::Stmt) -> bool {
    statement_with(stmt).is_some_and(|with| {
        with.ctes
            .iter()
            .any(|cte| matches!(cte.body, ast::CteBody::DataModifying(_)))
    })
}

fn statement_with(stmt: &ast::Stmt) -> Option<&ast::With> {
    match stmt {
        ast::Stmt::Select(select) => select.with.as_ref(),
        ast::Stmt::Insert { with, .. } | ast::Stmt::Delete { with, .. } => with.as_ref(),
        ast::Stmt::Update(update) => update.with.as_ref(),
        _ => None,
    }
}

/// Program builder state while a statement with INSERT, UPDATE or DELETE inside
/// WITH is translated.
pub(crate) struct WritableCteState {
    /// The CTEs of the statement's WITH clause whose rows are stored.
    stored_ctes: Vec<StoredCte>,
    /// Names of the statement's CTEs that the part being planned may read.
    visible_cte_names: Vec<String>,
    /// Tables, as (database id, normalized name), that the part being planned
    /// or a part that runs before it writes. Reads of them use a copy of the
    /// table.
    tables_read_from_copies: Vec<(usize, String)>,
    /// Copies of tables, taken before any part runs.
    table_copies: Vec<TableCopy>,
    /// Tables, as (database id, root page), whose written rowids the statement tracks.
    tracked_tables: Vec<(usize, i64)>,
    /// Set while the INSERT, UPDATE or DELETE of a part is emitted.
    dml_part: Option<DmlPartOutput>,
    /// A part wrote a CDC change; one CDC COMMIT record follows the last part.
    cdc_commit_needed: bool,
}

struct StoredCte {
    name: String,
    columns: Vec<Column>,
    rows: StoredCteRows,
}

enum StoredCteRows {
    /// The rows are in the materialized CTE registered under this id.
    Table { cte_id: usize },
    /// An INSERT, UPDATE or DELETE without RETURNING. Reading it is an error.
    NoReturning,
}

struct TableCopy {
    database_id: usize,
    table_name: String,
    table: Arc<BTreeTable>,
    /// Same as `table`, but with [STATEMENT_START_COPY_ROOT_PAGE] as the root page.
    copy_table: Arc<BTreeTable>,
    copy_cursor_id: CursorID,
}

#[derive(Default)]
struct DmlPartOutput {
    returning: Option<ReturningRows>,
}

struct ReturningRows {
    buffer: ReturningBufferCtx,
    result_columns: Vec<ResultSetColumn>,
    columns: Vec<Column>,
}

/// Translates a statement that [has_writable_cte].
pub(crate) fn translate_writable_cte_statement(
    mut stmt: ast::Stmt,
    resolver: &mut Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    if program.writable_cte.is_some() || program.flags.is_subprogram() {
        bail_parse_error!(
            "WITH clause containing a data-modifying statement must be at the top level"
        );
    }
    let with = take_statement_with(&mut stmt).expect("has_writable_cte found a WITH clause");
    let mut parts = parts_of_statement(with.ctes, stmt, resolver, connection)?;
    find_part_references(&mut parts, with.recursive)?;
    let order = run_order(&parts)?;
    check_tables_written_by_fk_actions(&parts)?;
    check_replace_on_shared_tables(&parts)?;

    program.writable_cte = Some(Box::new(WritableCteState {
        stored_ctes: Vec::new(),
        visible_cte_names: Vec::new(),
        tables_read_from_copies: Vec::new(),
        table_copies: Vec::new(),
        tracked_tables: tracked_tables(&parts),
        dml_part: None,
        cdc_commit_needed: false,
    }));

    // Table copies are emitted last, when the parts have asked for them, but they
    // run first: the program jumps to them and back.
    let copy_tables_label = program.allocate_label();
    let parts_label = program.allocate_label();
    program.emit_insn(Insn::Goto {
        target_pc: copy_tables_label,
    });
    program.preassign_label_to_next_insn(parts_label);

    let main_index = parts.len() - 1;
    let mut main_output = None;
    let mut tables_written_so_far = Vec::new();
    for &part_index in &order {
        // A part reads the tables that it or an earlier part writes from
        // copies. The rows that an UPDATE or DELETE changes are found by a scan
        // of the table itself, which does not go through the planner's FROM
        // clause handling.
        for table in parts[part_index].tables_written() {
            if !tables_written_so_far.contains(&table) {
                tables_written_so_far.push(table);
            }
        }
        let visible_cte_names = parts[part_index]
            .visible_parts
            .iter()
            .map(|&index| parts[index].name.clone().expect("only CTEs are visible"))
            .collect();
        let state = writable_cte_state_mut(program);
        state.visible_cte_names = visible_cte_names;
        state
            .tables_read_from_copies
            .clone_from(&tables_written_so_far);

        let part = &mut parts[part_index];
        let body = part.body.take().expect("each part is emitted once");
        if part_index == main_index {
            main_output = emit_main_part(body, resolver, program, connection)?;
        } else {
            let stored = emit_cte_part(part, body, with.recursive, resolver, program, connection)?;
            writable_cte_state_mut(program).stored_ctes.push(stored);
        }
    }

    let finish_label = program.allocate_label();
    program.emit_insn(Insn::Goto {
        target_pc: finish_label,
    });
    program.preassign_label_to_next_insn(copy_tables_label);
    emit_table_copies(program, resolver)?;
    program.emit_insn(Insn::Goto {
        target_pc: parts_label,
    });
    program.preassign_label_to_next_insn(finish_label);

    // Like PostgreSQL, foreign keys are checked after every part ran: the
    // parts count violations and repay the ones a later part repairs.
    program.emit_insn(Insn::FkCheck { deferred: false });
    let state = writable_cte_state_mut(program);
    state.visible_cte_names.clear();
    state.tables_read_from_copies.clear();
    if state.cdc_commit_needed {
        if let Some((cdc_cursor_id, _)) =
            prepare_cdc_if_necessary(program, resolver.schema(), None)?
        {
            emit_cdc_autocommit_commit(program, resolver, cdc_cursor_id)?;
        }
    }
    match main_output {
        Some(output) => {
            emit_returning_scan_back(program, &output.rows);
            program.result_columns = output.result_columns;
        }
        None => program.result_columns.clear(),
    }

    // The parts write separately, so a failure in a later part must roll back
    // the writes of the earlier ones.
    program.set_multi_write(true);
    program.set_may_abort(true);
    Ok(())
}

fn take_statement_with(stmt: &mut ast::Stmt) -> Option<ast::With> {
    match stmt {
        ast::Stmt::Select(select) => select.with.take(),
        ast::Stmt::Insert { with, .. } | ast::Stmt::Delete { with, .. } => with.take(),
        ast::Stmt::Update(update) => update.with.take(),
        _ => None,
    }
}

fn writable_cte_state_mut(program: &mut ProgramBuilder) -> &mut WritableCteState {
    program
        .writable_cte
        .as_deref_mut()
        .expect("the statement has INSERT, UPDATE or DELETE inside WITH")
}

/// One CTE of the statement's WITH clause, or the main statement (always last).
struct Part {
    /// `None` for the main statement.
    name: Option<String>,
    explicit_columns: Vec<String>,
    /// Taken when the part is emitted.
    body: Option<PartBody>,
    /// The table an INSERT, UPDATE or DELETE writes.
    target: Option<DmlTarget>,
    /// Parts whose names this part can read.
    visible_parts: Vec<usize>,
    /// Visible parts that this part reads, in declaration order.
    reads: Vec<usize>,
}

enum PartBody {
    Select(ast::Select),
    Dml(ast::Stmt),
}

struct DmlTarget {
    database_id: usize,
    table: Arc<BTreeTable>,
    kind: DmlKind,
    /// A conflict can be resolved with REPLACE, which deletes existing rows.
    may_replace: bool,
    /// Tables, as (database id, normalized name), that foreign key actions of
    /// this part can change.
    tables_changed_by_fk_actions: Vec<(usize, String)>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DmlKind {
    Insert { do_update: bool },
    Update,
    Delete,
}

impl DmlTarget {
    /// (database id, root page)
    fn table_key(&self) -> (usize, i64) {
        (self.database_id, self.table.root_page)
    }
}

impl Part {
    fn display_name(&self) -> &str {
        self.name.as_deref().unwrap_or("the main statement")
    }

    fn tables_written(&self) -> Vec<(usize, String)> {
        let Some(target) = &self.target else {
            return Vec::new();
        };
        let mut tables = vec![(target.database_id, normalize_ident(&target.table.name))];
        for table in &target.tables_changed_by_fk_actions {
            if !tables.contains(table) {
                tables.push(table.clone());
            }
        }
        tables
    }
}

fn parts_of_statement(
    ctes: Vec<ast::CommonTableExpr>,
    main: ast::Stmt,
    resolver: &Resolver,
    connection: &Arc<Connection>,
) -> Result<Vec<Part>> {
    let mut parts = Vec::with_capacity(ctes.len() + 1);
    for cte in ctes {
        let name = normalize_ident(cte.tbl_name.as_str());
        if parts
            .iter()
            .any(|part: &Part| part.name.as_deref() == Some(name.as_str()))
        {
            bail_parse_error!("duplicate WITH table name: {}", cte.tbl_name.as_str());
        }
        let explicit_columns = cte
            .columns
            .iter()
            .map(|column| normalize_ident(column.col_name.as_str()))
            .collect();
        let (body, target) = match cte.body {
            ast::CteBody::Select(select) => (PartBody::Select(select), None),
            ast::CteBody::DataModifying(stmt) => {
                let target = dml_target(&stmt, resolver, connection)?;
                (PartBody::Dml(*stmt), Some(target))
            }
        };
        parts.push(Part {
            name: Some(name),
            explicit_columns,
            body: Some(body),
            target,
            visible_parts: Vec::new(),
            reads: Vec::new(),
        });
    }
    let (body, target) = match main {
        ast::Stmt::Select(select) => (PartBody::Select(select), None),
        stmt => {
            let target = dml_target(&stmt, resolver, connection)?;
            (PartBody::Dml(stmt), Some(target))
        }
    };
    parts.push(Part {
        name: None,
        explicit_columns: Vec::new(),
        body: Some(body),
        target,
        visible_parts: Vec::new(),
        reads: Vec::new(),
    });
    Ok(parts)
}

fn dml_target(
    stmt: &ast::Stmt,
    resolver: &Resolver,
    connection: &Arc<Connection>,
) -> Result<DmlTarget> {
    let (tbl_name, kind, or_conflict) = match stmt {
        ast::Stmt::Insert {
            tbl_name,
            body,
            or_conflict,
            ..
        } => {
            let do_update = match body {
                ast::InsertBody::Select(_, Some(upsert)) => upsert_has_do_update(upsert),
                _ => false,
            };
            (tbl_name, DmlKind::Insert { do_update }, *or_conflict)
        }
        ast::Stmt::Update(update) => (&update.tbl_name, DmlKind::Update, update.or_conflict),
        ast::Stmt::Delete { tbl_name, .. } => (tbl_name, DmlKind::Delete, None),
        _ => {
            return Err(LimboError::InternalError(
                "a writable CTE part is not INSERT, UPDATE or DELETE".to_string(),
            ))
        }
    };
    let database_id = resolver.resolve_existing_table_database_id_qualified(tbl_name)?;
    let table_name = normalize_ident(tbl_name.name.as_str());
    let table = resolver.with_schema(database_id, |schema| schema.get_table(&table_name));
    let Some(table) = table.and_then(|table| table.btree()) else {
        bail_parse_error!(
            "INSERT, UPDATE and DELETE inside WITH can only change ordinary tables, and \"{}\" is not one",
            tbl_name.name.as_str()
        );
    };
    check_target_table(resolver, database_id, &table)?;

    let may_replace = !matches!(kind, DmlKind::Delete)
        && resolver.with_schema(database_id, |schema| {
            any_effective_replace(
                or_conflict.is_some(),
                or_conflict.unwrap_or(ResolveType::Abort),
                table.rowid_alias_conflict_clause,
                schema
                    .get_indices(&table.name)
                    .map(|index| index.on_conflict),
            )
        });
    let (fk_deletes, fk_updates) = match kind {
        DmlKind::Insert { do_update } => (may_replace, may_replace || do_update),
        DmlKind::Update => (may_replace, true),
        DmlKind::Delete => (true, false),
    };
    let tables_changed_by_fk_actions = if connection.foreign_keys_enabled() {
        tables_changed_by_fk_actions(resolver, database_id, &table_name, fk_deletes, fk_updates)?
    } else {
        Vec::new()
    };
    for (database_id, table_name) in &tables_changed_by_fk_actions {
        let table = resolver
            .with_schema(*database_id, |schema| schema.get_btree_table(table_name))
            .ok_or_else(|| {
                LimboError::InternalError(format!("no such foreign key child table: {table_name}"))
            })?;
        check_target_table(resolver, *database_id, &table)?;
    }
    Ok(DmlTarget {
        database_id,
        table,
        kind,
        may_replace,
        tables_changed_by_fk_actions,
    })
}

fn upsert_has_do_update(upsert: &ast::Upsert) -> bool {
    matches!(upsert.do_clause, UpsertDo::Set { .. })
        || upsert.next.as_deref().is_some_and(upsert_has_do_update)
}

/// Rejects tables whose writes this module cannot order like PostgreSQL.
fn check_target_table(resolver: &Resolver, database_id: usize, table: &BTreeTable) -> Result<()> {
    if !table.has_rowid {
        bail_parse_error!(
            "INSERT, UPDATE and DELETE inside WITH do not support WITHOUT ROWID table \"{}\"",
            table.name
        );
    }
    // A trigger body can write any table at any time, and its reads see the
    // writes of earlier parts.
    let has_triggers = [
        TriggerEvent::Insert,
        TriggerEvent::Update,
        TriggerEvent::Delete,
    ]
    .into_iter()
    .any(|event| has_triggers_including_temp(resolver, database_id, event, None, table));
    if has_triggers {
        bail_parse_error!(
            "INSERT, UPDATE and DELETE inside WITH do not support table \"{}\", which has triggers",
            table.name
        );
    }
    let has_materialized_views = resolver.with_schema(database_id, |schema| {
        !schema
            .get_dependent_materialized_views(&table.name)
            .is_empty()
    });
    if has_materialized_views {
        bail_parse_error!(
            "INSERT, UPDATE and DELETE inside WITH do not support table \"{}\", which has materialized views",
            table.name
        );
    }
    Ok(())
}

/// Tables that ON DELETE and ON UPDATE actions can change when rows of
/// `table_name` are deleted (`deletes`) or updated (`updates`), following
/// actions on the changed tables in turn.
fn tables_changed_by_fk_actions(
    resolver: &Resolver,
    database_id: usize,
    table_name: &str,
    deletes: bool,
    updates: bool,
) -> Result<Vec<(usize, String)>> {
    let writes = |action: RefAct| {
        matches!(
            action,
            RefAct::Cascade | RefAct::SetNull | RefAct::SetDefault
        )
    };
    let mut changed = Vec::new();
    let mut visited: HashSet<(String, bool, bool)> = HashSet::default();
    let mut queue = vec![(table_name.to_string(), deletes, updates)];
    while let Some((parent, deletes, updates)) = queue.pop() {
        if !visited.insert((parent.clone(), deletes, updates)) {
            continue;
        }
        let references = resolver.with_schema(database_id, |schema| {
            schema.resolved_fks_referencing(&parent)
        })?;
        for reference in references {
            let delete_action = deletes && writes(reference.fk.on_delete);
            let update_action = updates && writes(reference.fk.on_update);
            if !delete_action && !update_action {
                continue;
            }
            let child = normalize_ident(&reference.child_table.name);
            let child_deletes = delete_action && reference.fk.on_delete == RefAct::Cascade;
            let child_updates =
                update_action || (delete_action && reference.fk.on_delete != RefAct::Cascade);
            if !changed.contains(&(database_id, child.clone())) {
                changed.push((database_id, child.clone()));
            }
            queue.push((child, child_deletes, child_updates));
        }
    }
    Ok(changed)
}

/// Fills in which CTEs each part can read and which it does read.
///
/// Like PostgreSQL, a CTE of a WITH without RECURSIVE sees only the CTEs
/// declared before it, and a CTE of WITH RECURSIVE sees every other CTE. The
/// main statement sees all CTEs.
fn find_part_references(parts: &mut [Part], recursive: bool) -> Result<()> {
    let main_index = parts.len() - 1;
    let names: Vec<Option<String>> = parts.iter().map(|part| part.name.clone()).collect();
    for (part_index, part) in parts.iter_mut().enumerate() {
        part.visible_parts = (0..main_index)
            .filter(|&other| {
                if part_index == main_index {
                    true
                } else if recursive {
                    other != part_index
                } else {
                    other < part_index
                }
            })
            .collect();
        let read_names =
            ReadNames::of_part_body(part.body.as_ref().expect("parts are not emitted yet"))?;
        if recursive && part.target.is_some() {
            if let Some(name) = &part.name {
                if read_names.contains(name) {
                    bail_parse_error!(
                        "recursive query \"{}\" must not contain data-modifying statements",
                        name
                    );
                }
            }
        }
        part.reads = part
            .visible_parts
            .iter()
            .copied()
            .filter(|&other| {
                names[other]
                    .as_ref()
                    .is_some_and(|name| read_names.contains(name))
            })
            .collect();
    }
    Ok(())
}

/// The order in which the parts run; see the module documentation.
fn run_order(parts: &[Part]) -> Result<Vec<usize>> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Visit {
        NotVisited,
        InProgress,
        Done,
    }
    fn visit(
        part_index: usize,
        parts: &[Part],
        visits: &mut [Visit],
        order: &mut Vec<usize>,
    ) -> Result<()> {
        match visits[part_index] {
            Visit::Done => return Ok(()),
            Visit::InProgress => {
                bail_parse_error!(
                    "mutual recursion between WITH items is not implemented: {}",
                    parts[part_index].display_name()
                );
            }
            Visit::NotVisited => {}
        }
        visits[part_index] = Visit::InProgress;
        for &read in &parts[part_index].reads {
            visit(read, parts, visits, order)?;
        }
        visits[part_index] = Visit::Done;
        order.push(part_index);
        Ok(())
    }

    let main_index = parts.len() - 1;
    let mut visits = vec![Visit::NotVisited; parts.len()];
    let mut order = Vec::with_capacity(parts.len());
    visit(main_index, parts, &mut visits, &mut order)?;
    for part_index in (0..main_index).rev() {
        if parts[part_index].target.is_some() {
            visit(part_index, parts, &mut visits, &mut order)?;
        }
    }
    Ok(order)
}

/// Rejects a statement where a foreign key action of one part can change a
/// table that another part writes.
///
/// PostgreSQL queues foreign key checks and actions and runs them in order
/// after all parts. The engine runs actions inside the part that causes them,
/// and their effect on rows that other parts write differs from PostgreSQL in
/// both orders. For example, with `child.pid REFERENCES parent ON DELETE CASCADE`:
///
/// - `WITH d AS (DELETE FROM parent WHERE id = 7) INSERT INTO child VALUES (1, 7)`:
///   PostgreSQL inserts the child first, checks it at the end while parent 7
///   is already gone, and fails. The engine would delete the new child.
/// - The same rows in the other order: the engine fails the check of the new
///   child, while PostgreSQL's cascade deletes it at the end.
fn check_tables_written_by_fk_actions(parts: &[Part]) -> Result<()> {
    for (acting, acting_part) in parts.iter().enumerate() {
        let Some(acting_target) = &acting_part.target else {
            continue;
        };
        for writing in 0..parts.len() {
            if writing == acting {
                continue;
            }
            let Some(writing_target) = &parts[writing].target else {
                continue;
            };
            let written_table = (
                writing_target.database_id,
                normalize_ident(&writing_target.table.name),
            );
            if acting_target
                .tables_changed_by_fk_actions
                .contains(&written_table)
            {
                bail_parse_error!(
                    "cannot change table \"{}\" in {}: a foreign key action of {} also changes it",
                    writing_target.table.name,
                    parts[writing].display_name(),
                    parts[acting].display_name()
                );
            }
        }
    }
    Ok(())
}

/// Rejects REPLACE on a table that another part also writes. REPLACE deletes
/// the conflicting row, which can be a row the other part wrote, and
/// PostgreSQL never changes a row twice in one statement.
fn check_replace_on_shared_tables(parts: &[Part]) -> Result<()> {
    for part in parts {
        let Some(target) = part.target.as_ref().filter(|target| target.may_replace) else {
            continue;
        };
        if let Some(other) = parts.iter().find(|other| {
            !std::ptr::eq(*other, part)
                && other
                    .target
                    .as_ref()
                    .is_some_and(|other_target| other_target.table_key() == target.table_key())
        }) {
            bail_parse_error!(
                "cannot resolve conflicts with REPLACE on table \"{}\" in {}: {} also changes it",
                target.table.name,
                part.display_name(),
                other.display_name()
            );
        }
    }
    Ok(())
}

/// Tables whose written rowids the statement tracks: tables that two parts
/// write when one of them is an UPDATE or DELETE, and every table of an ON
/// CONFLICT DO UPDATE, which fails on a row the statement already wrote even
/// when the same part wrote it.
fn tracked_tables(parts: &[Part]) -> Vec<(usize, i64)> {
    let mut tracked = Vec::new();
    for (index, part) in parts.iter().enumerate() {
        let Some(target) = &part.target else {
            continue;
        };
        let key = target.table_key();
        let needs_tracking = match target.kind {
            DmlKind::Insert { do_update: false } => false,
            DmlKind::Insert { do_update: true } => true,
            DmlKind::Update | DmlKind::Delete => {
                parts.iter().enumerate().any(|(other_index, other)| {
                    other_index != index
                        && other
                            .target
                            .as_ref()
                            .is_some_and(|other_target| other_target.table_key() == key)
                })
            }
        };
        if needs_tracking && !tracked.contains(&key) {
            tracked.push(key);
        }
    }
    tracked
}

/// Names a statement reads as tables, leaving out names that a WITH clause
/// inside the statement defines. A name that is not a table (for example a
/// column qualifier) never appears in a FROM clause, so it is not collected.
struct ReadNames {
    names: HashSet<String>,
    /// Names defined by WITH clauses around the place being walked.
    defined_inside: Vec<String>,
}

impl ReadNames {
    fn of_part_body(body: &PartBody) -> Result<HashSet<String>> {
        let mut read_names = ReadNames {
            names: HashSet::default(),
            defined_inside: Vec::new(),
        };
        match body {
            PartBody::Select(select) => read_names.select(select)?,
            PartBody::Dml(stmt) => read_names.dml(stmt)?,
        }
        Ok(read_names.names)
    }

    fn table_name(&mut self, name: &ast::QualifiedName) {
        if name.db_name.is_some() {
            return;
        }
        let name = normalize_ident(name.name.as_str());
        if !self.defined_inside.contains(&name) {
            self.names.insert(name);
        }
    }

    fn with(&mut self, with: Option<&ast::With>) -> Result<()> {
        let Some(with) = with else {
            return Ok(());
        };
        for cte in &with.ctes {
            self.defined_inside
                .push(normalize_ident(cte.tbl_name.as_str()));
        }
        for cte in &with.ctes {
            match &cte.body {
                ast::CteBody::Select(select) => self.select(select)?,
                ast::CteBody::DataModifying(stmt) => self.dml(stmt)?,
            }
        }
        Ok(())
    }

    fn dml(&mut self, stmt: &ast::Stmt) -> Result<()> {
        let scope = self.defined_inside.len();
        match stmt {
            ast::Stmt::Insert {
                with,
                body,
                returning,
                ..
            } => {
                self.with(with.as_ref())?;
                if let ast::InsertBody::Select(select, upsert) = body {
                    self.select(select)?;
                    let mut upsert = upsert.as_deref();
                    while let Some(clause) = upsert {
                        if let Some(index) = &clause.index {
                            self.sorted_columns(&index.targets)?;
                            self.optional_expr(index.where_clause.as_deref())?;
                        }
                        if let UpsertDo::Set { sets, where_clause } = &clause.do_clause {
                            for set in sets {
                                self.expr(&set.expr)?;
                            }
                            self.optional_expr(where_clause.as_deref())?;
                        }
                        upsert = clause.next.as_deref();
                    }
                }
                self.result_columns(returning)?;
            }
            ast::Stmt::Update(update) => {
                self.with(update.with.as_ref())?;
                for set in &update.sets {
                    self.expr(&set.expr)?;
                }
                if let Some(from) = &update.from {
                    self.walk_from_clause(from)?;
                }
                self.optional_expr(update.where_clause.as_deref())?;
                self.result_columns(&update.returning)?;
            }
            ast::Stmt::Delete {
                with,
                where_clause,
                returning,
                ..
            } => {
                self.with(with.as_ref())?;
                self.optional_expr(where_clause.as_deref())?;
                self.result_columns(returning)?;
            }
            _ => {
                return Err(LimboError::InternalError(
                    "a writable CTE part is not INSERT, UPDATE or DELETE".to_string(),
                ))
            }
        }
        self.defined_inside.truncate(scope);
        Ok(())
    }

    fn select(&mut self, select: &ast::Select) -> Result<()> {
        let scope = self.defined_inside.len();
        self.with(select.with.as_ref())?;
        self.one_select(&select.body.select)?;
        for compound in &select.body.compounds {
            self.one_select(&compound.select)?;
        }
        self.sorted_columns(&select.order_by)?;
        if let Some(limit) = &select.limit {
            self.expr(&limit.expr)?;
            self.optional_expr(limit.offset.as_deref())?;
        }
        self.defined_inside.truncate(scope);
        Ok(())
    }

    fn one_select(&mut self, one_select: &ast::OneSelect) -> Result<()> {
        match one_select {
            ast::OneSelect::Select {
                columns,
                from,
                where_clause,
                group_by,
                window_clause,
                ..
            } => {
                self.result_columns(columns)?;
                if let Some(from) = from {
                    self.walk_from_clause(from)?;
                }
                self.optional_expr(where_clause.as_deref())?;
                if let Some(group_by) = group_by {
                    for expr in &group_by.exprs {
                        self.expr(expr)?;
                    }
                    self.optional_expr(group_by.having.as_deref())?;
                }
                for window_def in window_clause {
                    for expr in &window_def.window.partition_by {
                        self.expr(expr)?;
                    }
                    self.sorted_columns(&window_def.window.order_by)?;
                }
            }
            ast::OneSelect::Values(rows) => {
                for row in rows {
                    for expr in row {
                        self.expr(expr)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn walk_from_clause(&mut self, from: &ast::FromClause) -> Result<()> {
        self.select_table(&from.select)?;
        for join in &from.joins {
            self.select_table(&join.table)?;
            if let Some(ast::JoinConstraint::On(expr)) = &join.constraint {
                self.expr(expr)?;
            }
        }
        Ok(())
    }

    fn select_table(&mut self, table: &ast::SelectTable) -> Result<()> {
        match table {
            ast::SelectTable::Table(name, _, _) => self.table_name(name),
            ast::SelectTable::TableCall(name, args, _) => {
                self.table_name(name);
                for arg in args {
                    self.expr(arg)?;
                }
            }
            ast::SelectTable::Select(select, _) => self.select(select)?,
            ast::SelectTable::Sub(from, _) => self.walk_from_clause(from)?,
        }
        Ok(())
    }

    fn result_columns(&mut self, columns: &[ast::ResultColumn]) -> Result<()> {
        for column in columns {
            if let ast::ResultColumn::Expr(expr, _) = column {
                self.expr(expr)?;
            }
        }
        Ok(())
    }

    fn sorted_columns(&mut self, columns: &[ast::SortedColumn]) -> Result<()> {
        for column in columns {
            self.expr(&column.expr)?;
        }
        Ok(())
    }

    fn optional_expr(&mut self, expr: Option<&ast::Expr>) -> Result<()> {
        match expr {
            Some(expr) => self.expr(expr),
            None => Ok(()),
        }
    }

    fn expr(&mut self, expr: &ast::Expr) -> Result<()> {
        walk_expr(expr, &mut |expr: &ast::Expr| -> Result<WalkControl> {
            match expr {
                ast::Expr::Exists(select) | ast::Expr::Subquery(select) => self.select(select)?,
                ast::Expr::InSelect { rhs, .. } => self.select(rhs)?,
                ast::Expr::InTable { rhs, .. } => self.table_name(rhs),
                _ => {}
            }
            Ok(WalkControl::Continue)
        })?;
        Ok(())
    }
}

/// Emits a CTE of the statement's WITH clause and returns where its rows are.
fn emit_cte_part(
    part: &Part,
    body: PartBody,
    recursive: bool,
    resolver: &mut Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<StoredCte> {
    let name = part.name.clone().expect("a CTE part has a name");
    // The main statement's result columns are set after the last part.
    let saved_result_columns = std::mem::take(&mut program.result_columns);
    let stored = match body {
        PartBody::Select(select) => {
            let (columns, cte_id) = emit_select_cte_part(
                &name,
                select,
                &part.explicit_columns,
                recursive,
                resolver,
                program,
                connection,
            )?;
            StoredCte {
                name,
                columns,
                rows: StoredCteRows::Table { cte_id },
            }
        }
        PartBody::Dml(stmt) => {
            // Only the main statement's rows count as the statement's changes.
            let saved_change_count = program.alloc_register();
            program.emit_insn(Insn::ChangeCount {
                dest: saved_change_count,
            });
            let output = emit_dml_part(stmt, resolver, program, connection)?;
            program.emit_insn(Insn::SetChangeCount {
                src: saved_change_count,
            });
            match output.returning {
                Some(returning) => {
                    // Like PostgreSQL, the names rename the leading columns.
                    let mut columns = returning.columns;
                    if part.explicit_columns.len() > columns.len() {
                        bail_parse_error!(
                            "WITH query \"{}\" has {} columns available but {} columns specified",
                            name,
                            columns.len(),
                            part.explicit_columns.len()
                        );
                    }
                    for (column, name) in columns.iter_mut().zip(&part.explicit_columns) {
                        column.name = Some(name.clone());
                    }
                    let cte_id = program.alloc_cte_id();
                    let table = ephemeral_table_for_columns(&columns)?;
                    // The emitter opened the buffer with the target table's
                    // layout; readers duplicate the cursor, so it must have the
                    // layout of the RETURNING rows.
                    program.set_cursor_type(
                        returning.buffer.cursor_id,
                        CursorType::BTreeTable(table.clone()),
                    );
                    program.register_materialized_cte(
                        cte_id,
                        MaterializedCteInfo {
                            cursor_id: returning.buffer.cursor_id,
                            table,
                            num_columns: columns.len(),
                        },
                    );
                    StoredCte {
                        name,
                        columns,
                        rows: StoredCteRows::Table { cte_id },
                    }
                }
                None => StoredCte {
                    name,
                    columns: Vec::new(),
                    rows: StoredCteRows::NoReturning,
                },
            }
        }
    };
    program.result_columns = saved_result_columns;
    Ok(stored)
}

/// Emits a SELECT CTE into an ephemeral table. A recursive CTE or one with a
/// column list is planned as `WITH name AS (...) SELECT * FROM name`, so
/// recursion and column names follow the normal CTE rules.
fn emit_select_cte_part(
    name: &str,
    select: ast::Select,
    explicit_columns: &[String],
    recursive: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<(Vec<Column>, usize)> {
    let select = if recursive || !explicit_columns.is_empty() {
        select_all_from_cte(name, select, explicit_columns, recursive)
    } else {
        select
    };
    let (columns, cursor_id, table) =
        emit_select_into_ephemeral_table(select, resolver, program, connection)?;
    let cte_id = program.alloc_cte_id();
    program.register_materialized_cte(
        cte_id,
        MaterializedCteInfo {
            cursor_id,
            table,
            num_columns: columns.len(),
        },
    );
    Ok((columns, cte_id))
}

/// `WITH [RECURSIVE] name(explicit_columns) AS (select) SELECT * FROM name`.
/// The part stores the rows itself, so the inner CTE is not materialized.
fn select_all_from_cte(
    name: &str,
    select: ast::Select,
    explicit_columns: &[String],
    recursive: bool,
) -> ast::Select {
    let cte_name = ast::Name::exact(name.to_string());
    ast::Select {
        with: Some(ast::With {
            recursive,
            ctes: vec![ast::CommonTableExpr {
                tbl_name: cte_name.clone(),
                columns: explicit_columns
                    .iter()
                    .map(|column| ast::IndexedColumn {
                        col_name: ast::Name::exact(column.clone()),
                        collation_name: None,
                        order: None,
                    })
                    .collect(),
                materialized: ast::Materialized::Any,
                body: ast::CteBody::Select(select),
            }],
        }),
        body: ast::SelectBody {
            select: ast::OneSelect::Select {
                distinctness: None,
                columns: vec![ast::ResultColumn::Star],
                from: Some(ast::FromClause {
                    select: Box::new(ast::SelectTable::Table(
                        ast::QualifiedName::single(cte_name),
                        None,
                        None,
                    )),
                    joins: vec![],
                }),
                where_clause: None,
                group_by: None,
                window_clause: vec![],
            },
            compounds: vec![],
        },
        order_by: vec![],
        limit: None,
    }
}

/// Emits `select` with its rows going into a new ephemeral table.
fn emit_select_into_ephemeral_table(
    select: ast::Select,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<(Vec<Column>, CursorID, Arc<BTreeTable>)> {
    let mut plan = prepare_select_plan(
        select,
        resolver,
        program,
        &[],
        QueryDestination::placeholder_for_subquery(),
        connection,
    )?;
    let columns = subquery_output_columns(&plan, None)?;
    let table = ephemeral_table_for_columns(&columns)?;
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id,
        is_table: true,
    });
    let destination = plan.select_query_destination_mut().ok_or_else(|| {
        LimboError::InternalError("a SELECT part did not produce a SELECT plan".to_string())
    })?;
    *destination = QueryDestination::EphemeralTable {
        cursor_id,
        table: table.clone(),
        rowid_mode: EphemeralRowidMode::Auto,
    };
    emit_select_plan(plan, resolver, program, connection)?;
    Ok((columns, cursor_id, table))
}

/// Where the main statement's rows are stored until every part has run.
struct MainOutput {
    rows: ReturningBufferCtx,
    result_columns: Vec<ResultSetColumn>,
}

fn emit_main_part(
    body: PartBody,
    resolver: &mut Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<Option<MainOutput>> {
    match body {
        PartBody::Select(select) => {
            let (columns, cursor_id, _) =
                emit_select_into_ephemeral_table(select, resolver, program, connection)?;
            Ok(Some(MainOutput {
                rows: ReturningBufferCtx {
                    cursor_id,
                    num_columns: columns.len(),
                },
                result_columns: std::mem::take(&mut program.result_columns),
            }))
        }
        PartBody::Dml(stmt) => {
            let output = emit_dml_part(stmt, resolver, program, connection)?;
            program.result_columns.clear();
            Ok(output.returning.map(|returning| MainOutput {
                rows: returning.buffer,
                result_columns: returning.result_columns,
            }))
        }
    }
}

fn emit_dml_part(
    stmt: ast::Stmt,
    resolver: &mut Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<DmlPartOutput> {
    writable_cte_state_mut(program).dml_part = Some(DmlPartOutput::default());
    let result = translate_dml(stmt, resolver, program, connection);
    let output = writable_cte_state_mut(program).dml_part.take();
    result?;
    output.ok_or_else(|| LimboError::InternalError("DML part output is missing".to_string()))
}

fn translate_dml(
    stmt: ast::Stmt,
    resolver: &mut Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    match stmt {
        ast::Stmt::Insert {
            with,
            or_conflict,
            tbl_name,
            columns,
            body,
            returning,
        } => translate_insert(
            resolver,
            or_conflict,
            tbl_name,
            columns,
            body,
            returning,
            with,
            program,
            connection,
        ),
        ast::Stmt::Update(update) => {
            if update.where_clause.is_none() && connection.get_dml_require_where() {
                bail_parse_error!(
                    "UPDATE without a WHERE clause is not allowed when require_where (or i_am_a_dummy) is enabled"
                );
            }
            translate_update(update, resolver, program, connection)
        }
        ast::Stmt::Delete {
            tbl_name,
            where_clause,
            returning,
            indexed,
            with,
        } => {
            if where_clause.is_none() && connection.get_dml_require_where() {
                bail_parse_error!(
                    "DELETE without a WHERE clause is not allowed when require_where (or i_am_a_dummy) is enabled"
                );
            }
            translate_delete(
                &tbl_name,
                resolver,
                where_clause,
                returning,
                indexed,
                with,
                program,
                connection,
            )
        }
        _ => Err(LimboError::InternalError(
            "a writable CTE part is not INSERT, UPDATE or DELETE".to_string(),
        )),
    }
}

fn ephemeral_table_for_columns(columns: &[Column]) -> Result<Arc<BTreeTable>> {
    use crate::alloc::TursoSliceExt;
    Ok(Arc::new(BTreeTable::new(
        0,
        String::new(),
        crate::alloc::vec![],
        columns.try_to_vec()?,
        crate::schema::BTreeCharacteristics::HAS_ROWID,
        crate::alloc::vec![],
        crate::alloc::vec![],
        crate::alloc::vec![],
        None,
    )))
}

/// Copies every table that some part reads after an earlier part wrote it.
/// The copy keeps the rowids and the record bytes of the table.
fn emit_table_copies(program: &mut ProgramBuilder, resolver: &Resolver) -> Result<()> {
    let copies: Vec<(usize, Arc<BTreeTable>, CursorID)> = writable_cte_state_mut(program)
        .table_copies
        .iter()
        .map(|copy| (copy.database_id, copy.table.clone(), copy.copy_cursor_id))
        .collect();
    for (database_id, table, copy_cursor_id) in copies {
        let schema_cookie = resolver.with_schema(database_id, |schema| schema.schema_version);
        program.begin_read_on_database(database_id, schema_cookie)?;
        let table_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
        program.emit_insn(Insn::OpenRead {
            cursor_id: table_cursor_id,
            root_page: table.root_page,
            db: database_id,
        });
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: copy_cursor_id,
            is_table: true,
        });
        let done = program.allocate_label();
        let next_row = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: table_cursor_id,
            pc_if_empty: done,
        });
        program.preassign_label_to_next_insn(next_row);
        let rowid_reg = program.alloc_register();
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::RowId {
            cursor_id: table_cursor_id,
            dest: rowid_reg,
        });
        program.emit_insn(Insn::RowData {
            cursor_id: table_cursor_id,
            dest: record_reg,
        });
        // The key is the table's rowid, not a NewRowid result, so the insert
        // must seek to the key's position.
        program.emit_insn(Insn::Insert {
            cursor: copy_cursor_id,
            key_reg: rowid_reg,
            record_reg,
            flag: InsertFlags::new()
                .is_ephemeral_table_insert()
                .require_seek(),
            table_name: String::new(),
        });
        program.emit_insn(Insn::Next {
            cursor_id: table_cursor_id,
            pc_if_next: next_row,
            fullscan: false,
            is_index: false,
        });
        program.preassign_label_to_next_insn(done);
        program.emit_insn(Insn::Close {
            cursor_id: table_cursor_id,
        });
    }
    Ok(())
}

impl ProgramBuilder {
    /// A reference to a CTE of the statement's own WITH clause, if `name` is
    /// one that the part being planned may read.
    pub(crate) fn statement_cte_reference(
        &mut self,
        name: &str,
        identifier: String,
    ) -> Result<Option<JoinedTable>> {
        if self.is_expanding_view() {
            return Ok(None);
        }
        let Some(state) = self.writable_cte.as_deref() else {
            return Ok(None);
        };
        if !state
            .visible_cte_names
            .iter()
            .any(|visible| visible == name)
        {
            return Ok(None);
        }
        let Some(stored) = state.stored_ctes.iter().find(|stored| stored.name == name) else {
            return Err(LimboError::InternalError(format!(
                "WITH query {name} is read before it runs"
            )));
        };
        match stored.rows {
            StoredCteRows::NoReturning => {
                bail_parse_error!("WITH query \"{name}\" does not have a RETURNING clause")
            }
            StoredCteRows::Table { cte_id } => {
                let columns = stored.columns.clone();
                let internal_id = self.table_reference_counter.next();
                Ok(Some(JoinedTable::new_stored_cte_reference(
                    identifier,
                    columns,
                    cte_id,
                    internal_id,
                )))
            }
        }
    }

    /// The table that a read of `table` in the part being planned uses: a copy
    /// taken when the statement started if the part or a part that runs
    /// earlier writes the table, otherwise `table` itself.
    pub(crate) fn table_for_read(
        &mut self,
        database_id: usize,
        table: &Arc<BTreeTable>,
    ) -> Result<Arc<BTreeTable>> {
        let Some(state) = self.writable_cte.as_deref() else {
            return Ok(table.clone());
        };
        let table_name = normalize_ident(&table.name);
        if !state
            .tables_read_from_copies
            .iter()
            .any(|(db, name)| *db == database_id && *name == table_name)
        {
            return Ok(table.clone());
        }
        if let Some(copy) = state
            .table_copies
            .iter()
            .find(|copy| copy.database_id == database_id && copy.table_name == table_name)
        {
            return Ok(copy.copy_table.clone());
        }
        turso_assert!(
            table.has_rowid,
            "a part of a statement with INSERT, UPDATE or DELETE inside WITH wrote a WITHOUT ROWID table"
        );
        let mut copy_table = table.as_ref().clone();
        copy_table.root_page = STATEMENT_START_COPY_ROOT_PAGE;
        let copy_table = Arc::new(copy_table);
        let copy_cursor_id = self.alloc_cursor_id(CursorType::BTreeTable(copy_table.clone()));
        let state = self
            .writable_cte
            .as_deref_mut()
            .expect("writable CTE state was checked above");
        state.table_copies.push(TableCopy {
            database_id,
            table_name,
            table: table.clone(),
            copy_table: copy_table.clone(),
            copy_cursor_id,
        });
        Ok(copy_table)
    }

    /// Emits the end of an INSERT, UPDATE or DELETE: the CDC COMMIT record,
    /// the foreign key check and the scan of the buffered RETURNING rows. A
    /// part of a statement with INSERT, UPDATE or DELETE inside WITH leaves
    /// them to the statement, which emits them once after its last part.
    pub(crate) fn emit_dml_statement_end(
        &mut self,
        resolver: &Resolver,
        cdc_cursor_id: Option<CursorID>,
        returning_buffer: Option<&ReturningBufferCtx>,
    ) -> Result<()> {
        if let Some(state) = self
            .writable_cte
            .as_deref_mut()
            .filter(|state| state.dml_part.is_some())
        {
            state.cdc_commit_needed |= cdc_cursor_id.is_some();
            return Ok(());
        }
        if let Some(cdc_cursor_id) = cdc_cursor_id {
            emit_cdc_autocommit_commit(self, resolver, cdc_cursor_id)?;
        }
        // The foreign key check comes before the RETURNING rows, so a violation
        // returns no rows (like SQLite).
        if let Some(buffer) = returning_buffer {
            self.emit_insn(Insn::FkCheck { deferred: false });
            emit_returning_scan_back(self, buffer);
        }
        Ok(())
    }

    /// In a part of a statement with INSERT, UPDATE or DELETE inside WITH,
    /// gives the part's RETURNING rows to the statement. Does nothing for
    /// other statements.
    pub(crate) fn keep_writable_cte_part_returning(
        &mut self,
        buffer: Option<ReturningBufferCtx>,
        result_columns: &[ResultSetColumn],
        table_references: &TableReferences,
    ) -> Result<()> {
        let Some(buffer) = buffer else {
            return Ok(());
        };
        let Some(part) = self
            .writable_cte
            .as_deref_mut()
            .and_then(|state| state.dml_part.as_mut())
        else {
            return Ok(());
        };
        let columns = returning_output_columns(result_columns, table_references, None)?;
        part.returning = Some(ReturningRows {
            buffer,
            result_columns: result_columns.to_vec(),
            columns,
        });
        Ok(())
    }

    /// Whether the statement tracks the rowids it writes into this table.
    pub(crate) fn tracks_rows_changed_by_statement(
        &self,
        database_id: usize,
        root_page: i64,
    ) -> bool {
        self.tracked_table(database_id, root_page).is_some()
    }

    /// For the UPDATE or DELETE of a part: emits a jump to `skip_label` for the
    /// row whose rowid is in `rowid_reg` if the statement already wrote it, and
    /// returns whether it emitted anything.
    pub(crate) fn emit_skip_of_row_changed_by_statement(
        &mut self,
        database_id: usize,
        root_page: i64,
        rowid_reg: usize,
        skip_label: BranchOffset,
    ) -> bool {
        let Some(tracked_table) = self.tracked_table(database_id, root_page) else {
            return false;
        };
        self.emit_insn(Insn::IfRowChangedByStatement {
            tracked_table,
            rowid_reg,
            target_pc: skip_label,
        });
        true
    }

    /// For ON CONFLICT DO UPDATE: emits an error for the conflicting row whose
    /// rowid is in `rowid_reg` if the statement already wrote it.
    pub(crate) fn emit_upsert_error_if_row_changed_by_statement(
        &mut self,
        database_id: usize,
        root_page: i64,
        rowid_reg: usize,
    ) {
        let Some(tracked_table) = self.tracked_table(database_id, root_page) else {
            return;
        };
        let row_changed = self.allocate_label();
        let row_not_changed = self.allocate_label();
        self.emit_insn(Insn::IfRowChangedByStatement {
            tracked_table,
            rowid_reg,
            target_pc: row_changed,
        });
        self.emit_insn(Insn::Goto {
            target_pc: row_not_changed,
        });
        self.preassign_label_to_next_insn(row_changed);
        self.emit_insn(Insn::Halt {
            err_code: crate::error::SQLITE_ERROR,
            description: "ON CONFLICT DO UPDATE command cannot affect row a second time"
                .to_string(),
            on_error: None,
            description_reg: None,
        });
        self.preassign_label_to_next_insn(row_not_changed);
    }

    /// The tracked-table number of the table with this root page.
    fn tracked_table(&self, database_id: usize, root_page: i64) -> Option<usize> {
        self.writable_cte.as_deref().and_then(|state| {
            state
                .tracked_tables
                .iter()
                .position(|&tracked| tracked == (database_id, root_page))
        })
    }
}

impl WritableCteState {
    /// Makes every cursor that opens a table at [STATEMENT_START_COPY_ROOT_PAGE]
    /// open the copy of that table instead.
    pub(crate) fn open_table_copies_instead_of_tables(
        &self,
        insns: &mut [(Insn, usize)],
        cursor_ref: &[(Option<CursorKey>, CursorType)],
    ) -> Result<()> {
        for (insn, _) in insns.iter_mut() {
            let (cursor_id, database_id) = match insn {
                Insn::OpenRead {
                    cursor_id,
                    root_page: STATEMENT_START_COPY_ROOT_PAGE,
                    db,
                } => (*cursor_id, *db),
                Insn::OpenWrite {
                    root_page: RegisterOrLiteral::Literal(STATEMENT_START_COPY_ROOT_PAGE),
                    ..
                } => {
                    return Err(LimboError::InternalError(
                        "a statement wrote to a copy of a table".to_string(),
                    ))
                }
                _ => continue,
            };
            let Some((_, CursorType::BTreeTable(table))) = cursor_ref.get(cursor_id) else {
                return Err(LimboError::InternalError(
                    "a table copy is opened through a cursor that is not a table cursor"
                        .to_string(),
                ));
            };
            let table_name = normalize_ident(&table.name);
            let copy = self
                .table_copies
                .iter()
                .find(|copy| copy.database_id == database_id && copy.table_name == table_name)
                .ok_or_else(|| {
                    LimboError::InternalError(format!("no copy of table {table_name} was made"))
                })?;
            *insn = Insn::OpenDup {
                new_cursor_id: cursor_id,
                original_cursor_id: copy.copy_cursor_id,
            };
        }
        Ok(())
    }

    /// For each tracked table, the cursors that write into it.
    pub(crate) fn tracked_table_cursors(&self, insns: &[(Insn, usize)]) -> Vec<Vec<CursorID>> {
        let mut cursors = vec![Vec::new(); self.tracked_tables.len()];
        for (insn, _) in insns {
            let Insn::OpenWrite {
                cursor_id,
                root_page: RegisterOrLiteral::Literal(root_page),
                db,
            } = insn
            else {
                continue;
            };
            if let Some(tracked) = self
                .tracked_tables
                .iter()
                .position(|&tracked| tracked == (*db, *root_page))
            {
                if !cursors[tracked].contains(cursor_id) {
                    cursors[tracked].push(*cursor_id);
                }
            }
        }
        cursors
    }
}
