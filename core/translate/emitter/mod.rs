// This module contains code for emitting bytecode instructions for SQL query execution.
// It handles translating high-level SQL operations into low-level bytecode that can be executed by the virtual machine.
use super::{collate::CollationSeq, plan::BitSet};
use crate::alloc::TursoIteratorExt;
use crate::schema::{BTreeTable, Column, ColumnLayout, Schema, Table};
use crate::translate::semantic::context::{DoubleQuotedDml, SemanticContext};
use crate::vdbe::{
    builder::{CursorType, ProgramBuilder},
    insn::{to_u32, InsertFlags, Insn},
};
use crate::{
    function::Func, sync::Arc, turso_assert_ne, util::normalize_ident, CaptureDataChangesExt,
    Database, DatabaseCatalog, LimboError, Result, RwLock, SymbolTable,
};
use rustc_hash::FxHashMap as HashMap;
use std::cell::RefCell;
use turso_parser::ast;

/// Catalog access used while compiling DDL and control statements.
///
/// SQL expression binding belongs to `semantic::Analyzer`; this context only
/// performs the live catalog operations that DDL cannot freeze into HIR.
pub struct DdlContext<'a> {
    schema: &'a Schema,
    database_schemas: &'a RwLock<HashMap<usize, Arc<Schema>>>,
    temp_database: &'a RwLock<Option<crate::connection::TempDatabase>>,
    attached_databases: &'a RwLock<DatabaseCatalog>,
    non_main_schema_cache: RefCell<HashMap<usize, Arc<Schema>>>,
    pub symbol_table: &'a SymbolTable,
    pub enable_custom_types: bool,
    /// Schema dialect of the database being compiled against; used when a
    /// fresh placeholder schema must be constructed during resolution.
    pub(crate) dialect: Arc<dyn crate::dialect::Dialect>,
    /// Cached flag: true when this connection has an active temp database.
    ///
    /// Computed once at construction to avoid repeated `RwLock` reads on every
    /// table-name lookup. Safe because this context is short-lived (one
    /// translate pass) and a
    /// connection is single-threaded at the VDBE layer: the temp
    /// database can only be initialized / torn down *between*
    /// Resolvers on the same connection, not during. If you add a
    /// path that can initialize the temp database *inside* translate
    /// (e.g. via a nested sub-program), update this field on that
    /// path or switch to a live read.
    has_temp_schema: bool,
}

impl<'a> DdlContext<'a> {
    const MAIN_DB: &'static str = "main";
    const TEMP_DB: &'static str = "temp";

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        schema: &'a Schema,
        database_schemas: &'a RwLock<HashMap<usize, Arc<Schema>>>,
        temp_database: &'a RwLock<Option<crate::connection::TempDatabase>>,
        attached_databases: &'a RwLock<DatabaseCatalog>,
        symbol_table: &'a SymbolTable,
        enable_custom_types: bool,
        dialect: Arc<dyn crate::dialect::Dialect>,
    ) -> Self {
        let has_temp_schema = temp_database.read().is_some();
        Self {
            schema,
            database_schemas,
            temp_database,
            attached_databases,
            non_main_schema_cache: RefCell::new(HashMap::default()),
            symbol_table,
            enable_custom_types,
            dialect,
            has_temp_schema,
        }
    }

    pub fn schema(&self) -> &Schema {
        self.schema
    }

    pub(crate) fn semantic_context(&self, dqs_dml: DoubleQuotedDml) -> SemanticContext<'a> {
        SemanticContext::new(
            self.schema,
            self.database_schemas,
            self.temp_database,
            self.attached_databases,
            self.symbol_table,
            self.enable_custom_types,
            dqs_dml,
            self.dialect.clone(),
        )
    }

    pub fn has_temp_database(&self) -> bool {
        self.has_temp_schema
    }

    fn cached_non_main_schema(&self, database_id: usize) -> Arc<Schema> {
        turso_assert_ne!(database_id, crate::MAIN_DB_ID);

        if let Some(schema) = self
            .non_main_schema_cache
            .borrow()
            .get(&database_id)
            .cloned()
        {
            return schema;
        }

        // TEMP uses `temp_db.db.schema` as its single source of truth; skip
        // `database_schemas` which is never populated for TEMP.
        if database_id != crate::TEMP_DB_ID {
            if let Some(schema) = self.database_schemas.read().get(&database_id).cloned() {
                self.non_main_schema_cache
                    .borrow_mut()
                    .insert(database_id, schema.clone());
                return schema;
            }
        }

        let loaded_schema = match database_id {
            crate::TEMP_DB_ID => self
                .temp_database
                .read()
                .as_ref()
                .map(|temp_db| temp_db.db.schema.lock().clone())
                .unwrap_or_else(|| {
                    // with_options only fails if built-in type SQL is malformed (programmer bug).
                    Arc::new(
                        Schema::with_options(self.enable_custom_types, self.dialect.as_ref())
                            .expect("built-in type definitions are malformed"),
                    )
                }),
            _ => {
                let attached_dbs = self.attached_databases.read();
                let (db, _pager) = attached_dbs
                    .index_to_data
                    .get(&database_id)
                    .expect("Database ID should be valid after resolve_database_id");
                let schema = db.schema.lock().clone();
                schema
            }
        };

        self.non_main_schema_cache
            .borrow_mut()
            .insert(database_id, loaded_schema.clone());
        loaded_schema
    }

    pub fn resolve_function(
        &self,
        func_name: &str,
        arg_count: usize,
    ) -> Result<Option<Func>, LimboError> {
        // The dialect owns the function name surface of user SQL; extension
        // functions resolve after it.
        match self.dialect.resolve_function(func_name, arg_count)? {
            Some(func) => Ok(Some(func)),
            None => Ok(self
                .symbol_table
                .resolve_function(func_name, arg_count)
                .map(Func::External)),
        }
    }

    pub fn resolve_collation(&self, name: &str) -> Result<CollationSeq> {
        if let Some(collation) = self.symbol_table.resolve_collation(name) {
            return Ok(collation);
        }
        CollationSeq::new(name)
    }

    /// Access schema for a database using a closure pattern to avoid cloning
    pub(crate) fn with_schema<T>(&self, database_id: usize, f: impl FnOnce(&Schema) -> T) -> T {
        match database_id {
            crate::MAIN_DB_ID => f(self.schema),
            _ => {
                let schema = self.cached_non_main_schema(database_id);
                f(&schema)
            }
        }
    }

    pub(crate) fn attached_database_ids_in_search_order(&self) -> Result<BitSet> {
        Ok(self
            .attached_databases
            .read()
            .index_to_data
            .keys()
            .copied()
            .try_collect()?)
    }

    fn resolve_unqualified_existing_database_id<F>(
        &self,
        object_name: &str,
        schema_contains_object: F,
    ) -> Result<usize>
    where
        F: Fn(&Schema, &str) -> bool,
    {
        // Only check the temp schema when a temp database actually exists.
        // This avoids expensive schema construction/lookup on every table
        // resolution when no temp objects have been created.
        if self.has_temp_schema
            && self.with_schema(crate::TEMP_DB_ID, |schema| {
                schema_contains_object(schema, object_name)
            })
        {
            return Ok(crate::TEMP_DB_ID);
        }

        if self.with_schema(crate::MAIN_DB_ID, |schema| {
            schema_contains_object(schema, object_name)
        }) {
            return Ok(crate::MAIN_DB_ID);
        }

        for database_id in self.attached_database_ids_in_search_order()? {
            if self.with_schema(database_id, |schema| {
                schema_contains_object(schema, object_name)
            }) {
                return Ok(database_id);
            }
        }

        Ok(crate::MAIN_DB_ID)
    }

    fn schema_has_table_like_object(schema: &Schema, table_name: &str) -> bool {
        schema.get_table(table_name).is_some()
            || schema.get_view(table_name).is_some()
            || schema.get_materialized_view(table_name).is_some()
    }

    fn schema_has_index(schema: &Schema, index_name: &str) -> bool {
        schema
            .indexes
            .values()
            .flat_map(|indexes| indexes.iter())
            .any(|index| index.name.eq_ignore_ascii_case(index_name))
    }

    fn schema_has_trigger(schema: &Schema, trigger_name: &str) -> bool {
        schema.get_trigger(trigger_name).is_some()
    }

    fn resolve_schema_table_database_id(table_name: &str) -> Option<usize> {
        if table_name.eq_ignore_ascii_case(crate::schema::TEMP_SCHEMA_TABLE_NAME)
            || table_name.eq_ignore_ascii_case(crate::schema::TEMP_SCHEMA_TABLE_NAME_ALT)
        {
            return Some(crate::TEMP_DB_ID);
        }

        if table_name.eq_ignore_ascii_case(crate::schema::SCHEMA_TABLE_NAME)
            || table_name.eq_ignore_ascii_case(crate::schema::SCHEMA_TABLE_NAME_ALT)
        {
            return Some(crate::MAIN_DB_ID);
        }

        None
    }

    pub(crate) fn resolve_existing_table_database_id_qualified(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<usize> {
        if qualified_name.db_name.is_some() {
            return self.resolve_database_id(qualified_name);
        }
        self.resolve_existing_table_database_id(qualified_name.name.as_str())
    }

    pub(crate) fn resolve_existing_table_database_id(&self, table_name: &str) -> Result<usize> {
        if let Some(database_id) = Self::resolve_schema_table_database_id(table_name) {
            return Ok(database_id);
        }

        self.resolve_unqualified_existing_database_id(
            table_name,
            Self::schema_has_table_like_object,
        )
    }

    pub(crate) fn resolve_existing_index_database_id(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<usize> {
        if qualified_name.db_name.is_some() {
            return self.resolve_database_id(qualified_name);
        }

        let index_name = normalize_ident(qualified_name.name.as_str());
        self.resolve_unqualified_existing_database_id(&index_name, Self::schema_has_index)
    }

    pub(crate) fn resolve_existing_trigger_database_id(
        &self,
        qualified_name: &ast::QualifiedName,
    ) -> Result<usize> {
        if qualified_name.db_name.is_some() {
            return self.resolve_database_id(qualified_name);
        }

        let trigger_name = qualified_name.name.as_str();
        self.resolve_unqualified_existing_database_id(trigger_name, Self::schema_has_trigger)
    }

    /// Resolve database ID from a qualified name
    pub(crate) fn resolve_database_id(&self, qualified_name: &ast::QualifiedName) -> Result<usize> {
        // Check if this is a qualified name (database.table) or unqualified
        let resolved_id = if let Some(db_name) = &qualified_name.db_name {
            let db_name_normalized = normalize_ident(db_name.as_str());
            match db_name_normalized.as_str() {
                "main" => Ok(crate::MAIN_DB_ID),
                "temp" => Ok(crate::TEMP_DB_ID),
                _ => {
                    // Look up attached database
                    if let Some((idx, _attached_db)) =
                        self.get_attached_database(&db_name_normalized)
                    {
                        Ok(idx)
                    } else {
                        Err(LimboError::InvalidArgument(format!(
                            "no such database: {db_name_normalized}"
                        )))
                    }
                }
            }
        } else {
            Ok(crate::MAIN_DB_ID)
        }?;

        Ok(resolved_id)
    }

    // Get an attached database by alias name
    pub(crate) fn get_attached_database(&self, alias: &str) -> Option<(usize, Arc<Database>)> {
        self.attached_databases.read().get_database_by_name(alias)
    }

    /// Get the database name for a given database index.
    /// Returns "main" for index 0, "temp" for index 1, and the alias for attached databases.
    pub(crate) fn get_database_name_by_index(&self, index: usize) -> Option<String> {
        match index {
            crate::MAIN_DB_ID => Some(Self::MAIN_DB.to_string()),
            crate::TEMP_DB_ID => Some(Self::TEMP_DB.to_string()),
            _ => self.attached_databases.read().get_name_by_index(index),
        }
    }
}

/// Used to distinguish database operations
#[allow(clippy::upper_case_acronyms, dead_code)]
#[derive(Debug, Clone)]
pub enum OperationMode {
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Sqlite always considers Read transactions implicit
pub enum TransactionMode {
    None,
    Read,
    Write,
    Concurrent,
}

pub fn prepare_cdc_if_necessary(
    program: &mut ProgramBuilder,
    schema: &Schema,
    changed_table_name: Option<&str>,
) -> Result<Option<(usize, Arc<BTreeTable>)>> {
    let mode = program.capture_data_changes_info();
    let cdc_table = mode.table();
    let Some(cdc_table) = cdc_table else {
        return Ok(None);
    };
    // Self-exclusion: never capture changes to CDC's own bookkeeping tables. `None` means the
    // caller has no associated table (e.g. a transaction-boundary COMMIT record) and always
    // gets the cursor.
    if let Some(changed_table_name) = changed_table_name {
        if changed_table_name == cdc_table
            || changed_table_name == crate::translate::pragma::TURSO_CDC_VERSION_TABLE_NAME
        {
            return Ok(None);
        }
    }
    let Some(turso_cdc_table) = schema.get_table(cdc_table) else {
        crate::bail_parse_error!("no such table: {}", cdc_table);
    };
    let Some(cdc_btree) = turso_cdc_table.btree() else {
        crate::bail_parse_error!("no such table: {}", cdc_table);
    };
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(cdc_btree.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: cdc_btree.root_page.into(),
        db: crate::MAIN_DB_ID, // CDC table always lives in the main database
    });
    Ok(Some((cursor_id, cdc_btree)))
}

pub fn emit_cdc_patch_record(
    program: &mut ProgramBuilder,
    table: &Table,
    columns_reg: usize,
    record_reg: usize,
    rowid_reg: usize,
    layout: &ColumnLayout,
) -> usize {
    let columns = table.columns();
    let rowid_alias_position = columns.iter().position(|x| x.is_rowid_alias());
    if let Some(rowid_alias_position) = rowid_alias_position {
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::Copy {
            src_reg: rowid_reg,
            dst_reg: layout.to_register(columns_reg, rowid_alias_position),
            extra_amount: 0,
        });
        let storable_count = columns.iter().filter(|c| !c.is_virtual_generated()).count();
        let is_strict = table.btree().is_some_and(|btree| btree.is_strict);
        let affinity_str = columns
            .iter()
            .filter(|col| !col.is_virtual_generated())
            .map(|col| col.affinity_with_strict(is_strict).aff_mask())
            .collect::<String>();

        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(columns_reg),
            count: to_u32(storable_count),
            dest_reg: to_u32(record_reg),
            index_name: None,
            affinity_str: Some(affinity_str),
        });
        record_reg
    } else {
        record_reg
    }
}

pub(super) fn emit_make_record<'a>(
    program: &mut ProgramBuilder,
    cols: impl IntoIterator<Item = &'a Column>,
    start_reg: usize,
    dest_reg: usize,
    is_strict: bool,
) {
    let storable_cols: Vec<&Column> = cols
        .into_iter()
        .filter(|c| !c.is_virtual_generated())
        .collect();
    let storable_count = storable_cols.len();

    let affinity_str: String = storable_cols
        .iter()
        .map(|c| c.affinity_with_strict(is_strict).aff_mask())
        .collect();

    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(start_reg),
        count: to_u32(storable_count),
        dest_reg: to_u32(dest_reg),
        index_name: None,
        affinity_str: Some(affinity_str),
    });
}

pub fn emit_cdc_full_record(
    program: &mut ProgramBuilder,
    columns: &[Column],
    table_cursor_id: usize,
    rowid_reg: usize,
    is_strict: bool,
) -> usize {
    let storable_count = columns.iter().filter(|c| !c.is_virtual_generated()).count();
    let columns_reg = program.alloc_registers(storable_count + 1);
    let mut slot = 0;
    for (i, column) in columns.iter().enumerate() {
        if column.is_virtual_generated() {
            continue;
        }
        if column.is_rowid_alias() {
            program.emit_insn(Insn::Copy {
                src_reg: rowid_reg,
                dst_reg: columns_reg + 1 + slot,
                extra_amount: 0,
            });
        } else {
            program.emit_column_or_rowid(table_cursor_id, i, columns_reg + 1 + slot);
        }
        slot += 1;
    }
    let affinity_str = columns
        .iter()
        .filter(|col| !col.is_virtual_generated())
        .map(|col| col.affinity_with_strict(is_strict).aff_mask())
        .collect::<String>();

    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(columns_reg + 1),
        count: to_u32(storable_count),
        dest_reg: to_u32(columns_reg),
        index_name: None,
        affinity_str: Some(affinity_str),
    });
    columns_reg
}

#[allow(clippy::too_many_arguments)]
/// Allocate the rowid for a CDC row into `dest_reg`. The CDC table's `change_id`
/// column is `INTEGER PRIMARY KEY`, so the rowid IS the change id.
///
/// In MVCC journal mode the id is drawn from the CDC table's implicit
/// AUTOINCREMENT sequence. This makes change ids monotonic and never reused after
/// CDC rows are pruned, and registers each in-flight allocation with the MVCC
/// store so the sync push loop can call `sequence_watermark_experimental` to
/// avoid advancing the push watermark past a change id that a concurrent
/// transaction commits out of change-id order under snapshot isolation. In WAL
/// mode we keep the cheaper `NewRowid` (max rowid + 1) assignment; the WAL push
/// loop does not depend on the sequence watermark, so its insert path is
/// unchanged and pays no per-row sequence cost.
fn emit_cdc_change_id(
    program: &mut ProgramBuilder,
    ddl_context: &DdlContext,
    cdc_cursor_id: usize,
    dest_reg: usize,
) -> Result<()> {
    if !program.is_mvcc_enabled() {
        program.emit_insn(Insn::NewRowid {
            cursor: cdc_cursor_id,
            rowid_reg: dest_reg,
            prev_largest_reg: 0,
        });
        return Ok(());
    }
    let Some(cdc_table) = program
        .capture_data_changes_info()
        .as_ref()
        .map(|info| info.table.clone())
    else {
        return Err(crate::LimboError::InternalError(
            "CDC change-id allocation requested without an active CDC config".to_string(),
        ));
    };
    let seq_name = crate::schema::autoincrement_sequence_name(&cdc_table);
    let seq = ddl_context
        .with_schema(crate::MAIN_DB_ID, |s| s.get_sequence(&seq_name).cloned())
        .ok_or_else(|| {
            crate::LimboError::InternalError(format!(
                "missing implicit AUTOINCREMENT sequence for CDC table \"{cdc_table}\""
            ))
        })?;
    crate::translate::sequence::emit_disk_read_nextval(
        program,
        ddl_context,
        crate::MAIN_DB_ID,
        &seq_name,
        &seq,
        dest_reg,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn emit_cdc_insns(
    program: &mut ProgramBuilder,
    ddl_context: &DdlContext,
    operation_mode: OperationMode,
    cdc_cursor_id: usize,
    rowid_reg: usize,
    before_record_reg: Option<usize>,
    after_record_reg: Option<usize>,
    updates_record_reg: Option<usize>,
    table_name: &str,
) -> Result<()> {
    let cdc_info = program.capture_data_changes_info().as_ref();
    match cdc_info.map(|info| info.cdc_version()) {
        Some(crate::CdcVersion::V2) => emit_cdc_insns_v2(
            program,
            ddl_context,
            operation_mode,
            cdc_cursor_id,
            rowid_reg,
            before_record_reg,
            after_record_reg,
            updates_record_reg,
            table_name,
        ),
        Some(crate::CdcVersion::V1) => emit_cdc_insns_v1(
            program,
            operation_mode,
            cdc_cursor_id,
            rowid_reg,
            before_record_reg,
            after_record_reg,
            updates_record_reg,
            table_name,
        ),
        None => Err(crate::LimboError::InternalError(
            "cdc info not set".to_string(),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_cdc_insns_v1(
    program: &mut ProgramBuilder,
    operation_mode: OperationMode,
    cdc_cursor_id: usize,
    rowid_reg: usize,
    before_record_reg: Option<usize>,
    after_record_reg: Option<usize>,
    updates_record_reg: Option<usize>,
    table_name: &str,
) -> Result<()> {
    // v1: (change_id, change_time, change_type, table_name, id, before, after, updates)
    let turso_cdc_registers = program.alloc_registers(8);
    program.emit_insn(Insn::Null {
        dest: turso_cdc_registers,
        dest_end: None,
    });
    program.mark_last_insn_constant();

    let unixepoch_fn_ctx = crate::function::FuncCtx {
        func: Func::Scalar(crate::function::ScalarFunc::UnixEpoch),
        arg_count: 0,
    };

    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: 0,
        dest: turso_cdc_registers + 1,
        func: unixepoch_fn_ctx,
    });

    let change_type = match operation_mode {
        OperationMode::INSERT => 1,
        OperationMode::UPDATE | OperationMode::SELECT => 0,
        OperationMode::DELETE => -1,
    };
    program.emit_int(change_type, turso_cdc_registers + 2);
    program.mark_last_insn_constant();

    program.emit_string8(table_name.to_string(), turso_cdc_registers + 3);
    program.mark_last_insn_constant();

    program.emit_insn(Insn::Copy {
        src_reg: rowid_reg,
        dst_reg: turso_cdc_registers + 4,
        extra_amount: 0,
    });

    if let Some(before_record_reg) = before_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: before_record_reg,
            dst_reg: turso_cdc_registers + 5,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 5, None);
        program.mark_last_insn_constant();
    }

    if let Some(after_record_reg) = after_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: after_record_reg,
            dst_reg: turso_cdc_registers + 6,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 6, None);
        program.mark_last_insn_constant();
    }

    if let Some(updates_record_reg) = updates_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: updates_record_reg,
            dst_reg: turso_cdc_registers + 7,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 7, None);
        program.mark_last_insn_constant();
    }

    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: cdc_cursor_id,
        rowid_reg,
        prev_largest_reg: 0, // todo(sivukhin): properly set value here from sqlite_sequence table when AUTOINCREMENT will be properly implemented in Turso
    });

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(turso_cdc_registers),
        count: to_u32(8),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: cdc_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new()
            .skip_last_rowid()
            .skip_statement_change_count(),
        table_name: "".to_string(),
    });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
fn emit_cdc_insns_v2(
    program: &mut ProgramBuilder,
    ddl_context: &DdlContext,
    operation_mode: OperationMode,
    cdc_cursor_id: usize,
    rowid_reg: usize,
    before_record_reg: Option<usize>,
    after_record_reg: Option<usize>,
    updates_record_reg: Option<usize>,
    table_name: &str,
) -> Result<()> {
    // v2: (change_id, change_time, change_txn_id, change_type, table_name, id, before, after, updates)
    let turso_cdc_registers = program.alloc_registers(9);
    program.emit_insn(Insn::Null {
        dest: turso_cdc_registers,
        dest_end: None,
    });
    program.mark_last_insn_constant();

    // change_time = unixepoch()
    let unixepoch_fn_ctx = crate::function::FuncCtx {
        func: Func::Scalar(crate::function::ScalarFunc::UnixEpoch),
        arg_count: 0,
    };
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: 0,
        dest: turso_cdc_registers + 1,
        func: unixepoch_fn_ctx,
    });

    // change_txn_id = conn_txn_id(change_id)
    // First allocate the change id (the CDC rowid), then pass it to conn_txn_id
    // for get-or-set. In MVCC mode this draws from the CDC AUTOINCREMENT sequence
    // (see `emit_cdc_change_id`); in WAL mode it is a plain NewRowid.
    let candidate_reg = program.alloc_register();
    emit_cdc_change_id(program, ddl_context, cdc_cursor_id, candidate_reg)?;
    let conn_txn_id_fn_ctx = crate::function::FuncCtx {
        func: Func::Scalar(crate::function::ScalarFunc::ConnTxnId),
        arg_count: 1,
    };
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: candidate_reg,
        dest: turso_cdc_registers + 2,
        func: conn_txn_id_fn_ctx,
    });

    // change_type
    let change_type = match operation_mode {
        OperationMode::INSERT => 1,
        OperationMode::UPDATE | OperationMode::SELECT => 0,
        OperationMode::DELETE => -1,
    };
    program.emit_int(change_type, turso_cdc_registers + 3);
    program.mark_last_insn_constant();

    // table_name
    program.emit_string8(table_name.to_string(), turso_cdc_registers + 4);
    program.mark_last_insn_constant();

    // id
    program.emit_insn(Insn::Copy {
        src_reg: rowid_reg,
        dst_reg: turso_cdc_registers + 5,
        extra_amount: 0,
    });

    // before
    if let Some(before_record_reg) = before_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: before_record_reg,
            dst_reg: turso_cdc_registers + 6,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 6, None);
        program.mark_last_insn_constant();
    }

    // after
    if let Some(after_record_reg) = after_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: after_record_reg,
            dst_reg: turso_cdc_registers + 7,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 7, None);
        program.mark_last_insn_constant();
    }

    // updates
    if let Some(updates_record_reg) = updates_record_reg {
        program.emit_insn(Insn::Copy {
            src_reg: updates_record_reg,
            dst_reg: turso_cdc_registers + 8,
            extra_amount: 0,
        });
    } else {
        program.emit_null(turso_cdc_registers + 8, None);
        program.mark_last_insn_constant();
    }

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(turso_cdc_registers),
        count: to_u32(9),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: cdc_cursor_id,
        key_reg: candidate_reg,
        record_reg,
        flag: InsertFlags::new()
            .skip_last_rowid()
            .skip_statement_change_count(),
        table_name: "".to_string(),
    });
    Ok(())
}

/// Emit a COMMIT record into the CDC table (v2 only).
/// change_type=2, all other data fields NULL.
pub fn emit_cdc_commit_insns(
    program: &mut ProgramBuilder,
    ddl_context: &DdlContext,
    cdc_cursor_id: usize,
) -> Result<()> {
    // v2 COMMIT record: (NULL, unixepoch(), conn_txn_id(-1), 2, NULL, NULL, NULL, NULL, NULL)
    let regs = program.alloc_registers(9);
    // reg+0: NULL (change_id, autoincrement)
    program.emit_insn(Insn::Null {
        dest: regs,
        dest_end: None,
    });
    program.mark_last_insn_constant();

    // reg+1: change_time = unixepoch()
    let unixepoch_fn_ctx = crate::function::FuncCtx {
        func: Func::Scalar(crate::function::ScalarFunc::UnixEpoch),
        arg_count: 0,
    };
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: 0,
        dest: regs + 1,
        func: unixepoch_fn_ctx,
    });

    // reg+2: change_txn_id = conn_txn_id(-1)
    // Pass -1 as candidate: if a txn_id exists, return it; if not, -1 is stored (and will be reset).
    let minus_one_reg = program.alloc_register();
    program.emit_int(-1, minus_one_reg);
    let conn_txn_id_fn_ctx = crate::function::FuncCtx {
        func: Func::Scalar(crate::function::ScalarFunc::ConnTxnId),
        arg_count: 1,
    };
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: minus_one_reg,
        dest: regs + 2,
        func: conn_txn_id_fn_ctx,
    });

    // reg+3: change_type = 2 (COMMIT)
    program.emit_int(2, regs + 3);
    program.mark_last_insn_constant();

    // reg+4..8: NULL (table_name, id, before, after, updates)
    program.emit_insn(Insn::Null {
        dest: regs + 4,
        dest_end: Some(regs + 8),
    });
    program.mark_last_insn_constant();

    // Allocate the COMMIT record's change id from the same source as row records
    // (the CDC AUTOINCREMENT sequence in MVCC mode) so COMMIT and row change ids
    // stay in one monotonic, never-reused stream.
    let rowid_reg = program.alloc_register();
    emit_cdc_change_id(program, ddl_context, cdc_cursor_id, rowid_reg)?;

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(regs),
        count: to_u32(9),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: cdc_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new()
            .skip_last_rowid()
            .skip_statement_change_count(),
        table_name: "".to_string(),
    });
    Ok(())
}

/// Emit a CDC COMMIT record at end-of-statement when in autocommit mode (v2 only).
/// This should be called once per statement, after the main loop, not per-row.
pub fn emit_cdc_autocommit_commit(
    program: &mut ProgramBuilder,
    ddl_context: &DdlContext,
    cdc_cursor_id: usize,
) -> Result<()> {
    let cdc_info = program.capture_data_changes_info().as_ref();
    if cdc_info.is_some_and(|info| info.cdc_version().has_commit_record()) {
        // Check if we're in autocommit mode; if so, emit a COMMIT record.
        let is_autocommit_fn_ctx = crate::function::FuncCtx {
            func: Func::Scalar(crate::function::ScalarFunc::IsAutocommit),
            arg_count: 0,
        };
        let autocommit_reg = program.alloc_register();
        program.emit_insn(Insn::Function {
            constant_mask: 0,
            start_reg: 0,
            dest: autocommit_reg,
            func: is_autocommit_fn_ctx,
        });

        // IfNot jumps when reg == 0 (not autocommit). Skip the COMMIT in that case.
        let skip_label = program.allocate_label();
        program.emit_insn(Insn::IfNot {
            reg: autocommit_reg,
            target_pc: skip_label,
            jump_if_null: true,
        });

        emit_cdc_commit_insns(program, ddl_context, cdc_cursor_id)?;

        program.preassign_label_to_next_insn(skip_label);
    }

    Ok(())
}

/// Emit the CDC COMMIT record for an explicit `COMMIT` statement, gated on the transaction
/// having actually captured a change.
///
/// Data-modifying statements always establish a write transaction before reaching their CDC
/// emission, but an explicit `COMMIT` does not: for an empty or read-only transaction the
/// connection's `tx_state` is still `None`/`Read`. Emitting the record unconditionally would
/// then dirty the CDC table page without a write transaction; the commit path neither flushes
/// nor clears that page, so it leaks into the next transaction and trips the "dirty pages
/// should be empty for read txn" assertion on a later ROLLBACK
/// (https://github.com/tursodatabase/turso/issues/7677).
///
/// `conn_txn_id(-1)` returns the active CDC transaction id, or -1 when nothing was captured.
/// When it is set, the transaction already performed a write (the data-change statement
/// established the write transaction), so inserting the commit record is safe. When it is -1
/// the transaction made no changes and we skip the record entirely, leaving the transaction
/// read-only.
pub fn emit_cdc_explicit_commit_insns(
    program: &mut ProgramBuilder,
    schema: &Schema,
    ddl_context: &DdlContext,
) -> Result<()> {
    let minus_one_reg = program.alloc_register();
    program.emit_int(-1, minus_one_reg);
    let txn_id_reg = program.alloc_register();
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: minus_one_reg,
        dest: txn_id_reg,
        func: crate::function::FuncCtx {
            func: Func::Scalar(crate::function::ScalarFunc::ConnTxnId),
            arg_count: 1,
        },
    });

    // Skip the whole record (including the CDC OpenWrite) when no change was captured.
    // `emit_cdc_commit_insns` recomputes `conn_txn_id(-1)` for the record itself; because the
    // opcode is an idempotent get-or-set, the second call returns the same value we gated on.
    let skip_label = program.allocate_label();
    program.emit_insn(Insn::Eq {
        lhs: txn_id_reg,
        rhs: minus_one_reg,
        target_pc: skip_label,
        flags: crate::vdbe::insn::CmpInsFlags::default(),
        collation: None,
    });

    // A COMMIT record has no associated table, so pass `None` (no self-exclusion check).
    if let Some((cdc_cursor_id, _)) = prepare_cdc_if_necessary(program, schema, None)? {
        emit_cdc_commit_insns(program, ddl_context, cdc_cursor_id)?;
    }

    program.preassign_label_to_next_insn(skip_label);
    Ok(())
}
