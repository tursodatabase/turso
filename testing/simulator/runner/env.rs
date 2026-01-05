use std::collections::HashMap;
use std::fmt::Display;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::panic::UnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bitmaps::Bitmap;
use garde::Validate;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sql_generation::generation::GenerationContext;
use sql_generation::model::query::select::Select;
use sql_generation::model::query::transaction::Rollback;
use sql_generation::model::table::{SimValue, Table};
use sql_generation::model::view::View;
use tracing::trace;
use turso_core::Database;

use crate::generation::Shadow;
use crate::model::Query;
use crate::profiles::Profile;
use crate::runner::cli::IoBackend;
use crate::runner::io::SimulatorIO;
use crate::runner::memory::io::MemorySimIO;
const DEFAULT_CACHE_SIZE: usize = 2000;
use super::cli::SimulatorCLI;

/// Pre-create attached DB files with MVCC journal mode so that journal modes
/// are compatible when ATTACH happens later during simulation.
fn enable_mvcc_on_attached_dbs(io: &Arc<dyn SimIO>, aux_paths: impl Iterator<Item = PathBuf>) {
    for aux_path in aux_paths {
        let aux_db = Database::open_file_with_flags(
            io.clone(),
            aux_path.to_str().unwrap(),
            turso_core::OpenFlags::default(),
            turso_core::DatabaseOpts::new().with_attach(true),
            None,
        )
        .unwrap_or_else(|e| panic!("Failed to open aux DB {aux_path:?}: {e}"));
        let aux_conn = aux_db
            .connect()
            .expect("Failed to connect to aux DB for MVCC setup");
        aux_conn
            .execute("PRAGMA journal_mode = 'experimental_mvcc'")
            .expect("Failed to enable MVCC on aux DB");
        aux_conn.close().expect("Failed to close aux DB connection");
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum SimulationType {
    Default,
    Doublecheck,
    Differential,
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum SimulationPhase {
    Test,
    Shrink,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TransactionMode {
    Read = 0,
    Concurrent = 1,
    Write = 2,
}

/// Represents a single operation during a transaction, applied in order.
#[derive(Debug, Clone)]
pub enum TxOperation {
    Insert {
        table_name: String,
        row: Vec<SimValue>,
    },
    Update {
        table_name: String,
        old_row: Vec<SimValue>,
        new_row: Vec<SimValue>,
    },
    Delete {
        table_name: String,
        row: Vec<SimValue>,
    },
    CreateTable {
        table: Table,
    },
    CreateIndex {
        table_name: String,
        index: sql_generation::model::table::Index,
    },
    DropIndex {
        table_name: String,
        index_name: String,
    },
    DropTable {
        table_name: String,
    },
    RenameTable {
        old_name: String,
        new_name: String,
    },
    AddColumn {
        table_name: String,
        column: sql_generation::model::table::Column,
    },
    DropColumn {
        table_name: String,
        column_index: usize,
    },
    RenameColumn {
        table_name: String,
        old_name: String,
        new_name: String,
    },
    AlterColumn {
        table_name: String,
        old_name: String,
        new_column: sql_generation::model::table::Column,
    },
    CreateView {
        view: View,
    },
    DropView {
        view_name: String,
    },
}

/// Database snapshot
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// The current state after applying transaction's changes (used for reads within the transaction)
    current_tables: Vec<Table>,
    /// The current views after applying transaction's changes
    current_views: Vec<View>,
    /// Operations recorded during this transaction, in order
    operations: Vec<TxOperation>,

    transaction_mode: TransactionMode,
}

impl Snapshot {
    #[inline]
    fn set_transaction_mode(&mut self, transaction_mode: TransactionMode) {
        self.transaction_mode = transaction_mode;
    }
}

#[derive(Debug, Clone)]
pub enum TransactionTables {
    /// Deferred transaction. Snapshot of the tables has not been taken yet
    Deferred,
    Snapshot(Snapshot),
}

impl TransactionTables {
    #[inline]
    fn into_snapshot(self) -> Option<Snapshot> {
        match self {
            TransactionTables::Deferred => None,
            TransactionTables::Snapshot(snapshot) => Some(snapshot),
        }
    }

    #[inline]
    fn as_snaphot_opt(&self) -> Option<&Snapshot> {
        match self {
            TransactionTables::Deferred => None,
            TransactionTables::Snapshot(snapshot) => Some(snapshot),
        }
    }

    #[inline]
    fn as_snapshot_mut_opt(&mut self) -> Option<&mut Snapshot> {
        match self {
            TransactionTables::Deferred => None,
            TransactionTables::Snapshot(snapshot) => Some(snapshot),
        }
    }

    #[inline]
    fn expect_snaphot(&self) -> &Snapshot {
        self.as_snaphot_opt()
            .expect("snapshot must have been taken already")
    }

    #[inline]
    fn expect_snapshot_mut(&mut self) -> &mut Snapshot {
        self.as_snapshot_mut_opt()
            .expect("snapshot must have been taken already")
    }

    #[inline]
    pub fn record_insert(&mut self, table_name: String, row: Vec<SimValue>) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::Insert { table_name, row });
    }

    #[inline]
    pub fn record_update(
        &mut self,
        table_name: String,
        old_row: Vec<SimValue>,
        new_row: Vec<SimValue>,
    ) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::Update {
                table_name,
                old_row,
                new_row,
            });
    }

    #[inline]
    pub fn record_delete(&mut self, table_name: String, row: Vec<SimValue>) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::Delete { table_name, row });
    }

    pub fn record_create_table(&mut self, table: Table) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::CreateTable { table });
    }

    pub fn record_create_index(
        &mut self,
        table_name: String,
        index: sql_generation::model::table::Index,
    ) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::CreateIndex { table_name, index });
    }

    pub fn record_drop_index(&mut self, table_name: String, index_name: String) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::DropIndex {
                table_name,
                index_name,
            });
    }

    #[inline]
    pub fn record_drop_table(&mut self, table_name: String) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::DropTable { table_name });
    }

    #[inline]
    pub fn record_rename_table(&mut self, old_name: String, new_name: String) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::RenameTable { old_name, new_name });
    }

    pub fn record_add_column(
        &mut self,
        table_name: String,
        column: sql_generation::model::table::Column,
    ) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::AddColumn { table_name, column });
    }

    pub fn record_drop_column(&mut self, table_name: String, column_index: usize) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::DropColumn {
                table_name,
                column_index,
            });
    }

    pub fn record_rename_column(&mut self, table_name: String, old_name: String, new_name: String) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::RenameColumn {
                table_name,
                old_name,
                new_name,
            });
    }

    pub fn record_alter_column(
        &mut self,
        table_name: String,
        old_name: String,
        new_column: sql_generation::model::table::Column,
    ) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::AlterColumn {
                table_name,
                old_name,
                new_column,
            });
    }

    pub fn record_create_view(&mut self, view: View) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::CreateView { view });
    }

    pub fn record_drop_view(&mut self, view_name: String) {
        self.expect_snapshot_mut()
            .operations
            .push(TxOperation::DropView { view_name });
    }
}

#[derive(Debug)]
pub struct ShadowTables<'a> {
    commited_tables: &'a Vec<Table>,
    commited_views: &'a Vec<View>,
    transaction_tables: Option<&'a TransactionTables>,
}

#[derive(Debug)]
pub struct ShadowTablesMut<'a> {
    commited_tables: &'a mut Vec<Table>,
    commited_views: &'a mut Vec<View>,
    transaction_tables: &'a mut Option<TransactionTables>,
}

impl<'a> ShadowTables<'a> {
    fn tables(&self) -> &'a Vec<Table> {
        self.transaction_tables
            .and_then(|v| v.as_snaphot_opt())
            .map_or(self.commited_tables, |v| &v.current_tables)
    }

    fn views(&self) -> &'a Vec<View> {
        self.transaction_tables
            .and_then(|v| v.as_snaphot_opt())
            .map_or(self.commited_views, |v| &v.current_views)
    }
}

impl<'a> Deref for ShadowTables<'a> {
    type Target = Vec<Table>;

    fn deref(&self) -> &Self::Target {
        self.tables()
    }
}

impl<'a, 'b> ShadowTablesMut<'a>
where
    'a: 'b,
{
    fn tables(&'a self) -> &'a Vec<Table> {
        self.transaction_tables
            .as_ref()
            .map_or(self.commited_tables, |v| &v.expect_snaphot().current_tables)
    }

    fn tables_mut(&'b mut self) -> &'b mut Vec<Table> {
        self.transaction_tables
            .as_mut()
            .map_or(self.commited_tables, |v| {
                &mut v.expect_snapshot_mut().current_tables
            })
    }

    /// Find a view by name and return a clone of its Select definition.
    /// Uses a short-lived borrow so callers can still use &mut self afterwards.
    pub fn find_view_select(&self, name: &str) -> Option<(Select, bool)> {
        let views = self
            .transaction_tables
            .as_ref()
            .map_or(&*self.commited_views, |v| &v.expect_snaphot().current_views);
        views
            .iter()
            .find(|v| v.name == name)
            .map(|v| (v.select.clone(), v.materialized))
    }

    #[allow(dead_code)]
    pub fn views(&'a self) -> &'a Vec<View> {
        self.transaction_tables
            .as_ref()
            .map_or(self.commited_views, |v| &v.expect_snaphot().current_views)
    }

    pub fn views_mut(&'b mut self) -> &'b mut Vec<View> {
        self.transaction_tables
            .as_mut()
            .map_or(self.commited_views, |v| {
                &mut v.expect_snapshot_mut().current_views
            })
    }

    /// Record that a row was inserted during the current transaction
    pub fn record_insert(&mut self, table_name: String, row: Vec<SimValue>) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_insert(table_name, row);
        }
    }

    pub fn record_update(
        &mut self,
        table_name: String,
        old_row: Vec<SimValue>,
        new_row: Vec<SimValue>,
    ) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_update(table_name, old_row, new_row);
        }
    }

    /// Record that a row was deleted during the current transaction
    pub fn record_delete(&mut self, table_name: String, row: Vec<SimValue>) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_delete(table_name, row);
        }
    }

    /// Record that a table was created during the current transaction
    pub fn record_create_table(&mut self, table: Table) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_create_table(table);
        }
    }

    /// Record that an index was created during the current transaction
    pub fn record_create_index(
        &mut self,
        table_name: String,
        index: sql_generation::model::table::Index,
    ) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_create_index(table_name, index);
        }
    }

    /// Record that an index was dropped during the current transaction
    pub fn record_drop_index(&mut self, table_name: String, index_name: String) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_drop_index(table_name, index_name);
        }
    }

    /// Record that a table was dropped during the current transaction
    pub fn record_drop_table(&mut self, table_name: String) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_drop_table(table_name);
        }
    }

    /// Record that a table was renamed during the current transaction
    pub fn record_rename_table(&mut self, old_name: String, new_name: String) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_rename_table(old_name, new_name);
        }
    }

    /// Record that a column was added during the current transaction
    pub fn record_add_column(
        &mut self,
        table_name: String,
        column: sql_generation::model::table::Column,
    ) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_add_column(table_name, column);
        }
    }

    /// Record that a column was dropped during the current transaction
    pub fn record_drop_column(&mut self, table_name: String, column_index: usize) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_drop_column(table_name, column_index);
        }
    }

    /// Record that a column was renamed during the current transaction
    pub fn record_rename_column(&mut self, table_name: String, old_name: String, new_name: String) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_rename_column(table_name, old_name, new_name);
        }
    }

    /// Record that a column was altered during the current transaction
    pub fn record_alter_column(
        &mut self,
        table_name: String,
        old_name: String,
        new_column: sql_generation::model::table::Column,
    ) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_alter_column(table_name, old_name, new_column);
        }
    }

    /// Record that a view was created during the current transaction
    pub fn record_create_view(&mut self, view: View) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_create_view(view);
        }
    }

    /// Record that a view was dropped during the current transaction
    pub fn record_drop_view(&mut self, view_name: String) {
        if let Some(txn) = &mut *self.transaction_tables {
            txn.record_drop_view(view_name);
        }
    }

    /// Tries to upgrade the Transaction Mode
    #[inline]
    pub fn upgrade_transaction(&mut self, query: &Query) {
        let transaction_mode = if query.is_write() {
            TransactionMode::Write
        } else if query.is_select() {
            TransactionMode::Read
        } else {
            return;
        };
        if let Some(txn) = self.transaction_tables.as_mut() {
            match txn {
                TransactionTables::Deferred => self.create_snapshot(transaction_mode),
                TransactionTables::Snapshot(snapshot) => {
                    match (snapshot.transaction_mode, transaction_mode) {
                        (_, TransactionMode::Concurrent) => {
                            unreachable!();
                        }
                        (TransactionMode::Read, TransactionMode::Write) => {
                            snapshot.set_transaction_mode(transaction_mode)
                        }
                        (TransactionMode::Concurrent, TransactionMode::Write) => {
                            if query.is_ddl() {
                                // Only upgrade on DDL for MVCC as MVCC requires exclusive TX for DDL statements
                                snapshot.set_transaction_mode(transaction_mode)
                            }
                        }
                        _ => {}
                    };
                }
            }
        }
    }

    #[inline]
    pub fn create_deferred_snapshot(&mut self) {
        *self.transaction_tables = Some(TransactionTables::Deferred);
    }

    #[inline]
    pub fn create_snapshot(&mut self, transaction_mode: TransactionMode) {
        *self.transaction_tables = Some(TransactionTables::Snapshot(Snapshot {
            current_tables: self.commited_tables.clone(),
            current_views: self.commited_views.clone(),
            operations: Vec::new(),
            transaction_mode,
        }));
    }

    pub fn apply_snapshot(&mut self) {
        if let Some(transaction_tables) = self
            .transaction_tables
            .take()
            .and_then(|transaction_tables| transaction_tables.into_snapshot())
        {
            // Build a mapping from any table name used during the transaction to its final name.
            // This is needed because operations store the table name at the time they were recorded,
            // but transaction_tables.current_tables has tables with their final names.
            let mut name_to_final: HashMap<String, String> = HashMap::new();
            for op in &transaction_tables.operations {
                if let TxOperation::RenameTable { old_name, new_name } = op {
                    name_to_final.insert(old_name.clone(), new_name.clone());
                    // Update all existing mappings that pointed to old_name
                    for v in name_to_final.values_mut() {
                        if v == old_name {
                            v.clone_from(new_name);
                        }
                    }
                }
            }

            // Apply all operations in recorded order.
            // This ensures operations like CREATE TABLE, ADD COLUMN, DELETE are applied correctly
            // where DELETE sees rows with the same shape as when it was recorded.
            for op in &transaction_tables.operations {
                match op {
                    TxOperation::Insert { table_name, row } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == table_name)
                            .expect("Table should exist in committed tables");
                        committed.rows.push(row.clone());
                    }
                    TxOperation::Update {
                        table_name,
                        old_row,
                        new_row,
                    } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == table_name)
                            .expect("Table should exist in committed tables");

                        let Some(pos) = committed.rows.iter().position(|r| r == old_row) else {
                            panic!(
                                "failed to apply UPDATE to table '{}': row not found (old_row={:?}, committed_rows={})",
                                table_name,
                                old_row,
                                committed.rows.len()
                            );
                        };
                        committed.rows[pos].clone_from(new_row);
                    }
                    TxOperation::Delete { table_name, row } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == table_name)
                            .expect("Table should exist in committed tables");
                        if let Some(pos) = committed.rows.iter().position(|r| r == row) {
                            committed.rows.remove(pos);
                        }
                    }
                    TxOperation::CreateTable { table } => {
                        self.commited_tables.push(table.clone());
                    }
                    TxOperation::CreateIndex { table_name, index } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == table_name)
                            .expect("Table should exist in committed tables");
                        committed.indexes.push(index.clone());
                    }
                    TxOperation::DropIndex {
                        table_name,
                        index_name,
                    } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == table_name)
                            .expect("Table should exist in committed tables");
                        committed.indexes.retain(|i| &i.index_name != index_name);
                    }
                    TxOperation::DropTable { table_name } => {
                        self.commited_tables.retain(|t| &t.name != table_name);
                    }
                    TxOperation::RenameTable { old_name, new_name } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == old_name)
                            .expect("Table should exist in committed tables");
                        committed.name.clone_from(new_name);
                    }
                    TxOperation::AddColumn { table_name, column } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == table_name)
                            .expect("Table should exist in committed tables");
                        // Use the final name to look up in transaction_tables (which has current state).
                        // Note: current_tables represents the FINAL state after all transaction
                        // operations, not intermediate states. If a table was dropped later in
                        // the same transaction, it won't exist in current_tables even though
                        // we're processing an earlier AddColumn operation. In that case, we skip
                        // the sanity check but still apply the column change to committed_tables
                        // (which will be removed when we later process the DropTable operation).
                        let final_name = name_to_final.get(table_name).unwrap_or(table_name);
                        if let Some(txn_table) = transaction_tables
                            .current_tables
                            .iter()
                            .find(|t| &t.name == final_name)
                        {
                            assert!(
                                txn_table.columns.len() > committed.columns.len(),
                                "Transaction table should have more columns than committed table"
                            );
                        }
                        committed.columns.push(column.clone());
                        let new_col_count = committed.columns.len();
                        // Add NULL only for rows that need it.
                        // Rows inserted after ADD COLUMN in the same transaction
                        // already have the correct number of values.
                        for row in &mut committed.rows {
                            while row.len() < new_col_count {
                                row.push(SimValue::NULL);
                            }
                        }
                    }
                    TxOperation::DropColumn {
                        table_name,
                        column_index,
                    } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == table_name)
                            .expect("Table should exist in committed tables");
                        // Use the final name to look up in transaction_tables (which has current state).
                        // See comment in AddColumn above for why this lookup is optional.
                        let final_name = name_to_final.get(table_name).unwrap_or(table_name);
                        if let Some(txn_table) = transaction_tables
                            .current_tables
                            .iter()
                            .find(|t| &t.name == final_name)
                        {
                            assert!(
                                txn_table.columns.len() < committed.columns.len(),
                                "Transaction table should have fewer columns than committed table"
                            );
                        }
                        let old_col_count = committed.columns.len();
                        committed.columns.remove(*column_index);
                        // Only remove from rows that have the old column count.
                        // Rows inserted after DROP COLUMN in the same transaction
                        // already have the correct (new) number of values.
                        for row in &mut committed.rows {
                            if row.len() == old_col_count {
                                row.remove(*column_index);
                            }
                        }
                    }
                    TxOperation::RenameColumn {
                        table_name,
                        old_name,
                        new_name,
                    } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == table_name)
                            .expect("Table should exist in committed tables");
                        let col = committed
                            .columns
                            .iter_mut()
                            .find(|c| &c.name == old_name)
                            .expect("Column should exist");
                        col.name.clone_from(new_name);
                        // Update index column names
                        for index in &mut committed.indexes {
                            for (col_name, _) in &mut index.columns {
                                if col_name == old_name {
                                    col_name.clone_from(new_name);
                                }
                            }
                        }
                    }
                    TxOperation::AlterColumn {
                        table_name,
                        old_name,
                        new_column,
                    } => {
                        let committed = self
                            .commited_tables
                            .iter_mut()
                            .find(|t| &t.name == table_name)
                            .expect("Table should exist in committed tables");
                        if let Some(col) =
                            committed.columns.iter_mut().find(|c| &c.name == old_name)
                        {
                            *col = new_column.clone();
                        }
                        // Update index column names if the column was renamed
                        for index in &mut committed.indexes {
                            for (col_name, _) in &mut index.columns {
                                if col_name == old_name {
                                    col_name.clone_from(&new_column.name);
                                }
                            }
                        }
                    }
                    TxOperation::CreateView { view } => {
                        self.commited_views.push(view.clone());
                    }
                    TxOperation::DropView { view_name } => {
                        self.commited_views.retain(|v| &v.name != view_name);
                    }
                }
            }
        }
    }

    pub fn delete_snapshot(&mut self) {
        *self.transaction_tables = None;
    }
}

impl<'a> Deref for ShadowTablesMut<'a> {
    type Target = Vec<Table>;

    fn deref(&self) -> &Self::Target {
        self.tables()
    }
}

impl<'a> DerefMut for ShadowTablesMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.tables_mut()
    }
}

pub(crate) struct SimulatorEnv {
    pub(crate) opts: SimulatorOpts,
    pub profile: Profile,
    pub(crate) connections: Vec<SimConnection>,
    pub(crate) io: Arc<dyn SimIO>,
    pub(crate) db: Option<Arc<Database>>,
    pub(crate) rng: ChaCha8Rng,

    seed: u64,
    pub(crate) paths: Paths,
    pub(crate) type_: SimulationType,
    pub(crate) phase: SimulationPhase,
    pub io_backend: IoBackend,

    /// If connection state is None, means we are not in a transaction
    pub connection_tables: Vec<Option<TransactionTables>>,
    /// Bit map indicating whether a connection has executed a query that is not transaction related
    ///
    /// E.g Select, Insert, Create
    /// and not Begin, Commit, Rollback \
    /// Has max size of 64 to accomodate 64 connections
    connection_last_query: Bitmap<64>,
    // Table data that is committed into the database or wal
    pub committed_tables: Vec<Table>,
    /// Names of attached databases (e.g. ["aux0", "aux1", "aux2"])
    pub(crate) attached_dbs: Vec<String>,
    // View data that is committed into the database
    pub committed_views: Vec<View>,
}

impl UnwindSafe for SimulatorEnv {}

impl SimulatorEnv {
    pub(crate) fn clone_without_connections(&self) -> Self {
        SimulatorEnv {
            opts: self.opts.clone(),
            io: self.io.clone(),
            db: self.db.clone(),
            rng: self.rng.clone(),
            seed: self.seed,
            paths: self.paths.clone(),
            type_: self.type_,
            phase: self.phase,
            io_backend: self.io_backend,
            profile: self.profile.clone(),
            connections: (0..self.connections.len())
                .map(|_| SimConnection::Disconnected)
                .collect(),
            // TODO: not sure if connection_tables should be recreated instead
            connection_tables: self.connection_tables.clone(),
            connection_last_query: self.connection_last_query,
            committed_tables: self.committed_tables.clone(),
            attached_dbs: self.attached_dbs.clone(),
            committed_views: self.committed_views.clone(),
        }
    }
    pub(crate) fn clear(&mut self) {
        self.clear_tables();
        self.connections.iter_mut().for_each(|c| c.disconnect());
        self.rng = ChaCha8Rng::seed_from_u64(self.opts.seed);

        let latency_prof = &self.profile.io.latency;

        let io: Arc<dyn SimIO> = match self.io_backend {
            IoBackend::Memory => Arc::new(MemorySimIO::new(
                self.opts.seed,
                self.opts.page_size,
                latency_prof.latency_probability,
                latency_prof.min_tick,
                latency_prof.max_tick,
            )),
            _ => Arc::new(
                SimulatorIO::new(
                    self.opts.seed,
                    self.opts.page_size,
                    latency_prof.latency_probability,
                    latency_prof.min_tick,
                    latency_prof.max_tick,
                    self.io_backend,
                )
                .unwrap(),
            ),
        };

        // Remove existing database file
        let db_path = self.get_db_path();
        if db_path.exists() {
            std::fs::remove_file(&db_path).unwrap();
        }

        let wal_path = db_path.with_extension("db-wal");
        if wal_path.exists() {
            std::fs::remove_file(&wal_path).unwrap();
        }

        // Remove MVCC logical log file
        let log_path = db_path.with_extension("db-log");
        if log_path.exists() {
            std::fs::remove_file(&log_path).unwrap();
        }

        // Remove attached database files
        for name in &self.attached_dbs {
            let aux_path = self.paths.aux_db(&self.type_, &self.phase, name);
            if aux_path.exists() {
                std::fs::remove_file(&aux_path).unwrap();
            }
            let aux_wal = aux_path.with_extension("db-wal");
            if aux_wal.exists() {
                std::fs::remove_file(&aux_wal).unwrap();
            }
            let aux_log = aux_path.with_extension("db-log");
            if aux_log.exists() {
                std::fs::remove_file(&aux_log).unwrap();
            }
        }

        self.db = None;

        let mut db_opts = turso_core::DatabaseOpts::new().with_autovacuum(true);
        if self.profile.enable_views || self.profile.experimental_mvcc {
            db_opts = db_opts.with_views(true);
        }
        let db = match Database::open_file_with_flags(
            io.clone(),
            db_path.to_str().unwrap(),
            turso_core::OpenFlags::default(),
            db_opts.with_attach(true),
            None,
        ) {
            Ok(db) => db,
            Err(e) => {
                tracing::error!(%e);
                panic!("error opening simulator test file {db_path:?}: {e:?}");
            }
        };

        // Re-enable MVCC mode if the profile says to use MVCC
        if self.profile.experimental_mvcc {
            let conn = db
                .connect()
                .expect("Failed to create connection for MVCC setup");
            conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
                .expect("Failed to enable MVCC mode");

            enable_mvcc_on_attached_dbs(
                &io,
                self.attached_dbs
                    .iter()
                    .map(|name| self.get_aux_db_path(name)),
            );
        }

        self.io = io;

        // Switch to MVCC mode if the profile says to use MVCC
        if self.profile.experimental_mvcc {
            let conn = db
                .connect()
                .expect("Failed to create connection for MVCC setup");
            conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
                .expect("Failed to enable MVCC mode");
            conn.close().expect("Failed to close MVCC setup connection");
        }

        self.db = Some(db);
    }

    pub(crate) fn get_db_path(&self) -> PathBuf {
        self.paths.db(&self.type_, &self.phase)
    }

    pub(crate) fn get_aux_db_path(&self, name: &str) -> PathBuf {
        self.paths.aux_db(&self.type_, &self.phase, name)
    }

    pub(crate) fn get_plan_path(&self) -> PathBuf {
        self.paths.plan(&self.type_, &self.phase)
    }

    pub(crate) fn clone_as(&self, simulation_type: SimulationType) -> Self {
        let mut env = self.clone_without_connections();
        env.type_ = simulation_type;
        env.clear();
        env
    }

    pub(crate) fn clone_at_phase(&self, phase: SimulationPhase) -> Self {
        let mut env = self.clone_without_connections();
        env.phase = phase;
        env.clear();
        env
    }

    pub fn choose_conn(&self, rng: &mut impl Rng) -> usize {
        rng.random_range(0..self.connections.len())
    }

    /// Rng only used for generating interactions. By having a separate Rng we can guarantee that a particular seed
    /// will always create the same interactions plan, regardless of the changes that happen in the Database code
    pub fn gen_rng(&self) -> ChaCha8Rng {
        // Seed + 1 so that there is no relation with the original seed, and so we have no way to accidently generate
        // the first Create statement twice in a row
        ChaCha8Rng::seed_from_u64(self.seed + 1)
    }
}

impl SimulatorEnv {
    pub(crate) fn new(
        seed: u64,
        cli_opts: &SimulatorCLI,
        paths: Paths,
        simulation_type: SimulationType,
        profile: &Profile,
    ) -> Self {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        let num_attached = rng.random_range(1..=3usize);
        let attached_dbs: Vec<String> = (0..num_attached).map(|i| format!("aux{i}")).collect();

        let mut opts = SimulatorOpts {
            seed,
            ticks: usize::MAX,
            disable_select_optimizer: cli_opts.disable_select_optimizer,
            disable_insert_values_select: cli_opts.disable_insert_values_select,
            disable_double_create_failure: cli_opts.disable_double_create_failure,
            disable_select_limit: cli_opts.disable_select_limit,
            disable_delete_select: cli_opts.disable_delete_select,
            disable_drop_select: cli_opts.disable_drop_select,
            disable_where_true_false_null: cli_opts.disable_where_true_false_null,
            disable_union_all_preserves_cardinality: cli_opts
                .disable_union_all_preserves_cardinality,
            disable_fsync_no_wait: cli_opts.disable_fsync_no_wait,
            disable_faulty_query: cli_opts.disable_faulty_query,
            page_size: 4096, // TODO: randomize this too
            max_interactions: rng.random_range(cli_opts.minimum_tests..=cli_opts.maximum_tests),
            max_time_simulation: cli_opts.maximum_time,
            disable_reopen_database: cli_opts.disable_reopen_database,
            disable_integrity_check: cli_opts.disable_integrity_check,
            cache_size: profile.cache_size_pages.unwrap_or(DEFAULT_CACHE_SIZE),
        };

        // Remove existing database file if it exists
        let db_path = paths.db(&simulation_type, &SimulationPhase::Test);

        if db_path.exists() {
            std::fs::remove_file(&db_path).unwrap();
        }

        let wal_path = db_path.with_extension("db-wal");
        if wal_path.exists() {
            std::fs::remove_file(&wal_path).unwrap();
        }

        // Remove MVCC logical log file
        let log_path = db_path.with_extension("db-log");
        if log_path.exists() {
            std::fs::remove_file(&log_path).unwrap();
        }

        // Remove attached database files if they exist
        for name in &attached_dbs {
            let aux_path = paths.aux_db(&simulation_type, &SimulationPhase::Test, name);
            if aux_path.exists() {
                std::fs::remove_file(&aux_path).unwrap();
            }
            let aux_wal = aux_path.with_extension("db-wal");
            if aux_wal.exists() {
                std::fs::remove_file(&aux_wal).unwrap();
            }
            let aux_log = aux_path.with_extension("db-log");
            if aux_log.exists() {
                std::fs::remove_file(&aux_log).unwrap();
            }
        }

        let mut profile = profile.clone();
        // Conditionals here so that we can override some profile options from the CLI
        if let Some(mvcc) = cli_opts.experimental_mvcc {
            profile.experimental_mvcc = mvcc;
        }
        if let Some(latency_prob) = cli_opts.latency_probability {
            profile.io.latency.latency_probability = latency_prob;
        }
        if let Some(max_tick) = cli_opts.max_tick {
            profile.io.latency.max_tick = max_tick;
        }
        if let Some(min_tick) = cli_opts.min_tick {
            profile.io.latency.min_tick = min_tick;
        }
        if cli_opts.differential {
            // Disable faults when running against sqlite as we cannot control faults on it
            profile.io.enable = false;
            // Disable limits due to differences in return order from turso and rusqlite
            opts.disable_select_limit = true;

            // There is no `ALTER COLUMN` in SQLite
            profile.query.gen_opts.query.alter_table.alter_column = false;
        }

        profile.validate().unwrap();

        let latency_prof = &profile.io.latency;

        let io_backend = cli_opts.io_backend;
        let io: Arc<dyn SimIO> = match io_backend {
            IoBackend::Memory => Arc::new(MemorySimIO::new(
                opts.seed,
                opts.page_size,
                latency_prof.latency_probability,
                latency_prof.min_tick,
                latency_prof.max_tick,
            )),
            _ => Arc::new(
                SimulatorIO::new(
                    opts.seed,
                    opts.page_size,
                    latency_prof.latency_probability,
                    latency_prof.min_tick,
                    latency_prof.max_tick,
                    io_backend,
                )
                .unwrap(),
            ),
        };

        let mut db_opts = turso_core::DatabaseOpts::new().with_autovacuum(true);
        if profile.enable_views || profile.experimental_mvcc {
            db_opts = db_opts.with_views(true);
        }
        let db = match Database::open_file_with_flags(
            io.clone(),
            db_path.to_str().unwrap(),
            turso_core::OpenFlags::default(),
            db_opts.with_attach(true),
            None,
        ) {
            Ok(db) => db,
            Err(e) => {
                panic!("error opening simulator test file {db_path:?}: {e:?}");
            }
        };

        // Switch to MVCC mode if the profile says to use MVCC
        if profile.experimental_mvcc {
            let conn = db
                .connect()
                .expect("Failed to create connection for MVCC setup");
            conn.execute("PRAGMA journal_mode = 'experimental_mvcc'")
                .expect("Failed to enable MVCC mode");

            enable_mvcc_on_attached_dbs(
                &io,
                attached_dbs
                    .iter()
                    .map(|name| paths.aux_db(&simulation_type, &SimulationPhase::Test, name)),
            );
            conn.close().expect("Failed to close MVCC setup connection");
        }

        let connections = (0..profile.max_connections)
            .map(|_| SimConnection::Disconnected)
            .collect::<Vec<_>>();

        SimulatorEnv {
            opts,
            connections,
            paths,
            rng,
            seed,
            io,
            db: Some(db),
            type_: simulation_type,
            phase: SimulationPhase::Test,
            io_backend,
            profile: profile.clone(),
            committed_tables: Vec::new(),
            committed_views: Vec::new(),
            connection_tables: vec![None; profile.max_connections],
            connection_last_query: Bitmap::new(),
            attached_dbs,
        }
    }

    pub(crate) fn connect(&mut self, connection_index: usize) {
        if connection_index >= self.connections.len() {
            panic!("connection index out of bounds");
        }

        if self.connections[connection_index].is_connected() {
            trace!("Connection {connection_index} is already connected, skipping reconnection");
            return;
        }

        match self.type_ {
            SimulationType::Default | SimulationType::Doublecheck => {
                let conn = self
                    .db
                    .as_ref()
                    .expect("db to be Some")
                    .connect()
                    .expect("Failed to connect to Limbo database");
                if self.opts.cache_size != DEFAULT_CACHE_SIZE {
                    conn.execute(format!("PRAGMA cache_size = {}", self.opts.cache_size))
                        .expect("set pragma cache_size");
                }
                self.connections[connection_index] = SimConnection::LimboConnection(conn);
            }
            SimulationType::Differential => {
                self.connections[connection_index] = SimConnection::SQLiteConnection(
                    rusqlite::Connection::open(self.get_db_path())
                        .expect("Failed to open SQLite connection"),
                );
            }
        };

        self.attach_databases(connection_index);
    }

    fn attach_databases(&self, connection_index: usize) {
        for name in &self.attached_dbs {
            let aux_path = self.get_aux_db_path(name);
            match &self.connections[connection_index] {
                SimConnection::LimboConnection(conn) => {
                    conn.execute(format!("ATTACH '{}' AS {name}", aux_path.display()))
                        .unwrap_or_else(|e| panic!("Failed to ATTACH {name}: {e}"));
                }
                SimConnection::SQLiteConnection(conn) => {
                    conn.execute(&format!("ATTACH '{}' AS {name}", aux_path.display()), [])
                        .unwrap_or_else(|e| panic!("Failed to ATTACH {name} on SQLite: {e}"));
                }
                SimConnection::Disconnected => {}
            }
        }
    }

    /// Clears the commited tables and the connection tables
    pub fn clear_tables(&mut self) {
        self.committed_tables.clear();
        self.connection_tables.iter_mut().for_each(|t| *t = None);
        self.connection_last_query = Bitmap::new();
    }

    // TODO: does not yet create the appropriate context to avoid WriteWriteConflitcs
    pub fn connection_context(&self, conn_index: usize) -> impl GenerationContext {
        struct ConnectionGenContext<'a> {
            tables: &'a Vec<sql_generation::model::table::Table>,
            views: &'a Vec<View>,
            opts: &'a sql_generation::generation::Opts,
        }

        impl<'a> GenerationContext for ConnectionGenContext<'a> {
            fn tables(&self) -> &Vec<sql_generation::model::table::Table> {
                self.tables
            }

            fn views(&self) -> &Vec<View> {
                self.views
            }

            fn opts(&self) -> &sql_generation::generation::Opts {
                self.opts
            }
        }

        let shadow = self.get_conn_tables(conn_index);
        let tables = shadow.tables();
        let views = shadow.views();

        ConnectionGenContext {
            opts: &self.profile.query.gen_opts,
            tables,
            views,
        }
    }

    pub fn conn_in_transaction(&self, conn_index: usize) -> bool {
        self.connection_tables
            .get(conn_index)
            .is_some_and(|t| t.is_some())
    }

    pub fn has_conn_executed_query_after_transaction(&self, conn_index: usize) -> bool {
        self.connection_last_query.get(conn_index)
    }

    pub fn update_conn_last_interaction(&mut self, conn_index: usize, query: Option<&Query>) {
        // If the conn will execute a transaction statement then we set the bitmap to false
        // to indicate we have not executed any queries yet after the transaction begun
        let value = query.is_some_and(|query| {
            matches!(
                query,
                Query::Begin(..) | Query::Commit(..) | Query::Rollback(..)
            )
        });
        self.connection_last_query.set(conn_index, value);
    }

    pub fn rollback_conn(&mut self, conn_index: usize) {
        Rollback.shadow(&mut self.get_conn_tables_mut(conn_index));
        self.update_conn_last_interaction(conn_index, Some(&Query::Rollback(Rollback)));
    }

    pub fn get_conn_tables(&self, conn_index: usize) -> ShadowTables<'_> {
        ShadowTables {
            transaction_tables: self.connection_tables.get(conn_index).unwrap().as_ref(),
            commited_tables: &self.committed_tables,
            commited_views: &self.committed_views,
        }
    }

    pub fn get_conn_tables_mut(&mut self, conn_index: usize) -> ShadowTablesMut<'_> {
        ShadowTablesMut {
            transaction_tables: self.connection_tables.get_mut(conn_index).unwrap(),
            commited_tables: &mut self.committed_tables,
            commited_views: &mut self.committed_views,
        }
    }
}

pub(crate) enum SimConnection {
    LimboConnection(Arc<turso_core::Connection>),
    SQLiteConnection(rusqlite::Connection),
    Disconnected,
}

impl SimConnection {
    pub(crate) fn is_connected(&self) -> bool {
        match self {
            SimConnection::LimboConnection(_) | SimConnection::SQLiteConnection(_) => true,
            SimConnection::Disconnected => false,
        }
    }

    pub(crate) fn disconnect(&mut self) {
        let conn = mem::replace(self, SimConnection::Disconnected);

        match conn {
            SimConnection::LimboConnection(conn) => {
                conn.close().unwrap();
            }
            SimConnection::SQLiteConnection(conn) => {
                conn.close().unwrap();
            }
            SimConnection::Disconnected => {}
        }
    }
}

impl Display for SimConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SimConnection::LimboConnection(_) => {
                write!(f, "LimboConnection")
            }
            SimConnection::SQLiteConnection(_) => {
                write!(f, "SQLiteConnection")
            }
            SimConnection::Disconnected => {
                write!(f, "Disconnected")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SimulatorOpts {
    pub(crate) seed: u64,
    pub(crate) ticks: usize,

    pub(crate) disable_select_optimizer: bool,
    pub(crate) disable_insert_values_select: bool,
    pub(crate) disable_double_create_failure: bool,
    pub(crate) disable_select_limit: bool,
    pub(crate) disable_delete_select: bool,
    pub(crate) disable_drop_select: bool,
    pub(crate) disable_where_true_false_null: bool,
    pub(crate) disable_union_all_preserves_cardinality: bool,
    pub(crate) disable_fsync_no_wait: bool,
    pub(crate) disable_faulty_query: bool,
    pub(crate) disable_reopen_database: bool,
    pub(crate) disable_integrity_check: bool,

    pub(crate) max_interactions: u32,
    pub(crate) page_size: usize,
    pub(crate) cache_size: usize,
    pub(crate) max_time_simulation: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct Paths {
    pub(crate) base: PathBuf,
    pub(crate) history: PathBuf,
}

impl Paths {
    pub(crate) fn new(output_dir: &Path) -> Self {
        Paths {
            base: output_dir.to_path_buf(),
            history: PathBuf::from(output_dir).join("history.txt"),
        }
    }

    fn path_(&self, type_: &SimulationType, phase: &SimulationPhase) -> PathBuf {
        match (type_, phase) {
            (SimulationType::Default, SimulationPhase::Test) => self.base.join(Path::new("test")),
            (SimulationType::Default, SimulationPhase::Shrink) => {
                self.base.join(Path::new("shrink"))
            }
            (SimulationType::Differential, SimulationPhase::Test) => {
                self.base.join(Path::new("diff"))
            }
            (SimulationType::Differential, SimulationPhase::Shrink) => {
                self.base.join(Path::new("diff_shrink"))
            }
            (SimulationType::Doublecheck, SimulationPhase::Test) => {
                self.base.join(Path::new("doublecheck"))
            }
            (SimulationType::Doublecheck, SimulationPhase::Shrink) => {
                self.base.join(Path::new("doublecheck_shrink"))
            }
        }
    }

    pub(crate) fn db(&self, type_: &SimulationType, phase: &SimulationPhase) -> PathBuf {
        self.path_(type_, phase).with_extension("db")
    }

    pub(crate) fn aux_db(
        &self,
        type_: &SimulationType,
        phase: &SimulationPhase,
        name: &str,
    ) -> PathBuf {
        self.path_(type_, phase)
            .with_extension(format!("{name}.db"))
    }

    pub(crate) fn plan(&self, type_: &SimulationType, phase: &SimulationPhase) -> PathBuf {
        self.path_(type_, phase).with_extension("sql")
    }

    pub fn delete_all_files(&self) {
        if self.base.exists() {
            let res = std::fs::remove_dir_all(&self.base);
            if res.is_err() {
                tracing::error!(error = %res.unwrap_err(),"failed to remove directory");
            }
        }
    }
}
