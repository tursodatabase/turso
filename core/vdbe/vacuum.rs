//! Shared vacuum temp-build types and helpers.
//!
//! This module contains the reusable types for the vacuum temp-build pipeline
//! shared between `VACUUM INTO` and plain `VACUUM`. The actual opcode
//! implementations live in `execute.rs` and delegate to these types for
//! schema classification and phase ordering.

use crate::numeric::Numeric;
use crate::types::Value;

/// A typed representation of a row from `sqlite_schema`.
///
/// Carries `rootpage` so we can distinguish storage-backed tables/indexes
/// (rootpage != 0) from virtual tables, views, and triggers (rootpage = 0)
/// without relying on name-prefix heuristics.
#[derive(Debug, Clone)]
pub(crate) struct SchemaEntry {
    pub entry_type: SchemaEntryType,
    pub name: String,
    pub rootpage: i64,
    pub sql: String,
    /// Original rowid-order position from `sqlite_schema`, used to preserve
    /// creation order within each replay phase.
    pub ordinal: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SchemaEntryType {
    Table,
    Index,
    Trigger,
    View,
}

impl SchemaEntryType {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "table" => Some(Self::Table),
            "index" => Some(Self::Index),
            "trigger" => Some(Self::Trigger),
            "view" => Some(Self::View),
            _ => None,
        }
    }
}

impl SchemaEntry {
    /// Parse a schema entry from a row of values in the order:
    /// `(type, name, tbl_name, rootpage, sql)`.
    pub fn from_row(values: &[Value], ordinal: usize) -> Option<Self> {
        if values.len() < 5 {
            return None;
        }
        let entry_type = match &values[0] {
            Value::Text(t) => SchemaEntryType::from_str(t.as_str())?,
            _ => return None,
        };
        let name = match &values[1] {
            Value::Text(t) => t.as_str().to_string(),
            _ => return None,
        };
        // values[2] is tbl_name — skip, not needed for current phases.
        let rootpage = match &values[3] {
            Value::Numeric(Numeric::Integer(n)) => *n,
            _ => return None,
        };
        let sql = match &values[4] {
            Value::Text(t) => t.as_str().to_string(),
            _ => return None,
        };
        Some(Self {
            entry_type,
            name,
            rootpage,
            sql,
            ordinal,
        })
    }

    /// Whether this entry represents a storage-backed object (table or index
    /// with rootpage != 0). Note: MVCC mode uses negative rootpage values for
    /// versioned tables, so we check `!= 0` rather than `> 0`.
    pub fn is_storage_backed(&self) -> bool {
        self.rootpage != 0
    }

    /// Whether this is `sqlite_sequence` (needs special ordering care).
    pub fn is_sqlite_sequence(&self) -> bool {
        self.name == "sqlite_sequence"
    }
}

/// Classify schema entries into ordered replay phases for the temp-build
/// pipeline. Returns `(tables_to_create, tables_to_copy, indexes_to_create,
/// post_data_entries)`.
///
/// Phase ordering:
/// 1. Create storage-backed tables (rootpage != 0, type=table), excluding
///    `sqlite_sequence` (auto-created by AUTOINCREMENT table creation)
/// 2. Copy table data for all storage-backed tables (including sqlite_sequence
///    if destination materialized it, and including sqlite_stat1 etc.)
/// 3. Create user-defined secondary indexes (type=index, rootpage != 0,
///    sql IS NOT NULL — autoindexes are created implicitly by CREATE TABLE)
/// 4. Replay views, triggers, and rootpage=0 schema objects
///
/// Within each phase, entries preserve their original `sqlite_schema` rowid
/// order.
pub(crate) fn classify_schema_entries(
    entries: &[SchemaEntry],
) -> (
    Vec<&SchemaEntry>,
    Vec<&SchemaEntry>,
    Vec<&SchemaEntry>,
    Vec<&SchemaEntry>,
) {
    let mut tables_to_create: Vec<&SchemaEntry> = Vec::new();
    let mut tables_to_copy: Vec<&SchemaEntry> = Vec::new();
    let mut indexes_to_create: Vec<&SchemaEntry> = Vec::new();
    let mut post_data_entries: Vec<&SchemaEntry> = Vec::new();

    for entry in entries {
        match entry.entry_type {
            SchemaEntryType::Table if entry.is_storage_backed() => {
                // Storage-backed table.
                // Skip sqlite_sequence in the schema creation phase. When we
                // create an AUTOINCREMENT table, Turso automatically creates
                // sqlite_sequence if it doesn't exist (see translate/schema.rs).
                // Since entries are ordered by rowid, an AUTOINCREMENT table may
                // appear before sqlite_sequence. If we create that table first
                // (which auto-creates sqlite_sequence), then later try to run
                // "CREATE TABLE sqlite_sequence(name,seq)", it fails with
                // "table already exists".
                // We still copy sqlite_sequence data in the copy phase to
                // preserve AUTOINCREMENT counters.
                if !entry.is_sqlite_sequence() {
                    tables_to_create.push(entry);
                }
                // All storage-backed tables get their data copied in phase 2,
                // including sqlite_stat1 and other internal storage-backed tables.
                // sqlite_sequence data copy is handled specially by the caller
                // (only if destination materialized it).
                tables_to_copy.push(entry);
            }
            SchemaEntryType::Index if entry.is_storage_backed() => {
                // User-defined secondary index — defer creation until after
                // data copy for performance (avoids maintaining indexes during
                // bulk insert).
                indexes_to_create.push(entry);
            }
            SchemaEntryType::Trigger | SchemaEntryType::View => {
                // Triggers and views are replayed after data copy to avoid
                // triggers firing during the copy phase.
                post_data_entries.push(entry);
            }
            _ => {
                // rootpage=0 tables (virtual tables) and rootpage=0 indexes
                // (autoindexes — but these have sql=NULL and are already
                // filtered out by the schema query). Treat remaining
                // rootpage=0 entries as post-data.
                post_data_entries.push(entry);
            }
        }
    }

    (
        tables_to_create,
        tables_to_copy,
        indexes_to_create,
        post_data_entries,
    )
}
