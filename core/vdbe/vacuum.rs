use turso_macros::turso_assert;

/// A representation of a row from `sqlite_schema`.
///
/// Carries `rootpage` so we can distinguish storage-backed tables/indexes
/// (rootpage != 0) from virtual tables, views, and triggers (rootpage = 0)
#[derive(Debug, Clone)]
pub(crate) struct SchemaEntry {
    pub entry_type: SchemaEntryType,
    pub name: String,
    /// `sqlite_schema.tbl_name`: for indexes and triggers, this is the table
    /// the object belongs to; for tables and views it usually matches `name`.
    pub tbl_name: String,
    pub rootpage: i64,
    pub sql: String,
    /// Original rowid-order position from `sqlite_schema`, used to preserve
    /// creation order within each VACUUM replay phase.
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
    pub fn from_str(s: &str) -> crate::Result<Self> {
        match s {
            "table" => Ok(Self::Table),
            "index" => Ok(Self::Index),
            "trigger" => Ok(Self::Trigger),
            "view" => Ok(Self::View),
            other => Err(crate::error::LimboError::Corrupt(format!(
                "unexpected sqlite_schema type: {other}"
            ))),
        }
    }
}

impl SchemaEntry {
    /// Parse from a sqlite_schema row: (type, name, tbl_name, rootpage, sql)
    pub fn from_row(row: &crate::vdbe::Row, ordinal: usize) -> crate::Result<Self> {
        let entry_type = SchemaEntryType::from_str(row.get::<&str>(0)?)?;
        Ok(Self {
            entry_type,
            name: row.get::<&str>(1)?.to_string(),
            tbl_name: row.get::<&str>(2)?.to_string(),
            rootpage: row.get::<i64>(3)?,
            sql: row.get::<&str>(4)?.to_string(),
            ordinal,
        })
    }

    /// Whether this entry represents a storage-backed object (table or index
    /// with rootpage != 0). Note: MVCC mode uses negative rootpage values for
    /// versioned tables, so we check `!= 0` rather than `> 0`.
    pub fn is_storage_backed(&self) -> bool {
        self.rootpage != 0
    }

    pub fn is_sqlite_sequence(&self) -> bool {
        self.name == "sqlite_sequence"
    }
}

/// Classify schema entries, in a specific order required, for vacuum op.
/// Returns `(tables_to_create, tables_to_copy, indexes_to_create,
/// post_data_entries)`.
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
                if !entry.is_sqlite_sequence() {
                    tables_to_create.push(entry);
                }
                // All storage-backed tables get their data copied including sqlite_stat1 and
                // other internal storage-backed tables.
                // sqlite_sequence data copy is handled specially by the caller
                // (only if destination materialized it).
                tables_to_copy.push(entry);
            }
            SchemaEntryType::Index if entry.is_storage_backed() => {
                // User-defined secondary index: we do it after we copy data
                // for performance (avoids maintaining indexes during
                // bulk insert).
                indexes_to_create.push(entry);
            }
            SchemaEntryType::Trigger | SchemaEntryType::View => {
                // Triggers and views are replayed after data copy to avoid
                // triggers firing during the copy phase.
                post_data_entries.push(entry);
            }
            SchemaEntryType::Table => {
                // Virtual tables (rootpage=0, type=table) land here.
                // Replayed after data copy alongside triggers and views.
                turso_assert!(
                    !entry.is_storage_backed(),
                    "unexpected storage-backed table with rootpage=0: {entry.name}"
                );
                post_data_entries.push(entry);
            }
            SchemaEntryType::Index => {
                // Custom index-method indexes (FTS, vector, etc.) have rootpage=0
                // in sqlite_schema because their storage is managed by the index
                // method, not a B-tree (see translate/index.rs:253).
                // Replayed after data copy.
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
