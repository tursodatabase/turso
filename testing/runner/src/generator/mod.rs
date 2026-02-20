//! Database generator module for creating test databases with fake data.
//!
//! This module provides functionality to generate SQLite databases populated
//! with fake user and product data for testing purposes.

use crate::backends::DefaultDatabaseResolver;
use crate::parser::ast::{DatabaseLocation, TestFile};
use anyhow::{Context, Result};
use fake::Dummy;
use fake::Fake;
use fake::faker::address::en::{CityName, StateAbbr, StreetName, ZipCode};
use fake::faker::internet::en::SafeEmail;
use fake::faker::name::en::{FirstName, LastName};
use fake::faker::phone_number::en::PhoneNumber;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use turso::{Builder, Connection};

/// Product list for generating product data
const PRODUCT_LIST: &[&str] = &[
    "hat",
    "cap",
    "shirt",
    "sweater",
    "sweatshirt",
    "shorts",
    "jeans",
    "sneakers",
    "boots",
    "coat",
    "accessories",
];

pub const INTEGRITY_FIXTURE_MISSING_INDEX_ENTRY_REL_PATH: &str =
    "database/integrity_missing_index_entry.db";
pub const INTEGRITY_FIXTURE_MISSING_EXPRESSION_INDEX_ENTRY_REL_PATH: &str =
    "database/integrity_missing_expression_index_entry.db";
pub const INTEGRITY_FIXTURE_MISSING_PARTIAL_INDEX_ENTRY_REL_PATH: &str =
    "database/integrity_missing_partial_index_entry.db";
pub const INTEGRITY_FIXTURE_CHECK_CONSTRAINT_VIOLATION_REL_PATH: &str =
    "database/integrity_check_constraint_violation.db";
pub const INTEGRITY_FIXTURE_CHECK_CONSTRAINT_VIOLATION_QUICK_REL_PATH: &str =
    "database/integrity_check_constraint_violation_quick.db";
pub const INTEGRITY_FIXTURE_NOT_NULL_VIOLATION_REL_PATH: &str =
    "database/integrity_not_null_violation.db";
pub const INTEGRITY_FIXTURE_NON_UNIQUE_INDEX_ENTRY_REL_PATH: &str =
    "database/integrity_non_unique_index_entry.db";
pub const INTEGRITY_FIXTURE_MISSING_UNIQUE_INDEX_ENTRY_REL_PATH: &str =
    "database/integrity_missing_unique_index_entry.db";
pub const INTEGRITY_FIXTURE_FREELIST_COUNT_MISMATCH_REL_PATH: &str =
    "database/integrity_freelist_count_mismatch.db";
pub const INTEGRITY_FIXTURE_FREELIST_TRUNK_CORRUPT_REL_PATH: &str =
    "database/integrity_freelist_trunk_corrupt.db";
pub const INTEGRITY_FIXTURE_OVERFLOW_LIST_LENGTH_MISMATCH_REL_PATH: &str =
    "database/integrity_overflow_list_length_mismatch.db";
pub const INTEGRITY_FIXTURE_VALUES_DIFFER_NOCASE_REL_PATH: &str =
    "database/integrity_values_differ_nocase.db";

pub const INTEGRITY_FIXTURE_RELATIVE_PATHS: &[&str] = &[
    INTEGRITY_FIXTURE_MISSING_INDEX_ENTRY_REL_PATH,
    INTEGRITY_FIXTURE_MISSING_EXPRESSION_INDEX_ENTRY_REL_PATH,
    INTEGRITY_FIXTURE_MISSING_PARTIAL_INDEX_ENTRY_REL_PATH,
    INTEGRITY_FIXTURE_CHECK_CONSTRAINT_VIOLATION_REL_PATH,
    INTEGRITY_FIXTURE_CHECK_CONSTRAINT_VIOLATION_QUICK_REL_PATH,
    INTEGRITY_FIXTURE_NOT_NULL_VIOLATION_REL_PATH,
    INTEGRITY_FIXTURE_NON_UNIQUE_INDEX_ENTRY_REL_PATH,
    INTEGRITY_FIXTURE_MISSING_UNIQUE_INDEX_ENTRY_REL_PATH,
    INTEGRITY_FIXTURE_FREELIST_COUNT_MISMATCH_REL_PATH,
    INTEGRITY_FIXTURE_FREELIST_TRUNK_CORRUPT_REL_PATH,
    INTEGRITY_FIXTURE_OVERFLOW_LIST_LENGTH_MISMATCH_REL_PATH,
    INTEGRITY_FIXTURE_VALUES_DIFFER_NOCASE_REL_PATH,
];

/// A fake user record
#[derive(Debug, Dummy)]
pub struct User {
    #[dummy(faker = "FirstName()")]
    pub first_name: String,
    #[dummy(faker = "LastName()")]
    pub last_name: String,
    #[dummy(faker = "SafeEmail()")]
    pub email: String,
    #[dummy(faker = "PhoneNumber()")]
    pub phone_number: String,
    #[dummy(faker = "StreetName()")]
    pub address: String,
    #[dummy(faker = "CityName()")]
    pub city: String,
    #[dummy(faker = "StateAbbr()")]
    pub state: String,
    #[dummy(faker = "ZipCode()")]
    pub zipcode: String,
    #[dummy(faker = "1..=100")]
    pub age: i64,
}

/// A product record
#[derive(Debug)]
pub struct Product {
    pub name: String,
    pub price: f64,
}

impl Product {
    fn new(name: &str, rng: &mut impl Rng) -> Self {
        Self {
            name: name.to_string(),
            price: rng.random_range(1.0..=100.0),
        }
    }
}

/// Configuration for database generation
#[derive(Debug, Clone)]
pub struct GeneratorConfig {
    /// Path to the database file
    pub db_path: String,
    /// Number of users to generate
    pub user_count: usize,
    /// Seed for reproducible random generation
    pub seed: u64,
    /// If true, use INT PRIMARY KEY instead of INTEGER PRIMARY KEY
    /// This prevents the rowid alias optimization in SQLite
    pub no_rowid_alias: bool,
    /// Enable MVCC mode (experimental journal mode)
    pub mvcc: bool,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            db_path: "database.db".to_string(),
            user_count: 10000,
            seed: 42,
            no_rowid_alias: false,
            mvcc: false,
        }
    }
}

/// Generate a database with fake user and product data
pub async fn generate_database(config: &GeneratorConfig) -> Result<()> {
    let db = Builder::new_local(&config.db_path)
        .build()
        .await
        .with_context(|| format!("failed to create database at '{}'", config.db_path))?;

    let conn = db
        .connect()
        .with_context(|| format!("failed to connect to database '{}'", config.db_path))?;

    // Enable MVCC mode if requested (must be done before any transactions)
    if config.mvcc {
        // Use query instead of execute since PRAGMA returns a result row
        let mut rows = conn
            .query("PRAGMA journal_mode = 'experimental_mvcc'", ())
            .await
            .context("failed to enable MVCC mode")?;
        // Consume the result row
        while (rows.next().await?).is_some() {}
    }

    let mut rng = ChaCha8Rng::seed_from_u64(config.seed);

    conn.execute("BEGIN", ())
        .await
        .context("failed to execute BEGIN transaction")?;

    create_tables(&conn, config.no_rowid_alias)
        .await
        .context("failed to create tables")?;

    insert_users(&conn, config.user_count, config.no_rowid_alias, &mut rng)
        .await
        .context("failed to insert users")?;

    insert_products(&conn, config.no_rowid_alias, &mut rng)
        .await
        .context("failed to insert products")?;

    conn.execute("COMMIT", ())
        .await
        .context("failed to execute COMMIT transaction")?;

    // Checkpoint to ensure data is written to the main database file.
    // This is required for SQLite to read the database with immutable=1 mode,
    // which doesn't read WAL files.
    let mut rows = conn
        .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
        .await
        .context("failed to checkpoint database")?;
    // Consume the result
    while (rows.next().await?).is_some() {}

    // Explicitly close connection and database to release locks
    drop(conn);
    drop(db);

    Ok(())
}

fn clear_existing_db_and_sidecars(db_path: &Path) -> Result<()> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create fixture directory '{}'",
                parent.to_string_lossy()
            )
        })?;
    }

    for suffix in ["", "-wal", "-shm"] {
        let p = PathBuf::from(format!("{}{}", db_path.display(), suffix));
        if p.exists() {
            std::fs::remove_file(&p).with_context(|| {
                format!("failed to remove existing fixture file '{}'", p.display())
            })?;
        }
    }
    Ok(())
}

fn remove_db_sidecars(db_path: &Path) -> Result<()> {
    for suffix in ["-wal", "-shm"] {
        let p = PathBuf::from(format!("{}{}", db_path.display(), suffix));
        if p.exists() {
            std::fs::remove_file(&p).with_context(|| {
                format!("failed to remove sidecar fixture file '{}'", p.display())
            })?;
        }
    }
    Ok(())
}

async fn checkpoint_truncate(conn: &Connection, context: &str) -> Result<()> {
    let mut rows = conn
        .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
        .await
        .with_context(|| format!("failed to checkpoint {context} WAL"))?;
    while (rows.next().await?).is_some() {}
    Ok(())
}

async fn get_page_size(conn: &Connection, context: &str) -> Result<usize> {
    let mut page_rows = conn
        .query("PRAGMA page_size", ())
        .await
        .with_context(|| format!("failed to query page_size for {context}"))?;
    let page_row = page_rows
        .next()
        .await
        .with_context(|| format!("failed to step page_size query for {context}"))?
        .with_context(|| format!("page_size query returned no rows for {context}"))?;
    Ok(page_row
        .get::<i64>(0)
        .with_context(|| format!("failed to decode page_size for {context}"))? as usize)
}

async fn get_root_page(conn: &Connection, object_name: &str, context: &str) -> Result<usize> {
    let sql = format!("SELECT rootpage FROM sqlite_schema WHERE name='{object_name}'");
    let mut root_rows = conn
        .query(&sql, ())
        .await
        .with_context(|| format!("failed to query rootpage of '{object_name}' for {context}"))?;
    let root_row = root_rows
        .next()
        .await
        .with_context(|| format!("failed to step rootpage query for {context}"))?
        .with_context(|| format!("rootpage query returned no rows for {context}"))?;
    Ok(root_row
        .get::<i64>(0)
        .with_context(|| format!("failed to decode rootpage for {context}"))? as usize)
}

async fn query_single_i64(conn: &Connection, sql: &str, context: &str) -> Result<i64> {
    let mut rows = conn
        .query(sql, ())
        .await
        .with_context(|| format!("failed to run scalar query for {context}: {sql}"))?;
    let row = rows
        .next()
        .await
        .with_context(|| format!("failed to step scalar query for {context}: {sql}"))?
        .with_context(|| format!("scalar query returned no rows for {context}: {sql}"))?;
    row.get::<i64>(0)
        .with_context(|| format!("failed to decode scalar result for {context}: {sql}"))
}

fn read_db_header_freelist_fields(db_path: &Path) -> Result<(u32, u32)> {
    let bytes = std::fs::read(db_path)
        .with_context(|| format!("failed to read fixture '{}'", db_path.display()))?;
    anyhow::ensure!(
        bytes.len() >= 40,
        "fixture '{}' is too small to contain database header freelist fields",
        db_path.display()
    );
    let trunk = u32::from_be_bytes([bytes[32], bytes[33], bytes[34], bytes[35]]);
    let count = u32::from_be_bytes([bytes[36], bytes[37], bytes[38], bytes[39]]);
    Ok((trunk, count))
}

fn patch_set_db_header_freelist_count(db_path: &Path, new_count: u32) -> Result<()> {
    let mut bytes = std::fs::read(db_path)
        .with_context(|| format!("failed to read fixture '{}'", db_path.display()))?;
    anyhow::ensure!(
        bytes.len() >= 40,
        "fixture '{}' is too small to patch freelist count",
        db_path.display()
    );
    bytes[36..40].copy_from_slice(&new_count.to_be_bytes());
    std::fs::write(db_path, bytes)
        .with_context(|| format!("failed to write fixture '{}'", db_path.display()))?;
    Ok(())
}

fn patch_set_freelist_trunk_leaf_count(
    db_path: &Path,
    page_size: usize,
    trunk_page: u32,
) -> Result<()> {
    anyhow::ensure!(trunk_page > 0, "cannot patch freelist trunk page 0");
    let mut bytes = std::fs::read(db_path)
        .with_context(|| format!("failed to read fixture '{}'", db_path.display()))?;
    let page_start = (trunk_page as usize - 1) * page_size;
    anyhow::ensure!(
        bytes.len() >= page_start + 8,
        "fixture too small to patch freelist trunk page {trunk_page}"
    );

    let max_pointers = page_size.saturating_sub(8) / 4;
    let new_count = (max_pointers as u32).saturating_add(1);
    bytes[page_start + 4..page_start + 8].copy_from_slice(&new_count.to_be_bytes());

    std::fs::write(db_path, bytes)
        .with_context(|| format!("failed to write fixture '{}'", db_path.display()))?;
    Ok(())
}

fn patch_truncate_first_overflow_chain_for_first_table_row(
    db_path: &Path,
    page_size: usize,
    table_root_page: usize,
) -> Result<()> {
    let mut bytes = std::fs::read(db_path)
        .with_context(|| format!("failed to read fixture '{}'", db_path.display()))?;
    let page_start = (table_root_page - 1) * page_size;
    anyhow::ensure!(
        bytes.len() > page_start + 8,
        "fixture too small to patch page {table_root_page}"
    );

    let page_flags = bytes[page_start];
    anyhow::ensure!(
        page_flags == 13,
        "expected table-leaf root page for overflow fixture, got flags={page_flags}"
    );

    let cell_count = u16::from_be_bytes([bytes[page_start + 3], bytes[page_start + 4]]) as usize;
    anyhow::ensure!(
        cell_count >= 1,
        "cannot patch overflow chain on page {table_root_page}: no cells"
    );
    let ptr_array_start = page_start + 8;
    let first_cell_ptr =
        u16::from_be_bytes([bytes[ptr_array_start], bytes[ptr_array_start + 1]]) as usize;
    let cell_start = page_start + first_cell_ptr;

    let (payload_size_u64, payload_varint_len) = parse_sqlite_varint(&bytes, cell_start)?;
    let (_, rowid_varint_len) = parse_sqlite_varint(&bytes, cell_start + payload_varint_len)?;
    let payload_size = usize::try_from(payload_size_u64)
        .with_context(|| format!("payload size {payload_size_u64} does not fit usize"))?;

    let local_with_pointer = sqlite_payload_overflow_local_with_pointer(payload_size, page_size)
        .with_context(|| format!("row payload does not overflow for page {table_root_page}"))?;
    anyhow::ensure!(
        local_with_pointer >= 4,
        "invalid local payload/pointer size while patching overflow fixture"
    );
    let first_overflow_ptr_offset =
        cell_start + payload_varint_len + rowid_varint_len + local_with_pointer - 4;
    anyhow::ensure!(
        first_overflow_ptr_offset + 4 <= bytes.len(),
        "overflow pointer offset out of bounds on page {table_root_page}"
    );

    let first_overflow_page = u32::from_be_bytes([
        bytes[first_overflow_ptr_offset],
        bytes[first_overflow_ptr_offset + 1],
        bytes[first_overflow_ptr_offset + 2],
        bytes[first_overflow_ptr_offset + 3],
    ]);
    anyhow::ensure!(first_overflow_page > 0, "fixture row has no overflow chain");

    let overflow_start = (first_overflow_page as usize - 1) * page_size;
    anyhow::ensure!(
        overflow_start + 4 <= bytes.len(),
        "first overflow page {first_overflow_page} is out of range"
    );

    let next_overflow = u32::from_be_bytes([
        bytes[overflow_start],
        bytes[overflow_start + 1],
        bytes[overflow_start + 2],
        bytes[overflow_start + 3],
    ]);
    anyhow::ensure!(
        next_overflow != 0,
        "fixture payload must span at least two overflow pages"
    );

    bytes[overflow_start..overflow_start + 4].copy_from_slice(&0u32.to_be_bytes());
    std::fs::write(db_path, bytes)
        .with_context(|| format!("failed to write fixture '{}'", db_path.display()))?;
    Ok(())
}

fn patch_drop_last_cell_from_btree_page(
    db_path: &Path,
    page_size: usize,
    root_page: usize,
) -> Result<()> {
    let mut bytes = std::fs::read(db_path)
        .with_context(|| format!("failed to read fixture '{}'", db_path.display()))?;
    let page_start = (root_page - 1) * page_size;
    anyhow::ensure!(
        bytes.len() > page_start + 8,
        "fixture too small to patch page {root_page}"
    );

    // SQLite btree page header offsets (relative to page start):
    // 0: flags, 3..=4: cell count, 5..=6: cell content area, 7: fragmented bytes, 8..: cell ptrs.
    let cell_count = u16::from_be_bytes([bytes[page_start + 3], bytes[page_start + 4]]) as usize;
    anyhow::ensure!(
        cell_count >= 2,
        "cannot drop index cell from page {root_page}: only {cell_count} cells"
    );
    let ptr_array_start = page_start + 8;
    let dropped_ptr_offset = ptr_array_start + (cell_count - 1) * 2;
    let dropped_cell_start =
        u16::from_be_bytes([bytes[dropped_ptr_offset], bytes[dropped_ptr_offset + 1]]) as usize;

    let new_count = (cell_count - 1) as u16;
    let [new_hi, new_lo] = new_count.to_be_bytes();
    bytes[page_start + 3] = new_hi;
    bytes[page_start + 4] = new_lo;

    // If the removed cell was the one at content-area start, move content-area
    // to the smallest still-referenced cell start so structure stays consistent.
    let old_content_area =
        u16::from_be_bytes([bytes[page_start + 5], bytes[page_start + 6]]) as usize;
    if old_content_area == dropped_cell_start {
        let mut min_ptr = usize::MAX;
        for i in 0..(cell_count - 1) {
            let o = ptr_array_start + i * 2;
            let ptr = u16::from_be_bytes([bytes[o], bytes[o + 1]]) as usize;
            min_ptr = min_ptr.min(ptr);
        }
        anyhow::ensure!(
            min_ptr != usize::MAX,
            "failed to compute updated content area"
        );
        let [ca_hi, ca_lo] = (min_ptr as u16).to_be_bytes();
        bytes[page_start + 5] = ca_hi;
        bytes[page_start + 6] = ca_lo;
    }

    std::fs::write(db_path, bytes)
        .with_context(|| format!("failed to write fixture '{}'", db_path.display()))?;
    Ok(())
}

fn parse_sqlite_varint(bytes: &[u8], mut offset: usize) -> Result<(u64, usize)> {
    anyhow::ensure!(offset < bytes.len(), "varint offset out of bounds");
    let start = offset;
    let mut value = 0u64;
    for i in 0..8 {
        let b = bytes[offset];
        offset += 1;
        value = (value << 7) | u64::from(b & 0x7f);
        if b & 0x80 == 0 {
            return Ok((value, offset - start));
        }
        anyhow::ensure!(offset < bytes.len(), "truncated varint");
        if i == 7 {
            let b9 = bytes[offset];
            value = (value << 8) | u64::from(b9);
            return Ok((value, offset + 1 - start));
        }
    }
    anyhow::bail!("invalid varint encoding")
}

fn sqlite_serial_type_payload_len(serial_type: u64) -> Option<usize> {
    match serial_type {
        0 | 8 | 9 => Some(0),
        1 => Some(1),
        2 => Some(2),
        3 => Some(3),
        4 => Some(4),
        5 => Some(6),
        6 | 7 => Some(8),
        n if n >= 12 && n % 2 == 0 => Some(((n - 12) / 2) as usize), // blob
        n if n >= 13 && n % 2 == 1 => Some(((n - 13) / 2) as usize), // text
        _ => None,
    }
}

fn sqlite_payload_overflow_local_with_pointer(
    payload_size: usize,
    usable_size: usize,
) -> Option<usize> {
    let max_local = usable_size.checked_sub(35)?;
    if payload_size <= max_local {
        return None;
    }

    let min_local = ((usable_size.checked_sub(12)? * 32) / 255).checked_sub(23)?;
    let overflow_page_payload = usable_size.checked_sub(4)?;
    if overflow_page_payload == 0 {
        return None;
    }
    let mut local = min_local + (payload_size - min_local) % overflow_page_payload;
    if local > max_local {
        local = min_local;
    }
    Some(local + 4)
}

fn patch_set_second_table_column_to_null_in_first_row(
    db_path: &Path,
    page_size: usize,
    root_page: usize,
) -> Result<()> {
    let mut bytes = std::fs::read(db_path)
        .with_context(|| format!("failed to read fixture '{}'", db_path.display()))?;
    let page_start = (root_page - 1) * page_size;
    anyhow::ensure!(
        bytes.len() > page_start + 8,
        "fixture too small to patch page {root_page}"
    );

    let cell_count = u16::from_be_bytes([bytes[page_start + 3], bytes[page_start + 4]]) as usize;
    anyhow::ensure!(
        cell_count >= 1,
        "cannot patch table row on page {root_page}: no cells"
    );

    let ptr_array_start = page_start + 8;
    let first_cell_ptr =
        u16::from_be_bytes([bytes[ptr_array_start], bytes[ptr_array_start + 1]]) as usize;
    let cell_start = page_start + first_cell_ptr;
    anyhow::ensure!(
        cell_start < bytes.len(),
        "cell pointer out of bounds on page {root_page}"
    );

    let (_, payload_varint_len) = parse_sqlite_varint(&bytes, cell_start)?;
    let (_, rowid_varint_len) = parse_sqlite_varint(&bytes, cell_start + payload_varint_len)?;
    let payload_start = cell_start + payload_varint_len + rowid_varint_len;
    anyhow::ensure!(
        payload_start < bytes.len(),
        "payload start out of bounds on page {root_page}"
    );

    let (header_size, header_size_varint_len) = parse_sqlite_varint(&bytes, payload_start)?;
    let header_end = payload_start + header_size as usize;
    anyhow::ensure!(
        header_end <= bytes.len(),
        "record header out of bounds on page {root_page}"
    );

    let serials_start = payload_start + header_size_varint_len;
    let (_, first_serial_len) = parse_sqlite_varint(&bytes, serials_start)?;
    let second_serial_offset = serials_start + first_serial_len;
    anyhow::ensure!(
        second_serial_offset < header_end,
        "record does not contain a second column on page {root_page}"
    );

    // The fixture inserts an empty string, so the second serial-type is expected to be
    // TEXT(0) = 13 encoded in one byte. Replacing it with 0 flips value to NULL without
    // changing record payload layout.
    anyhow::ensure!(
        bytes[second_serial_offset] == 13,
        "unexpected serial type {} for fixture row",
        bytes[second_serial_offset]
    );
    bytes[second_serial_offset] = 0;

    std::fs::write(db_path, bytes)
        .with_context(|| format!("failed to write fixture '{}'", db_path.display()))?;
    Ok(())
}

fn patch_set_second_table_column_i8_in_first_row(
    db_path: &Path,
    page_size: usize,
    root_page: usize,
    new_value: i8,
) -> Result<()> {
    let mut bytes = std::fs::read(db_path)
        .with_context(|| format!("failed to read fixture '{}'", db_path.display()))?;
    let page_start = (root_page - 1) * page_size;
    anyhow::ensure!(
        bytes.len() > page_start + 8,
        "fixture too small to patch page {root_page}"
    );

    let cell_count = u16::from_be_bytes([bytes[page_start + 3], bytes[page_start + 4]]) as usize;
    anyhow::ensure!(
        cell_count >= 1,
        "cannot patch table row on page {root_page}: no cells"
    );

    let ptr_array_start = page_start + 8;
    let first_cell_ptr =
        u16::from_be_bytes([bytes[ptr_array_start], bytes[ptr_array_start + 1]]) as usize;
    let cell_start = page_start + first_cell_ptr;

    let (_, payload_varint_len) = parse_sqlite_varint(&bytes, cell_start)?;
    let (_, rowid_varint_len) = parse_sqlite_varint(&bytes, cell_start + payload_varint_len)?;
    let payload_start = cell_start + payload_varint_len + rowid_varint_len;

    let (header_size, header_size_varint_len) = parse_sqlite_varint(&bytes, payload_start)?;
    let header_end = payload_start + header_size as usize;
    anyhow::ensure!(
        header_end <= bytes.len(),
        "record header out of bounds on page {root_page}"
    );

    let serials_start = payload_start + header_size_varint_len;
    let (first_serial_type, first_serial_len) = parse_sqlite_varint(&bytes, serials_start)?;
    let (second_serial_type, _) = parse_sqlite_varint(&bytes, serials_start + first_serial_len)?;

    let data_start = payload_start + header_size as usize;
    let first_data_len = sqlite_serial_type_payload_len(first_serial_type)
        .with_context(|| format!("unsupported first-column serial type {first_serial_type}"))?;
    let second_data_len = sqlite_serial_type_payload_len(second_serial_type)
        .with_context(|| format!("unsupported second-column serial type {second_serial_type}"))?;
    anyhow::ensure!(
        second_data_len == 1,
        "expected second-column i8 payload, got serial type {second_serial_type}"
    );

    let second_data_offset = data_start + first_data_len;
    anyhow::ensure!(
        second_data_offset < bytes.len(),
        "second-column payload offset out of bounds on page {root_page}"
    );
    bytes[second_data_offset] = new_value as u8;

    std::fs::write(db_path, bytes)
        .with_context(|| format!("failed to write fixture '{}'", db_path.display()))?;
    Ok(())
}

fn patch_change_first_index_key_i8(
    db_path: &Path,
    page_size: usize,
    root_page: usize,
    from_value: i8,
    to_value: i8,
) -> Result<()> {
    let mut bytes = std::fs::read(db_path)
        .with_context(|| format!("failed to read fixture '{}'", db_path.display()))?;
    let page_start = (root_page - 1) * page_size;
    anyhow::ensure!(
        bytes.len() > page_start + 8,
        "fixture too small to patch page {root_page}"
    );

    let cell_count = u16::from_be_bytes([bytes[page_start + 3], bytes[page_start + 4]]) as usize;
    anyhow::ensure!(
        cell_count >= 1,
        "cannot patch index entry on page {root_page}: no cells"
    );

    let ptr_array_start = page_start + 8;
    let mut patched = false;
    for i in 0..cell_count {
        let ptr_offset = ptr_array_start + i * 2;
        let cell_ptr = u16::from_be_bytes([bytes[ptr_offset], bytes[ptr_offset + 1]]) as usize;
        let cell_start = page_start + cell_ptr;

        let (_, payload_varint_len) = parse_sqlite_varint(&bytes, cell_start)?;
        let payload_start = cell_start + payload_varint_len;
        let (header_size, header_size_varint_len) = parse_sqlite_varint(&bytes, payload_start)?;
        let serials_start = payload_start + header_size_varint_len;
        let (first_serial_type, _) = parse_sqlite_varint(&bytes, serials_start)?;
        anyhow::ensure!(
            first_serial_type == 1,
            "expected first index key serial type 1, got {first_serial_type}"
        );

        let data_start = payload_start + header_size as usize;
        anyhow::ensure!(
            data_start < bytes.len(),
            "index key payload offset out of bounds on page {root_page}"
        );
        if bytes[data_start] == from_value as u8 {
            bytes[data_start] = to_value as u8;
            patched = true;
            break;
        }
    }

    anyhow::ensure!(
        patched,
        "failed to find index key value {from_value} to patch on page {root_page}"
    );
    std::fs::write(db_path, bytes)
        .with_context(|| format!("failed to write fixture '{}'", db_path.display()))?;
    Ok(())
}

async fn generate_missing_index_entry_fixture(
    db_path: &Path,
    setup_sql: &str,
    index_name: &str,
    context: &str,
) -> Result<()> {
    clear_existing_db_and_sidecars(db_path)?;

    let db_path_str = db_path.to_string_lossy().to_string();
    let db = Builder::new_local(&db_path_str)
        .build()
        .await
        .with_context(|| {
            format!(
                "failed to create integrity fixture database at '{}'",
                db_path.display()
            )
        })?;
    let conn = db
        .connect()
        .with_context(|| format!("failed to connect to fixture '{}'", db_path.display()))?;

    conn.execute_batch(setup_sql)
        .await
        .with_context(|| format!("failed to initialize fixture '{context}'"))?;

    checkpoint_truncate(&conn, context).await?;
    let page_size = get_page_size(&conn, context).await?;
    let index_root_page = get_root_page(&conn, index_name, context).await?;
    drop(conn);
    drop(db);

    patch_drop_last_cell_from_btree_page(db_path, page_size, index_root_page)?;
    remove_db_sidecars(db_path)?;

    Ok(())
}

async fn generate_check_constraint_violation_fixture(db_path: &Path) -> Result<()> {
    clear_existing_db_and_sidecars(db_path)?;

    let db_path_str = db_path.to_string_lossy().to_string();
    let db = Builder::new_local(&db_path_str)
        .build()
        .await
        .with_context(|| {
            format!(
                "failed to create integrity fixture database at '{}'",
                db_path.display()
            )
        })?;
    let conn = db
        .connect()
        .with_context(|| format!("failed to connect to fixture '{}'", db_path.display()))?;

    conn.execute_batch(
        r#"
        PRAGMA page_size=4096;
        CREATE TABLE t(id INT PRIMARY KEY, b INTEGER CHECK(b > 0));
        INSERT INTO t VALUES(1, 2);
        "#,
    )
    .await
    .with_context(|| {
        format!(
            "failed to initialize fixture '{INTEGRITY_FIXTURE_CHECK_CONSTRAINT_VIOLATION_REL_PATH}'"
        )
    })?;
    checkpoint_truncate(&conn, INTEGRITY_FIXTURE_CHECK_CONSTRAINT_VIOLATION_REL_PATH).await?;
    let page_size =
        get_page_size(&conn, INTEGRITY_FIXTURE_CHECK_CONSTRAINT_VIOLATION_REL_PATH).await?;
    let table_root_page = get_root_page(
        &conn,
        "t",
        INTEGRITY_FIXTURE_CHECK_CONSTRAINT_VIOLATION_REL_PATH,
    )
    .await?;

    drop(conn);
    drop(db);
    patch_set_second_table_column_i8_in_first_row(db_path, page_size, table_root_page, -1)?;
    remove_db_sidecars(db_path)?;
    Ok(())
}

async fn generate_not_null_violation_fixture(db_path: &Path) -> Result<()> {
    clear_existing_db_and_sidecars(db_path)?;

    let db_path_str = db_path.to_string_lossy().to_string();
    let db = Builder::new_local(&db_path_str)
        .build()
        .await
        .with_context(|| {
            format!(
                "failed to create integrity fixture database at '{}'",
                db_path.display()
            )
        })?;
    let conn = db
        .connect()
        .with_context(|| format!("failed to connect to fixture '{}'", db_path.display()))?;

    conn.execute_batch(
        r#"
        PRAGMA page_size=4096;
        CREATE TABLE t(id INT PRIMARY KEY, b TEXT NOT NULL);
        INSERT INTO t VALUES(1, '');
        "#,
    )
    .await
    .with_context(|| {
        format!("failed to initialize fixture '{INTEGRITY_FIXTURE_NOT_NULL_VIOLATION_REL_PATH}'")
    })?;
    checkpoint_truncate(&conn, INTEGRITY_FIXTURE_NOT_NULL_VIOLATION_REL_PATH).await?;
    let page_size = get_page_size(&conn, INTEGRITY_FIXTURE_NOT_NULL_VIOLATION_REL_PATH).await?;
    let table_root_page =
        get_root_page(&conn, "t", INTEGRITY_FIXTURE_NOT_NULL_VIOLATION_REL_PATH).await?;

    drop(conn);
    drop(db);
    patch_set_second_table_column_to_null_in_first_row(db_path, page_size, table_root_page)?;
    remove_db_sidecars(db_path)?;
    Ok(())
}

async fn generate_non_unique_index_entry_fixture(db_path: &Path) -> Result<()> {
    clear_existing_db_and_sidecars(db_path)?;

    let db_path_str = db_path.to_string_lossy().to_string();
    let db = Builder::new_local(&db_path_str)
        .build()
        .await
        .with_context(|| {
            format!(
                "failed to create integrity fixture database at '{}'",
                db_path.display()
            )
        })?;
    let conn = db
        .connect()
        .with_context(|| format!("failed to connect to fixture '{}'", db_path.display()))?;

    conn.execute_batch(
        r#"
        PRAGMA page_size=4096;
        CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER);
        CREATE UNIQUE INDEX idx_u ON t(b);
        INSERT INTO t VALUES (1,10),(2,20),(3,30);
        "#,
    )
    .await
    .with_context(|| {
        format!(
            "failed to initialize fixture '{INTEGRITY_FIXTURE_NON_UNIQUE_INDEX_ENTRY_REL_PATH}'"
        )
    })?;

    checkpoint_truncate(&conn, INTEGRITY_FIXTURE_NON_UNIQUE_INDEX_ENTRY_REL_PATH).await?;
    let page_size = get_page_size(&conn, INTEGRITY_FIXTURE_NON_UNIQUE_INDEX_ENTRY_REL_PATH).await?;
    let index_root_page = get_root_page(
        &conn,
        "idx_u",
        INTEGRITY_FIXTURE_NON_UNIQUE_INDEX_ENTRY_REL_PATH,
    )
    .await?;
    drop(conn);
    drop(db);

    patch_change_first_index_key_i8(db_path, page_size, index_root_page, 30, 20)?;
    remove_db_sidecars(db_path)?;
    Ok(())
}

async fn generate_missing_unique_index_entry_fixture(db_path: &Path) -> Result<()> {
    clear_existing_db_and_sidecars(db_path)?;

    let db_path_str = db_path.to_string_lossy().to_string();
    let db = Builder::new_local(&db_path_str)
        .build()
        .await
        .with_context(|| {
            format!(
                "failed to create integrity fixture database at '{}'",
                db_path.display()
            )
        })?;
    let conn = db
        .connect()
        .with_context(|| format!("failed to connect to fixture '{}'", db_path.display()))?;

    conn.execute_batch(
        r#"
        PRAGMA page_size=4096;
        CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER);
        CREATE UNIQUE INDEX idx_u_missing ON t(b);
        INSERT INTO t VALUES (1,10),(2,20),(3,30);
        "#,
    )
    .await
    .with_context(|| {
        format!(
            "failed to initialize fixture '{INTEGRITY_FIXTURE_MISSING_UNIQUE_INDEX_ENTRY_REL_PATH}'"
        )
    })?;

    checkpoint_truncate(&conn, INTEGRITY_FIXTURE_MISSING_UNIQUE_INDEX_ENTRY_REL_PATH).await?;
    let page_size =
        get_page_size(&conn, INTEGRITY_FIXTURE_MISSING_UNIQUE_INDEX_ENTRY_REL_PATH).await?;
    let index_root_page = get_root_page(
        &conn,
        "idx_u_missing",
        INTEGRITY_FIXTURE_MISSING_UNIQUE_INDEX_ENTRY_REL_PATH,
    )
    .await?;
    drop(conn);
    drop(db);

    // Corrupt exactly one key value so the row for b=20 is missing from the
    // unique index, but no duplicate key is introduced.
    patch_change_first_index_key_i8(db_path, page_size, index_root_page, 20, 25)?;
    remove_db_sidecars(db_path)?;
    Ok(())
}

async fn generate_freelist_fixture_base(
    db_path: &Path,
    context: &str,
) -> Result<(usize, u32, u32)> {
    clear_existing_db_and_sidecars(db_path)?;

    let db_path_str = db_path.to_string_lossy().to_string();
    let db = Builder::new_local(&db_path_str)
        .build()
        .await
        .with_context(|| {
            format!(
                "failed to create integrity fixture database at '{}'",
                db_path.display()
            )
        })?;
    let conn = db
        .connect()
        .with_context(|| format!("failed to connect to fixture '{}'", db_path.display()))?;

    conn.execute_batch(
        r#"
        PRAGMA page_size=4096;
        CREATE TABLE t(a INTEGER PRIMARY KEY, b BLOB);
        "#,
    )
    .await
    .with_context(|| format!("failed to initialize fixture '{context}'"))?;

    for i in 1..=200 {
        conn.execute("INSERT INTO t VALUES (?1, zeroblob(3500))", [i.to_string()])
            .await
            .with_context(|| {
                format!("failed to insert row {i} while building fixture '{context}'")
            })?;
    }

    conn.execute("DROP TABLE t", ())
        .await
        .with_context(|| format!("failed to drop table while building fixture '{context}'"))?;

    checkpoint_truncate(&conn, context).await?;
    let page_size = get_page_size(&conn, context).await?;
    let freelist_count = query_single_i64(&conn, "PRAGMA freelist_count", context).await?;
    anyhow::ensure!(
        freelist_count > 0,
        "fixture '{context}' did not create freelist pages"
    );

    drop(conn);
    drop(db);

    let (trunk_page, header_count) = read_db_header_freelist_fields(db_path)?;
    anyhow::ensure!(
        trunk_page > 0,
        "fixture '{context}' did not create a freelist trunk page"
    );
    anyhow::ensure!(
        header_count > 0,
        "fixture '{context}' has zero freelist count in database header"
    );
    Ok((page_size, trunk_page, header_count))
}

async fn generate_freelist_count_mismatch_fixture(db_path: &Path) -> Result<()> {
    let (_, _, header_count) =
        generate_freelist_fixture_base(db_path, INTEGRITY_FIXTURE_FREELIST_COUNT_MISMATCH_REL_PATH)
            .await?;
    let new_count = header_count
        .checked_add(1)
        .context("freelist count overflow while patching fixture")?;
    patch_set_db_header_freelist_count(db_path, new_count)?;
    remove_db_sidecars(db_path)?;
    Ok(())
}

async fn generate_freelist_trunk_corrupt_fixture(db_path: &Path) -> Result<()> {
    let (page_size, trunk_page, _) =
        generate_freelist_fixture_base(db_path, INTEGRITY_FIXTURE_FREELIST_TRUNK_CORRUPT_REL_PATH)
            .await?;
    patch_set_freelist_trunk_leaf_count(db_path, page_size, trunk_page)?;
    remove_db_sidecars(db_path)?;
    Ok(())
}

async fn generate_overflow_list_length_mismatch_fixture(db_path: &Path) -> Result<()> {
    clear_existing_db_and_sidecars(db_path)?;

    let db_path_str = db_path.to_string_lossy().to_string();
    let db = Builder::new_local(&db_path_str)
        .build()
        .await
        .with_context(|| {
            format!(
                "failed to create integrity fixture database at '{}'",
                db_path.display()
            )
        })?;
    let conn = db
        .connect()
        .with_context(|| format!("failed to connect to fixture '{}'", db_path.display()))?;

    conn.execute_batch(
        r#"
        PRAGMA page_size=4096;
        CREATE TABLE t(a INTEGER PRIMARY KEY, b BLOB);
        INSERT INTO t VALUES (1, zeroblob(30000));
        "#,
    )
    .await
    .with_context(|| {
        format!(
            "failed to initialize fixture '{INTEGRITY_FIXTURE_OVERFLOW_LIST_LENGTH_MISMATCH_REL_PATH}'"
        )
    })?;

    checkpoint_truncate(
        &conn,
        INTEGRITY_FIXTURE_OVERFLOW_LIST_LENGTH_MISMATCH_REL_PATH,
    )
    .await?;
    let page_size = get_page_size(
        &conn,
        INTEGRITY_FIXTURE_OVERFLOW_LIST_LENGTH_MISMATCH_REL_PATH,
    )
    .await?;
    let table_root_page = get_root_page(
        &conn,
        "t",
        INTEGRITY_FIXTURE_OVERFLOW_LIST_LENGTH_MISMATCH_REL_PATH,
    )
    .await?;
    drop(conn);
    drop(db);

    patch_truncate_first_overflow_chain_for_first_table_row(db_path, page_size, table_root_page)?;
    remove_db_sidecars(db_path)?;
    Ok(())
}

/// Uppercase the first text key in the first index cell on the given page.
/// This creates a NOCASE-equivalent but binary-different index entry, which
/// `PRAGMA integrity_check` should report as "values differ from index".
fn patch_uppercase_first_index_text_key(
    db_path: &Path,
    page_size: usize,
    root_page: usize,
) -> Result<()> {
    let mut bytes = std::fs::read(db_path)
        .with_context(|| format!("failed to read fixture '{}'", db_path.display()))?;
    let page_start = (root_page - 1) * page_size;
    anyhow::ensure!(
        bytes.len() > page_start + 8,
        "fixture too small to patch page {root_page}"
    );

    let cell_count = u16::from_be_bytes([bytes[page_start + 3], bytes[page_start + 4]]) as usize;
    anyhow::ensure!(
        cell_count >= 1,
        "cannot patch index entry on page {root_page}: no cells"
    );

    let ptr_array_start = page_start + 8;
    let first_cell_ptr =
        u16::from_be_bytes([bytes[ptr_array_start], bytes[ptr_array_start + 1]]) as usize;
    let cell_start = page_start + first_cell_ptr;

    let (_, payload_varint_len) = parse_sqlite_varint(&bytes, cell_start)?;
    let payload_start = cell_start + payload_varint_len;
    let (header_size, header_size_varint_len) = parse_sqlite_varint(&bytes, payload_start)?;
    let serials_start = payload_start + header_size_varint_len;
    let (first_serial_type, _) = parse_sqlite_varint(&bytes, serials_start)?;

    // Text serial types are odd numbers >= 13: serial_type = 13 + 2*len
    anyhow::ensure!(
        first_serial_type >= 13 && first_serial_type % 2 == 1,
        "expected text serial type for first index key, got {first_serial_type}"
    );
    let text_len = ((first_serial_type - 13) / 2) as usize;
    anyhow::ensure!(text_len > 0, "text key is empty, cannot uppercase");

    let data_start = payload_start + header_size as usize;
    for i in 0..text_len {
        let b = bytes[data_start + i];
        if b.is_ascii_lowercase() {
            bytes[data_start + i] = b.to_ascii_uppercase();
        }
    }

    std::fs::write(db_path, bytes)
        .with_context(|| format!("failed to write fixture '{}'", db_path.display()))?;
    Ok(())
}

async fn generate_values_differ_nocase_fixture(db_path: &Path) -> Result<()> {
    clear_existing_db_and_sidecars(db_path)?;

    let db_path_str = db_path.to_string_lossy().to_string();
    let db = Builder::new_local(&db_path_str)
        .build()
        .await
        .with_context(|| {
            format!(
                "failed to create integrity fixture database at '{}'",
                db_path.display()
            )
        })?;
    let conn = db
        .connect()
        .with_context(|| format!("failed to connect to fixture '{}'", db_path.display()))?;

    conn.execute_batch(
        r#"
        PRAGMA page_size=4096;
        CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT COLLATE NOCASE);
        CREATE INDEX idx_nc ON t(b);
        INSERT INTO t VALUES (1,'abc'),(2,'def'),(3,'ghi');
        "#,
    )
    .await
    .with_context(|| {
        format!("failed to initialize fixture '{INTEGRITY_FIXTURE_VALUES_DIFFER_NOCASE_REL_PATH}'")
    })?;

    checkpoint_truncate(&conn, INTEGRITY_FIXTURE_VALUES_DIFFER_NOCASE_REL_PATH).await?;
    let page_size = get_page_size(&conn, INTEGRITY_FIXTURE_VALUES_DIFFER_NOCASE_REL_PATH).await?;
    let index_root_page = get_root_page(
        &conn,
        "idx_nc",
        INTEGRITY_FIXTURE_VALUES_DIFFER_NOCASE_REL_PATH,
    )
    .await?;
    drop(conn);
    drop(db);

    // Uppercase the first index key: "abc" -> "ABC". Found with NOCASE
    // still matches, but binary comparison detects the mismatch.
    patch_uppercase_first_index_text_key(db_path, page_size, index_root_page)?;
    remove_db_sidecars(db_path)?;
    Ok(())
}

/// Generate one of the integrity-check parity fixtures identified by its
/// repository-relative path (e.g. `database/integrity_missing_index_entry.db`).
pub async fn generate_integrity_fixture(db_path: &Path, relative_path: &str) -> Result<()> {
    match relative_path {
        INTEGRITY_FIXTURE_MISSING_INDEX_ENTRY_REL_PATH => {
            generate_missing_index_entry_fixture(
                db_path,
                r#"
                PRAGMA page_size=4096;
                CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);
                CREATE INDEX idx_b ON t(b);
                INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c');
                "#,
                "idx_b",
                INTEGRITY_FIXTURE_MISSING_INDEX_ENTRY_REL_PATH,
            )
            .await
        }
        INTEGRITY_FIXTURE_MISSING_EXPRESSION_INDEX_ENTRY_REL_PATH => {
            generate_missing_index_entry_fixture(
                db_path,
                r#"
                PRAGMA page_size=4096;
                CREATE TABLE t(a INTEGER PRIMARY KEY, d TEXT);
                CREATE INDEX idx_expr ON t(lower(d));
                INSERT INTO t VALUES (1,'Alpha'),(2,'Bravo'),(3,'Charlie');
                "#,
                "idx_expr",
                INTEGRITY_FIXTURE_MISSING_EXPRESSION_INDEX_ENTRY_REL_PATH,
            )
            .await
        }
        INTEGRITY_FIXTURE_MISSING_PARTIAL_INDEX_ENTRY_REL_PATH => {
            generate_missing_index_entry_fixture(
                db_path,
                r#"
                PRAGMA page_size=4096;
                CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT, c INTEGER);
                CREATE INDEX idx_partial ON t(b) WHERE c = 1;
                INSERT INTO t VALUES (1,'one',1),(2,'two',0),(3,'three',1),(4,'four',0);
                "#,
                "idx_partial",
                INTEGRITY_FIXTURE_MISSING_PARTIAL_INDEX_ENTRY_REL_PATH,
            )
            .await
        }
        INTEGRITY_FIXTURE_CHECK_CONSTRAINT_VIOLATION_REL_PATH => {
            generate_check_constraint_violation_fixture(db_path).await
        }
        INTEGRITY_FIXTURE_CHECK_CONSTRAINT_VIOLATION_QUICK_REL_PATH => {
            generate_check_constraint_violation_fixture(db_path).await
        }
        INTEGRITY_FIXTURE_NOT_NULL_VIOLATION_REL_PATH => {
            generate_not_null_violation_fixture(db_path).await
        }
        INTEGRITY_FIXTURE_NON_UNIQUE_INDEX_ENTRY_REL_PATH => {
            generate_non_unique_index_entry_fixture(db_path).await
        }
        INTEGRITY_FIXTURE_MISSING_UNIQUE_INDEX_ENTRY_REL_PATH => {
            generate_missing_unique_index_entry_fixture(db_path).await
        }
        INTEGRITY_FIXTURE_FREELIST_COUNT_MISMATCH_REL_PATH => {
            generate_freelist_count_mismatch_fixture(db_path).await
        }
        INTEGRITY_FIXTURE_FREELIST_TRUNK_CORRUPT_REL_PATH => {
            generate_freelist_trunk_corrupt_fixture(db_path).await
        }
        INTEGRITY_FIXTURE_OVERFLOW_LIST_LENGTH_MISMATCH_REL_PATH => {
            generate_overflow_list_length_mismatch_fixture(db_path).await
        }
        INTEGRITY_FIXTURE_VALUES_DIFFER_NOCASE_REL_PATH => {
            generate_values_differ_nocase_fixture(db_path).await
        }
        _ => anyhow::bail!("unknown integrity fixture path '{relative_path}'"),
    }
}

/// Backward-compatible helper for older call-sites.
pub async fn generate_integrity_missing_index_entry_fixture(db_path: &Path) -> Result<()> {
    generate_integrity_fixture(db_path, INTEGRITY_FIXTURE_MISSING_INDEX_ENTRY_REL_PATH).await
}

async fn create_tables(conn: &Connection, no_rowid_alias: bool) -> Result<()> {
    let pk_type = if no_rowid_alias {
        "INT PRIMARY KEY"
    } else {
        "INTEGER PRIMARY KEY"
    };

    let users_sql = format!(
        r#"
        CREATE TABLE users (
        id {pk_type},
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        phone_number TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        zipcode TEXT,
        age INTEGER
    );
        "#
    );

    conn.execute(&users_sql, ())
        .await
        .with_context(|| format!("failed to create users table: {}", users_sql.trim()))?;

    if !no_rowid_alias {
        let index_sql = "CREATE INDEX age_idx ON users (age);".to_string();

        conn.execute(&index_sql, ())
            .await
            .with_context(|| format!("failed to create user index table: {}", index_sql.trim()))?;
    }

    let products_sql = format!(
        r#"
        CREATE TABLE IF NOT EXISTS products (
            id {pk_type},
            name TEXT,
            price REAL
        )
        "#
    );

    conn.execute(&products_sql, ())
        .await
        .with_context(|| format!("failed to create products table: {}", products_sql.trim()))?;

    Ok(())
}

async fn insert_users(
    conn: &Connection,
    count: usize,
    no_rowid_alias: bool,
    rng: &mut ChaCha8Rng,
) -> Result<()> {
    for i in 0..count {
        let user: User = fake::Faker.fake_with_rng(rng);

        if no_rowid_alias {
            // For INT PRIMARY KEY, we need to explicitly provide the id
            conn.execute(
                r#"
                INSERT INTO users (id, first_name, last_name, email, phone_number, address, city, state, zipcode, age)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                "#,
                [
                    (i + 1).to_string(),
                    user.first_name,
                    user.last_name,
                    user.email,
                    user.phone_number,
                    user.address,
                    user.city,
                    user.state,
                    user.zipcode,
                    user.age.to_string(),
                ],
            )
            .await
            .with_context(|| format!("failed to insert user {} of {}", i + 1, count))?;
        } else {
            conn.execute(
                r#"
                INSERT INTO users (first_name, last_name, email, phone_number, address, city, state, zipcode, age)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                "#,
                [
                    user.first_name,
                    user.last_name,
                    user.email,
                    user.phone_number,
                    user.address,
                    user.city,
                    user.state,
                    user.zipcode,
                    user.age.to_string(),
                ],
            )
            .await
            .with_context(|| format!("failed to insert user {} of {}", i + 1, count))?;
        }
    }

    Ok(())
}

async fn insert_products(
    conn: &Connection,
    no_rowid_alias: bool,
    rng: &mut ChaCha8Rng,
) -> Result<()> {
    for (idx, product_name) in PRODUCT_LIST.iter().enumerate() {
        let product = Product::new(product_name, rng);

        if no_rowid_alias {
            // For INT PRIMARY KEY, we need to explicitly provide the id
            conn.execute(
                r#"
                INSERT INTO products (id, name, price)
                VALUES (?1, ?2, ?3)
                "#,
                [
                    (idx + 1).to_string(),
                    product.name.clone(),
                    product.price.to_string(),
                ],
            )
            .await
            .with_context(|| {
                format!(
                    "failed to insert product '{}' (id={}, price={})",
                    product.name,
                    idx + 1,
                    product.price
                )
            })?;
        } else {
            conn.execute(
                r#"
                INSERT INTO products (name, price)
                VALUES (?1, ?2)
                "#,
                [product.name.clone(), product.price.to_string()],
            )
            .await
            .with_context(|| {
                format!(
                    "failed to insert product '{}' (price={})",
                    product.name, product.price
                )
            })?;
        }
    }

    Ok(())
}

/// Which default databases are needed for a test run
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultDatabaseNeeds {
    /// Need the default database with INTEGER PRIMARY KEY (rowid alias)
    pub default: bool,
    /// Need the default database with INT PRIMARY KEY (no rowid alias)
    pub no_rowid_alias: bool,
}

impl DefaultDatabaseNeeds {
    /// Check if any default databases are needed
    pub fn any(&self) -> bool {
        self.default || self.no_rowid_alias
    }
}

/// Holds paths to generated default databases
///
/// The temp directory is kept alive as long as this struct exists.
/// When dropped, the temp directory and all generated databases are cleaned up.
pub struct DefaultDatabases {
    /// Temp directory holding the generated databases
    _temp_dir: TempDir,
    /// Path to the default database (INTEGER PRIMARY KEY)
    pub default_path: Option<PathBuf>,
    /// Path to the no-rowid-alias database (INT PRIMARY KEY)
    pub no_rowid_alias_path: Option<PathBuf>,
}

impl DefaultDatabases {
    /// Scan test files to determine which default databases are needed
    pub fn scan_needs<'a>(
        test_files: impl IntoIterator<Item = &'a TestFile>,
    ) -> DefaultDatabaseNeeds {
        let mut needs = DefaultDatabaseNeeds::default();

        for file in test_files {
            for db_config in &file.databases {
                match db_config.location {
                    DatabaseLocation::Default => needs.default = true,
                    DatabaseLocation::DefaultNoRowidAlias => needs.no_rowid_alias = true,
                    _ => {}
                }
            }
        }

        needs
    }

    /// Generate the needed default databases
    ///
    /// Returns None if no default databases are needed.
    pub async fn generate(
        needs: DefaultDatabaseNeeds,
        seed: u64,
        user_count: usize,
        mvcc: bool,
    ) -> Result<Option<Self>> {
        if !needs.any() {
            return Ok(None);
        }

        let temp_dir = TempDir::new().context("failed to create temp directory for databases")?;

        let mut default_path = None;
        let mut no_rowid_alias_path = None;

        if needs.default {
            let path = temp_dir.path().join("database.db");
            let config = GeneratorConfig {
                db_path: path.to_string_lossy().to_string(),
                user_count,
                seed,
                no_rowid_alias: false,
                mvcc,
            };
            generate_database(&config)
                .await
                .context("failed to generate default database (INTEGER PRIMARY KEY)")?;
            default_path = Some(path);
        }

        if needs.no_rowid_alias {
            let path = temp_dir.path().join("database-no-rowidalias.db");
            let config = GeneratorConfig {
                db_path: path.to_string_lossy().to_string(),
                user_count,
                seed,
                no_rowid_alias: true,
                mvcc,
            };
            generate_database(&config)
                .await
                .context("failed to generate no-rowid-alias database (INT PRIMARY KEY)")?;
            no_rowid_alias_path = Some(path);
        }

        Ok(Some(Self {
            _temp_dir: temp_dir,
            default_path,
            no_rowid_alias_path,
        }))
    }
}

impl DefaultDatabaseResolver for DefaultDatabases {
    fn resolve(&self, location: &DatabaseLocation) -> Option<PathBuf> {
        match location {
            DatabaseLocation::Default => self.default_path.clone(),
            DatabaseLocation::DefaultNoRowidAlias => self.no_rowid_alias_path.clone(),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn new_config(db_path: &str) -> GeneratorConfig {
        GeneratorConfig {
            db_path: db_path.to_string(),
            user_count: 10,
            seed: 42,
            no_rowid_alias: false,
            mvcc: false,
        }
    }

    async fn generate_db(config: GeneratorConfig) {
        generate_database(&config).await.unwrap();

        // Verify the data was inserted
        let db = Builder::new_local(&config.db_path).build().await.unwrap();
        let conn = db.connect().unwrap();

        // Check user count
        let mut rows = conn.query("SELECT COUNT(*) FROM users", ()).await.unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let count = row.get::<i64>(0).unwrap();
        assert_eq!(count, 10);

        // Check product count
        let mut rows = conn
            .query("SELECT COUNT(*) FROM products", ())
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let count = row.get::<i64>(0).unwrap();
        assert_eq!(count, PRODUCT_LIST.len() as i64);
    }

    #[tokio::test]
    async fn test_generate_database() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let config = new_config(db_path);

        generate_db(config).await;

        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let mut config = new_config(db_path);
        config.no_rowid_alias = true;

        generate_db(config).await;
    }
}
