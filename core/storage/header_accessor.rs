use crate::{
    storage::{
        self,
        pager::{PageRef, Pager},
        sqlite3_ondisk::DATABASE_HEADER_PAGE_ID,
    },
    types::CursorResult,
    LimboError, Result,
};
use std::sync::atomic::Ordering;

// const HEADER_OFFSET_MAGIC: usize = 0;
const HEADER_OFFSET_PAGE_SIZE: usize = 16;
const HEADER_OFFSET_WRITE_VERSION: usize = 18;
const HEADER_OFFSET_READ_VERSION: usize = 19;
const HEADER_OFFSET_RESERVED_SPACE: usize = 20;
const HEADER_OFFSET_MAX_EMBED_FRAC: usize = 21;
const HEADER_OFFSET_MIN_EMBED_FRAC: usize = 22;
const HEADER_OFFSET_MIN_LEAF_FRAC: usize = 23;
const HEADER_OFFSET_CHANGE_COUNTER: usize = 24;
const HEADER_OFFSET_DATABASE_SIZE: usize = 28;
const HEADER_OFFSET_FREELIST_TRUNK_PAGE: usize = 32;
const HEADER_OFFSET_FREELIST_PAGES: usize = 36;
const HEADER_OFFSET_SCHEMA_COOKIE: usize = 40;
const HEADER_OFFSET_SCHEMA_FORMAT: usize = 44;
const HEADER_OFFSET_DEFAULT_PAGE_CACHE_SIZE: usize = 48;
const HEADER_OFFSET_VACUUM_MODE_LARGEST_ROOT_PAGE: usize = 52;
const HEADER_OFFSET_TEXT_ENCODING: usize = 56;
const HEADER_OFFSET_USER_VERSION: usize = 60;
const HEADER_OFFSET_INCREMENTAL_VACUUM_ENABLED: usize = 64;
const HEADER_OFFSET_APPLICATION_ID: usize = 68;
//const HEADER_OFFSET_RESERVED_FOR_EXPANSION: usize = 72;
const HEADER_OFFSET_VERSION_VALID_FOR: usize = 92;
const HEADER_OFFSET_VERSION_NUMBER: usize = 96;

// Helper to get a read-only reference to the header page.
fn get_header_page(pager: &Pager) -> Result<CursorResult<PageRef>> {
    if pager.is_empty.load(Ordering::SeqCst) < 2 {
        return Err(LimboError::InternalError(
            "Database is empty, header does not exist - page 1 should've been allocated before this".to_string(),
        ));
    }
    let page = pager.read_page(DATABASE_HEADER_PAGE_ID)?;
    if page.is_locked() {
        return Ok(CursorResult::IO);
    }
    Ok(CursorResult::Ok(page))
}

// Helper to get a writable reference to the header page and mark it dirty.
fn get_header_page_for_write(pager: &Pager) -> Result<CursorResult<PageRef>> {
    if pager.is_empty.load(Ordering::SeqCst) < 2 {
        // This should not be called on an empty DB for writing, as page 1 is allocated on first transaction.
        return Err(LimboError::InternalError(
            "Cannot write to header of an empty database - page 1 should've been allocated before this".to_string(),
        ));
    }
    let page = pager.read_page(DATABASE_HEADER_PAGE_ID)?;
    if page.is_locked() {
        return Ok(CursorResult::IO);
    }
    page.set_dirty();
    pager.add_dirty(DATABASE_HEADER_PAGE_ID);
    Ok(CursorResult::Ok(page))
}

/// Helper function to run async header accessors until completion
fn run_header_accessor_until_done<T, F>(pager: &Pager, mut accessor: F) -> Result<T>
where
    F: FnMut() -> Result<CursorResult<T>>,
{
    loop {
        match accessor()? {
            CursorResult::Ok(value) => return Ok(value),
            CursorResult::IO => {
                pager.io.run_once()?;
            }
        }
    }
}

/// Helper macro to implement getters and setters for header fields.
/// For example, `impl_header_field_accessor!(page_size, u16, HEADER_OFFSET_PAGE_SIZE);`
/// will generate the following functions:
/// - `pub fn get_page_size(pager: &Pager) -> Result<u16>` (sync)
/// - `pub fn get_page_size_async(pager: &Pager) -> Result<CursorResult<u16>>` (async)
/// - `pub fn set_page_size(pager: &Pager, value: u16) -> Result<()>` (sync)
/// - `pub fn set_page_size_async(pager: &Pager, value: u16) -> Result<CursorResult<()>>` (async)
///
/// The macro takes three required arguments:
/// - `$field_name`: The name of the field to implement.
/// - `$type`: The type of the field.
/// - `$offset`: The offset of the field in the header page.
///
/// And a fourth optional argument:
/// - `$ifzero`: A value to return if the field is 0.
///
/// The macro will generate both sync and async versions of the functions.
///
macro_rules! impl_header_field_accessor {
    ($field_name:ident, $type:ty, $offset:expr $(, $ifzero:expr)?) => {
        paste::paste! {
            // Async version
            #[allow(dead_code)]
            pub fn [<get_ $field_name _async>](pager: &Pager) -> Result<CursorResult<$type>> {
                if pager.is_empty.load(Ordering::SeqCst) < 2 {
                    return Err(LimboError::InternalError(format!("Database is empty, header does not exist - page 1 should've been allocated before this")));
                }
                let page = match get_header_page(pager)? {
                    CursorResult::Ok(page) => page,
                    CursorResult::IO => return Ok(CursorResult::IO),
                };
                let page_inner = page.get();
                let page_content = page_inner.contents.as_ref().unwrap();
                let buf = page_content.buffer.borrow();
                let buf_slice = buf.as_slice();
                let mut bytes = [0; std::mem::size_of::<$type>()];
                bytes.copy_from_slice(&buf_slice[$offset..$offset + std::mem::size_of::<$type>()]);
                let value = <$type>::from_be_bytes(bytes);
                $(
                    if value == 0 {
                        return Ok(CursorResult::Ok($ifzero));
                    }
                )?
                Ok(CursorResult::Ok(value))
            }

            // Sync version
            #[allow(dead_code)]
            pub fn [<get_ $field_name>](pager: &Pager) -> Result<$type> {
                run_header_accessor_until_done(pager, || [<get_ $field_name _async>](pager))
            }

            // Async setter
            #[allow(dead_code)]
            pub fn [<set_ $field_name _async>](pager: &Pager, value: $type) -> Result<CursorResult<()>> {
                let page = match get_header_page_for_write(pager)? {
                    CursorResult::Ok(page) => page,
                    CursorResult::IO => return Ok(CursorResult::IO),
                };
                let page_inner = page.get();
                let page_content = page_inner.contents.as_ref().unwrap();
                let mut buf = page_content.buffer.borrow_mut();
                let buf_slice = buf.as_mut_slice();
                buf_slice[$offset..$offset + std::mem::size_of::<$type>()].copy_from_slice(&value.to_be_bytes());
                page.set_dirty();
                pager.add_dirty(1);
                Ok(CursorResult::Ok(()))
            }

            // Sync setter
            #[allow(dead_code)]
            pub fn [<set_ $field_name>](pager: &Pager, value: $type) -> Result<()> {
                run_header_accessor_until_done(pager, || [<set_ $field_name _async>](pager, value))
            }
        }
    };
}

// impl_header_field_accessor!(magic, [u8; 16], HEADER_OFFSET_MAGIC);
impl_header_field_accessor!(page_size, u16, HEADER_OFFSET_PAGE_SIZE);
impl_header_field_accessor!(write_version, u8, HEADER_OFFSET_WRITE_VERSION);
impl_header_field_accessor!(read_version, u8, HEADER_OFFSET_READ_VERSION);
impl_header_field_accessor!(reserved_space, u8, HEADER_OFFSET_RESERVED_SPACE);
impl_header_field_accessor!(max_embed_frac, u8, HEADER_OFFSET_MAX_EMBED_FRAC);
impl_header_field_accessor!(min_embed_frac, u8, HEADER_OFFSET_MIN_EMBED_FRAC);
impl_header_field_accessor!(min_leaf_frac, u8, HEADER_OFFSET_MIN_LEAF_FRAC);
impl_header_field_accessor!(change_counter, u32, HEADER_OFFSET_CHANGE_COUNTER);
impl_header_field_accessor!(database_size, u32, HEADER_OFFSET_DATABASE_SIZE);
impl_header_field_accessor!(freelist_trunk_page, u32, HEADER_OFFSET_FREELIST_TRUNK_PAGE);
impl_header_field_accessor!(freelist_pages, u32, HEADER_OFFSET_FREELIST_PAGES);
impl_header_field_accessor!(schema_cookie, u32, HEADER_OFFSET_SCHEMA_COOKIE);
impl_header_field_accessor!(schema_format, u32, HEADER_OFFSET_SCHEMA_FORMAT);
impl_header_field_accessor!(
    default_page_cache_size,
    i32,
    HEADER_OFFSET_DEFAULT_PAGE_CACHE_SIZE,
    storage::sqlite3_ondisk::DEFAULT_CACHE_SIZE
);
impl_header_field_accessor!(
    vacuum_mode_largest_root_page,
    u32,
    HEADER_OFFSET_VACUUM_MODE_LARGEST_ROOT_PAGE
);
impl_header_field_accessor!(text_encoding, u32, HEADER_OFFSET_TEXT_ENCODING);
impl_header_field_accessor!(user_version, i32, HEADER_OFFSET_USER_VERSION);
impl_header_field_accessor!(
    incremental_vacuum_enabled,
    u32,
    HEADER_OFFSET_INCREMENTAL_VACUUM_ENABLED
);
impl_header_field_accessor!(application_id, u32, HEADER_OFFSET_APPLICATION_ID);
//impl_header_field_accessor!(reserved_for_expansion, [u8; 20], HEADER_OFFSET_RESERVED_FOR_EXPANSION);
impl_header_field_accessor!(version_valid_for, u32, HEADER_OFFSET_VERSION_VALID_FOR);
impl_header_field_accessor!(version_number, u32, HEADER_OFFSET_VERSION_NUMBER);
