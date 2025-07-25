use std::sync::Arc;

use crate::storage::sqlite3_ondisk::MAX_PAGE_SIZE;
use crate::Completion;
use crate::turso_assert;
use crate::{
    storage::{
        self,
        pager::{PageRef, Pager},
        sqlite3_ondisk::DATABASE_HEADER_PAGE_ID,
    },
    LimboError, Result,
};

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
fn get_header_page(pager: &Pager) -> Result<(PageRef, Arc<Completion>)> {
    if !pager.db_state.is_initialized() {
        return Err(LimboError::InternalError(
            "Database is empty, header does not exist - page 1 should've been allocated before this".to_string(),
        ));
    }
    pager.read_page(DATABASE_HEADER_PAGE_ID)
}

// Helper to get a writable reference to the header page and mark it dirty.
fn get_header_page_for_write(pager: &Pager) -> Result<(PageRef, Arc<Completion>)> {
    if !pager.db_state.is_initialized() {
        // This should not be called on an empty DB for writing, as page 1 is allocated on first transaction.
        return Err(LimboError::InternalError(
            "Cannot write to header of an empty database - page 1 should've been allocated before this".to_string(),
        ));
    }
    let (page, c) = pager.read_page(DATABASE_HEADER_PAGE_ID)?;
    turso_assert!(
        page.get().id == DATABASE_HEADER_PAGE_ID,
        "page must have number 1"
    );
    pager.add_dirty(&page);
    Ok((page, c))
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
            // Async version really does not exist anymore as we track completions now
            // so the caller is reponsible for doing either wait for the completion
            // or bubbling the completion up
            // Sync version
            #[allow(dead_code)]
            pub fn [<get_ $field_name>](pager: &Pager) -> Result<$type> {
                if !pager.db_state.is_initialized() {
                    return Err(LimboError::InternalError(format!("Database is empty, header does not exist - page 1 should've been allocated before this")));
                }
                let (page, c) = get_header_page(pager)?;
                pager.io.wait_for_completion(c)?;
                let page_inner = page.get();
                let page_content = page_inner.contents.as_ref().unwrap();
                let buf = page_content.buffer.borrow();
                let buf_slice = buf.as_slice();
                let mut bytes = [0; std::mem::size_of::<$type>()];
                bytes.copy_from_slice(&buf_slice[$offset..$offset + std::mem::size_of::<$type>()]);
                let value = <$type>::from_be_bytes(bytes);
                $(
                    if value == 0 {
                        return Ok($ifzero);
                    }
                )?
                Ok(value)
            }

            // Sync setter
            #[allow(dead_code)]
            pub fn [<set_ $field_name>](pager: &Pager, value: $type) -> Result<()> {
                let (page, c) = get_header_page_for_write(pager)?;
                pager.io.wait_for_completion(c)?;
                let page_inner = page.get();
                let page_content = page_inner.contents.as_ref().unwrap();
                let mut buf = page_content.buffer.borrow_mut();
                let buf_slice = buf.as_mut_slice();
                buf_slice[$offset..$offset + std::mem::size_of::<$type>()].copy_from_slice(&value.to_be_bytes());
                turso_assert!(page.get().id == 1, "page must have number 1");
                pager.add_dirty(&page);
                Ok(())
            }
        }
    };
}

// impl_header_field_accessor!(magic, [u8; 16], HEADER_OFFSET_MAGIC);
impl_header_field_accessor!(page_size_u16, u16, HEADER_OFFSET_PAGE_SIZE);
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
impl_header_field_accessor!(application_id, i32, HEADER_OFFSET_APPLICATION_ID);
//impl_header_field_accessor!(reserved_for_expansion, [u8; 20], HEADER_OFFSET_RESERVED_FOR_EXPANSION);
impl_header_field_accessor!(version_valid_for, u32, HEADER_OFFSET_VERSION_VALID_FOR);
impl_header_field_accessor!(version_number, u32, HEADER_OFFSET_VERSION_NUMBER);

pub fn get_page_size(pager: &Pager) -> Result<u32> {
    let size = get_page_size_u16(pager)?;
    if size == 1 {
        return Ok(MAX_PAGE_SIZE);
    }
    Ok(size as u32)
}

#[allow(dead_code)]
pub fn set_page_size(pager: &Pager, value: u32) -> Result<()> {
    let page_size = if value == MAX_PAGE_SIZE {
        1
    } else {
        value as u16
    };
    set_page_size_u16(pager, page_size)
}
