use crate::storage::pager::PageRef;
use crate::{LimboError, Result};

/// Check if a page has an error state (e.g., decryption failed) and return appropriate error
pub fn check_page_error_state(page: &PageRef) -> Result<()> {
    if page.has_error() {
        return Err(LimboError::InternalError(format!(
            "Page {} has error state (likely decryption failed)",
            page.get().id
        )));
    }
    Ok(())
}

/// Check if page is loaded and doesn't have error state, return appropriate error if not
pub fn ensure_page_loaded_without_error(page: &PageRef) -> Result<()> {
    check_page_error_state(page)?;

    if !page.is_loaded() {
        return Err(LimboError::InternalError(format!(
            "Page {} is not loaded",
            page.get().id
        )));
    }

    Ok(())
}
