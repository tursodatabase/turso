use super::sqlite3_ondisk::{DatabaseHeader, PageContent};
use crate::turso_assert;
use crate::{
    storage::pager::{PageRef, Pager},
    types::IOResult,
    LimboError, Result,
};
use std::cell::{Ref, RefMut};

pub struct HeaderRef(PageRef);

impl HeaderRef {
    pub fn from_pager(pager: &Pager) -> Result<IOResult<Self>> {
        if !pager.db_state.is_initialized() {
            return Err(LimboError::Page1NotAlloc);
        }

        let (page, _c) = pager.read_page(DatabaseHeader::PAGE_ID)?;
        if page.is_locked() {
            return Ok(IOResult::IO);
        }

        turso_assert!(
            page.get().id == DatabaseHeader::PAGE_ID,
            "incorrect header page id"
        );

        Ok(IOResult::Done(Self(page)))
    }

    pub fn borrow(&self) -> Ref<'_, DatabaseHeader> {
        // TODO: Instead of erasing mutability, implement `get_mut_contents` and return a shared reference.
        let content: &PageContent = self.0.get_contents();
        Ref::map(content.buffer.borrow(), |buffer| {
            bytemuck::from_bytes::<DatabaseHeader>(&buffer.as_slice()[0..DatabaseHeader::SIZE])
        })
    }
}

pub struct HeaderRefMut(PageRef);

impl HeaderRefMut {
    pub fn from_pager(pager: &Pager) -> Result<IOResult<Self>> {
        if !pager.db_state.is_initialized() {
            return Err(LimboError::Page1NotAlloc);
        }

        let (page, _c) = pager.read_page(DatabaseHeader::PAGE_ID)?;
        if page.is_locked() {
            return Ok(IOResult::IO);
        }

        turso_assert!(
            page.get().id == DatabaseHeader::PAGE_ID,
            "incorrect header page id"
        );

        pager.add_dirty(&page);

        Ok(IOResult::Done(Self(page)))
    }

    pub fn borrow_mut(&self) -> RefMut<'_, DatabaseHeader> {
        let content = self.0.get_contents();
        RefMut::map(content.buffer.borrow_mut(), |buffer| {
            bytemuck::from_bytes_mut::<DatabaseHeader>(
                &mut buffer.as_mut_slice()[0..DatabaseHeader::SIZE],
            )
        })
    }
}
