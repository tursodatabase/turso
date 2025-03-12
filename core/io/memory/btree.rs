//! Normal memory page using btree

use crate::Result;
use core::ops::{Deref, DerefMut};
use std::collections::BTreeMap;

use super::MemPage;

#[derive(Debug)]
pub struct MemoryPages {
    inner: BTreeMap<usize, MemPage>,
}

impl MemoryPages {
    pub fn new(_len: usize) -> Result<Self> {
        Ok(MemoryPages {
            inner: BTreeMap::new(),
        })
    }
}

impl Deref for MemoryPages {
    type Target = BTreeMap<usize, MemPage>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for MemoryPages {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
