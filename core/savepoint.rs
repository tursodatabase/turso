//! Savepoint management for transaction nesting.
//!
//! This module implements the savepoint stack that allows for nested transactions
//! as described in the SQLite documentation. Savepoints provide a way to create
//! named transaction checkpoints that can be rolled back to or released. There are
//! two types of savepoints:
//!     - PagerSavepoints: Manage the low-level data required to rollback/release a savepoint
//!     - Savepoints: At connection level, each one of this savepoint represents a savepoint
//!             done by the user.

use crate::error::LimboError;
use crate::storage::wal::SavepointWalData;
use crate::{Result, Wal};
use bitvec::vec::BitVec;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

/// Savepoint operation types
#[derive(Clone, Copy, Debug)]
pub enum SavepointOp {
    Begin = 0,
    Release = 1,
    Rollback = 2,
}

/// Savepoint stored by the Pager
#[derive(Debug, Clone)]
pub struct PagerSavepoint {
    /// Offset in subjournal
    pub offset: i64,
    /// Original number of pages in file
    pub n_origin: usize,
    /// Index of first record in sub_journal
    pub sub_rec_idx: usize,
    /// Set of pages in this savepoint
    pub pages: BitVec,
    /// If the subjournal should be trucated on release
    /// Not implemented yet
    pub truncate_on_release: bool,
    /// Wal context
    pub wal_data: SavepointWalData,
}

impl PagerSavepoint {
    pub fn new(
        wal: Rc<RefCell<dyn Wal>>,
        db_size: usize,
        journal_offset: i64,
        sub_rec_idx: usize,
    ) -> Self {
        Self {
            wal_data: wal.borrow().savepoint(),
            offset: journal_offset,
            pages: BitVec::with_capacity(db_size),
            n_origin: db_size,
            sub_rec_idx,
            truncate_on_release: true,
        }
    }
}

pub struct PagerSavepointVec {
    savepoints: VecDeque<PagerSavepoint>,
}

impl PagerSavepointVec {
    pub fn new() -> Self {
        Self {
            savepoints: VecDeque::new(),
        }
    }

    pub fn close(&mut self) {
        self.savepoints.clear();
    }

    pub fn len(&self) -> usize {
        self.savepoints.len()
    }

    pub fn is_empty(&self) -> bool {
        self.savepoints.is_empty()
    }

    pub fn add_page(&mut self, page_id: usize) {
        for sp in self.savepoints.iter_mut() {
            if page_id <= sp.n_origin {
                // safety: Insert may panic if index is out of bounds
                if page_id >= sp.pages.len() {
                    sp.pages.resize(page_id + 1, false);
                }

                sp.pages.insert(page_id, true);
            }
        }
    }

    pub fn requires_page(&mut self, page_id: usize) -> bool {
        for (i, sp) in self.savepoints.iter().enumerate() {
            if sp.n_origin >= page_id && sp.pages.get(page_id).is_none() {
                for j in (i + 1)..self.savepoints.len() {
                    self.savepoints[j].truncate_on_release = false;
                }
                return true;
            }
        }
        false
    }

    pub fn get(&mut self, idx: usize) -> &mut PagerSavepoint {
        self.savepoints.get_mut(idx).unwrap()
    }

    /// Release savepoints backwards to the given position.
    /// This removes all savepoints from the matching one to the end of the stack.
    pub fn drain(&mut self, idx: usize) {
        self.savepoints.drain(idx..);
    }

    pub fn open_savepoint(
        &mut self,
        n_savepoint: usize,
        wal: Rc<RefCell<dyn Wal>>,
        db_size: usize,
        journal_offset: i64,
        sub_rec_idx: usize,
    ) -> Result<()> {
        let n_current = self.savepoints.len();

        // Only create new savepoints if we need more
        if n_savepoint > n_current {
            self.savepoints.reserve(n_savepoint - n_current);

            // Create new savepoints from current count to target count
            for _ in n_current..n_savepoint {
                let savepoint =
                    PagerSavepoint::new(wal.clone(), db_size, journal_offset, sub_rec_idx);
                self.savepoints.push_back(savepoint);
            }
        }

        Ok(())
    }
}

/// Represents a single savepoint in the transaction stack.
#[derive(Debug, Clone)]
pub struct Savepoint(String);

/// Manages a stack of savepoints for nested transaction support.
// Maybe use a generic structure to handle both types of savepoints?
#[derive(Debug, Default)]
pub struct SavepointStack {
    /// The stack of savepoints, with the most recent at the back
    savepoints: VecDeque<Savepoint>,
}

impl SavepointStack {
    pub fn new() -> Self {
        Self {
            savepoints: VecDeque::new(),
        }
    }

    pub fn close(&mut self) {
        self.savepoints.clear();
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.savepoints.is_empty()
    }

    pub fn len(&self) -> usize {
        self.savepoints.len()
    }

    pub fn push_savepoint(&mut self, name: String) {
        self.savepoints.push_back(Savepoint(name));
    }

    pub fn find_savepoint(&self, name: &str) -> Option<usize> {
        self.savepoints.iter().rposition(|sp| sp.0 == name)
    }

    /// Release savepoints backwards until we find a matching name.
    /// This removes all savepoints from the matching one to the end of the stack.
    /// Returns the savepoint that was released and whether it was the outermost.
    pub fn release(&mut self, name: &str) -> Result<(Savepoint, bool)> {
        if let Some(index) = self.find_savepoint(name) {
            let savepoint = self.savepoints[index].clone();
            let is_last_savepoint = index == 0;

            self.savepoints.drain(index..);

            Ok((savepoint, is_last_savepoint))
        } else {
            Err(LimboError::NoSuchSavepoint(name.to_string()))
        }
    }

    /// Rollback to a savepoint, removing all savepoints after it but keeping the target.
    /// Returns the savepoint we're rolling back to.
    pub fn rollback_to(&mut self, name: &str) -> Result<Savepoint> {
        if let Some(index) = self.find_savepoint(name) {
            let savepoint = self.savepoints[index].clone();

            // Remove all savepoints after the target (but keep the target)
            self.savepoints.drain((index + 1)..);

            Ok(savepoint)
        } else {
            Err(LimboError::NoSuchSavepoint(name.to_string()))
        }
    }
}
