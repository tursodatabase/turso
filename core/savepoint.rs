//! Savepoint management for transaction nesting.
//!
//! This module implements the savepoint stack that allows for nested transactions
//! as described in the SQLite documentation. Savepoints provide a way to create
//! named transaction checkpoints that can be rolled back to or released.

use crate::error::LimboError;
use crate::storage::wal::SavepointWalData;
use crate::{Result, Wal};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

/// Represents a single savepoint in the transaction stack.
#[derive(Debug, Clone)]
pub struct Savepoint {
    pub name: String,
    pub wal_data: SavepointWalData,
}

impl Savepoint {
    pub fn new(name: String, wal: Rc<RefCell<dyn Wal>>) -> Self {
        Self {
            name,
            wal_data: wal.borrow().savepoint(),
        }
    }
}

/// Manages a stack of savepoints for nested transaction support.
///
/// The stack follows these rules:
/// 1. SAVEPOINT pushes a new savepoint onto the stack
/// 2. RELEASE removes savepoints from the stack backwards until it finds a matching name
/// 3. ROLLBACK TO rolls back to a savepoint and removes all savepoints after it
/// 4. The outermost savepoint behaves like BEGIN DEFERRED if not in a transaction
/// 5. RELEASE of the outermost savepoint is equivalent to COMMIT
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

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.savepoints.is_empty()
    }

    pub fn len(&self) -> usize {
        self.savepoints.len()
    }

    pub fn push_savepoint(&mut self, name: String, wal: Rc<RefCell<dyn Wal>>) {
        let savepoint = Savepoint::new(name, wal);
        self.savepoints.push_back(savepoint);
    }

    pub fn find_savepoint(&self, name: &str) -> Option<usize> {
        self.savepoints.iter().rposition(|sp| sp.name == name)
    }

    /// Release savepoints backwards until we find a matching name.
    /// This removes all savepoints from the matching one to the end of the stack.
    /// Returns the savepoint that was released and whether it was the outermost.
    pub fn release_savepoint(&mut self, name: &str) -> Result<(Savepoint, bool)> {
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
    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<Savepoint> {
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
