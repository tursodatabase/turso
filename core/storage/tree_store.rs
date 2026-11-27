//! A seam for the place where the trees of a database live.
//!
//! By default the trees are B-trees in the pages of the pager, and nothing here is used. A
//! database can instead have a [`TreeStorage`]: then each connection gets a [`TreeStore`], and
//! the opcodes that open cursors or create, destroy and clear trees, and the transaction and
//! savepoint entry points of the pager, ask it. The pager still keeps the header, the locks and
//! the root page numbers. Everything that runs on a cursor goes through `CursorTrait`, so the
//! VDBE does not change.

use crate::schema::Index;
use crate::storage::btree::CursorTrait;
use crate::storage::pager::Pager;
use crate::sync::Arc;
use crate::types::IndexInfo;
use crate::Result;
use std::any::Any;

/// The tree storage of one database. It is shared by all connections.
pub trait TreeStorage: Send + Sync {
    /// The handle of one connection, which holds its transaction state.
    fn connect(&self) -> Arc<dyn TreeStore>;
}

/// What a cursor is opened on.
pub enum TreeKind<'a> {
    /// A table with a rowid.
    Table,
    /// A table without a rowid: its rows are stored as index records.
    WithoutRowidTable,
    Index(&'a Index),
    /// An index tree that has no schema entry (the backing store of an index method).
    Keys(&'a Arc<IndexInfo>),
}

/// A saved version of the trees, for a savepoint.
pub type SavedTrees = Arc<dyn Any + Send + Sync>;

/// The tree storage as one connection sees it.
pub trait TreeStore: Send + Sync {
    /// A cursor on the tree with root page `root`.
    fn open_cursor(
        &self,
        pager: Arc<Pager>,
        root: i64,
        kind: TreeKind<'_>,
    ) -> Result<Box<dyn CursorTrait>>;
    /// A cursor on a new tree that only this cursor (and its `OpenDup` copies) can see.
    fn open_ephemeral(&self, pager: Arc<Pager>, kind: TreeKind<'_>)
        -> Result<Box<dyn CursorTrait>>;
    /// The pager allocated root page `root` for a new tree.
    fn create_tree(&self, root: i64);
    fn destroy_tree(&self, root: i64);
    /// Removes every entry. Returns how many there were.
    fn clear_tree(&self, root: i64) -> u64;

    fn begin_read(&self);
    fn begin_write(&self);
    fn end_read(&self);
    fn commit(&self);
    fn rollback(&self);
    /// The current version of the trees of the transaction, for a savepoint.
    fn save(&self) -> SavedTrees;
    /// Goes back to a version that `save` returned.
    fn restore(&self, saved: &SavedTrees);

    /// `Err` if this storage does not support `feature` (for example `ATTACH` or a pragma
    /// that reads pages). The error names the feature.
    fn check_feature(&self, feature: &str) -> Result<()>;
}

/// The path of a database whose trees are all in the in-memory spec-model of the cursor contract
/// (`storage::spec_model`). It is like `:memory:`, and exists only in debug builds.
pub const SPEC_MODEL_PATH: &str = ":spec-model:";

/// The tree storage for a database at `path`: the spec-model storage for
/// [`SPEC_MODEL_PATH`], else none.
/// This is the only place that picks a storage.
pub fn tree_storage_for_path(path: &str) -> Result<Option<Arc<dyn TreeStorage>>> {
    if path != SPEC_MODEL_PATH {
        return Ok(None);
    }
    #[cfg(debug_assertions)]
    {
        Ok(Some(Arc::new(
            crate::storage::spec_model::SpecModelStorage::default(),
        )))
    }
    #[cfg(not(debug_assertions))]
    {
        Err(crate::LimboError::InvalidArgument(
            "the :spec-model: database exists only in debug builds".to_string(),
        ))
    }
}
