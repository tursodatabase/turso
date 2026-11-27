//! The tree storage of a `:model:` database: every tree is a model tree in memory.
//!
//! A version of the database is an immutable map from root page to tree. The connections share
//! the committed version. A read transaction keeps the version it started with. A write
//! transaction works on its own copy of the map; the first write to a tree copies that tree.
//! Commit publishes the copy. A savepoint keeps a copy of the map, and a rollback to it puts
//! the copy back. The real pager still decides who may write, so two writers never meet here.

use super::cursor::{ModelCursor, SharedTree};
use crate::storage::btree::CursorTrait;
use crate::storage::pager::Pager;
use crate::storage::tree_store::{SavedTrees, TreeKind, TreeStorage, TreeStore};
use crate::sync::{Arc, Mutex};
use crate::types::IndexInfo;
use crate::{LimboError, Result};
use std::collections::BTreeMap;

/// One version of all trees of the database.
type Trees = BTreeMap<i64, Arc<SharedTree>>;

/// The model storage of one database.
#[derive(Default)]
pub struct ModelStorage {
    committed: Arc<Mutex<Arc<Trees>>>,
}

impl TreeStorage for ModelStorage {
    fn connect(&self) -> Arc<dyn TreeStore> {
        Arc::new(ConnStore {
            trees: Arc::new(ConnTrees {
                committed: self.committed.clone(),
                tx: Mutex::new(Tx::default()),
            }),
        })
    }
}

/// The transaction state of one connection.
#[derive(Default)]
struct Tx {
    /// The version that the read transaction sees.
    read: Option<Arc<Trees>>,
    /// The copy that the write transaction changes.
    write: Option<Trees>,
}

/// The trees as one connection sees them. Cursors look up their tree here at each op.
pub(super) struct ConnTrees {
    committed: Arc<Mutex<Arc<Trees>>>,
    tx: Mutex<Tx>,
}

impl ConnTrees {
    /// Runs `f` on the tree `root` of the version that the connection sees now. A tree that does
    /// not exist reads as empty.
    pub(super) fn read_tree<R>(
        &self,
        root: i64,
        info: &Option<Arc<IndexInfo>>,
        f: impl FnOnce(&SharedTree) -> R,
    ) -> R {
        let tx = self.tx.lock();
        let tree = match (&tx.write, &tx.read) {
            (Some(write), _) => write.get(&root).cloned(),
            (None, Some(read)) => read.get(&root).cloned(),
            (None, None) => self.committed.lock().get(&root).cloned(),
        };
        match tree {
            Some(tree) if tree.fits(info) => f(&tree),
            _ => f(&SharedTree::missing(info.clone())),
        }
    }

    /// Runs `f` on the tree `root` of the write transaction.
    pub(super) fn write_tree<R>(
        &self,
        root: i64,
        info: &Option<Arc<IndexInfo>>,
        f: impl FnOnce(&mut SharedTree) -> R,
    ) -> R {
        let mut tx = self.tx.lock();
        let write = self.write_version(&mut tx);
        let entry = write
            .entry(root)
            .or_insert_with(|| Arc::new(SharedTree::new(info.clone())));
        if !entry.fits(info) {
            *entry = Arc::new(SharedTree::new(info.clone()));
        }
        f(Arc::make_mut(entry))
    }

    /// The copy of the write transaction. A write outside a write transaction starts one.
    fn write_version<'a>(&self, tx: &'a mut Tx) -> &'a mut Trees {
        if tx.write.is_none() {
            let base = match &tx.read {
                Some(read) => read.clone(),
                None => self.committed.lock().clone(),
            };
            tx.write = Some((*base).clone());
        }
        tx.write.as_mut().expect("the write version exists")
    }
}

/// The `TreeStore` of one connection.
struct ConnStore {
    trees: Arc<ConnTrees>,
}

fn unsupported(feature: &str) -> LimboError {
    LimboError::InternalError(format!("unsupported in model storage: {feature}"))
}

fn index_info(kind: &TreeKind<'_>) -> Result<Option<Arc<IndexInfo>>> {
    match kind {
        TreeKind::Table => Ok(None),
        TreeKind::WithoutRowidTable => Err(unsupported("WITHOUT ROWID")),
        TreeKind::Index(index) => Ok(Some(Arc::new(IndexInfo::new_from_index(index)?))),
        TreeKind::Keys(info) => Ok(Some(Arc::clone(info))),
    }
}

impl TreeStore for ConnStore {
    fn open_cursor(
        &self,
        pager: Arc<Pager>,
        root: i64,
        kind: TreeKind<'_>,
    ) -> Result<Box<dyn CursorTrait>> {
        let info = index_info(&kind)?;
        Ok(Box::new(ModelCursor::on_tree(
            pager,
            self.trees.clone(),
            root,
            info,
        )))
    }

    fn open_ephemeral(
        &self,
        pager: Arc<Pager>,
        kind: TreeKind<'_>,
    ) -> Result<Box<dyn CursorTrait>> {
        let info = index_info(&kind)?;
        Ok(Box::new(ModelCursor::ephemeral(pager, info)))
    }

    fn create_tree(&self, root: i64) {
        // The first write gives the tree its kind (table or index).
        let mut tx = self.trees.tx.lock();
        self.trees.write_version(&mut tx).remove(&root);
    }

    fn destroy_tree(&self, root: i64) {
        let mut tx = self.trees.tx.lock();
        self.trees.write_version(&mut tx).remove(&root);
    }

    fn clear_tree(&self, root: i64) -> u64 {
        let mut tx = self.trees.tx.lock();
        let write = self.trees.write_version(&mut tx);
        let Some(tree) = write.get_mut(&root) else {
            return 0;
        };
        let n = tree.len() as u64;
        Arc::make_mut(tree).clear();
        n
    }

    fn begin_read(&self) {
        let mut tx = self.trees.tx.lock();
        if tx.read.is_none() {
            tx.read = Some(self.trees.committed.lock().clone());
        }
    }

    fn begin_write(&self) {
        let mut tx = self.trees.tx.lock();
        self.trees.write_version(&mut tx);
    }

    fn end_read(&self) {
        let mut tx = self.trees.tx.lock();
        if tx.write.is_none() {
            tx.read = None;
        }
    }

    fn commit(&self) {
        let mut tx = self.trees.tx.lock();
        if let Some(write) = tx.write.take() {
            *self.trees.committed.lock() = Arc::new(write);
        }
        tx.read = None;
    }

    fn rollback(&self) {
        let mut tx = self.trees.tx.lock();
        tx.write = None;
        tx.read = None;
    }

    fn save(&self) -> SavedTrees {
        // `None`: no write yet; a rollback then goes back to the read version.
        let tx = self.trees.tx.lock();
        Arc::new(tx.write.clone())
    }

    fn restore(&self, saved: &SavedTrees) {
        let trees = saved
            .downcast_ref::<Option<Trees>>()
            .expect("a model savepoint holds model trees");
        self.trees.tx.lock().write = trees.clone();
    }

    fn check_feature(&self, feature: &str) -> Result<()> {
        Err(unsupported(feature))
    }
}
