use crate::schema::Schema;
use crate::storage::btree::BTreeCursor;
use crate::storage::btree::CursorTrait;
use crate::storage::pager::Pager;
use crate::sync::Arc;
use crate::sync::RwLock;
use crate::util::IOExt;
use crate::vtab::{InternalVirtualTable, InternalVirtualTableCursor};
use crate::{Connection, Result, Value};
use turso_ext::{
    ConstraintInfo, ConstraintOp, ConstraintUsage, IndexInfo, OrderByInfo, ResultCode,
};

#[derive(Debug)]
pub struct BtreeDumpTable;

impl Default for BtreeDumpTable {
    fn default() -> Self {
        Self::new()
    }
}

impl BtreeDumpTable {
    pub fn new() -> Self {
        Self
    }
}

impl InternalVirtualTable for BtreeDumpTable {
    fn name(&self) -> String {
        "btree_dump".to_string()
    }

    fn sql(&self) -> String {
        "CREATE TABLE btree_dump(record BLOB, name TEXT HIDDEN)".to_string()
    }

    fn open(&self, conn: Arc<Connection>) -> Result<Arc<RwLock<dyn InternalVirtualTableCursor>>> {
        let pager = conn.get_pager();
        let schema = conn.schema.read().clone();
        let cursor = BtreeDumpCursor::new(pager, schema);
        Ok(Arc::new(RwLock::new(cursor)))
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> std::result::Result<IndexInfo, ResultCode> {
        let mut usages = vec![
            ConstraintUsage {
                argv_index: None,
                omit: false,
            };
            constraints.len()
        ];

        let mut name_idx: Option<usize> = None;
        for (i, c) in constraints.iter().enumerate() {
            if !c.usable || c.op != ConstraintOp::Eq {
                continue;
            }
            // column 1 is the hidden `name` column
            if c.column_index == 1 {
                name_idx = Some(i);
            }
        }

        if let Some(idx) = name_idx {
            usages[idx] = ConstraintUsage {
                argv_index: Some(1),
                omit: true,
            };
        }

        let (cost, rows) = if name_idx.is_some() {
            (1.0, 100)
        } else {
            (f64::MAX, 100)
        };

        Ok(IndexInfo {
            idx_num: if name_idx.is_some() { 1 } else { 0 },
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: cost,
            estimated_rows: rows,
            constraint_usages: usages,
        })
    }
}

pub struct BtreeDumpCursor {
    pager: Arc<Pager>,
    schema: Arc<Schema>,
    cursor: Option<BTreeCursor>,
    current_record: Option<Vec<u8>>,
    row_idx: i64,
}

impl BtreeDumpCursor {
    fn new(pager: Arc<Pager>, schema: Arc<Schema>) -> Self {
        Self {
            pager,
            schema,
            cursor: None,
            current_record: None,
            row_idx: 0,
        }
    }

    /// Look up the root page for a given name (index first, then table).
    fn find_root_page(&self, name: &str) -> Option<i64> {
        // Search indexes first (indexes are stored by table name, so iterate all)
        for indexes in self.schema.indexes.values() {
            for index in indexes {
                if index.name.eq_ignore_ascii_case(name) {
                    return Some(index.root_page);
                }
            }
        }
        // Then search tables
        if let Some(table) = self.schema.get_table(name) {
            return table.get_root_page().ok();
        }
        None
    }

    fn read_current_record(&mut self) -> Result<()> {
        self.current_record = None;
        if let Some(ref mut cursor) = self.cursor {
            let payload = self.pager.io.block(|| {
                let record = cursor.record()?;
                match record {
                    crate::types::IOResult::Done(Some(rec)) => Ok(crate::types::IOResult::Done(
                        Some(rec.get_payload().to_vec()),
                    )),
                    crate::types::IOResult::Done(None) => Ok(crate::types::IOResult::Done(None)),
                    crate::types::IOResult::IO(io) => Ok(crate::types::IOResult::IO(io)),
                }
            })?;
            self.current_record = payload;
        }
        Ok(())
    }
}

impl InternalVirtualTableCursor for BtreeDumpCursor {
    fn filter(&mut self, args: &[Value], _idx_str: Option<String>, idx_num: i32) -> Result<bool> {
        self.cursor = None;
        self.current_record = None;
        self.row_idx = 0;

        if idx_num != 1 {
            // No name constraint provided — return no rows
            return Ok(false);
        }

        let name = match args.first() {
            Some(Value::Text(s)) => s.as_str(),
            _ => return Ok(false),
        };

        let root_page = match self.find_root_page(name) {
            Some(rp) if rp > 0 => rp,
            Some(_) => {
                return Err(crate::LimboError::InternalError(format!(
                    "btree_dump: '{name}' has no physical btree (MVCC non-checkpointed)",
                )));
            }
            None => {
                return Err(crate::LimboError::InternalError(format!(
                    "btree_dump: no such table or index: '{name}'",
                )));
            }
        };

        let mut btree_cursor = BTreeCursor::new(self.pager.clone(), root_page, 0);
        self.pager.io.block(|| btree_cursor.rewind())?;
        self.cursor = Some(btree_cursor);
        self.read_current_record()?;
        Ok(self.current_record.is_some())
    }

    fn next(&mut self) -> Result<bool> {
        if let Some(ref mut cursor) = self.cursor {
            self.pager.io.block(|| cursor.next())?;
            self.row_idx += 1;
            self.read_current_record()?;
            Ok(self.current_record.is_some())
        } else {
            Ok(false)
        }
    }

    fn column(&self, column: usize) -> Result<Value> {
        match column {
            0 => match &self.current_record {
                Some(data) => Ok(Value::from_blob(data.clone())),
                None => Ok(Value::Null),
            },
            _ => Ok(Value::Null),
        }
    }

    fn rowid(&self) -> i64 {
        self.row_idx
    }
}
