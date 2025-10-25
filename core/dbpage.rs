use crate::storage::pager::Pager;
use crate::util::IOExt;
use crate::vtab::{InternalVirtualTable, InternalVirtualTableCursor};
use crate::{Connection, Result, Value};
use parking_lot::RwLock;
use std::sync::Arc;
use tracing::debug;
use turso_ext::{ConstraintInfo, ConstraintOp, ConstraintUsage, IndexInfo, OrderByInfo, ResultCode};

#[derive(Debug)]
pub struct DbPageTable;

impl DbPageTable {
    pub fn new() -> Self {
        debug!("DbPageTable::new - creating new sqlite_dbpage virtual table module");
        Self
    }
}

impl InternalVirtualTable for DbPageTable {
    fn name(&self) -> String {
        "sqlite_dbpage".to_string()
    }

    fn sql(&self) -> String {
        let schema = "CREATE TABLE sqlite_dbpage(pgno INTEGER PRIMARY KEY, data BLOB, schema HIDDEN)".to_string();
        debug!(schema = schema, "DbPageTable::sql - declaring schema");
        schema
    }

    fn open(&self, conn: Arc<Connection>) -> Result<Arc<RwLock<dyn InternalVirtualTableCursor>>> {
        debug!("DbPageTable::open - creating a new cursor");
        let pager = conn.get_pager();
        let cursor = DbPageCursor::new(pager);
        Ok(Arc::new(RwLock::new(cursor)))
    }

    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> std::result::Result<IndexInfo, ResultCode> {
        let mut idx_num = 0;
        let mut estimated_cost = 1_000_000.0;

        let constraint_usages = constraints
            .iter()
            .enumerate()
            .map(|(i, constraint)| {
                let mut usage = ConstraintUsage {
                    argv_index: None,
                    omit: false,
                };
                if constraint.op == ConstraintOp::Eq {
                    match constraint.column_index {
                        // pgno column
                        0 => {
                            idx_num |= 1;
                            usage.argv_index = Some(1);
                            usage.omit = true;
                            estimated_cost = 1.0;
                        }
                        // schema column
                        2 => {
                            idx_num |= 2;
                            usage.omit = true;
                        }
                        _ => {}
                    }
                }
                usage
            })
            .collect();

        let index_info = IndexInfo {
            idx_num,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost,
            estimated_rows: if (idx_num & 1) != 0 { 1 } else { 1_000_000 },
            constraint_usages,
            ..Default::default()
        };

        debug!(
            constraints = ?constraints,
            plan = ?index_info,
            "DbPageTable::best_index - determined query plan"
        );
        Ok(index_info)
    }
}

pub struct DbPageCursor {
    pager: Arc<Pager>,
    pgno: i64,
    mx_pgno: i64,
}

impl DbPageCursor {
    fn new(pager: Arc<Pager>) -> Self {
        debug!("DbPageCursor::new - initializing cursor state");
        Self {
            pager,
            pgno: 1,
            mx_pgno: 0,
        }
    }
}

impl InternalVirtualTableCursor for DbPageCursor {
    fn filter(
        &mut self,
        args: &[Value],
        _idx_str: Option<String>,
        idx_num: i32,
    ) -> Result<bool> {
        debug!(idx_num, args = ?args, "DbPageCursor::filter - initializing scan");

        let db_size = self.pager.io.block(|| {
            self.pager.with_header(|header| header.database_size.get())
        })?;

        self.mx_pgno = db_size as i64;
        
        if (idx_num & 1) != 0 {

            let pgno = if let Some(Value::Integer(val)) = args.get(0) {
                *val

            } else {
                0
            };
            
            debug!(pgno, "DbPageCursor::filter - point query for specific page");
            if pgno > 0 && pgno <= self.mx_pgno {
                self.pgno = pgno;
                self.mx_pgno = pgno;
            } else {
                self.mx_pgno = 0;
            }
        } else {
            debug!(max_page = self.mx_pgno, "DbPageCursor::filter - full table scan");
            self.pgno = 1;
        }

        Ok(self.pgno <= self.mx_pgno)
    }

    fn next(&mut self) -> Result<bool> {
        debug!(current_pgno = self.pgno, max_pgno = self.mx_pgno, "DbPageCursor::next - advancing cursor");
        self.pgno += 1;
        Ok(self.pgno <= self.mx_pgno)
    }

    fn column(&self, column: usize) -> Result<Value> {
        debug!(column_idx = column, pgno = self.pgno, "DbPageCursor::column - fetching data");
        match column {
            0 => {

                Ok(Value::Integer(self.pgno))
            },
            1 => {
                let (page_ref, completion) = self.pager.read_page(self.pgno)?;
                if let Some(c) = completion {
                    self.pager.io.wait_for_completion(c)?;
                }

                let page_contents = page_ref.get_contents();
                let data_slice = page_contents.buffer.as_slice();
                Ok(Value::from_blob(data_slice.to_vec()))
            }
            2 => Ok(Value::from_text("main")),
            _ => Ok(Value::Null),
        }
    }

    fn rowid(&self) -> i64 {
        debug!(pgno = self.pgno, "DbPageCursor::rowid - returning current page number");
        self.pgno
    }
}