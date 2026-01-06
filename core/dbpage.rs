use crate::storage::pager::Pager;
use crate::util::IOExt;
use crate::vtab::{InternalVirtualTable, InternalVirtualTableCursor};
use crate::{Connection, Result, Value};
use parking_lot::RwLock;
use std::sync::Arc;
use turso_ext::{
    ConstraintInfo, ConstraintOp, ConstraintUsage, IndexInfo, OrderByInfo, ResultCode,
};

#[derive(Debug)]
pub struct DbPageTable;

impl Default for DbPageTable {
    fn default() -> Self {
        Self::new()
    }
}

impl DbPageTable {
    pub fn new() -> Self {
        Self
    }
}

impl InternalVirtualTable for DbPageTable {
    fn name(&self) -> String {
        "sqlite_dbpage".to_string()
    }

    fn sql(&self) -> String {
        "CREATE TABLE sqlite_dbpage(pgno INTEGER PRIMARY KEY, data BLOB, schema HIDDEN)".to_string()
    }

    fn open(&self, conn: Arc<Connection>) -> Result<Arc<RwLock<dyn InternalVirtualTableCursor>>> {
        let pager = conn.get_pager();
        let cursor = DbPageCursor::new(pager);
        Ok(Arc::new(RwLock::new(cursor)))
    }

    /// TODO: sqlite does where_onerow optimization using idx_flag, we should do that eventually.. probably not needed for now.
    /// Analyzes query constraints and returns cost estimates to help pick the best query plan.
    ///
    /// We encode constraint info into `idx_num` as a bitmask, which `filter()` later uses:
    /// - Bit 0 (0x1): equality on `pgno` - enables single-page lookup
    /// - Bit 1 (0x2): equality on `schema` - we only support "main", so we let Turso handle filtering if the user provides a different schema.
    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> std::result::Result<IndexInfo, ResultCode> {
        let mut idx_num = 0;
        let mut estimated_cost = 1_000_000.0;

        let constraint_usages = constraints
            .iter()
            .map(|constraint| {
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
                            usage.argv_index = Some(if (idx_num & 1) != 0 { 2 } else { 1 });
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
        };

        Ok(index_info)
    }
}

pub struct DbPageCursor {
    pager: Arc<Pager>,
    pgno: i64,
    mx_pgno: i64,
    /// If true, schema constraint was for non-"main" schema, so return no rows
    schema_mismatch: bool,
}

impl DbPageCursor {
    fn new(pager: Arc<Pager>) -> Self {
        Self {
            pager,
            pgno: 1,
            mx_pgno: 0,
            schema_mismatch: false,
        }
    }
}

impl InternalVirtualTableCursor for DbPageCursor {
    /// iterates based on constraints identified by `best_index()`.
    ///
    /// When `idx_num` has bit 0 set, we do a single-page lookup using `args[0]` as the page number.
    /// If the requested page is out of range (≤0 or beyond db size), the scan returns empty.
    /// Otherwise, we do a full table scan over all pages starting from page 1.
    fn filter(&mut self, args: &[Value], _idx_str: Option<String>, idx_num: i32) -> Result<bool> {
        self.schema_mismatch = false;

        let db_size = self
            .pager
            .io
            .block(|| self.pager.with_header(|header| header.database_size.get()))?;

        self.mx_pgno = db_size as i64;

        let mut arg_idx = 0;

        if (idx_num & 1) != 0 {
            let pgno = if let Some(Value::Integer(val)) = args.get(arg_idx) {
                *val
            } else {
                0
            };
            arg_idx += 1;

            if pgno > 0 && pgno <= self.mx_pgno {
                self.pgno = pgno;
                self.mx_pgno = pgno;
            } else {
                self.mx_pgno = 0;
            }
        } else {
            self.pgno = 1;
        }

        if (idx_num & 2) != 0 {
            if let Some(Value::Text(schema)) = args.get(arg_idx) {
                if schema.as_str() != "main" {
                    self.schema_mismatch = true;
                    return Ok(false);
                }
            }
        }

        Ok(self.pgno <= self.mx_pgno)
    }

    fn next(&mut self) -> Result<bool> {
        if self.schema_mismatch {
            return Ok(false);
        }
        self.pgno += 1;
        Ok(self.pgno <= self.mx_pgno)
    }

    fn column(&self, column: usize) -> Result<Value> {
        match column {
            0 => Ok(Value::Integer(self.pgno)),
            1 => {
                // check for the pending byte page  - this only needs when db is more than 1 gb.
                if let Some(pending_page) = self.pager.pending_byte_page_id() {
                    if self.pgno == pending_page as i64 {
                        let page_size = self.pager.usable_space()
                            + self.pager.get_reserved_space().unwrap_or(0) as usize;
                        return Ok(Value::from_blob(vec![0u8; page_size]));
                    }
                }

                let (page_ref, completion) = self.pager.read_page(self.pgno)?;
                if let Some(c) = completion {
                    self.pager.io.wait_for_completion(c)?;
                }

                let page_contents = page_ref.get_contents();
                let data_slice = page_contents.as_ptr();
                Ok(Value::from_blob(data_slice.to_vec()))
            }
            2 => Ok(Value::from_text("main")), // we don't support multiple databases - todo when we do
            _ => Ok(Value::Null),
        }
    }

    fn rowid(&self) -> i64 {
        self.pgno
    }
}
