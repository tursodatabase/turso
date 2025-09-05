use crate::incremental::view::IncrementalView;
use crate::{Connection, LimboError, Value, VirtualTable};
use std::sync::{Arc, Mutex};

/// Create a virtual table wrapper for a view
pub fn create_view_virtual_table(
    view_name: &str,
    view: Arc<Mutex<IncrementalView>>,
) -> crate::Result<Arc<VirtualTable>> {
    // Use the VirtualTable::view method we added
    let view_locked = view.lock().map_err(|_| {
        LimboError::InternalError("Failed to lock view for virtual table creation".to_string())
    })?;
    let columns = view_locked.columns.clone();
    drop(view_locked); // Release the lock before passing the Arc
    VirtualTable::view(view_name, columns, view)
}

/// Virtual table wrapper for incremental views
#[derive(Clone, Debug)]
pub struct ViewVirtualTable {
    pub view: Arc<Mutex<IncrementalView>>,
}

impl ViewVirtualTable {
    pub fn best_index(&self) -> Result<turso_ext::IndexInfo, turso_ext::ResultCode> {
        // Views don't use indexes - return a simple index info
        Ok(turso_ext::IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1000000.0,
            estimated_rows: 1000,
            constraint_usages: Vec::new(),
        })
    }

    pub fn open(&self, conn: Arc<Connection>) -> crate::Result<ViewVirtualTableCursor> {
        // Views are now populated during schema parsing (in parse_schema_rows)
        // so we just get the current data from the view.

        let view = self.view.lock().map_err(|_| {
            LimboError::InternalError("Failed to lock view for reading".to_string())
        })?;

        let tx_states = conn.view_transaction_states.borrow();
        let tx_state = tx_states.get(view.name());

        let data: Vec<(i64, Vec<Value>)> = view.current_data(tx_state);
        Ok(ViewVirtualTableCursor {
            data,
            current_pos: 0,
        })
    }
}

/// Cursor for iterating over view data
pub struct ViewVirtualTableCursor {
    data: Vec<(i64, Vec<Value>)>,
    current_pos: usize,
}

impl ViewVirtualTableCursor {
    pub fn next(&mut self) -> crate::Result<bool> {
        if self.current_pos < self.data.len() {
            self.current_pos += 1;
            Ok(self.current_pos < self.data.len())
        } else {
            Ok(false)
        }
    }

    pub fn rowid(&self) -> i64 {
        if self.current_pos < self.data.len() {
            self.data[self.current_pos].0
        } else {
            -1
        }
    }

    pub fn column(&self, column: usize) -> crate::Result<Value> {
        if self.current_pos >= self.data.len() {
            return Ok(Value::Null);
        }

        let (_row_key, values) = &self.data[self.current_pos];

        // Return the value at the requested column index
        if let Some(value) = values.get(column) {
            Ok(value.clone())
        } else {
            Ok(Value::Null)
        }
    }

    pub fn filter(&mut self, _args: Vec<Value>) -> crate::Result<bool> {
        // Reset to beginning for new filter
        self.current_pos = 0;
        Ok(!self.data.is_empty())
    }

    pub fn eof(&self) -> bool {
        self.current_pos >= self.data.len()
    }
}
