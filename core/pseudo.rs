use std::cell::{Ref, RefCell};

use crate::{
    types::{ImmutableRecord, RecordCursor},
    Result, Value,
};

pub struct PseudoCursor {
    record_cursor: RecordCursor,
    current: RefCell<Option<ImmutableRecord>>,
}

impl Default for PseudoCursor {
    fn default() -> Self {
        Self {
            record_cursor: RecordCursor::new(),
            current: RefCell::new(None),
        }
    }
}

impl PseudoCursor {
    pub fn record(&self) -> Ref<Option<ImmutableRecord>> {
        self.current.borrow()
    }

    pub fn insert(&mut self, record: ImmutableRecord) {
        self.record_cursor.invalidate();
        self.current.replace(Some(record));
    }

    pub fn get_value(&mut self, column: usize) -> Result<Value> {
        if let Some(record) = self.current.borrow().as_ref() {
            Ok(self.record_cursor.get_value(record, column)?.to_owned())
        } else {
            Ok(Value::Null)
        }
    }
}
