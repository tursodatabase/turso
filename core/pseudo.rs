use crate::{
    types::{ImmutableRecord, RecordCursor},
    Result, Value,
};

pub struct PseudoCursor {
    record_cursor: RecordCursor,
    current: Option<ImmutableRecord>,
}

impl Default for PseudoCursor {
    #[inline]
    fn default() -> Self {
        Self {
            record_cursor: RecordCursor::new(),
            current: None,
        }
    }
}

impl PseudoCursor {
    #[inline]
    pub fn record(&self) -> Option<&ImmutableRecord> {
        self.current.as_ref()
    }

    #[inline]
    pub fn insert(&mut self, record: ImmutableRecord) {
        self.record_cursor.invalidate();
        self.current = Some(record);
    }

    #[inline]
    pub fn get_value(&mut self, column: usize) -> Result<Value> {
        if let Some(record) = self.current.as_ref() {
            Ok(self.record_cursor.get_value(record, column)?.to_owned())
        } else {
            Ok(Value::Null)
        }
    }
}
