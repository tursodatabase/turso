use crate::{types::ImmutableRecord, Result, Value};

pub struct PseudoCursor {
    current: Option<ImmutableRecord>,
}

impl Default for PseudoCursor {
    #[inline]
    fn default() -> Self {
        Self { current: None }
    }
}

impl PseudoCursor {
    #[inline]
    pub fn record(&self) -> Option<&ImmutableRecord> {
        self.current.as_ref()
    }

    #[inline]
    pub fn insert(&mut self, record: ImmutableRecord) {
        self.current = Some(record);
    }

    #[inline]
    pub fn get_value(&self, column: usize) -> Result<Value> {
        if let Some(record) = self.current.as_ref() {
            Ok(record.get_value(column)?.to_owned())
        } else {
            Ok(Value::Null)
        }
    }
}
