use crate::types::LazyRecord;

pub struct PseudoCursor {
    current: Option<LazyRecord>,
}

impl PseudoCursor {
    pub fn new() -> Self {
        Self { current: None }
    }

    pub fn record(&self) -> Option<&LazyRecord> {
        self.current.as_ref()
    }

    pub fn insert(&mut self, record: LazyRecord) {
        self.current = Some(record);
    }
}
