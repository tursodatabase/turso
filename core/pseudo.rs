use crate::types::ImmutableRecord;

pub struct PseudoCursor {
    current: Option<ImmutableRecord>,
}

impl PseudoCursor {
    pub fn new() -> Self {
        Self { current: None }
    }

    pub fn record(&self) -> Option<&ImmutableRecord> {
        self.current.as_ref()
    }

    pub fn insert(&mut self, record: ImmutableRecord) {
        self.current = Some(record);
    }
}
