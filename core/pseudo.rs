use crate::types::ImmutableRecord;

#[derive(Default)]
pub struct PseudoCursor {
    current: Option<ImmutableRecord>,
}

impl PseudoCursor {
    pub fn record(&self) -> Option<&ImmutableRecord> {
        self.current.as_ref()
    }

    pub fn insert(&mut self, record: ImmutableRecord) {
        self.current = Some(record);
    }
}
