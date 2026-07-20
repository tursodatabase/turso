use crate::types::ImmutableRecord;

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
}
