/// A pseudo cursor exposes the record stored in its content register as a
/// one-row table, mirroring SQLite's OpenPseudo. It holds no data itself:
/// SorterData moves each record into the content register and Column ops
/// decode straight out of it.
pub struct PseudoCursor {
    content_reg: usize,
}

impl PseudoCursor {
    #[inline]
    pub fn new(content_reg: usize) -> Self {
        Self { content_reg }
    }

    #[inline]
    pub fn content_reg(&self) -> usize {
        self.content_reg
    }
}
