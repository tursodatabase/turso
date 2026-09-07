use super::IndexWriter;
use crate::schema::document::Document;
use crate::{FutureResult, Opstamp, TantivyDocument};

/// A prepared commit
pub struct PreparedCommit<'a, D: Document = TantivyDocument> {
    index_writer: &'a mut IndexWriter<D>,
    payload: Option<String>,
    opstamp: Opstamp,
}

impl<'a, D: Document> PreparedCommit<'a, D> {
    pub(crate) fn new(index_writer: &'a mut IndexWriter<D>, opstamp: Opstamp) -> Self {
        Self {
            index_writer,
            payload: None,
            opstamp,
        }
    }

    /// Returns the opstamp associated with the prepared commit.
    pub fn opstamp(&self) -> Opstamp {
        self.opstamp
    }

    /// Adds an arbitrary payload to the commit.
    pub fn set_payload(&mut self, payload: &str) {
        self.payload = Some(payload.to_string())
    }

    /// Rollbacks any change.
    pub fn abort(self) -> crate::Result<Opstamp> {
        self.index_writer.rollback()
    }

    /// Proceeds to commit.
    /// See `.commit_future()`.
    pub fn commit(self) -> crate::Result<Opstamp> {
        self.commit_future().wait()
    }

    /// Proceeds to commit.
    ///
    /// Unfortunately, contrary to what `PrepareCommit` may suggests,
    /// this operation is not at all really light.
    /// At this point deletes have not been flushed yet.
    pub fn commit_future(self) -> FutureResult<Opstamp> {
        info!("committing {}", self.opstamp);
        self.index_writer
            .segment_updater()
            .schedule_commit(self.opstamp, self.payload)
    }
}
