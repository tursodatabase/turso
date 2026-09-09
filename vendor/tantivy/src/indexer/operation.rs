use crate::query::Weight;
use crate::schema::document::Document;
use crate::schema::{TantivyDocument, Term};
use crate::Opstamp;

/// Timestamped Delete operation.
pub struct DeleteOperation {
    /// Operation stamp.
    /// It is used to check whether the delete operation
    /// applies to an added document operation.
    pub opstamp: Opstamp,
    /// Weight is used to define the set of documents to be deleted.
    pub target: Box<dyn Weight>,
}

/// Timestamped Add operation.
#[derive(Eq, PartialEq, Debug)]
pub struct AddOperation<D: Document = TantivyDocument> {
    /// Operation stamp.
    pub opstamp: Opstamp,
    /// Document to be added.
    pub document: D,
}

/// UserOperation is an enum type that encapsulates other operation types.
#[derive(Eq, PartialEq, Debug)]
pub enum UserOperation<D: Document = TantivyDocument> {
    /// Add operation
    Add(D),
    /// Delete operation
    Delete(Term),
}
