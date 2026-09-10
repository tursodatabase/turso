use crate::{DocId, Opstamp};

// Doc to opstamp is used to identify which
// document should be deleted.
//
// Since the docset matching the query of a delete operation
// is not computed right when the delete operation is received,
// we need to find a way to evaluate, for each document,
// whether the document was added before or after
// the delete operation. This anteriority is used by comparing
// the docstamp of the document.
//
// The doc to opstamp mapping stores precisely an array
// indexed by doc id and storing the opstamp of the document.
//
// This mapping is NOT necessarily increasing, because
// we might be sorting documents according to a fast field.
#[derive(Clone)]
pub enum DocToOpstampMapping<'a> {
    WithMap(&'a [Opstamp]),
    None,
}

impl DocToOpstampMapping<'_> {
    /// Assess whether a document should be considered deleted given that it contains
    /// a deleted term that was deleted at the opstamp: `delete_opstamp`.
    ///
    /// This function returns true if the `DocToOpstamp` mapping is none or if
    /// the `doc_opstamp` is anterior to the delete opstamp.
    pub fn is_deleted(&self, doc_id: DocId, delete_opstamp: Opstamp) -> bool {
        match self {
            Self::WithMap(doc_opstamps) => {
                let doc_opstamp = doc_opstamps[doc_id as usize];
                doc_opstamp < delete_opstamp
            }
            Self::None => true,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::DocToOpstampMapping;

    #[test]
    fn test_doc_to_opstamp_mapping_none() {
        let doc_to_opstamp_mapping = DocToOpstampMapping::None;
        assert!(doc_to_opstamp_mapping.is_deleted(1u32, 0u64));
        assert!(doc_to_opstamp_mapping.is_deleted(1u32, 2u64));
    }

    #[test]
    fn test_doc_to_opstamp_mapping_with_map() {
        let doc_to_opstamp_mapping = DocToOpstampMapping::WithMap(&[5u64, 1u64, 0u64, 4u64, 3u64]);
        assert_eq!(doc_to_opstamp_mapping.is_deleted(0u32, 2u64), false);
        assert_eq!(doc_to_opstamp_mapping.is_deleted(1u32, 2u64), true);
        assert_eq!(doc_to_opstamp_mapping.is_deleted(2u32, 2u64), true);
        assert_eq!(doc_to_opstamp_mapping.is_deleted(3u32, 2u64), false);
        assert_eq!(doc_to_opstamp_mapping.is_deleted(4u32, 2u64), false);
    }
}
