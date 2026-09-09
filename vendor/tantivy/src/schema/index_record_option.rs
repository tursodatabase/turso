use serde::{Deserialize, Serialize};

/// `IndexRecordOption` describes an amount information associated
/// with a given indexed field.
///
/// It is both used to:
///
///  * describe in the schema the amount of information that should be retained during indexing (See
///    [`TextFieldIndexing::set_index_option()`](crate::schema::TextFieldIndexing::set_index_option))
///  * request that a given amount of information to be decoded as one goes through a posting list.
///    (See [`InvertedIndexReader::read_postings()`](crate::InvertedIndexReader::read_postings))
#[derive(
    Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash, Serialize, Deserialize, Default,
)]
pub enum IndexRecordOption {
    /// records only the `DocId`s
    #[serde(rename = "basic")]
    #[default]
    Basic,
    /// records the document ids as well as the term frequency.
    /// The term frequency can help giving better scoring of the documents.
    #[serde(rename = "freq")]
    WithFreqs,
    /// records the document id, the term frequency and the positions of
    /// the occurrences in the document.
    /// Positions are required to run a [`PhraseQuery`](crate::query::PhraseQuery).
    #[serde(rename = "position")]
    WithFreqsAndPositions,
}

impl IndexRecordOption {
    /// Returns true if this option includes encoding
    /// term frequencies.
    pub fn has_freq(self) -> bool {
        match self {
            IndexRecordOption::Basic => false,
            IndexRecordOption::WithFreqs | IndexRecordOption::WithFreqsAndPositions => true,
        }
    }

    /// Returns true if this option include encoding
    ///  term positions.
    pub fn has_positions(self) -> bool {
        match self {
            IndexRecordOption::Basic | IndexRecordOption::WithFreqs => false,
            IndexRecordOption::WithFreqsAndPositions => true,
        }
    }

    /// Downgrades to the next level if provided `IndexRecordOption` is unavailable.
    pub fn downgrade(&self, other: IndexRecordOption) -> IndexRecordOption {
        use IndexRecordOption::*;

        match (other, self) {
            (WithFreqsAndPositions, WithFreqsAndPositions) => WithFreqsAndPositions,
            (WithFreqs, WithFreqs) => WithFreqs,
            (WithFreqsAndPositions, WithFreqs) => WithFreqs,
            (WithFreqs, WithFreqsAndPositions) => WithFreqs,
            _ => Basic,
        }
    }
}
