use std::ops::Bound;

use super::{prefix_end, PhrasePrefixWeight};
use crate::query::bm25::Bm25Weight;
use crate::query::{EnableScoring, InvertedIndexRangeWeight, Query, Weight};
use crate::schema::{Field, IndexRecordOption, Term};

const DEFAULT_MAX_EXPANSIONS: u32 = 50;

/// `PhrasePrefixQuery` matches a specific sequence of words followed by term of which only a
/// prefix is known.
///
/// For instance the phrase prefix query for `"part t"` will match
/// the sentence
///
/// **Alan just got a part time job.**
///
/// On the other hand it will not match the sentence.
///
/// **This is my favorite part of the job.**
///
/// Using a `PhrasePrefixQuery` on a field requires positions
/// to be indexed for this field.
#[derive(Clone, Debug)]
pub struct PhrasePrefixQuery {
    field: Field,
    phrase_terms: Vec<(usize, Term)>,
    prefix: (usize, Term),
    max_expansions: u32,
}

impl PhrasePrefixQuery {
    /// Creates a new `PhrasePrefixQuery` given a list of terms.
    ///
    /// There must be at least two terms, and all terms
    /// must belong to the same field.
    /// Offset for each term will be same as index in the Vector
    /// The last Term is a prefix and not a full value
    pub fn new(terms: Vec<Term>) -> PhrasePrefixQuery {
        let terms_with_offset = terms.into_iter().enumerate().collect();
        PhrasePrefixQuery::new_with_offset(terms_with_offset)
    }

    /// Creates a new `PhrasePrefixQuery` given a list of terms and their offsets.
    ///
    /// Can be used to provide custom offset for each term.
    pub fn new_with_offset(mut terms: Vec<(usize, Term)>) -> PhrasePrefixQuery {
        assert!(
            !terms.is_empty(),
            "A phrase prefix query is required to have at least one term."
        );
        terms.sort_by_key(|&(offset, _)| offset);
        let field = terms[0].1.field();
        assert!(
            terms[1..].iter().all(|term| term.1.field() == field),
            "All terms from a phrase query must belong to the same field"
        );
        PhrasePrefixQuery {
            field,
            prefix: terms.pop().unwrap(),
            phrase_terms: terms,
            max_expansions: DEFAULT_MAX_EXPANSIONS,
        }
    }

    /// Maximum number of terms to which the last provided term will expand.
    pub fn set_max_expansions(&mut self, value: u32) {
        self.max_expansions = value;
    }

    /// The [`Field`] this `PhrasePrefixQuery` is targeting.
    pub fn field(&self) -> Field {
        self.field
    }

    /// `Term`s in the phrase without the associated offsets.
    pub fn phrase_terms(&self) -> Vec<Term> {
        // TODO should we include the last term too?
        self.phrase_terms
            .iter()
            .map(|(_, term)| term.clone())
            .collect::<Vec<Term>>()
    }

    /// Returns the [`PhrasePrefixWeight`] for the given phrase query given a specific `searcher`.
    ///
    /// This function is the same as [`Query::weight()`] except it returns
    /// a specialized type [`PhraseQueryWeight`] instead of a Boxed trait.
    /// If the query was only one term long, this returns `None` whereas [`Query::weight`]
    /// returns a boxed [`RangeWeight`]
    pub(crate) fn phrase_prefix_query_weight(
        &self,
        enable_scoring: EnableScoring<'_>,
    ) -> crate::Result<Option<PhrasePrefixWeight>> {
        if self.phrase_terms.is_empty() {
            return Ok(None);
        }
        let schema = enable_scoring.schema();
        let field_entry = schema.get_field_entry(self.field);
        let has_positions = field_entry
            .field_type()
            .get_index_record_option()
            .map(IndexRecordOption::has_positions)
            .unwrap_or(false);
        if !has_positions {
            let field_name = field_entry.name();
            return Err(crate::TantivyError::SchemaError(format!(
                "Applied phrase query on field {field_name:?}, which does not have positions \
                 indexed"
            )));
        }
        let terms = self.phrase_terms();
        let bm25_weight_opt = match enable_scoring {
            EnableScoring::Enabled { searcher, .. } => {
                Some(Bm25Weight::for_terms(searcher, &terms)?)
            }
            EnableScoring::Disabled { .. } => None,
        };
        let weight = PhrasePrefixWeight::new(
            self.phrase_terms.clone(),
            self.prefix.clone(),
            bm25_weight_opt,
            self.max_expansions,
        );
        Ok(Some(weight))
    }
}

impl Query for PhrasePrefixQuery {
    /// Create the weight associated with a query.
    ///
    /// See [`Weight`].
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        if let Some(phrase_weight) = self.phrase_prefix_query_weight(enable_scoring)? {
            Ok(Box::new(phrase_weight))
        } else {
            // There are no prefix. Let's just match the suffix.
            let end_term =
                if let Some(end_value) = prefix_end(self.prefix.1.serialized_value_bytes()) {
                    let mut end_term = Term::with_capacity(end_value.len());
                    end_term.set_field_and_type(self.field, self.prefix.1.typ());
                    end_term.append_bytes(&end_value);
                    Bound::Excluded(end_term)
                } else {
                    Bound::Unbounded
                };

            let lower_bound = Bound::Included(self.prefix.1.clone());
            let upper_bound = end_term;

            Ok(Box::new(InvertedIndexRangeWeight::new(
                self.field,
                &lower_bound,
                &upper_bound,
                Some(self.max_expansions as u64),
            )))
        }
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
        for (_, term) in &self.phrase_terms {
            visitor(term, true);
        }
    }
}
