use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

use tokenizer_api::Token;

use crate::query::bm25::idf;
use crate::query::{BooleanQuery, BoostQuery, Occur, Query, TermQuery};
use crate::schema::document::{Document, Value};
use crate::schema::{Field, FieldType, IndexRecordOption, Term};
use crate::tokenizer::{FacetTokenizer, PreTokenizedStream, TokenStream, Tokenizer};
use crate::{DocAddress, Result, Searcher, TantivyDocument, TantivyError};

#[derive(Debug, PartialEq)]
struct ScoreTerm {
    pub term: Term,
    pub score: f32,
}

impl ScoreTerm {
    fn new(term: Term, score: f32) -> Self {
        Self { term, score }
    }
}

impl Eq for ScoreTerm {}

impl PartialOrd for ScoreTerm {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoreTerm {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// A struct used as helper to build [`MoreLikeThisQuery`](crate::query::MoreLikeThisQuery)
/// This more-like-this implementation is inspired by the Apache Lucene
/// and closely follows the same implementation with adaptation to Tantivy vocabulary and API.
///
/// [MoreLikeThis](https://github.com/apache/lucene/blob/main/lucene/queries/src/java/org/apache/lucene/queries/mlt/MoreLikeThis.java#L147)
/// [MoreLikeThisQuery](https://github.com/apache/lucene/blob/main/lucene/queries/src/java/org/apache/lucene/queries/mlt/MoreLikeThisQuery.java#L36)
#[derive(Debug, Clone)]
pub struct MoreLikeThis {
    /// Ignore words which do not occur in at least this many docs.
    pub min_doc_frequency: Option<u64>,
    /// Ignore words which occur in more than this many docs.
    pub max_doc_frequency: Option<u64>,
    /// Ignore words less frequent than this.
    pub min_term_frequency: Option<usize>,
    /// Don't return a query longer than this.
    pub max_query_terms: Option<usize>,
    /// Ignore words if less than this length.
    pub min_word_length: Option<usize>,
    /// Ignore words if greater than this length.
    pub max_word_length: Option<usize>,
    /// Boost factor to use when boosting the terms
    pub boost_factor: Option<f32>,
    /// Current set of stop words.
    pub stop_words: Vec<String>,
}

impl Default for MoreLikeThis {
    fn default() -> Self {
        Self {
            min_doc_frequency: Some(5),
            max_doc_frequency: None,
            min_term_frequency: Some(2),
            max_query_terms: Some(25),
            min_word_length: None,
            max_word_length: None,
            boost_factor: Some(1.0),
            stop_words: vec![],
        }
    }
}

impl MoreLikeThis {
    /// Creates a [`BooleanQuery`] using a document address to collect
    /// the top stored field values.
    pub fn query_with_document(
        &self,
        searcher: &Searcher,
        doc_address: DocAddress,
    ) -> Result<BooleanQuery> {
        let score_terms = self.retrieve_terms_from_doc_address(searcher, doc_address)?;
        let query = self.create_query(score_terms);
        Ok(query)
    }

    /// Creates a [`BooleanQuery`] using a set of field values.
    pub fn query_with_document_fields<'a, V: Value<'a>>(
        &self,
        searcher: &Searcher,
        doc_fields: &[(Field, Vec<V>)],
    ) -> Result<BooleanQuery> {
        let score_terms = self.retrieve_terms_from_doc_fields(searcher, doc_fields)?;
        let query = self.create_query(score_terms);
        Ok(query)
    }

    /// Creates a [`BooleanQuery`] from an ascendingly sorted list of ScoreTerm
    /// This will map the list of ScoreTerm to a list of [`TermQuery`]  and compose a
    /// BooleanQuery using that list as sub queries.
    fn create_query(&self, mut score_terms: Vec<ScoreTerm>) -> BooleanQuery {
        score_terms.sort_by(|left_ts, right_ts| right_ts.cmp(left_ts));
        let best_score = score_terms.first().map_or(1f32, |x| x.score);
        let mut queries = Vec::new();

        for ScoreTerm { term, score } in score_terms {
            let mut query: Box<dyn Query> =
                Box::new(TermQuery::new(term, IndexRecordOption::Basic));
            if let Some(factor) = self.boost_factor {
                query = Box::new(BoostQuery::new(query, score * factor / best_score));
            }
            queries.push((Occur::Should, query));
        }
        BooleanQuery::from(queries)
    }

    /// Finds terms for a more-like-this query.
    /// doc_address is the address of document from which to find terms.
    fn retrieve_terms_from_doc_address(
        &self,
        searcher: &Searcher,
        doc_address: DocAddress,
    ) -> Result<Vec<ScoreTerm>> {
        let doc = searcher.doc::<TantivyDocument>(doc_address)?;

        let field_to_values = doc.get_sorted_field_values();
        self.retrieve_terms_from_doc_fields(searcher, &field_to_values)
    }

    /// Finds terms for a more-like-this query.
    /// field_to_field_values is a mapping from field to possible values of that field.
    fn retrieve_terms_from_doc_fields<'a, V: Value<'a>>(
        &self,
        searcher: &Searcher,
        field_to_values: &[(Field, Vec<V>)],
    ) -> Result<Vec<ScoreTerm>> {
        if field_to_values.is_empty() {
            return Err(TantivyError::InvalidArgument(
                "Cannot create more like this query on empty field values. The document may not \
                 have stored fields"
                    .to_string(),
            ));
        }
        let mut field_to_term_freq_map = HashMap::new();
        for (field, values) in field_to_values {
            self.add_term_frequencies(searcher, *field, values, &mut field_to_term_freq_map)?;
        }
        self.create_score_term(searcher, field_to_term_freq_map)
    }

    /// Computes the frequency of values for a field while updating the term frequencies
    /// Note: A FieldValue can be made up of multiple terms.
    /// We are interested in extracting terms within FieldValue
    fn add_term_frequencies<'a, V: Value<'a>>(
        &self,
        searcher: &Searcher,
        field: Field,
        values: &[V],
        term_frequencies: &mut HashMap<Term, usize>,
    ) -> Result<()> {
        let schema = searcher.schema();
        let tokenizer_manager = searcher.index().tokenizers();

        let field_entry = schema.get_field_entry(field);
        if !field_entry.is_indexed() {
            return Ok(());
        }

        // extract the raw value, possibly tokenizing & filtering to update the term frequency map
        match field_entry.field_type() {
            FieldType::Facet(_) => {
                let facets: Vec<&str> = values
                    .iter()
                    .map(|value| {
                        value.as_facet().ok_or_else(|| {
                            TantivyError::InvalidArgument("invalid field value".to_string())
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                for fake_str in facets {
                    FacetTokenizer::default()
                        .token_stream(fake_str)
                        .process(&mut |token| {
                            if self.is_noise_word(token.text.clone()) {
                                let term = Term::from_field_text(field, &token.text);
                                *term_frequencies.entry(term).or_insert(0) += 1;
                            }
                        });
                }
            }
            FieldType::Str(text_options) => {
                let mut tokenizer_opt = text_options
                    .get_indexing_options()
                    .map(|options| options.tokenizer())
                    .and_then(|tokenizer_name| tokenizer_manager.get(tokenizer_name));

                let sink = &mut |token: &Token| {
                    if !self.is_noise_word(token.text.clone()) {
                        let term = Term::from_field_text(field, &token.text);
                        *term_frequencies.entry(term).or_insert(0) += 1;
                    }
                };

                // TODO: Validate these changed align with the HEAD branch.
                for value in values {
                    if let Some(text) = value.as_str() {
                        let tokenizer = match &mut tokenizer_opt {
                            None => continue,
                            Some(tokenizer) => tokenizer,
                        };

                        let mut token_stream = tokenizer.token_stream(text);
                        token_stream.process(sink);
                    } else if let Some(tok_str) = value.as_pre_tokenized_text() {
                        let mut token_stream = PreTokenizedStream::from(*tok_str.clone());
                        token_stream.process(sink);
                    }
                }
            }
            FieldType::U64(_) => {
                for value in values {
                    let val = value.as_u64().ok_or_else(|| {
                        TantivyError::InvalidArgument("invalid value".to_string())
                    })?;
                    if !self.is_noise_word(val.to_string()) {
                        let term = Term::from_field_u64(field, val);
                        *term_frequencies.entry(term).or_insert(0) += 1;
                    }
                }
            }
            FieldType::Date(_) => {
                for value in values {
                    let timestamp = value.as_datetime().ok_or_else(|| {
                        TantivyError::InvalidArgument("invalid value".to_string())
                    })?;
                    let term = Term::from_field_date_for_search(field, timestamp);
                    *term_frequencies.entry(term).or_insert(0) += 1;
                }
            }
            FieldType::I64(_) => {
                for value in values {
                    let val = value.as_i64().ok_or_else(|| {
                        TantivyError::InvalidArgument("invalid value".to_string())
                    })?;
                    if !self.is_noise_word(val.to_string()) {
                        let term = Term::from_field_i64(field, val);
                        *term_frequencies.entry(term).or_insert(0) += 1;
                    }
                }
            }
            FieldType::F64(_) => {
                for value in values {
                    let val = value.as_f64().ok_or_else(|| {
                        TantivyError::InvalidArgument("invalid value".to_string())
                    })?;
                    if !self.is_noise_word(val.to_string()) {
                        let term = Term::from_field_f64(field, val);
                        *term_frequencies.entry(term).or_insert(0) += 1;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Determines if the term is likely to be of interest based on "more-like-this" settings
    fn is_noise_word(&self, word: String) -> bool {
        let word_length = word.len();
        if word_length == 0 {
            return true;
        }
        if self
            .min_word_length
            .map(|min| word_length < min)
            .unwrap_or(false)
        {
            return true;
        }
        if self
            .max_word_length
            .map(|max| word_length > max)
            .unwrap_or(false)
        {
            return true;
        }
        self.stop_words.contains(&word)
    }

    /// Computes the score for each term while ignoring not useful terms
    fn create_score_term(
        &self,
        searcher: &Searcher,
        per_field_term_frequencies: HashMap<Term, usize>,
    ) -> Result<Vec<ScoreTerm>> {
        let mut score_terms: BinaryHeap<Reverse<ScoreTerm>> = BinaryHeap::new();
        let num_docs = searcher
            .segment_readers()
            .iter()
            .map(|segment_reader| segment_reader.num_docs() as u64)
            .sum::<u64>();

        for (term, term_frequency) in per_field_term_frequencies.iter() {
            // ignore terms with less than min_term_frequency
            if self
                .min_term_frequency
                .map(|min_term_frequency| *term_frequency < min_term_frequency)
                .unwrap_or(false)
            {
                continue;
            }

            let doc_freq = searcher.doc_freq(term)?;

            // ignore terms with less than min_doc_frequency
            if self
                .min_doc_frequency
                .map(|min_doc_frequency| doc_freq < min_doc_frequency)
                .unwrap_or(false)
            {
                continue;
            }

            // ignore terms with more than max_doc_frequency
            if self
                .max_doc_frequency
                .map(|max_doc_frequency| doc_freq > max_doc_frequency)
                .unwrap_or(false)
            {
                continue;
            }

            // ignore terms with zero frequency
            if doc_freq == 0 {
                continue;
            }

            // compute similarity & score
            let idf = idf(doc_freq, num_docs);
            let score = (*term_frequency as f32) * idf;
            if let Some(limit) = self.max_query_terms {
                if score_terms.len() > limit {
                    // update the least significant term
                    let least_significant_term_score = score_terms.peek().unwrap().0.score;
                    if least_significant_term_score < score {
                        score_terms.peek_mut().unwrap().0 = ScoreTerm::new(term.clone(), score);
                    }
                } else {
                    score_terms.push(Reverse(ScoreTerm::new(term.clone(), score)));
                }
            } else {
                score_terms.push(Reverse(ScoreTerm::new(term.clone(), score)));
            }
        }

        let score_terms_vec: Vec<ScoreTerm> = score_terms
            .into_iter()
            .map(|reverse_score_term| reverse_score_term.0)
            .collect();

        Ok(score_terms_vec)
    }
}
