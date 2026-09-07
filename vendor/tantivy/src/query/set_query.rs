use std::collections::HashMap;

use tantivy_fst::raw::CompiledAddr;
use tantivy_fst::{Automaton, Map};

use crate::query::score_combiner::DoNothingCombiner;
use crate::query::{AutomatonWeight, BooleanWeight, EnableScoring, Occur, Query, Weight};
use crate::schema::{Field, Schema};
use crate::Term;

/// A Term Set Query matches all of the documents containing any of the Term provided
#[derive(Debug, Clone)]
pub struct TermSetQuery {
    terms_map: HashMap<Field, Vec<Term>>,
}

impl TermSetQuery {
    /// Create a Term Set Query
    pub fn new<T: IntoIterator<Item = Term>>(terms: T) -> Self {
        let mut terms_map: HashMap<_, Vec<_>> = HashMap::new();
        for term in terms {
            terms_map.entry(term.field()).or_default().push(term);
        }

        for terms in terms_map.values_mut() {
            terms.sort_unstable();
            terms.dedup();
        }

        TermSetQuery { terms_map }
    }

    fn specialized_weight(
        &self,
        schema: &Schema,
    ) -> crate::Result<BooleanWeight<DoNothingCombiner>> {
        let mut sub_queries: Vec<(_, Box<dyn Weight>)> = Vec::with_capacity(self.terms_map.len());

        for (&field, sorted_terms) in self.terms_map.iter() {
            let field_entry = schema.get_field_entry(field);
            let field_type = field_entry.field_type();
            if !field_type.is_indexed() {
                let error_msg = format!("Field {:?} is not indexed.", field_entry.name());
                return Err(crate::TantivyError::SchemaError(error_msg));
            }

            // In practice this won't fail because:
            // - we are writing to memory, so no IoError
            // - Terms are ordered
            let map = Map::from_iter(
                sorted_terms
                    .iter()
                    .map(|key| (key.serialized_value_bytes(), 0)),
            )
            .map_err(std::io::Error::other)?;

            sub_queries.push((
                Occur::Should,
                Box::new(AutomatonWeight::new(field, SetDfaWrapper(map))),
            ));
        }

        Ok(BooleanWeight::new(
            sub_queries,
            false,
            Box::new(|| DoNothingCombiner),
        ))
    }
}

impl Query for TermSetQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        Ok(Box::new(self.specialized_weight(enable_scoring.schema())?))
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
        for terms in self.terms_map.values() {
            for term in terms {
                visitor(term, false);
            }
        }
    }
}

struct SetDfaWrapper(Map<Vec<u8>>);

impl Automaton for SetDfaWrapper {
    type State = Option<CompiledAddr>;

    fn start(&self) -> Option<CompiledAddr> {
        Some(self.0.as_ref().root().addr())
    }

    fn is_match(&self, state_opt: &Option<CompiledAddr>) -> bool {
        if let Some(state) = state_opt {
            self.0.as_ref().node(*state).is_final()
        } else {
            false
        }
    }

    fn accept(&self, state_opt: &Option<CompiledAddr>, byte: u8) -> Option<CompiledAddr> {
        let state = state_opt.as_ref()?;
        let node = self.0.as_ref().node(*state);
        let transition = node.find_input(byte)?;
        Some(node.transition_addr(transition))
    }

    fn can_match(&self, state: &Self::State) -> bool {
        state.is_some()
    }
}

#[cfg(test)]
mod tests {
    use crate::collector::TopDocs;
    use crate::query::{QueryParser, TermSetQuery};
    use crate::schema::{Schema, TEXT};
    use crate::{assert_nearly_equals, Index, IndexWriter, Term};

    #[test]
    pub fn test_term_set_query() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field1 = schema_builder.add_text_field("field1", TEXT);
        let field2 = schema_builder.add_text_field("field2", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(
                field1 => "doc1",
                field2 => "val1",
            ))?;
            index_writer.add_document(doc!(
                field1 => "doc2",
                field2 => "val2",
            ))?;
            index_writer.add_document(doc!(
                field1 => "doc3",
                field2 => "val3",
            ))?;
            index_writer.add_document(doc!(
                field1 => "val3",
                field2 => "doc3",
            ))?;
            index_writer.commit()?;
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();

        {
            // single element
            let terms = vec![Term::from_field_text(field1, "doc1")];

            let term_set_query = TermSetQuery::new(terms);
            let top_docs =
                searcher.search(&term_set_query, &TopDocs::with_limit(2).order_by_score())?;
            assert_eq!(top_docs.len(), 1, "Expected 1 document");
            let (score, _) = top_docs[0];
            assert_nearly_equals!(1.0, score);
        }

        {
            // single element, absent
            let terms = vec![Term::from_field_text(field1, "doc4")];

            let term_set_query = TermSetQuery::new(terms);
            let top_docs =
                searcher.search(&term_set_query, &TopDocs::with_limit(1).order_by_score())?;
            assert!(top_docs.is_empty(), "Expected 0 document");
        }

        {
            // multiple elements
            let terms = vec![
                Term::from_field_text(field1, "doc1"),
                Term::from_field_text(field1, "doc2"),
            ];

            let term_set_query = TermSetQuery::new(terms);
            let top_docs =
                searcher.search(&term_set_query, &TopDocs::with_limit(2).order_by_score())?;
            assert_eq!(top_docs.len(), 2, "Expected 2 documents");
            for (score, _) in top_docs {
                assert_nearly_equals!(1.0, score);
            }
        }

        {
            // multiple elements, mixed fields
            let terms = vec![
                Term::from_field_text(field1, "doc1"),
                Term::from_field_text(field1, "doc1"),
                Term::from_field_text(field2, "val2"),
            ];

            let term_set_query = TermSetQuery::new(terms);
            let top_docs =
                searcher.search(&term_set_query, &TopDocs::with_limit(3).order_by_score())?;

            assert_eq!(top_docs.len(), 2, "Expected 2 document");
            for (score, _) in top_docs {
                assert_nearly_equals!(1.0, score);
            }
        }

        {
            // no field crosstalk
            let terms = vec![Term::from_field_text(field1, "doc3")];

            let term_set_query = TermSetQuery::new(terms);
            let top_docs =
                searcher.search(&term_set_query, &TopDocs::with_limit(3).order_by_score())?;
            assert_eq!(top_docs.len(), 1, "Expected 1 document");

            let terms = vec![Term::from_field_text(field2, "doc3")];

            let term_set_query = TermSetQuery::new(terms);
            let top_docs =
                searcher.search(&term_set_query, &TopDocs::with_limit(3).order_by_score())?;
            assert_eq!(top_docs.len(), 1, "Expected 1 document");

            let terms = vec![
                Term::from_field_text(field1, "doc3"),
                Term::from_field_text(field2, "doc3"),
            ];

            let term_set_query = TermSetQuery::new(terms);
            let top_docs =
                searcher.search(&term_set_query, &TopDocs::with_limit(3).order_by_score())?;
            assert_eq!(top_docs.len(), 2, "Expected 2 document");
        }

        Ok(())
    }

    #[test]
    fn test_term_set_query_parser() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("field", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        let field = schema.get_field("field").unwrap();
        index_writer.add_document(doc!(
          field => "val1",
        ))?;
        index_writer.add_document(doc!(
          field => "val2",
        ))?;
        index_writer.add_document(doc!(
          field => "val3",
        ))?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query_parser = QueryParser::for_index(&index, vec![]);
        let query = query_parser.parse_query("field: IN [val1 val2]")?;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(3).order_by_score())?;
        assert_eq!(top_docs.len(), 2);
        Ok(())
    }
}
