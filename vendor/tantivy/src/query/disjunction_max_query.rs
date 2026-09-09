use crate::query::{BooleanWeight, DisjunctionMaxCombiner, EnableScoring, Occur, Query, Weight};
use crate::{Score, Term};

/// The disjunction max query returns documents matching one or more wrapped queries,
/// called query clauses or clauses.
///
/// If a returned document matches multiple query clauses,
/// the `DisjunctionMaxQuery` assigns the document the highest relevance score from any matching
/// clause, plus a tie breaking increment for any additional matching subqueries.
///
/// ```rust
/// use tantivy::collector::TopDocs;
/// use tantivy::doc;
/// use tantivy::query::{DisjunctionMaxQuery, Query, QueryClone, TermQuery};
/// use tantivy::schema::{IndexRecordOption, Schema, TEXT};
/// use tantivy::Term;
/// use tantivy::Index;
/// use tantivy::IndexWriter;
///
/// fn main() -> tantivy::Result<()> {
///    let mut schema_builder = Schema::builder();
///    let title = schema_builder.add_text_field("title", TEXT);
///    let body = schema_builder.add_text_field("body", TEXT);
///    let schema = schema_builder.build();
///    let index = Index::create_in_ram(schema);
///    {
///        let mut index_writer: IndexWriter = index.writer(15_000_000)?;
///        index_writer.add_document(doc!(
///            title => "The Name of Girl",
///        ))?;
///        index_writer.add_document(doc!(
///            title => "The Diary of Muadib",
///        ))?;
///        index_writer.add_document(doc!(
///            title => "The Diary of Girl",
///        ))?;
///        index_writer.commit()?;
///    }
///
///    let reader = index.reader()?;
///    let searcher = reader.searcher();
///
///    // Make TermQuery's for "girl" and "diary" in the title
///    let girl_term_query: Box<dyn Query> = Box::new(TermQuery::new(
///        Term::from_field_text(title, "girl"),
///        IndexRecordOption::Basic,
///    ));
///    let diary_term_query: Box<dyn Query> = Box::new(TermQuery::new(
///        Term::from_field_text(title, "diary"),
///        IndexRecordOption::Basic,
///    ));
///
///    // TermQuery "diary" and "girl" should be present and only one should be accounted in score
///    let queries1 = vec![diary_term_query.box_clone(), girl_term_query.box_clone()];
///    let diary_and_girl = DisjunctionMaxQuery::new(queries1);
///    let documents = searcher.search(&diary_and_girl, &TopDocs::with_limit(3).order_by_score())?;
///    assert_eq!(documents[0].0, documents[1].0);
///    assert_eq!(documents[1].0, documents[2].0);
///
///    // TermQuery "diary" and "girl" should be present
///    // and one should be accounted with multiplier 0.7
///    let queries2 = vec![diary_term_query.box_clone(), girl_term_query.box_clone()];
///    let tie_breaker = 0.7;
///    let diary_and_girl_with_tie_breaker = DisjunctionMaxQuery::with_tie_breaker(queries2, tie_breaker);
///    let documents = searcher.search(&diary_and_girl_with_tie_breaker, &TopDocs::with_limit(3).order_by_score())?;
///    assert_eq!(documents[1].0, documents[2].0);
///    // For this test all terms brings the same score. So we can do easy math and assume that
///    // `DisjunctionMaxQuery` with tie breakers score should be equal
///    // to term1 score + `tie_breaker` * term2 score or (1.0 + tie_breaker) * term score
///    assert!(f32::abs(documents[0].0 - documents[1].0 * (1.0 + tie_breaker)) < 0.001);
///    Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct DisjunctionMaxQuery {
    disjuncts: Vec<Box<dyn Query>>,
    tie_breaker: Score,
}

impl Clone for DisjunctionMaxQuery {
    fn clone(&self) -> Self {
        DisjunctionMaxQuery::with_tie_breaker(
            self.disjuncts
                .iter()
                .map(|disjunct| disjunct.box_clone())
                .collect::<Vec<_>>(),
            self.tie_breaker,
        )
    }
}

impl Query for DisjunctionMaxQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        let disjuncts = self
            .disjuncts
            .iter()
            .map(|disjunct| Ok((Occur::Should, disjunct.weight(enable_scoring)?)))
            .collect::<crate::Result<_>>()?;
        let tie_breaker = self.tie_breaker;
        Ok(Box::new(BooleanWeight::new(
            disjuncts,
            enable_scoring.is_scoring_enabled(),
            Box::new(move || DisjunctionMaxCombiner::with_tie_breaker(tie_breaker)),
        )))
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
        for disjunct in &self.disjuncts {
            disjunct.query_terms(visitor);
        }
    }
}

impl DisjunctionMaxQuery {
    /// Creates a new `DisjunctionMaxQuery` with tie breaker.
    pub fn with_tie_breaker(
        disjuncts: Vec<Box<dyn Query>>,
        tie_breaker: Score,
    ) -> DisjunctionMaxQuery {
        DisjunctionMaxQuery {
            disjuncts,
            tie_breaker,
        }
    }

    /// Creates a new `DisjunctionMaxQuery` with no tie breaker.
    pub fn new(disjuncts: Vec<Box<dyn Query>>) -> DisjunctionMaxQuery {
        DisjunctionMaxQuery::with_tie_breaker(disjuncts, 0.0)
    }
}
