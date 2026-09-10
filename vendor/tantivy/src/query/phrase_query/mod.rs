mod phrase_query;
mod phrase_scorer;
mod phrase_weight;
pub mod regex_phrase_query;
mod regex_phrase_weight;

pub use self::phrase_query::PhraseQuery;
pub(crate) use self::phrase_scorer::intersection_count;
pub use self::phrase_scorer::PhraseScorer;
pub use self::phrase_weight::PhraseWeight;

#[cfg(test)]
pub(crate) mod tests {

    use serde_json::json;

    use super::*;
    use crate::collector::tests::{TEST_COLLECTOR_WITHOUT_SCORE, TEST_COLLECTOR_WITH_SCORE};
    use crate::index::Index;
    use crate::query::{EnableScoring, QueryParser, Weight};
    use crate::schema::{Schema, Term, TEXT};
    use crate::{assert_nearly_equals, DocAddress, DocId, IndexWriter, TERMINATED};

    pub fn create_index<S: AsRef<str>>(texts: &[S]) -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            for text in texts {
                let doc = doc!(text_field=>text.as_ref());
                index_writer.add_document(doc)?;
            }
            index_writer.commit()?;
        }
        Ok(index)
    }

    #[test]
    pub fn test_phrase_query() -> crate::Result<()> {
        let index = create_index(&[
            "b b b d c g c",
            "a b b d c g c",
            "a b a b c",
            "c a b a d ga a",
            "a b c",
        ])?;
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let test_query = |texts: Vec<&str>| {
            let terms: Vec<Term> = texts
                .iter()
                .map(|text| Term::from_field_text(text_field, text))
                .collect();
            let phrase_query = PhraseQuery::new(terms);
            let test_fruits = searcher
                .search(&phrase_query, &TEST_COLLECTOR_WITH_SCORE)
                .unwrap();
            test_fruits
                .docs()
                .iter()
                .map(|docaddr| docaddr.doc_id)
                .collect::<Vec<_>>()
        };
        assert_eq!(test_query(vec!["a", "b"]), vec![1, 2, 3, 4]);
        assert_eq!(test_query(vec!["a", "b", "c"]), vec![2, 4]);
        assert_eq!(test_query(vec!["b", "b"]), vec![0, 1]);
        assert!(test_query(vec!["g", "ewrwer"]).is_empty());
        assert!(test_query(vec!["g", "a"]).is_empty());
        Ok(())
    }

    #[test]
    pub fn test_phrase_query_simple() -> crate::Result<()> {
        let index = create_index(&["a b b d c g c", "a b a b c"])?;
        let text_field = index.schema().get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let terms: Vec<Term> = ["a", "b", "c"]
            .iter()
            .map(|text| Term::from_field_text(text_field, text))
            .collect();
        let phrase_query = PhraseQuery::new(terms);
        let phrase_weight =
            phrase_query.phrase_weight(EnableScoring::disabled_from_schema(searcher.schema()))?;
        let mut phrase_scorer = phrase_weight.scorer(searcher.segment_reader(0), 1.0)?;
        assert_eq!(phrase_scorer.doc(), 1);
        assert_eq!(phrase_scorer.advance(), TERMINATED);
        Ok(())
    }

    #[test]
    pub fn test_phrase_query_no_score() -> crate::Result<()> {
        let index = create_index(&[
            "b b b d c g c",
            "a b b d c g c",
            "a b a b c",
            "c a b a d ga a",
            "a b c",
        ])?;
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let test_query = |texts: Vec<&str>| {
            let terms: Vec<Term> = texts
                .iter()
                .map(|text| Term::from_field_text(text_field, text))
                .collect();
            let phrase_query = PhraseQuery::new(terms);
            let test_fruits = searcher
                .search(&phrase_query, &TEST_COLLECTOR_WITHOUT_SCORE)
                .unwrap();
            test_fruits
                .docs()
                .iter()
                .map(|docaddr| docaddr.doc_id)
                .collect::<Vec<_>>()
        };
        assert_eq!(test_query(vec!["a", "b", "c"]), vec![2, 4]);
        assert_eq!(test_query(vec!["a", "b"]), vec![1, 2, 3, 4]);
        assert_eq!(test_query(vec!["b", "b"]), vec![0, 1]);
        assert!(test_query(vec!["g", "ewrwer"]).is_empty());
        assert!(test_query(vec!["g", "a"]).is_empty());
        Ok(())
    }

    #[test]
    pub fn test_phrase_query_no_positions() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        use crate::schema::{IndexRecordOption, TextFieldIndexing, TextOptions};
        let no_positions = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs),
        );

        let text_field = schema_builder.add_text_field("text", no_positions);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(text_field=>"a b c"))?;
            index_writer.commit()?;
        }
        let searcher = index.reader()?.searcher();
        let phrase_query = PhraseQuery::new(vec![
            Term::from_field_text(text_field, "a"),
            Term::from_field_text(text_field, "b"),
        ]);

        let search_error = searcher
            .search(&phrase_query, &TEST_COLLECTOR_WITH_SCORE)
            .err();
        assert!(matches!(
            search_error,
            Some(crate::TantivyError::SchemaError(msg))
            if msg == "Applied phrase query on field \"text\", which does not have positions \
            indexed"
        ));
        Ok(())
    }

    #[test]
    pub fn test_phrase_score() -> crate::Result<()> {
        let index = create_index(&["a b c", "a b c a b"])?;
        let scores = test_query(0, &index, vec!["a", "b"]);
        assert_nearly_equals!(scores[0], 0.40618482);
        assert_nearly_equals!(scores[1], 0.46844664);
        Ok(())
    }

    #[ignore]
    #[test]
    pub fn test_phrase_score_with_slop() -> crate::Result<()> {
        let index = create_index(&["a c b", "a b c a b"])?;
        let scores = test_query(1, &index, vec!["a", "b"]);
        assert_nearly_equals!(scores[0], 0.40618482);
        assert_nearly_equals!(scores[1], 0.46844664);
        Ok(())
    }

    #[test]
    pub fn test_phrase_score_with_slop_bug() -> crate::Result<()> {
        let index = create_index(&["asdf asdf Captain Subject Wendy", "Captain"])?;
        let scores = test_query(1, &index, vec!["captain", "wendy"]);
        assert_eq!(scores.len(), 1);
        Ok(())
    }

    #[test]
    pub fn test_phrase_score_with_slop_bug_2() -> crate::Result<()> {
        // fails
        let index = create_index(&["a x b x c", "a a c"])?;
        let scores = test_query(2, &index, vec!["a", "b", "c"]);
        assert_eq!(scores.len(), 1);

        let index = create_index(&["a x b x c", "b c c"])?;
        let scores = test_query(2, &index, vec!["a", "b", "c"]);
        assert_eq!(scores.len(), 1);

        Ok(())
    }

    fn test_query(slop: u32, index: &Index, texts: Vec<&str>) -> Vec<f32> {
        let text_field = index.schema().get_field("text").unwrap();
        let searcher = index.reader().unwrap().searcher();
        let terms: Vec<Term> = texts
            .iter()
            .map(|text| Term::from_field_text(text_field, text))
            .collect();
        let mut phrase_query = PhraseQuery::new(terms);
        phrase_query.set_slop(slop);
        searcher
            .search(&phrase_query, &TEST_COLLECTOR_WITH_SCORE)
            .expect("search should succeed")
            .scores()
            .to_vec()
    }

    #[test]
    pub fn test_phrase_score_with_slop_repeating() -> crate::Result<()> {
        let index = create_index(&["wendy subject subject captain", "Captain"])?;
        let scores = test_query(1, &index, vec!["wendy", "subject", "captain"]);
        assert_eq!(scores.len(), 1);
        Ok(())
    }

    #[test]
    pub fn test_phrase_score_with_slop_size() -> crate::Result<()> {
        let index = create_index(&["a b e c", "a e e e c", "a e e e e c"])?;
        let scores = test_query(3, &index, vec!["a", "c"]);
        assert_eq!(scores.len(), 2);
        assert_nearly_equals!(scores[0], 0.29086056);
        assert_nearly_equals!(scores[1], 0.26706287);
        Ok(())
    }

    #[test]
    pub fn test_phrase_slop() -> crate::Result<()> {
        let index = create_index(&["a x b c"])?;
        let scores = test_query(1, &index, vec!["a", "b", "c"]);
        assert_eq!(scores.len(), 1);

        let index = create_index(&["a x b x c"])?;
        let scores = test_query(1, &index, vec!["a", "b", "c"]);
        assert_eq!(scores.len(), 0);

        let index = create_index(&["a b"])?;
        let scores = test_query(1, &index, vec!["b", "a"]);
        assert_eq!(scores.len(), 0);

        let index = create_index(&["a b"])?;
        let scores = test_query(2, &index, vec!["b", "a"]);
        assert_eq!(scores.len(), 1);

        Ok(())
    }

    #[test]
    pub fn test_phrase_score_with_slop_ordering() -> crate::Result<()> {
        let index = create_index(&[
            "a e b e c",
            "a e e e e e b e e e e c",
            "a c b", // also matches
            "a c e b e",
            "a e c b",
            "a e b c",
        ])?;
        let scores = test_query(3, &index, vec!["a", "b", "c"]);
        // The first and last matches.
        assert_nearly_equals!(scores[0], 0.23091172);
        assert_nearly_equals!(scores[1], 0.27310878);
        assert_nearly_equals!(scores[3], 0.25024384);
        Ok(())
    }

    #[test] // motivated by #234
    pub fn test_phrase_query_docfreq_order() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(text_field=>"b"))?;
            index_writer.add_document(doc!(text_field=>"a b"))?;
            index_writer.add_document(doc!(text_field=>"b a"))?;
            index_writer.commit()?;
        }

        let searcher = index.reader()?.searcher();
        let test_query = |texts: Vec<&str>| {
            let terms: Vec<Term> = texts
                .iter()
                .map(|text| Term::from_field_text(text_field, text))
                .collect();
            let phrase_query = PhraseQuery::new(terms);
            searcher
                .search(&phrase_query, &TEST_COLLECTOR_WITH_SCORE)
                .expect("search should succeed")
                .docs()
                .to_vec()
        };
        assert_eq!(test_query(vec!["a", "b"]), vec![DocAddress::new(0, 1)]);
        assert_eq!(test_query(vec!["b", "a"]), vec![DocAddress::new(0, 2)]);
        Ok(())
    }

    #[test] // motivated by #234
    pub fn test_phrase_query_non_trivial_offsets() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(text_field=>"a b c d e f g h"))?;
            index_writer.commit()?;
        }
        let searcher = index.reader().unwrap().searcher();
        let test_query = |texts: Vec<(usize, &str)>| {
            let terms: Vec<(usize, Term)> = texts
                .iter()
                .map(|(offset, text)| (*offset, Term::from_field_text(text_field, text)))
                .collect();
            let phrase_query = PhraseQuery::new_with_offset(terms);
            searcher
                .search(&phrase_query, &TEST_COLLECTOR_WITH_SCORE)
                .expect("search should succeed")
                .docs()
                .iter()
                .map(|doc_address| doc_address.doc_id)
                .collect::<Vec<DocId>>()
        };
        assert_eq!(test_query(vec![(0, "a"), (1, "b")]), vec![0]);
        assert_eq!(test_query(vec![(1, "b"), (0, "a")]), vec![0]);
        assert!(test_query(vec![(0, "a"), (2, "b")]).is_empty());
        assert_eq!(test_query(vec![(0, "a"), (2, "c")]), vec![0]);
        assert_eq!(test_query(vec![(0, "a"), (2, "c"), (3, "d")]), vec![0]);
        assert_eq!(test_query(vec![(0, "a"), (2, "c"), (4, "e")]), vec![0]);
        assert_eq!(test_query(vec![(4, "e"), (0, "a"), (2, "c")]), vec![0]);
        assert!(test_query(vec![(0, "a"), (2, "d")]).is_empty());
        assert_eq!(test_query(vec![(1, "a"), (3, "c")]), vec![0]);
        Ok(())
    }

    #[test]
    pub fn test_phrase_query_on_json() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(json_field=>json!({
                "text": "elliot smith the happy who"
            })))?;
            index_writer.add_document(doc!(json_field=>json!({
                "text": "the who elliot smith"
            })))?;
            index_writer.add_document(doc!(json_field=>json!({
                "arr": [{"text":"the who"}, {"text":"elliot smith"}]
            })))?;
            index_writer.add_document(doc!(json_field=>json!({
                "text2": "the smith"
            })))?;
            index_writer.commit()?;
        }
        let searcher = index.reader()?.searcher();
        let matching_docs = |query: &str| {
            let query_parser = QueryParser::for_index(&index, vec![json_field]);
            let phrase_query = query_parser.parse_query(query).unwrap();
            let phrase_weight = phrase_query
                .weight(EnableScoring::disabled_from_schema(searcher.schema()))
                .unwrap();
            let mut phrase_scorer = phrase_weight
                .scorer(searcher.segment_reader(0), 1.0f32)
                .unwrap();
            let mut docs = Vec::new();
            loop {
                let doc = phrase_scorer.doc();
                if doc == TERMINATED {
                    break;
                }
                docs.push(doc);
                phrase_scorer.advance();
            }
            docs
        };
        assert!(matching_docs(r#"text:"the smith""#).is_empty());
        assert_eq!(&matching_docs(r#"text:the"#), &[0u32, 1u32]);
        assert_eq!(&matching_docs(r#"text:"the""#), &[0u32, 1u32]);
        assert_eq!(&matching_docs(r#"text:"smith""#), &[0u32, 1u32]);
        assert_eq!(&matching_docs(r#"text:"elliot smith""#), &[0u32, 1u32]);
        assert_eq!(&matching_docs(r#"text2:"the smith""#), &[3u32]);
        assert!(&matching_docs(r#"arr.text:"the smith""#).is_empty());
        assert_eq!(&matching_docs(r#"arr.text:"elliot smith""#), &[2]);
        Ok(())
    }
}
