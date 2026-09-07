mod all_query;
mod automaton_weight;
mod bitset;
mod bm25;
mod boolean_query;
mod boost_query;
mod const_score_query;
mod disjunction;
mod disjunction_max_query;
mod empty_query;
mod exclude;
mod exist_query;
mod explanation;
mod fuzzy_query;
mod intersection;
mod more_like_this;
mod phrase_prefix_query;
mod phrase_query;
mod query;
mod query_parser;
mod range_query;
mod regex_query;
mod reqopt_scorer;
mod scorer;
mod set_query;
mod size_hint;
mod term_query;
mod union;
mod weight;

#[cfg(test)]
mod seek_danger_tests;
#[cfg(test)]
mod vec_docset;

pub(crate) mod score_combiner;
pub use query_grammar::Occur;

pub use self::all_query::{AllQuery, AllScorer, AllWeight};
pub use self::automaton_weight::AutomatonWeight;
pub use self::bitset::BitSetDocSet;
pub use self::bm25::{Bm25StatisticsProvider, Bm25Weight};
pub use self::boolean_query::{BooleanQuery, BooleanWeight};
pub use self::boost_query::{BoostQuery, BoostWeight};
pub use self::const_score_query::{ConstScoreQuery, ConstScorer};
pub use self::disjunction_max_query::DisjunctionMaxQuery;
pub use self::empty_query::{EmptyQuery, EmptyScorer, EmptyWeight};
pub use self::exclude::{Exclude, ExclusionSet};
pub use self::exist_query::ExistsQuery;
pub use self::explanation::Explanation;
#[cfg(test)]
pub(crate) use self::fuzzy_query::DfaWrapper;
pub use self::fuzzy_query::FuzzyTermQuery;
pub use self::intersection::{intersect_scorers, Intersection};
pub use self::more_like_this::{MoreLikeThisQuery, MoreLikeThisQueryBuilder};
pub use self::phrase_prefix_query::PhrasePrefixQuery;
pub use self::phrase_query::regex_phrase_query::{wildcard_query_to_regex_str, RegexPhraseQuery};
pub use self::phrase_query::PhraseQuery;
pub use self::query::{EnableScoring, Query, QueryClone};
pub use self::query_parser::{QueryParser, QueryParserError};
pub use self::range_query::*;
pub use self::regex_query::RegexQuery;
pub use self::reqopt_scorer::RequiredOptionalScorer;
pub use self::score_combiner::{DisjunctionMaxCombiner, ScoreCombiner, SumCombiner};
pub use self::scorer::Scorer;
pub use self::set_query::TermSetQuery;
pub use self::term_query::TermQuery;
pub use self::union::BufferedUnionScorer;
#[cfg(test)]
pub use self::vec_docset::VecDocSet;
pub use self::weight::Weight;

#[cfg(test)]
mod tests {
    use crate::collector::TopDocs;
    use crate::query::phrase_query::tests::create_index;
    use crate::query::QueryParser;
    use crate::schema::{Schema, TEXT};
    use crate::{DocAddress, Index, Term};

    #[test]
    pub fn test_mixed_intersection_and_union() -> crate::Result<()> {
        let index = create_index(&["a b", "a c", "a b c", "b"])?;
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        let searcher = index.reader()?.searcher();

        let do_search = |term: &str| {
            let query = QueryParser::for_index(&index, vec![text_field])
                .parse_query(term)
                .unwrap();
            let top_docs: Vec<(f32, DocAddress)> = searcher
                .search(&query, &TopDocs::with_limit(10).order_by_score())
                .unwrap();

            top_docs.iter().map(|el| el.1.doc_id).collect::<Vec<_>>()
        };

        assert_eq!(do_search("a AND b"), vec![0, 2]);
        assert_eq!(do_search("(a OR b) AND C"), vec![2, 1]);
        // The intersection code has special code for more than 2 intersections
        // left, right + others
        // The will place the union in the "others" insersection to that seek_into_the_danger_zone
        // is called
        assert_eq!(
            do_search("(a OR b) AND (c OR a) AND (b OR c)"),
            vec![2, 1, 0]
        );

        Ok(())
    }

    #[test]
    pub fn test_mixed_intersection_and_union_with_skip() -> crate::Result<()> {
        // Test 4096 skip in BufferedUnionScorer
        let mut data: Vec<&str> = Vec::new();
        data.push("a b");
        let zz_data = vec!["z z"; 5000];
        data.extend_from_slice(&zz_data);
        data.extend_from_slice(&["a c"]);
        data.extend_from_slice(&zz_data);
        data.extend_from_slice(&["a b c", "b"]);
        let index = create_index(&data)?;
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        let searcher = index.reader()?.searcher();

        let do_search = |term: &str| {
            let query = QueryParser::for_index(&index, vec![text_field])
                .parse_query(term)
                .unwrap();
            let top_docs: Vec<(f32, DocAddress)> = searcher
                .search(&query, &TopDocs::with_limit(10).order_by_score())
                .unwrap();

            top_docs.iter().map(|el| el.1.doc_id).collect::<Vec<_>>()
        };

        assert_eq!(do_search("a AND b"), vec![0, 10002]);
        assert_eq!(do_search("(a OR b) AND C"), vec![10002, 5001]);
        // The intersection code has special code for more than 2 intersections
        // left, right + others
        // The will place the union in the "others" insersection to that seek_into_the_danger_zone
        // is called
        assert_eq!(
            do_search("(a OR b) AND (c OR a) AND (b OR c)"),
            vec![10002, 5001, 0]
        );

        Ok(())
    }

    #[test]
    fn test_query_terms() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let term_a = Term::from_field_text(text_field, "a");
        let term_b = Term::from_field_text(text_field, "b");
        {
            let query = query_parser.parse_query("a").unwrap();
            let mut terms = Vec::new();
            query.query_terms(&mut |term, pos| terms.push((term, pos)));
            assert_eq!(vec![(&term_a, false)], terms);
        }
        {
            let query = query_parser.parse_query("a b").unwrap();
            let mut terms = Vec::new();
            query.query_terms(&mut |term, pos| terms.push((term, pos)));
            assert_eq!(vec![(&term_a, false), (&term_b, false)], terms);
        }
        {
            let query = query_parser.parse_query("\"a b\"").unwrap();
            let mut terms = Vec::new();
            query.query_terms(&mut |term, pos| terms.push((term, pos)));
            assert_eq!(vec![(&term_a, true), (&term_b, true)], terms);
        }
        {
            let query = query_parser.parse_query("a a a a a").unwrap();
            let mut terms = Vec::new();
            query.query_terms(&mut |term, pos| terms.push((term, pos)));
            assert_eq!(vec![(&term_a, false); 1], terms);
        }
        {
            let query = query_parser.parse_query("a -b").unwrap();
            let mut terms = Vec::new();
            query.query_terms(&mut |term, pos| terms.push((term, pos)));
            assert_eq!(vec![(&term_a, false), (&term_b, false)], terms);
        }
    }
}
