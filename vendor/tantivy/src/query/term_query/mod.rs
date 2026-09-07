mod term_query;
mod term_scorer;
mod term_weight;

pub use self::term_query::TermQuery;
pub use self::term_scorer::TermScorer;
#[cfg(test)]
mod tests {

    use crate::collector::TopDocs;
    use crate::docset::DocSet;
    use crate::postings::compression::COMPRESSION_BLOCK_SIZE;
    use crate::query::term_query::TermScorer;
    use crate::query::{
        AllScorer, EmptyScorer, EnableScoring, Query, QueryParser, Scorer, TermQuery,
    };
    use crate::schema::{Field, IndexRecordOption, Schema, FAST, STRING, TEXT};
    use crate::{assert_nearly_equals, DocAddress, Index, IndexWriter, Term, TERMINATED};

    #[test]
    pub fn test_term_query_no_freq() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", STRING);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            // writing the segment
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            let doc = doc!(text_field => "a");
            index_writer.add_document(doc)?;
            index_writer.commit()?;
        }
        let searcher = index.reader()?.searcher();
        let term_query = TermQuery::new(
            Term::from_field_text(text_field, "a"),
            IndexRecordOption::Basic,
        );
        let term_weight = term_query.weight(EnableScoring::enabled_from_searcher(&searcher))?;
        let segment_reader = searcher.segment_reader(0);
        let mut term_scorer = term_weight.scorer(segment_reader, 1.0)?;
        assert_eq!(term_scorer.doc(), 0);
        assert_nearly_equals!(term_scorer.score(), 0.28768212);
        Ok(())
    }

    #[test]
    pub fn test_term_query_multiple_of_block_len() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", STRING);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            // writing the segment
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            for _ in 0..COMPRESSION_BLOCK_SIZE {
                let doc = doc!(text_field => "a");
                index_writer.add_document(doc)?;
            }
            index_writer.commit()?;
        }
        let searcher = index.reader()?.searcher();
        let term_query = TermQuery::new(
            Term::from_field_text(text_field, "a"),
            IndexRecordOption::Basic,
        );
        let term_weight = term_query.weight(EnableScoring::enabled_from_searcher(&searcher))?;
        let segment_reader = searcher.segment_reader(0);
        let mut term_scorer = term_weight.scorer(segment_reader, 1.0)?;
        for i in 0u32..COMPRESSION_BLOCK_SIZE as u32 {
            assert_eq!(term_scorer.doc(), i);
            if i == COMPRESSION_BLOCK_SIZE as u32 - 1u32 {
                assert_eq!(term_scorer.advance(), TERMINATED);
            } else {
                assert_eq!(term_scorer.advance(), i + 1);
            }
        }
        assert_eq!(term_scorer.doc(), TERMINATED);
        Ok(())
    }

    #[test]
    pub fn test_term_weight() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let left_field = schema_builder.add_text_field("left", TEXT);
        let right_field = schema_builder.add_text_field("right", TEXT);
        let large_field = schema_builder.add_text_field("large", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(
                left_field => "left1 left2 left2 left2f2 left2f2 left3 abcde abcde abcde abcde abcde abcde abcde abcde abcde abcewde abcde abcde",
                right_field => "right1 right2",
                large_field => "large0 large1 large2 large3 large4 large5 large6 large7 large8 large9 large10 large11 large12 large13 large14 large15 large16 large17 large18 large19 large20 large21 large22 large23 large24 large25 large26 large27 large28 large29 large30 large31 large32 large33 large34 large35 large36 large37 large38 large39 large40 large41 large42 large43 large44 large45 large46 large47 large48 large49 large50 large51 large52 large53 large54 large55 large56 large57 large58 large59 large60 large61 large62 large63 large64 large65 large66 large67 large68 large69 large70 large71 large72 large73 large74 large75 large76 large77 large78 large79 large80 large81 large82 large83 large84 large85 large86 large87 large88 large89 large90 large91 large92 large93 large94 large95 large96 large97 large98 large99 large100 large101 large102 large103 large104 large105 large106 large107 large108 large109 large110 large111 large112 large113 large114 large115 large116 large117 large118 large119 large120 large121 large122 large123 large124 large125 large126 large127 large128 large129 large130 large131 large132 large133 large134 large135 large136 large137 large138 large139 large140 large141 large142 large143 large144 large145 large146 large147 large148 large149 large150 large151 large152 large153 large154 large155 large156 large157 large158 large159 large160 large161 large162 large163 large164 large165 large166 large167 large168 large169 large170 large171 large172 large173 large174 large175 large176 large177 large178 large179 large180 large181 large182 large183 large184 large185 large186 large187 large188 large189 large190 large191 large192 large193 large194 large195 large196 large197 large198 large199 large200 large201 large202 large203 large204 large205 large206 large207 large208 large209 large210 large211 large212 large213 large214 large215 large216 large217 large218 large219 large220 large221 large222 large223 large224 large225 large226 large227 large228 large229 large230 large231 large232 large233 large234 large235 large236 large237 large238 large239 large240 large241 large242 large243 large244 large245 large246 large247 large248 large249 large250 large251 large252 large253 large254 large255 large256 large257 large258 large259 large260 large261 large262 large263 large264 large265 large266 large267 large268 large269 large270 large271 large272 large273 large274 large275 large276 large277 large278 large279 large280 large281 large282 large283 large284 large285 large286"
            ))?;
            index_writer.add_document(doc!(left_field => "left4 left1"))?;
            index_writer.commit()?;
        }
        let searcher = index.reader()?.searcher();
        {
            let term = Term::from_field_text(left_field, "left2");
            let term_query = TermQuery::new(term, IndexRecordOption::WithFreqs);
            let topdocs = searcher.search(&term_query, &TopDocs::with_limit(2).order_by_score())?;
            assert_eq!(topdocs.len(), 1);
            let (score, _) = topdocs[0];
            assert_nearly_equals!(0.77802235, score);
        }
        {
            let term = Term::from_field_text(left_field, "left1");
            let term_query = TermQuery::new(term, IndexRecordOption::WithFreqs);
            let top_docs =
                searcher.search(&term_query, &TopDocs::with_limit(2).order_by_score())?;
            assert_eq!(top_docs.len(), 2);
            let (score1, _) = top_docs[0];
            assert_nearly_equals!(0.27101856, score1);
            let (score2, _) = top_docs[1];
            assert_nearly_equals!(0.13736556, score2);
        }
        {
            let query_parser = QueryParser::for_index(&index, Vec::new());
            let query = query_parser.parse_query("left:left2 left:left1")?;
            let top_docs = searcher.search(&query, &TopDocs::with_limit(2).order_by_score())?;
            assert_eq!(top_docs.len(), 2);
            let (score1, _) = top_docs[0];
            assert_nearly_equals!(0.9153879, score1);
            let (score2, _) = top_docs[1];
            assert_nearly_equals!(0.27101856, score2);
        }
        Ok(())
    }

    #[test]
    fn test_term_query_count_when_there_are_deletes() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        index_writer.add_document(doc!(text_field=>"a b"))?;
        index_writer.add_document(doc!(text_field=>"a c"))?;
        index_writer.delete_term(Term::from_field_text(text_field, "b"));
        index_writer.commit()?;
        let term_a = Term::from_field_text(text_field, "a");
        let term_query = TermQuery::new(term_a, IndexRecordOption::Basic);
        let reader = index.reader()?;
        assert_eq!(term_query.count(&reader.searcher())?, 1);
        Ok(())
    }

    #[test]
    fn test_term_query_simple_seek() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        index_writer.add_document(doc!(text_field=>"a"))?;
        index_writer.add_document(doc!(text_field=>"a"))?;
        index_writer.commit()?;
        let term_a = Term::from_field_text(text_field, "a");
        let term_query = TermQuery::new(term_a, IndexRecordOption::Basic);
        let searcher = index.reader()?.searcher();
        let term_weight =
            term_query.weight(EnableScoring::disabled_from_schema(searcher.schema()))?;
        let mut term_scorer = term_weight.scorer(searcher.segment_reader(0u32), 1.0)?;
        assert_eq!(term_scorer.doc(), 0u32);
        term_scorer.seek(1u32);
        assert_eq!(term_scorer.doc(), 1u32);
        Ok(())
    }

    #[test]
    fn test_term_query_debug() {
        let term_query = TermQuery::new(
            Term::from_field_text(Field::from_field_id(1), "hello"),
            IndexRecordOption::WithFreqs,
        );
        assert_eq!(
            format!("{term_query:?}"),
            r#"TermQuery(Term(field=1, type=Str, "hello"))"#
        );
    }

    #[test]
    fn test_term_query_explain() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        index_writer.add_document(doc!(text_field=>"b"))?;
        index_writer.add_document(doc!(text_field=>"a"))?;
        index_writer.add_document(doc!(text_field=>"a"))?;
        index_writer.add_document(doc!(text_field=>"b"))?;
        index_writer.commit()?;
        let term_a = Term::from_field_text(text_field, "a");
        let term_query = TermQuery::new(term_a, IndexRecordOption::Basic);
        let searcher = index.reader()?.searcher();
        {
            let explanation = term_query.explain(&searcher, DocAddress::new(0u32, 1u32))?;
            assert_nearly_equals!(explanation.value(), std::f32::consts::LN_2);
        }
        {
            let explanation_err = term_query.explain(&searcher, DocAddress::new(0u32, 0u32));
            assert!(matches!(
                explanation_err,
                Err(crate::TantivyError::InvalidArgument(_msg))
            ));
        }
        {
            let explanation_err = term_query.explain(&searcher, DocAddress::new(0u32, 3u32));
            assert!(matches!(
                explanation_err,
                Err(crate::TantivyError::InvalidArgument(_msg))
            ));
        }
        Ok(())
    }

    #[test]
    fn test_term_query_fallback_to_fastfield() -> crate::Result<()> {
        use crate::collector::Count;
        use crate::schema::FAST;

        // Create a FAST-only numeric field (not indexed)
        let mut schema_builder = Schema::builder();
        let num_field = schema_builder.add_u64_field("num", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(num_field => 10u64))?;
            index_writer.add_document(doc!(num_field => 20u64))?;
            index_writer.add_document(doc!(num_field => 10u64))?;
            index_writer.commit()?;
        }

        let reader = index.reader()?;
        let searcher = reader.searcher();

        // TermQuery should fall back to a fastfield range query and match correctly.
        let tq_10 = TermQuery::new(
            Term::from_field_u64(num_field, 10u64),
            IndexRecordOption::Basic,
        );
        let tq_20 = TermQuery::new(
            Term::from_field_u64(num_field, 20u64),
            IndexRecordOption::Basic,
        );
        let tq_30 = TermQuery::new(
            Term::from_field_u64(num_field, 30u64),
            IndexRecordOption::Basic,
        );

        let count_10 = searcher.search(&tq_10, &Count)?;
        let count_20 = searcher.search(&tq_20, &Count)?;
        let count_30 = searcher.search(&tq_30, &Count)?;

        assert_eq!(count_10, 2);
        assert_eq!(count_20, 1);
        assert_eq!(count_30, 0);

        Ok(())
    }

    #[test]
    fn test_term_query_fallback_text_fast_only() -> crate::Result<()> {
        use crate::collector::Count;

        // FAST-only text field (not indexed)
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(text_field => "hello"))?;
            index_writer.add_document(doc!(text_field => "world"))?;
            index_writer.add_document(doc!(text_field => "hello"))?;
            index_writer.commit()?;
        }

        let searcher = index.reader()?.searcher();
        let tq_hello = TermQuery::new(
            Term::from_field_text(text_field, "hello"),
            IndexRecordOption::Basic,
        );
        let tq_world = TermQuery::new(
            Term::from_field_text(text_field, "world"),
            IndexRecordOption::Basic,
        );
        let tq_missing = TermQuery::new(
            Term::from_field_text(text_field, "nope"),
            IndexRecordOption::Basic,
        );

        assert_eq!(searcher.search(&tq_hello, &Count)?, 2);
        assert_eq!(searcher.search(&tq_world, &Count)?, 1);
        assert_eq!(searcher.search(&tq_missing, &Count)?, 0);

        Ok(())
    }

    #[test]
    fn test_term_query_fallback_json_fast_only() -> crate::Result<()> {
        use crate::collector::Count;
        use crate::fastfield::FastValue;
        use crate::schema::FAST;

        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());

        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(json_field => json!({"a": 10, "b": "x"})))?;
            index_writer.add_document(doc!(json_field => json!({"a": 20, "b": "y"})))?;
            index_writer.add_document(doc!(json_field => json!({"a": 10, "b": "z"})))?;
            index_writer.commit()?;
        }

        fn json_term_fast<T: FastValue>(field: Field, path: &str, v: T) -> Term {
            let mut term = Term::from_field_json_path(field, path, true);
            term.append_type_and_fast_value(v);
            term
        }
        fn json_term_str(field: Field, path: &str, v: &str) -> Term {
            let mut term = Term::from_field_json_path(field, path, true);
            term.append_type_and_str(v);
            term
        }

        let searcher = index.reader()?.searcher();
        // numeric path match
        let tq_a10 = TermQuery::new(
            json_term_fast(json_field, "a", 10u64),
            IndexRecordOption::Basic,
        );
        let tq_a20 = TermQuery::new(
            json_term_fast(json_field, "a", 20u64),
            IndexRecordOption::Basic,
        );
        let tq_a30 = TermQuery::new(
            json_term_fast(json_field, "a", 30u64),
            IndexRecordOption::Basic,
        );
        assert_eq!(searcher.search(&tq_a10, &Count)?, 2);
        assert_eq!(searcher.search(&tq_a20, &Count)?, 1);
        assert_eq!(searcher.search(&tq_a30, &Count)?, 0);

        // string path match
        let tq_bx = TermQuery::new(
            json_term_str(json_field, "b", "x"),
            IndexRecordOption::Basic,
        );
        let tq_by = TermQuery::new(
            json_term_str(json_field, "b", "y"),
            IndexRecordOption::Basic,
        );
        let tq_bm = TermQuery::new(
            json_term_str(json_field, "b", "missing"),
            IndexRecordOption::Basic,
        );
        assert_eq!(searcher.search(&tq_bx, &Count)?, 1);
        assert_eq!(searcher.search(&tq_by, &Count)?, 1);
        assert_eq!(searcher.search(&tq_bm, &Count)?, 0);
        Ok(())
    }

    #[test]
    fn test_term_query_fallback_ip_fast_only() -> crate::Result<()> {
        use std::net::IpAddr;
        use std::str::FromStr;

        use crate::collector::Count;
        use crate::schema::{IntoIpv6Addr, FAST};

        let mut schema_builder = Schema::builder();
        let ip_field = schema_builder.add_ip_addr_field("ip", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        let ip1 = IpAddr::from_str("127.0.0.1").unwrap().into_ipv6_addr();
        let ip2 = IpAddr::from_str("127.0.0.2").unwrap().into_ipv6_addr();
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(ip_field => ip1))?;
            index_writer.add_document(doc!(ip_field => ip2))?;
            index_writer.add_document(doc!(ip_field => ip1))?;
            index_writer.commit()?;
        }

        let searcher = index.reader()?.searcher();
        let tq_ip1 = TermQuery::new(
            Term::from_field_ip_addr(ip_field, ip1),
            IndexRecordOption::Basic,
        );
        let tq_ip2 = TermQuery::new(
            Term::from_field_ip_addr(ip_field, ip2),
            IndexRecordOption::Basic,
        );
        let ip3 = IpAddr::from_str("127.0.0.3").unwrap().into_ipv6_addr();
        let tq_ip3 = TermQuery::new(
            Term::from_field_ip_addr(ip_field, ip3),
            IndexRecordOption::Basic,
        );

        assert_eq!(searcher.search(&tq_ip1, &Count)?, 2);
        assert_eq!(searcher.search(&tq_ip2, &Count)?, 1);
        assert_eq!(searcher.search(&tq_ip3, &Count)?, 0);
        Ok(())
    }

    #[test]
    fn test_term_query_fallback_fastfield_with_scores_errors() -> crate::Result<()> {
        use crate::collector::TopDocs;

        // FAST-only numeric field (not indexed) should error when scoring is required
        let mut schema_builder = Schema::builder();
        let num_field = schema_builder.add_u64_field("num", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(num_field => 10u64))?;
            index_writer.add_document(doc!(num_field => 20u64))?;
            index_writer.commit()?;
        }

        let searcher = index.reader()?.searcher();
        let tq = TermQuery::new(
            Term::from_field_u64(num_field, 10u64),
            IndexRecordOption::Basic,
        );

        // Using TopDocs requires scoring; since the field is not indexed,
        // TermQuery cannot score and should return a SchemaError.
        let res = searcher.search(&tq, &TopDocs::with_limit(1).order_by_score());
        assert!(matches!(res, Err(crate::TantivyError::SchemaError(_))));

        Ok(())
    }

    #[test]
    fn test_term_weight_all_query_optimization() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", crate::schema::TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer
            .add_document(doc!(text_field=>"hello"))
            .unwrap();
        index_writer
            .add_document(doc!(text_field=>"hello happy"))
            .unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let get_scorer_for_term = |term: &str| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, term),
                IndexRecordOption::Basic,
            );
            let term_weight = term_query
                .weight(EnableScoring::disabled_from_schema(&schema))
                .unwrap();
            term_weight
                .scorer(searcher.segment_reader(0u32), 1.0f32)
                .unwrap()
        };
        // Should be an allscorer
        let match_all_scorer = get_scorer_for_term("hello");
        // Should be a term scorer
        let match_some_scorer = get_scorer_for_term("happy");
        // Should be an empty scorer
        let empty_scorer = get_scorer_for_term("tax");
        assert!(match_all_scorer.is::<AllScorer>());
        assert!(match_some_scorer.is::<TermScorer>());
        assert!(empty_scorer.is::<EmptyScorer>());
    }

    #[test]
    fn test_term_weight_all_query_optimization_disable_when_scoring_enabled() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", crate::schema::TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer
            .add_document(doc!(text_field=>"hello"))
            .unwrap();
        index_writer
            .add_document(doc!(text_field=>"hello happy"))
            .unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let get_scorer_for_term = |term: &str| {
            let term_query = TermQuery::new(
                Term::from_field_text(text_field, term),
                IndexRecordOption::Basic,
            );
            let term_weight = term_query
                .weight(EnableScoring::enabled_from_searcher(&searcher))
                .unwrap();
            term_weight
                .scorer(searcher.segment_reader(0u32), 1.0f32)
                .unwrap()
        };
        // Should be an allscorer
        let match_all_scorer = get_scorer_for_term("hello");
        // Should be a term scorer
        let one_scorer = get_scorer_for_term("happy");
        // Should be an empty scorer
        let empty_scorer = get_scorer_for_term("tax");
        assert!(match_all_scorer.is::<TermScorer>());
        assert!(one_scorer.is::<TermScorer>());
        assert!(empty_scorer.is::<EmptyScorer>());
    }
}
