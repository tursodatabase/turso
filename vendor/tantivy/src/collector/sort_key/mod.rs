mod order;
mod sort_by_bytes;
mod sort_by_erased_type;
mod sort_by_score;
mod sort_by_static_fast_value;
mod sort_by_string;
mod sort_key_computer;

pub use order::*;
pub use sort_by_bytes::SortByBytes;
pub use sort_by_erased_type::SortByErasedType;
pub use sort_by_score::SortBySimilarityScore;
pub use sort_by_static_fast_value::SortByStaticFastValue;
pub use sort_by_string::SortByString;
pub use sort_key_computer::{SegmentSortKeyComputer, SortKeyComputer};

#[cfg(test)]
pub(crate) mod tests {

    // By spec, regardless of whether ascending or descending order was requested, in presence of a
    // tie, we sort by ascending doc id/doc address.
    pub(crate) fn sort_hits<TSortKey: Ord, D: Ord>(
        hits: &mut [ComparableDoc<TSortKey, D>],
        order: Order,
    ) {
        if order.is_asc() {
            hits.sort_by(|l, r| l.sort_key.cmp(&r.sort_key).then(l.doc.cmp(&r.doc)));
        } else {
            hits.sort_by(|l, r| {
                l.sort_key
                    .cmp(&r.sort_key)
                    .reverse() // This is descending
                    .then(l.doc.cmp(&r.doc))
            });
        }
    }

    use std::collections::HashMap;
    use std::ops::Range;

    use crate::collector::sort_key::{
        SortByErasedType, SortBySimilarityScore, SortByStaticFastValue, SortByString,
    };
    use crate::collector::{ComparableDoc, DocSetCollector, TopDocs};
    use crate::indexer::NoMergePolicy;
    use crate::query::{AllQuery, QueryParser};
    use crate::schema::{OwnedValue, Schema, FAST, TEXT};
    use crate::{DocAddress, Document, Index, Order, Score, Searcher};

    fn make_index() -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let id = schema_builder.add_u64_field("id", FAST);
        let city = schema_builder.add_text_field("city", TEXT | FAST);
        let catchphrase = schema_builder.add_text_field("catchphrase", TEXT);
        let altitude = schema_builder.add_f64_field("altitude", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        fn create_segment(index: &Index, docs: Vec<impl Document>) -> crate::Result<()> {
            let mut index_writer = index.writer_for_tests()?;
            index_writer.set_merge_policy(Box::new(NoMergePolicy));
            for doc in docs {
                index_writer.add_document(doc)?;
            }
            index_writer.commit()?;
            Ok(())
        }

        create_segment(
            &index,
            vec![
                doc!(
                    id => 0_u64,
                    city => "austin",
                    catchphrase => "Hills, Barbeque, Glow",
                    altitude => 149.0,
                ),
                doc!(
                    id => 1_u64,
                    city => "greenville",
                    catchphrase => "Grow, Glow, Glow",
                    altitude => 27.0,
                ),
            ],
        )?;
        create_segment(
            &index,
            vec![doc!(
                id => 2_u64,
                city => "tokyo",
                catchphrase => "Glow, Glow, Glow",
                altitude => 40.0,
            )],
        )?;
        create_segment(
            &index,
            vec![doc!(
                id => 3_u64,
                catchphrase => "No, No, No",
                altitude => 0.0,
            )],
        )?;
        Ok(index)
    }

    // NOTE: You cannot determine the SegmentIds that will be generated for Segments
    // ahead of time, so DocAddresses must be mapped back to a unique id for each Searcher.
    fn id_mapping(searcher: &Searcher) -> HashMap<DocAddress, u64> {
        searcher
            .search(&AllQuery, &DocSetCollector)
            .unwrap()
            .into_iter()
            .map(|doc_address| {
                let column = searcher.segment_readers()[doc_address.segment_ord as usize]
                    .fast_fields()
                    .u64("id")
                    .unwrap();
                (doc_address, column.first(doc_address.doc_id).unwrap())
            })
            .collect()
    }

    #[test]
    fn test_order_by_string() -> crate::Result<()> {
        let index = make_index()?;

        #[track_caller]
        fn assert_query(
            index: &Index,
            order: Order,
            doc_range: Range<usize>,
            expected: Vec<(Option<String>, u64)>,
        ) -> crate::Result<()> {
            let searcher = index.reader()?.searcher();
            let ids = id_mapping(&searcher);

            // Try as primitive.
            let top_collector = TopDocs::for_doc_range(doc_range)
                .order_by((SortByString::for_field("city"), order));
            let actual = searcher
                .search(&AllQuery, &top_collector)?
                .into_iter()
                .map(|(sort_key_opt, doc)| (sort_key_opt, ids[&doc]))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
            Ok(())
        }

        assert_query(
            &index,
            Order::Asc,
            0..4,
            vec![
                (Some("austin".to_owned()), 0),
                (Some("greenville".to_owned()), 1),
                (Some("tokyo".to_owned()), 2),
                (None, 3),
            ],
        )?;

        assert_query(
            &index,
            Order::Asc,
            0..3,
            vec![
                (Some("austin".to_owned()), 0),
                (Some("greenville".to_owned()), 1),
                (Some("tokyo".to_owned()), 2),
            ],
        )?;

        assert_query(
            &index,
            Order::Asc,
            0..2,
            vec![
                (Some("austin".to_owned()), 0),
                (Some("greenville".to_owned()), 1),
            ],
        )?;

        assert_query(
            &index,
            Order::Asc,
            0..1,
            vec![(Some("austin".to_string()), 0)],
        )?;

        assert_query(
            &index,
            Order::Asc,
            1..3,
            vec![
                (Some("greenville".to_owned()), 1),
                (Some("tokyo".to_owned()), 2),
            ],
        )?;

        assert_query(
            &index,
            Order::Desc,
            0..4,
            vec![
                (Some("tokyo".to_owned()), 2),
                (Some("greenville".to_owned()), 1),
                (Some("austin".to_owned()), 0),
                (None, 3),
            ],
        )?;

        assert_query(
            &index,
            Order::Desc,
            1..3,
            vec![
                (Some("greenville".to_owned()), 1),
                (Some("austin".to_owned()), 0),
            ],
        )?;

        assert_query(
            &index,
            Order::Desc,
            0..1,
            vec![(Some("tokyo".to_owned()), 2)],
        )?;

        Ok(())
    }

    #[test]
    fn test_order_by_f64() -> crate::Result<()> {
        let index = make_index()?;

        fn assert_query(
            index: &Index,
            order: Order,
            expected: Vec<(Option<f64>, u64)>,
        ) -> crate::Result<()> {
            let searcher = index.reader()?.searcher();
            let ids = id_mapping(&searcher);

            // Try as primitive.
            let top_collector = TopDocs::with_limit(3)
                .order_by((SortByStaticFastValue::<f64>::for_field("altitude"), order));
            let actual = searcher
                .search(&AllQuery, &top_collector)?
                .into_iter()
                .map(|(altitude_opt, doc)| (altitude_opt, ids[&doc]))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);

            Ok(())
        }

        assert_query(
            &index,
            Order::Asc,
            vec![(Some(0.0), 3), (Some(27.0), 1), (Some(40.0), 2)],
        )?;

        assert_query(
            &index,
            Order::Desc,
            vec![(Some(149.0), 0), (Some(40.0), 2), (Some(27.0), 1)],
        )?;

        Ok(())
    }

    #[test]
    fn test_order_by_score() -> crate::Result<()> {
        let index = make_index()?;

        fn query(index: &Index, order: Order) -> crate::Result<Vec<(Score, u64)>> {
            let searcher = index.reader()?.searcher();
            let ids = id_mapping(&searcher);

            let top_collector = TopDocs::with_limit(4).order_by((SortBySimilarityScore, order));
            let field = index.schema().get_field("catchphrase").unwrap();
            let query_parser = QueryParser::for_index(index, vec![field]);
            let text_query = query_parser.parse_query("glow")?;

            Ok(searcher
                .search(&text_query, &top_collector)?
                .into_iter()
                .map(|(score, doc)| (score, ids[&doc]))
                .collect())
        }

        assert_eq!(
            &query(&index, Order::Desc)?,
            &[(0.5604893, 2), (0.4904281, 1), (0.35667497, 0),]
        );

        assert_eq!(
            &query(&index, Order::Asc)?,
            &[(0.35667497, 0), (0.4904281, 1), (0.5604893, 2),]
        );

        Ok(())
    }

    #[test]
    fn test_order_by_score_then_string() -> crate::Result<()> {
        let index = make_index()?;

        type SortKey = (Score, Option<String>);

        fn query(
            index: &Index,
            score_order: Order,
            city_order: Order,
        ) -> crate::Result<Vec<(SortKey, u64)>> {
            let searcher = index.reader()?.searcher();
            let ids = id_mapping(&searcher);

            let top_collector = TopDocs::with_limit(4).order_by((
                (SortBySimilarityScore, score_order),
                (SortByString::for_field("city"), city_order),
            ));
            let results: Vec<((Score, Option<String>), DocAddress)> =
                searcher.search(&AllQuery, &top_collector)?;
            Ok(results.into_iter().map(|(f, doc)| (f, ids[&doc])).collect())
        }

        assert_eq!(
            &query(&index, Order::Asc, Order::Asc)?,
            &[
                ((1.0, Some("austin".to_owned())), 0),
                ((1.0, Some("greenville".to_owned())), 1),
                ((1.0, Some("tokyo".to_owned())), 2),
                ((1.0, None), 3),
            ]
        );

        assert_eq!(
            &query(&index, Order::Asc, Order::Desc)?,
            &[
                ((1.0, Some("tokyo".to_owned())), 2),
                ((1.0, Some("greenville".to_owned())), 1),
                ((1.0, Some("austin".to_owned())), 0),
                ((1.0, None), 3),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_order_by_score_then_owned_value() -> crate::Result<()> {
        let index = make_index()?;

        type SortKey = (Score, OwnedValue);

        fn query(
            index: &Index,
            score_order: Order,
            city_order: Order,
        ) -> crate::Result<Vec<(SortKey, u64)>> {
            let searcher = index.reader()?.searcher();
            let ids = id_mapping(&searcher);

            let top_collector = TopDocs::with_limit(4).order_by::<(Score, OwnedValue)>((
                (SortBySimilarityScore, score_order),
                (SortByErasedType::for_field("city"), city_order),
            ));
            let results: Vec<((Score, OwnedValue), DocAddress)> =
                searcher.search(&AllQuery, &top_collector)?;
            Ok(results.into_iter().map(|(f, doc)| (f, ids[&doc])).collect())
        }

        assert_eq!(
            &query(&index, Order::Asc, Order::Asc)?,
            &[
                ((1.0, OwnedValue::Str("austin".to_owned())), 0),
                ((1.0, OwnedValue::Str("greenville".to_owned())), 1),
                ((1.0, OwnedValue::Str("tokyo".to_owned())), 2),
                ((1.0, OwnedValue::Null), 3),
            ]
        );

        assert_eq!(
            &query(&index, Order::Asc, Order::Desc)?,
            &[
                ((1.0, OwnedValue::Str("tokyo".to_owned())), 2),
                ((1.0, OwnedValue::Str("greenville".to_owned())), 1),
                ((1.0, OwnedValue::Str("austin".to_owned())), 0),
                ((1.0, OwnedValue::Null), 3),
            ]
        );
        Ok(())
    }

    use proptest::prelude::*;

    proptest! {
    #[test]
    fn test_order_by_string_prop(
          order in prop_oneof!(Just(Order::Desc), Just(Order::Asc)),
          limit in 1..64_usize,
          offset in 0..64_usize,
          segments_terms in
            proptest::collection::vec(
                proptest::collection::vec(0..32_u8, 1..32_usize),
                0..8_usize,
            )
        ) {
            let mut schema_builder = Schema::builder();
            let city = schema_builder.add_text_field("city", TEXT | FAST);
            let schema = schema_builder.build();
            let index = Index::create_in_ram(schema);
            let mut index_writer = index.writer_for_tests()?;

            // A Vec<Vec<u8>>, where the outer Vec represents segments, and the inner Vec
            // represents terms.
            for segment_terms in segments_terms.into_iter() {
                for term in segment_terms.into_iter() {
                    let term = format!("{term:0>3}");
                    index_writer.add_document(doc!(
                        city => term,
                    ))?;
                }
                index_writer.commit()?;
            }

            let searcher = index.reader()?.searcher();
            let top_n_results = searcher.search(&AllQuery, &TopDocs::with_limit(limit)
                .and_offset(offset)
                .order_by_string_fast_field("city", order))?;
            let all_results = searcher.search(&AllQuery, &DocSetCollector)?.into_iter().map(|doc_address| {
                // Get the term for this address.
                let column = searcher.segment_readers()[doc_address.segment_ord as usize].fast_fields().str("city").unwrap().unwrap();
                let value = column.term_ords(doc_address.doc_id).next().map(|term_ord| {
                    let mut city = Vec::new();
                    column.dictionary().ord_to_term(term_ord, &mut city).unwrap();
                    String::try_from(city).unwrap()
                });
                (value, doc_address)
            });

            // Using the TopDocs collector should always be equivalent to sorting, skipping the
            // offset, and then taking the limit.
            let sorted_docs: Vec<_> = {
                let mut comparable_docs: Vec<ComparableDoc<_, _>> =
                    all_results.into_iter().map(|(sort_key, doc)| ComparableDoc { sort_key, doc}).collect();
                sort_hits(&mut comparable_docs, order);
                comparable_docs.into_iter().map(|cd| (cd.sort_key, cd.doc)).collect()
            };
            let expected_docs = sorted_docs.into_iter().skip(offset).take(limit).collect::<Vec<_>>();
            prop_assert_eq!(
                expected_docs,
                top_n_results
            );
        }
    }
}
