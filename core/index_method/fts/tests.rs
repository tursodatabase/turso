use super::*;
use crate::{
    index_method::{
        IndexMethodAttachment, IndexMethodConfiguration, IndexMethodCostContext,
        IndexMethodCostEstimate,
    },
    schema::IndexColumn,
};
use rustc_hash::FxHashMap;
use std::num::NonZeroU32;
use turso_parser::ast::{Expr, Literal, UnaryOperator, Variable};

fn test_attachment() -> FtsIndexAttachment {
    FtsIndexAttachment::new(IndexMethodConfiguration {
        table_name: "docs".to_string(),
        index_name: "docs_fts".to_string(),
        columns: vec![IndexColumn::new("title", 1), IndexColumn::new("body", 2)],
        parameters: FxHashMap::<String, Value>::default(),
    })
    .unwrap()
}

#[test]
fn indexed_text_is_not_duplicated_in_tantivy_document_store() {
    let attachment = test_attachment();
    for (_, field) in attachment.text_fields {
        assert!(
            !attachment.schema.get_field_entry(field).is_stored(),
            "FTS projections come from the base table, so storing indexed text duplicates data"
        );
    }
}

fn estimate_cost(pattern_idx: i64, limit: Option<Expr>) -> IndexMethodCostEstimate {
    let attachment = FtsIndexAttachment::new(IndexMethodConfiguration {
        table_name: "docs".to_string(),
        index_name: "docs_fts".to_string(),
        columns: vec![IndexColumn::new("body", 1)],
        parameters: FxHashMap::<String, Value>::default(),
    })
    .unwrap();
    let cursor = attachment.init().unwrap();
    let mut arguments = vec![Expr::Literal(Literal::String("'database'".to_string()))];
    arguments.extend(limit);

    cursor
        .estimate_cost(&IndexMethodCostContext {
            pattern_idx: pattern_idx as usize,
            base_table_rows: 100_000.0,
            arguments: &arguments,
        })
        .unwrap()
}

#[test]
fn fts_cost_estimate_applies_literal_limit_to_output_rows() {
    let unlimited = estimate_cost(FTS_PATTERN_MATCH, None);
    assert_eq!(unlimited.estimated_rows, 1_000);

    let limited = estimate_cost(
        FTS_PATTERN_MATCH_LIMIT,
        Some(Expr::Literal(Literal::Numeric("10".to_string()))),
    );
    assert_eq!(limited.estimated_rows, 10);
    assert!(limited.estimated_cost < unlimited.estimated_cost);
    let ranked = estimate_cost(
        FTS_PATTERN_SCORE,
        Some(Expr::Literal(Literal::Numeric("10".to_string()))),
    );
    assert_eq!(ranked.estimated_rows, 10);
    assert!(
        ranked.estimated_cost > limited.estimated_cost,
        "global score ordering must account for scoring all matches"
    );

    let zero = estimate_cost(
        FTS_PATTERN_MATCH_LIMIT,
        Some(Expr::Literal(Literal::Numeric("0".to_string()))),
    );
    assert_eq!(zero.estimated_rows, 0);

    let negative = estimate_cost(
        FTS_PATTERN_MATCH_LIMIT,
        Some(Expr::Unary(
            UnaryOperator::Negative,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        )),
    );
    assert_eq!(negative.estimated_rows, unlimited.estimated_rows);

    let dynamic = estimate_cost(
        FTS_PATTERN_MATCH_LIMIT,
        Some(Expr::Variable(Variable::indexed(NonZeroU32::MIN))),
    );
    assert_eq!(dynamic.estimated_rows, unlimited.estimated_rows);
}

#[test]
fn query_limit_is_exact_and_bounded_by_live_documents() {
    assert_eq!(bounded_query_limit(None, 1_500_000), 1_500_000);
    assert_eq!(bounded_query_limit(Some(-1), 1_500_000), 1_500_000);
    assert_eq!(bounded_query_limit(Some(i64::MAX), 37), 37);
    assert_eq!(bounded_query_limit(Some(12), 37), 12);
    assert_eq!(bounded_query_limit(Some(0), 37), 0);
    assert_eq!(bounded_query_limit(None, 0), 0);
}

/// Build one segment through the private write path and reopen it through a
/// synthesized snapshot view — the round trip every write and read takes,
/// without a database underneath.
fn build_and_load_segment(
    attachment: &FtsIndexAttachment,
    docs: &[(i64, &str)],
) -> (LoadedSegment, Vec<PendingRow>) {
    let mut cursor_docs = Vec::new();
    for (rowid, text) in docs {
        let mut doc = TantivyDocument::default();
        doc.add_i64(attachment.rowid_field, *rowid);
        doc.add_text(attachment.text_fields[0].1, *text);
        cursor_docs.push(BufferedDoc { rowid: *rowid, doc });
    }
    let mut cursor = FtsCursor::new(attachment);
    cursor.doc_buffer = cursor_docs;
    let (segment, rows) = cursor.build_segment().unwrap();
    (segment.expect("non-empty buffer builds a segment"), rows)
}

#[test]
fn segment_build_round_trips_through_synthesized_snapshot() {
    let attachment = test_attachment();
    let (segment, rows) = build_and_load_segment(
        &attachment,
        &[(1, "hello turso"), (2, "hello world"), (3, "goodbye")],
    );
    assert_eq!(segment.descriptor.max_doc, 3);
    // Rows: chunks for every captured file plus one descriptor row.
    let descriptor_rows = rows
        .iter()
        .filter(|row| row.path.starts_with(FTS2_SEGMENT_PREFIX))
        .count();
    assert_eq!(descriptor_rows, 1);
    assert!(rows
        .iter()
        .all(|row| row.path.starts_with(FTS2_PATH_PREFIX)));

    // Reopen through a snapshot view and query it.
    let mut cursor = FtsCursor::new(&attachment);
    cursor.segments = vec![segment];
    cursor.snapshot_loaded = true;
    cursor.ensure_searcher().unwrap();
    let searcher = cursor.searcher.as_ref().unwrap();
    assert_eq!(searcher.num_docs(), 3);

    let parser = cursor.cached_parser.as_ref().unwrap();
    let (query, errors) = parser.parse_query_lenient("hello");
    assert!(errors.is_empty());
    let hits = searcher.search(&query, &tantivy::collector::Count).unwrap();
    assert_eq!(hits, 2);
}

#[test]
fn merged_segment_files_can_be_rekeyed_to_a_minted_id() {
    let attachment = test_attachment();
    let (segment, _) = build_and_load_segment(
        &attachment,
        &[(1, "hello turso"), (2, "hello world"), (3, "goodbye")],
    );
    let minted = SegmentId::from_uuid_string("0123456789abcdef0123456789abcdef").unwrap();
    assert_ne!(segment.id(), minted);

    let files: HashMap<PathBuf, Arc<[u8]>> = segment
        .data
        .as_ref()
        .unwrap()
        .files
        .iter()
        .map(|(name, bytes)| (PathBuf::from(name), Arc::clone(bytes)))
        .collect();
    let renamed = rename_segment_files(files.clone(), &segment.id(), &minted).unwrap();
    assert_eq!(renamed.len(), files.len());
    assert!(renamed
        .keys()
        .all(|path| path.to_str().unwrap().starts_with(&minted.uuid_string())));

    // The renamed files open and answer queries under the new id: the
    // bytes never embed the segment id.
    let (rekeyed, _) =
        segment_rows_from_files(minted, segment.descriptor.max_doc, renamed).unwrap();
    let rekeyed = rekeyed.expect("non-empty segment");
    assert_eq!(rekeyed.id(), minted);
    let mut cursor = FtsCursor::new(&attachment);
    cursor.segments = vec![rekeyed];
    cursor.snapshot_loaded = true;
    cursor.ensure_searcher().unwrap();
    let searcher = cursor.searcher.as_ref().unwrap();
    let (query, _) = cursor
        .cached_parser
        .as_ref()
        .unwrap()
        .parse_query_lenient("hello");
    assert_eq!(
        searcher.search(&query, &tantivy::collector::Count).unwrap(),
        2
    );

    // A file that is not named after the source segment is a bug, not
    // something to rename silently.
    let mut stray = files;
    stray.insert(PathBuf::from("meta.json"), Arc::from(Vec::new()));
    assert!(matches!(
        rename_segment_files(stray, &segment.id(), &minted),
        Err(LimboError::InternalError(_))
    ));
}

#[test]
fn tombstoned_docs_are_invisible_at_the_reader_level() {
    let attachment = test_attachment();
    let (mut segment, _) = build_and_load_segment(
        &attachment,
        &[(1, "hello turso"), (2, "hello world"), (3, "goodbye")],
    );

    let mut cursor = FtsCursor::new(&attachment);
    cursor.segments = vec![segment.clone()];
    cursor.snapshot_loaded = true;
    cursor.ensure_searcher().unwrap();
    let term = tantivy::query::TermQuery::new(
        Term::from_field_i64(cursor.rowid_field, 2),
        IndexRecordOption::Basic,
    );
    let postings = cursor
        .searcher
        .as_ref()
        .unwrap()
        .search(
            &term,
            &tantivy::collector::TopDocs::with_limit(1).order_by_score(),
        )
        .unwrap();
    assert_eq!(postings.len(), 1);
    let doc_id = postings[0].1.doc_id;

    // Tombstone rowid 2 and rebuild the view: the posting must disappear
    // from every query path, including counts.
    segment.deleted.insert(doc_id);
    let mut cursor = FtsCursor::new(&attachment);
    cursor.segments = vec![segment];
    cursor.snapshot_loaded = true;
    cursor.ensure_searcher().unwrap();
    let searcher = cursor.searcher.as_ref().unwrap();
    assert_eq!(searcher.num_docs(), 2);
    let parser = cursor.cached_parser.as_ref().unwrap();
    let (query, _) = parser.parse_query_lenient("hello");
    let hits = searcher.search(&query, &tantivy::collector::Count).unwrap();
    assert_eq!(hits, 1, "the tombstoned posting must not match");
    assert_eq!(
        searcher.search(&term, &tantivy::collector::Count).unwrap(),
        0
    );
}

#[test]
fn snapshots_with_different_segment_sets_do_not_share_searchers() {
    let attachment = test_attachment();
    let (segment_a, _) = build_and_load_segment(&attachment, &[(1, "alpha")]);
    let (segment_b, _) = build_and_load_segment(&attachment, &[(2, "beta")]);

    let key_a = searcher_key(std::slice::from_ref(&segment_a));
    let key_ab = searcher_key(&[segment_a.clone(), segment_b]);
    assert_ne!(key_a, key_ab);

    // Tombstone state is part of the identity.
    let mut tombstoned = segment_a.clone();
    tombstoned.deleted.insert(0);
    assert_ne!(
        searcher_key(std::slice::from_ref(&segment_a)),
        searcher_key(std::slice::from_ref(&tombstoned))
    );
}

#[test]
fn segment_byte_cache_keeps_newest_and_respects_budget() {
    let mut cache = SegmentByteCache::default();
    let make_data = |bytes: usize| {
        let mut files = HashMap::default();
        files.insert("f".to_string(), Arc::<[u8]>::from(vec![0u8; bytes]));
        Arc::new(SegmentData::new(files))
    };
    let a = SegmentId::generate_random();
    let b = SegmentId::generate_random();
    let c = SegmentId::generate_random();
    cache.put(a, make_data(100), 250);
    cache.put(b, make_data(100), 250);
    cache.put(c, make_data(100), 250);
    assert!(cache.get(&a).is_none(), "oldest entry evicted over budget");
    assert!(cache.get(&b).is_some());
    assert!(cache.get(&c).is_some());

    // An entry larger than the whole budget is still kept (it is the
    // newest); older entries are evicted to make room.
    cache.put(a, make_data(1000), 250);
    assert!(cache.get(&a).is_some());
    assert!(cache.get(&b).is_none());
    assert!(cache.get(&c).is_none());
}

#[test]
fn query_weights_suspend_for_global_statistics_without_sync_fallback() {
    use tantivy::directory::{FileSlice, ReadQueue};
    use tantivy::query::{Bm25StatisticsProvider, ConstScoreQuery, DisjunctionMaxQuery};
    use tantivy::termdict::{PagedTermDictionary, TermDictionaryBuilder};

    let attachment = test_attachment();
    let (first, _) =
        build_and_load_segment(&attachment, &[(1, "alpha beta"), (2, "alpha gamma beta")]);
    let (second, _) = build_and_load_segment(&attachment, &[(3, "alpha beta"), (4, "beta gamma")]);
    let mut resident = FtsCursor::new(&attachment);
    resident.segments = vec![first, second];
    resident.ensure_searcher().unwrap();
    let searcher = resident.searcher.as_ref().unwrap();
    let field = attachment.text_fields[0].1;
    let mut counts = Vec::new();
    counts.extend_from_slice(&searcher.total_num_tokens(field).unwrap().to_le_bytes());
    counts.extend_from_slice(&searcher.total_num_docs().unwrap().to_le_bytes());
    let mut builder = TermDictionaryBuilder::create(Vec::new()).unwrap();
    for text in ["alpha", "beta", "gamma"] {
        builder
            .insert(
                text,
                &tantivy::postings::TermInfo {
                    doc_freq: searcher
                        .doc_freq(&tantivy::Term::from_field_text(field, text))
                        .unwrap() as u32,
                    ..Default::default()
                },
            )
            .unwrap();
    }
    let dictionary: Arc<[u8]> = builder.finish().unwrap().into();
    let queue = ReadQueue::default();
    let source = HashMap::from_iter([
        ("counts".to_owned(), Arc::from(counts)),
        ("statistics".to_owned(), dictionary.clone()),
    ]);
    let mut requests = Vec::new();
    let dictionary = drive_queued_future(
        PagedTermDictionary::open(FileSlice::new(
            queue.file("statistics".into(), dictionary.len()),
        )),
        &queue,
        &source,
        &mut requests,
    )
    .unwrap();
    let statistics = PagedStatistics {
        dictionary,
        counts: FileSlice::new(queue.file("counts".into(), 16)),
        field,
    };
    let parser = resident.cached_parser.as_ref().unwrap();
    let mut queries: Vec<Box<dyn Query>> = [
        "title:alpha",
        "title:\"alpha beta\"",
        "title:(alpha OR gamma)",
        "title:alpha^2",
        "title:(alpha AND beta)",
        "title:\"alpha beta\"~2",
    ]
    .into_iter()
    .map(|text| parser.parse_query(text).unwrap())
    .collect();
    queries.push(Box::new(ConstScoreQuery::new(
        parser.parse_query("title:alpha").unwrap(),
        3.0,
    )));
    queries.push(Box::new(DisjunctionMaxQuery::with_tie_breaker(
        vec![
            parser.parse_query("title:alpha").unwrap(),
            parser.parse_query("title:gamma").unwrap(),
        ],
        0.4,
    )));
    let scoring = EnableScoring::enabled_from_statistics_provider(&statistics, searcher);
    for query in &queries {
        let expected = query
            .weight(EnableScoring::enabled_from_searcher(searcher))
            .unwrap();
        let before = requests.len();
        let actual =
            drive_queued_future(query.weight_async(scoring), &queue, &source, &mut requests)
                .unwrap();
        assert!(
            requests.len() > before,
            "weight did not read injected statistics"
        );
        for reader in searcher.segment_readers() {
            let mut expected = expected.scorer(reader, 1.0).unwrap();
            let mut actual = actual.scorer(reader, 1.0).unwrap();
            while expected.doc() != tantivy::TERMINATED {
                assert_eq!(actual.doc(), expected.doc());
                assert_eq!(actual.score(), expected.score());
                expected.advance();
                actual.advance();
            }
            assert_eq!(actual.doc(), tantivy::TERMINATED);
        }
        let before = requests.len();
        drive_queued_future(
            query.weight_async(EnableScoring::disabled_from_searcher(searcher)),
            &queue,
            &source,
            &mut requests,
        )
        .unwrap();
        assert_eq!(requests.len(), before, "disabled scoring read statistics");
    }
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    {
        let mut weight = queries[2].weight_async(scoring);
        for _ in 0..2 {
            assert!(weight.as_mut().poll(&mut cx).is_pending());
            let request = queue.pop().unwrap();
            let bytes = tantivy::directory::OwnedBytes::new(source[request.name()].clone())
                .slice(request.range());
            request.complete(Ok(bytes));
        }
        assert!(weight.as_mut().poll(&mut cx).is_pending());
        let request = queue.pop().unwrap();
        request.complete(Err(std::io::Error::other("statistics failure")));
        let std::task::Poll::Ready(Err(error)) = weight.as_mut().poll(&mut cx) else {
            panic!("missing statistics error")
        };
        assert!(error.to_string().contains("statistics failure"));
    }
    {
        let mut weight = queries[2].weight_async(scoring);
        for _ in 0..2 {
            assert!(weight.as_mut().poll(&mut cx).is_pending());
            let request = queue.pop().unwrap();
            let bytes = tantivy::directory::OwnedBytes::new(source[request.name()].clone())
                .slice(request.range());
            request.complete(Ok(bytes));
        }
        assert!(weight.as_mut().poll(&mut cx).is_pending());
    }
    assert!(
        queue.pop().is_none(),
        "cancelled statistics request retained"
    );
    drive_queued_future(
        queries[2].weight_async(scoring),
        &queue,
        &source,
        &mut requests,
    )
    .unwrap();
    let error = drive_queued_future(
        SyncOnlyQuery.weight_async(scoring),
        &queue,
        &source,
        &mut requests,
    )
    .err()
    .unwrap();
    assert!(error
        .to_string()
        .contains("does not support asynchronous weight construction"));
}

struct PagedStatistics {
    dictionary: tantivy::termdict::PagedTermDictionary,
    counts: tantivy::directory::FileSlice,
    field: tantivy::schema::Field,
}

impl tantivy::query::Bm25StatisticsProvider for PagedStatistics {
    fn total_num_tokens(&self, _: tantivy::schema::Field) -> tantivy::Result<u64> {
        panic!("synchronous statistics")
    }
    fn total_num_docs(&self) -> tantivy::Result<u64> {
        panic!("synchronous statistics")
    }
    fn doc_freq(&self, _: &tantivy::Term) -> tantivy::Result<u64> {
        panic!("synchronous statistics")
    }

    fn total_num_tokens_async(
        &self,
        field: tantivy::schema::Field,
    ) -> tantivy::query::StatisticsFuture<'_> {
        assert_eq!(field, self.field);
        Box::pin(async move {
            let bytes = self.counts.slice(..8).read_bytes_async().await?;
            Ok(u64::from_le_bytes(bytes.as_slice().try_into().unwrap()))
        })
    }
    fn total_num_docs_async(&self) -> tantivy::query::StatisticsFuture<'_> {
        Box::pin(async move {
            let bytes = self.counts.slice(8..).read_bytes_async().await?;
            Ok(u64::from_le_bytes(bytes.as_slice().try_into().unwrap()))
        })
    }
    fn doc_freq_async<'a>(
        &'a self,
        term: &'a tantivy::Term,
    ) -> tantivy::query::StatisticsFuture<'a> {
        Box::pin(async move {
            assert_eq!(term.field(), self.field);
            Ok(self
                .dictionary
                .get(term.serialized_value_bytes())
                .await?
                .map_or(0, |info| u64::from(info.doc_freq)))
        })
    }
}

#[derive(Clone, Debug)]
struct SyncOnlyQuery;

impl Query for SyncOnlyQuery {
    fn weight(&self, _: EnableScoring<'_>) -> tantivy::Result<Box<dyn tantivy::query::Weight>> {
        panic!("async search must not call an unsupported synchronous query")
    }
}

#[test]
fn paged_dictionary_reads_resume_through_completions() {
    use tantivy::directory::{FileSlice, ReadQueue};
    use tantivy::termdict::{
        AsyncTermMerger, AsyncTermStreamer, PagedTermDictionary, TermDictionaryBuilder,
    };

    let mut builder = TermDictionaryBuilder::create(Vec::new()).unwrap();
    for i in 0..1_025usize {
        builder
            .insert(
                format!("word-{i:08}"),
                &tantivy::postings::TermInfo {
                    doc_freq: i as u32,
                    postings_range: i * i..(i + 1) * (i + 1),
                    positions_range: i * i * 2..(i + 1) * (i + 1) * 2,
                },
            )
            .unwrap();
    }
    let bytes: Arc<[u8]> = builder.finish().unwrap().into();
    let queue = ReadQueue::default();
    let file = FileSlice::new(queue.file("dictionary".into(), bytes.len()));
    let source = HashMap::from_iter([("dictionary".to_owned(), bytes)]);
    let mut requests = Vec::new();
    let dictionary = drive_queued_future(
        PagedTermDictionary::open(file),
        &queue,
        &source,
        &mut requests,
    )
    .unwrap();
    assert_eq!(
        requests.iter().map(|(_, range)| range.len()).sum::<usize>(),
        64
    );
    for ordinal in [0, 1, 255, 256, 1_024] {
        let key = format!("word-{ordinal:08}");
        let info = drive_queued_future(
            dictionary.get(key.as_bytes()),
            &queue,
            &source,
            &mut requests,
        )
        .unwrap()
        .unwrap();
        assert_eq!(info.doc_freq, ordinal);
    }
    let mut merger = AsyncTermMerger::new(vec![
        AsyncTermStreamer::Paged(dictionary.stream()),
        AsyncTermStreamer::Paged(dictionary.stream()),
    ]);
    let mut ordinal = 0;
    while drive_queued_future(merger.advance(), &queue, &source, &mut requests).unwrap() {
        assert_eq!(merger.key(), format!("word-{ordinal:08}").as_bytes());
        assert_eq!(
            merger.matching_segments().collect::<Vec<_>>(),
            [(0, ordinal), (1, ordinal)]
        );
        for (_, info) in merger.current_segment_ords_and_term_infos() {
            assert_eq!(u64::from(info.doc_freq), ordinal);
        }
        ordinal += 1;
    }
    assert_eq!(ordinal, 1_025);
    assert!(requests.iter().all(|(_, range)| range.len() <= 4_619));
    assert!(queue.pop().is_none());
}

#[test]
fn more_like_this_reads_stored_documents_through_completions() {
    use std::path::Path;
    use std::task::{Context, Poll, Waker};
    use tantivy::directory::{Directory, ReadQueue};
    use tantivy::query::MoreLikeThisQuery;
    use tantivy::schema::{OwnedValue, TantivyDocument, STORED, TEXT};

    let mut schema = tantivy::schema::Schema::builder();
    let field = schema.add_text_field("text", TEXT | STORED);
    let directory = BuildDirectory::default();
    let index = Index::create(directory.clone(), schema.build(), IndexSettings::default()).unwrap();
    let mut writer = index
        .writer_with_num_threads::<TantivyDocument>(1, 50_000_000)
        .unwrap();
    for text in ["alpha alpha beta", "alpha gamma"] {
        writer.add_document(tantivy::doc!(field => text)).unwrap();
    }
    writer.commit().unwrap();
    writer
        .add_document(tantivy::doc!(field => "alpha beta beta"))
        .unwrap();
    writer.commit().unwrap();
    writer.wait_merging_threads().unwrap();
    let expected = index.reader().unwrap().searcher();
    let queue = ReadQueue::default();
    let mut source = HashMap::default();
    let mut handles = HashMap::default();
    for (path, bytes) in directory.captured_files() {
        let name = path.to_str().unwrap().to_owned();
        handles.insert(path, queue.file(name.clone(), bytes.len()));
        source.insert(name, bytes);
    }
    let meta = directory.atomic_read(Path::new("meta.json")).unwrap();
    let queued_index =
        Index::open(SnapshotDirectory::new(HashMap::default(), meta).with_async_files(handles))
            .unwrap();
    let metas = queued_index.searchable_segment_metas().unwrap();
    let mut requests = Vec::new();
    let searcher = drive_queued_future(
        Searcher::open_async(queued_index, metas, 0),
        &queue,
        &source,
        &mut requests,
    )
    .unwrap();
    let address = DocAddress::new(0, 0);
    let builder = MoreLikeThisQuery::builder()
        .with_min_doc_frequency(1)
        .with_min_term_frequency(1);
    let query = builder.clone().with_document(address);
    for cancel in [false, true] {
        let mut future = query.weight_async(EnableScoring::enabled_from_searcher(&searcher));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        let request = queue.pop().unwrap();
        assert!(request.name().ends_with(".store"));
        assert!(future.as_mut().poll(&mut cx).is_pending());
        assert!(queue.pop().is_none());
        if cancel {
            drop(future);
            assert!(request.is_cancelled());
        } else {
            request.complete(Err(std::io::Error::other("stored document failure")));
            let Poll::Ready(Err(error)) = future.as_mut().poll(&mut cx) else {
                panic!("missing store error")
            };
            assert!(error.to_string().contains("stored document failure"));
        }
    }
    let actual: TantivyDocument =
        drive_queued_future(searcher.doc_async(address), &queue, &source, &mut requests).unwrap();
    assert_eq!(actual, expected.doc::<TantivyDocument>(address).unwrap());
    let collector = tantivy::collector::TopDocs::with_limit(10).order_by_score();
    for query in [
        query,
        builder.clone().with_document_fields(vec![(
            field,
            vec![OwnedValue::Str("alpha alpha beta".into())],
        )]),
        builder.with_document_fields(Vec::new()),
    ] {
        let want = expected.search(&query, &collector);
        let got = drive_queued_future(
            searcher.search_async(&query, &collector),
            &queue,
            &source,
            &mut requests,
        );
        match (want, got) {
            (Ok(want), Ok(got)) => {
                assert!(!want.is_empty());
                assert_eq!(got.len(), want.len());
                for ((score, doc), (expected_score, expected_doc)) in got.iter().zip(&want) {
                    assert_eq!(doc, expected_doc);
                    assert!((score - expected_score).abs() < 0.00001);
                }
            }
            (Err(want), Err(got)) => assert_eq!(got.to_string(), want.to_string()),
            results => panic!("more-like-this results differ: {results:?}"),
        }
        requests.clear();
        let error = drive_queued_future(
            query.weight_async(EnableScoring::disabled_from_searcher(&searcher)),
            &queue,
            &source,
            &mut requests,
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("requires to enable scoring"));
        assert!(requests.is_empty());
        assert!(queue.pop().is_none());
    }
}

#[test]
fn regex_phrase_scorers_suspend_for_payloads_and_preserve_scores() {
    use std::task::{Context, Poll, Waker};
    use tantivy::directory::{OwnedBytes, ReadQueue};
    use tantivy::query::RegexPhraseQuery;

    let attachment = test_attachment();
    let texts: Vec<_> = (0..600)
        .map(|i| format!("alpha gap beta alp{i:04} beta"))
        .collect();
    let docs: Vec<_> = texts
        .iter()
        .enumerate()
        .map(|(i, text)| (i as i64, text.as_str()))
        .collect();
    let (segment, _) = build_and_load_segment(&attachment, &docs);
    let mut resident = FtsCursor::new(&attachment);
    resident.segments = vec![segment.clone()];
    resident.ensure_searcher().unwrap();
    let expected = resident.searcher.as_ref().unwrap();
    let queue = ReadQueue::default();
    let mut source = HashMap::default();
    let mut handles = HashMap::default();
    for (name, bytes) in &segment.data.as_ref().unwrap().files {
        source.insert(name.clone(), bytes.clone());
        handles.insert(PathBuf::from(name), queue.file(name.clone(), bytes.len()));
    }
    let scratch = attachment.shared.scratch_index(&attachment.schema).unwrap();
    let meta = synthesize_meta_json(&scratch, &attachment.schema, &[segment]).unwrap();
    let index =
        Index::open(SnapshotDirectory::new(HashMap::default(), meta).with_async_files(handles))
            .unwrap();
    let metas = index.searchable_segment_metas().unwrap();
    let mut requests = Vec::new();
    let searcher = drive_queued_future(
        Searcher::open_async(index, metas, 0),
        &queue,
        &source,
        &mut requests,
    )
    .unwrap();
    let field = attachment.text_fields[0].1;
    let collector = tantivy::collector::TopDocs::with_limit(1_000).order_by_score();
    assert_eq!(
        expected
            .doc_freq(&tantivy::Term::from_field_text(field, "alpha"))
            .unwrap(),
        600
    );
    assert_eq!(
        expected
            .doc_freq(&tantivy::Term::from_field_text(field, "alp0000"))
            .unwrap(),
        1
    );
    for (pattern, slop, limit, count) in [
        ("alp.*", 0, 1000, Some(600)),
        ("alp.*", 1, 1000, Some(600)),
        ("missing", 0, 1000, Some(0)),
        ("alp.*", 0, 600, None),
        ("[", 0, 1000, None),
    ] {
        let mut query = RegexPhraseQuery::new(field, vec![pattern.into(), "beta".into()]);
        query.set_slop(slop);
        query.set_max_expansions(limit);
        let want = expected.search(&query, &collector);
        match count {
            Some(count) => assert_eq!(want.as_ref().unwrap().len(), count),
            None => assert!(want.is_err()),
        }
        let got = drive_queued_future(
            searcher.search_async(&query, &collector),
            &queue,
            &source,
            &mut requests,
        );
        match (want, got) {
            (Ok(want), Ok(got)) => assert_eq!(got, want),
            (Err(want), Err(got)) => assert_eq!(got.to_string(), want.to_string()),
            pair => panic!("regex phrase results differ: {pair:?}"),
        }
        let want = expected.search(&query, &tantivy::collector::Count);
        let got = drive_queued_future(
            searcher.search_async(&query, &tantivy::collector::Count),
            &queue,
            &source,
            &mut requests,
        );
        assert_eq!(
            got.map_err(|e| e.to_string()),
            want.map_err(|e| e.to_string())
        );
    }

    let query = RegexPhraseQuery::new(field, vec!["alp.*".into(), "beta".into()]);
    let weight = drive_queued_future(
        query.weight_async(EnableScoring::enabled_from_searcher(&searcher)),
        &queue,
        &source,
        &mut requests,
    )
    .unwrap();
    let reader = searcher.segment_reader(0);
    for cancel in [false, true] {
        let mut future = weight.scorer_async(reader, 2.0);
        let mut cx = Context::from_waker(Waker::noop());
        let mut position_reads = 0;
        loop {
            assert!(future.as_mut().poll(&mut cx).is_pending());
            let request = queue.pop().unwrap();
            assert!(future.as_mut().poll(&mut cx).is_pending());
            assert!(queue.pop().is_none());
            position_reads += usize::from(request.name().ends_with(".pos"));
            if position_reads == 2 {
                if cancel {
                    drop(future);
                    assert!(request.is_cancelled());
                } else {
                    request.complete(Err(std::io::Error::other("regex payload failure")));
                    let Poll::Ready(Err(error)) = future.as_mut().poll(&mut cx) else {
                        panic!("missing read error")
                    };
                    assert!(error.to_string().contains("regex payload failure"));
                }
                break;
            }
            let bytes = OwnedBytes::new(source[request.name()].clone()).slice(request.range());
            request.complete(Ok(bytes));
        }
        let mut got = drive_queued_future(
            weight.scorer_async(reader, 2.0),
            &queue,
            &source,
            &mut requests,
        )
        .unwrap();
        let expected_weight = query
            .weight(EnableScoring::enabled_from_searcher(expected))
            .unwrap();
        let mut want = expected_weight
            .scorer(expected.segment_reader(0), 2.0)
            .unwrap();
        while want.doc() != tantivy::TERMINATED {
            assert_eq!(got.doc(), want.doc());
            assert_eq!(got.score(), want.score());
            got.advance();
            want.advance();
        }
        assert_eq!(got.doc(), tantivy::TERMINATED);
        assert!(queue.pop().is_none());
    }
}

#[test]
fn async_snapshot_reads_only_requested_ranges_and_matches_resident_queries() {
    use tantivy::directory::{OwnedBytes, ReadQueue};
    let attachment = test_attachment();
    let long = "alpha beta ".repeat(6000);
    let (first, _) = build_and_load_segment(&attachment, &[(1, &long), (2, "alpha gamma beta")]);
    let (mut second, _) =
        build_and_load_segment(&attachment, &[(3, "alpha beta"), (4, "alpha beta deleted")]);
    second.deleted.insert(1);
    let segments = vec![first, second];
    let mut resident = FtsCursor::new(&attachment);
    resident.segments = segments.clone();
    resident.ensure_searcher().unwrap();
    let expected_searcher = resident.searcher.as_ref().unwrap();
    let queue = ReadQueue::default();
    let mut source = HashMap::default();
    let mut handles = HashMap::default();
    let mut files = HashMap::default();
    for segment in &segments {
        for (name, bytes) in &segment.data.as_ref().unwrap().files {
            source.insert(name.clone(), Arc::clone(bytes));
            handles.insert(PathBuf::from(name), queue.file(name.clone(), bytes.len()));
        }
        if !segment.deleted.is_empty() {
            files.insert(
                PathBuf::from(tombstone_del_file_name(&segment.id())),
                Arc::from(
                    with_tantivy_footer(alive_bitset_bytes(
                        segment.descriptor.max_doc,
                        &segment.deleted,
                    ))
                    .unwrap(),
                ),
            );
        }
    }
    let scratch = attachment.shared.scratch_index(&attachment.schema).unwrap();
    let meta = synthesize_meta_json(&scratch, &attachment.schema, &segments).unwrap();
    let index = Index::open(SnapshotDirectory::new(files, meta).with_async_files(handles)).unwrap();
    resident.register_tokenizers(&index);
    let metas = index.searchable_segment_metas().unwrap();
    let mut requests = Vec::new();
    let searcher = drive_queued_future(
        Searcher::open_async(index, metas, 0),
        &queue,
        &source,
        &mut requests,
    )
    .unwrap();
    let position_bytes: usize = source
        .iter()
        .filter(|(name, _)| name.ends_with(".pos"))
        .map(|(_, bytes)| bytes.len())
        .sum();
    let positions_read: usize = requests
        .iter()
        .filter(|(name, _)| name.ends_with(".pos"))
        .map(|(_, range)| range.len())
        .sum();
    assert!(
        positions_read * 4 < position_bytes,
        "opening read position payloads: {positions_read}/{position_bytes}"
    );

    for text in [
        "alpha",
        "\"alpha beta\"",
        "alpha -gamma",
        "alpha OR gamma",
        "alpha^2",
        "\"alpha be\"*",
        "title:[alpha TO gamma]",
        "title: IN [alpha gamma]",
        "rowid:[1 TO 3]",
        "*",
    ] {
        let query = resident
            .cached_parser
            .as_ref()
            .unwrap()
            .parse_query(text)
            .unwrap();
        let collector = tantivy::collector::TopDocs::with_limit(10).order_by_score();
        let expected = expected_searcher
            .search(query.as_ref(), &collector)
            .unwrap();
        let actual = drive_queued_future(
            searcher.search_async(query.as_ref(), &collector),
            &queue,
            &source,
            &mut requests,
        )
        .unwrap();
        assert_eq!(actual.len(), expected.len(), "{text}");
        for ((score, address), (expected_score, expected_address)) in actual.iter().zip(&expected) {
            assert_eq!(address, expected_address, "{text}");
            assert!(
                (score - expected_score).abs() < 0.00001,
                "{text}: {score} != {expected_score}"
            );
        }
    }
    {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        use tantivy::query::{InvertedIndexRangeWeight, PhrasePrefixQuery, Weight};
        let field = attachment.text_fields[0].1;
        let term = |text| tantivy::Term::from_field_text(field, text);
        for (lower, upper) in [
            (Unbounded, Unbounded),
            (Included(term("alpha")), Included(term("gamma"))),
            (Excluded(term("alpha")), Excluded(term("gamma"))),
            (Included(term("absent")), Excluded(term("alpha"))),
        ] {
            for limit in [None, Some(0), Some(1), Some(2)] {
                let weight = InvertedIndexRangeWeight::new(field, &lower, &upper, limit);
                requests.clear();
                let mut actual = drive_queued_future(
                    weight.scorer_async(searcher.segment_reader(0), 2.0),
                    &queue,
                    &source,
                    &mut requests,
                )
                .unwrap();
                let mut expected = weight
                    .scorer(expected_searcher.segment_reader(0), 2.0)
                    .unwrap();
                while expected.doc() != tantivy::TERMINATED {
                    assert_eq!(actual.doc(), expected.doc());
                    assert_eq!(actual.score(), expected.score());
                    actual.advance();
                    expected.advance();
                }
                assert_eq!(actual.doc(), tantivy::TERMINATED);
                if let Some(limit) = limit {
                    assert!(
                        requests
                            .iter()
                            .filter(|(name, _)| name.ends_with(".idx"))
                            .count() as u64
                            <= limit,
                        "range read postings beyond its expansion limit"
                    );
                }
            }
        }
        for prefix in ["", "b", "missing"] {
            for limit in [0, 1, 2, 10] {
                let mut query = PhrasePrefixQuery::new(vec![term("alpha"), term(prefix)]);
                query.set_max_expansions(limit);
                let collector = tantivy::collector::TopDocs::with_limit(10).order_by_score();
                let expected = expected_searcher.search(&query, &collector).unwrap();
                let actual = drive_queued_future(
                    searcher.search_async(&query, &collector),
                    &queue,
                    &source,
                    &mut requests,
                )
                .unwrap();
                assert_eq!(actual, expected, "prefix={prefix:?}, limit={limit}");
                let count = drive_queued_future(
                    searcher.search_async(&query, &tantivy::collector::Count),
                    &queue,
                    &source,
                    &mut requests,
                )
                .unwrap();
                assert_eq!(count, expected.len());
            }
        }
        for pattern in ["a.*", ".*", "missing"] {
            let query = tantivy::query::RegexQuery::from_pattern(pattern, field).unwrap();
            let collector = tantivy::collector::TopDocs::with_limit(10).order_by_score();
            let expected = expected_searcher.search(&query, &collector).unwrap();
            let actual = drive_queued_future(
                searcher.search_async(&query, &collector),
                &queue,
                &source,
                &mut requests,
            )
            .unwrap();
            assert_eq!(actual, expected, "pattern={pattern:?}");
        }
    }
    let column = drive_queued_future(
        searcher
            .segment_reader(1)
            .fast_fields()
            .column_opt_async::<i64>(ROWID_FIELD),
        &queue,
        &source,
        &mut requests,
    )
    .unwrap()
    .unwrap();
    assert_eq!(column.first(0), Some(3));

    for limit in [1, usize::MAX] {
        requests.clear();
        let query = resident
            .cached_parser
            .as_ref()
            .unwrap()
            .parse_query("alpha")
            .unwrap();
        let expected_scores = expected_searcher
            .search(
                query.as_ref(),
                &tantivy::collector::TopDocs::with_limit(10).order_by_score(),
            )
            .unwrap();
        let result = drive_queued_future(
            run_async_query(searcher.clone(), query, limit, Some(true)),
            &queue,
            &source,
            &mut requests,
        )
        .unwrap();
        let FtsQueryResult::Streaming(mut stream) = result else {
            panic!("expected stream")
        };
        assert_eq!(stream.rowids.as_ref().unwrap().0, 0);
        assert!(
            requests
                .iter()
                .all(|(name, _)| !name.starts_with(&segments[1].id().uuid_string())),
            "first hit must not load the next segment's scorer or rowids"
        );
        let mut rowids = Vec::new();
        while let Some((score, rowid)) = stream.current {
            let address = match rowid {
                1 => DocAddress::new(0, 0),
                2 => DocAddress::new(0, 1),
                3 => DocAddress::new(1, 0),
                _ => panic!("unexpected rowid {rowid}"),
            };
            let expected = expected_scores
                .iter()
                .find(|(_, doc)| *doc == address)
                .unwrap()
                .0;
            assert!(
                (score - expected).abs() < 0.00001,
                "global BM25 changed at {rowid}"
            );
            rowids.push(rowid);
            drive_queued_future(stream.advance(), &queue, &source, &mut requests).unwrap();
        }
        assert!(
            stream.hits.is_none() && stream.rowids.is_none(),
            "exhausted stream must release payloads"
        );
        if limit == 1 {
            assert_eq!(rowids, [1]);
            assert!(requests
                .iter()
                .all(|(name, _)| !name.starts_with(&segments[1].id().uuid_string())));
        } else {
            assert_eq!(rowids, [1, 2, 3]);
        }
    }

    let inputs: Vec<_> = searcher
        .index()
        .searchable_segment_metas()
        .unwrap()
        .into_iter()
        .map(|meta| searcher.index().segment(meta))
        .collect();
    let directory = BuildDirectory::default();
    let merged = drive_queued_future(
        tantivy::indexer::merge_filtered_segments_async(
            &inputs,
            IndexSettings::default(),
            vec![None; inputs.len()],
            directory,
        ),
        &queue,
        &source,
        &mut requests,
    )
    .unwrap();
    let merged_reader: IndexReader = merged.reader().unwrap();
    let merged_searcher = merged_reader.searcher();
    assert_eq!(merged_searcher.num_docs(), 3);
    for text in ["alpha", "\"alpha beta\"", "alpha -gamma", "rowid:[1 TO 3]"] {
        let query = resident
            .cached_parser
            .as_ref()
            .unwrap()
            .parse_query(text)
            .unwrap();
        assert_eq!(
            merged_searcher
                .search(query.as_ref(), &tantivy::collector::Count)
                .unwrap(),
            expected_searcher
                .search(query.as_ref(), &tantivy::collector::Count)
                .unwrap(),
            "merged: {text}",
        );
    }

    let handle = queue.file("error".into(), 4);
    let mut future = handle.read_bytes_async(0..4);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(future.as_mut().poll(&mut cx).is_pending());
    let request = queue.pop().unwrap();
    request.complete(Err(std::io::Error::other("injected failure")));
    assert!(
        matches!(future.as_mut().poll(&mut cx), std::task::Poll::Ready(Err(error)) if error.to_string() == "injected failure")
    );
    let mut abandoned = handle.read_bytes_async(0..4);
    assert!(abandoned.as_mut().poll(&mut cx).is_pending());
    let request = queue.pop().unwrap();
    drop(abandoned);
    assert!(request.is_cancelled());
    request.complete(Ok(OwnedBytes::new(vec![0; 4])));
}

#[test]
fn queued_file_validates_ranges_short_reads_and_dropped_requests() {
    use std::task::{Context, Poll, Waker};
    use tantivy::directory::{OwnedBytes, ReadQueue};
    let queue = ReadQueue::default();
    let file = queue.file("file".into(), 4);
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(
        file.read_bytes(0..1).unwrap_err().kind(),
        std::io::ErrorKind::Unsupported
    );
    for range in [0..5, std::ops::Range { start: 3, end: 2 }] {
        let mut read = file.read_bytes_async(range);
        assert!(
            matches!(read.as_mut().poll(&mut cx), Poll::Ready(Err(error))
            if error.kind() == std::io::ErrorKind::InvalidInput)
        );
        assert!(queue.pop().is_none());
    }
    let mut empty = file.read_bytes_async(4..4);
    assert!(matches!(empty.as_mut().poll(&mut cx), Poll::Ready(Ok(bytes)) if bytes.is_empty()));
    assert!(queue.pop().is_none());
    let mut short = file.read_bytes_async(0..4);
    assert!(short.as_mut().poll(&mut cx).is_pending());
    queue
        .pop()
        .unwrap()
        .complete(Ok(OwnedBytes::new(vec![0; 3])));
    assert!(
        matches!(short.as_mut().poll(&mut cx), Poll::Ready(Err(error))
        if error.kind() == std::io::ErrorKind::UnexpectedEof)
    );
    let mut cancelled = file.read_bytes_async(0..4);
    assert!(cancelled.as_mut().poll(&mut cx).is_pending());
    drop(queue.pop().unwrap());
    assert!(
        matches!(cancelled.as_mut().poll(&mut cx), Poll::Ready(Err(error))
        if error.kind() == std::io::ErrorKind::Interrupted)
    );
}

#[test]
fn managed_directory_awaits_file_lookup_before_reading_footer() {
    use std::path::Path;
    use tantivy::directory::{Directory, ManagedDirectory, ReadQueue};
    let queue = ReadQueue::default();
    let mut files = HashMap::default();
    files.insert(
        PathBuf::from("segment"),
        Arc::from(with_tantivy_footer(vec![1, 2, 3]).unwrap()),
    );
    let directory = DelayedOpenDirectory {
        inner: SnapshotDirectory::new(files, Vec::new()),
        gate: queue.file("lookup".into(), 1),
    };
    let boxed: Box<dyn Directory> = Box::new(directory);
    let mut source = HashMap::default();
    source.insert("lookup".into(), Arc::<[u8]>::from(vec![0]));
    let mut requests = Vec::new();
    let managed: Box<dyn Directory> = Box::new(
        drive_queued_future(
            ManagedDirectory::wrap_async(boxed),
            &queue,
            &source,
            &mut requests,
        )
        .unwrap(),
    );
    assert_eq!(requests.len(), 1);
    requests.clear();
    let file = drive_queued_future(
        managed.open_read_async(Path::new("segment")),
        &queue,
        &source,
        &mut requests,
    )
    .unwrap();
    assert_eq!(requests, [("lookup".into(), 0..1)]);
    assert_eq!(&*file.read_bytes().unwrap(), &[1, 2, 3]);

    let mut opening = managed.open_read_async(Path::new("segment"));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(opening.as_mut().poll(&mut cx).is_pending());
    queue
        .pop()
        .unwrap()
        .complete(Err(std::io::Error::other("lookup failure")));
    assert!(
        matches!(opening.as_mut().poll(&mut cx), std::task::Poll::Ready(Err(error))
        if error.to_string().contains("lookup failure"))
    );

    let mut cancelled = managed.open_read_async(Path::new("segment"));
    assert!(cancelled.as_mut().poll(&mut cx).is_pending());
    let request = queue.pop().unwrap();
    drop(cancelled);
    assert!(request.is_cancelled());
}

#[test]
fn index_open_and_reload_await_metadata_reads() {
    use tantivy::directory::ReadQueue;
    let attachment = test_attachment();
    let scratch = attachment.shared.scratch_index(&attachment.schema).unwrap();
    let meta = synthesize_meta_json(&scratch, &attachment.schema, &[]).unwrap();
    let queue = ReadQueue::default();
    let directory = DelayedOpenDirectory {
        inner: SnapshotDirectory::new(HashMap::default(), meta),
        gate: queue.file("lookup".into(), 1),
    };
    let mut source = HashMap::default();
    source.insert("lookup".into(), Arc::<[u8]>::from(vec![0]));
    let mut requests = Vec::new();
    let index = drive_queued_future(
        Index::open_async(directory.clone()),
        &queue,
        &source,
        &mut requests,
    )
    .unwrap();
    assert_eq!(index.schema(), attachment.schema);
    assert_eq!(
        requests.len(),
        2,
        "management and index metadata must both await"
    );
    let metas =
        drive_queued_future(index.load_metas_async(), &queue, &source, &mut requests).unwrap();
    assert!(metas.segments.is_empty());
    assert_eq!(requests.len(), 3);
    let mut corrupt = directory;
    corrupt.inner = SnapshotDirectory::new(HashMap::default(), b"not json".to_vec());
    assert!(
        drive_queued_future(Index::open_async(corrupt), &queue, &source, &mut requests).is_err()
    );
}

#[derive(Clone, Debug)]
struct DelayedOpenDirectory {
    inner: SnapshotDirectory,
    gate: Arc<dyn tantivy::directory::FileHandle>,
}

impl tantivy::Directory for DelayedOpenDirectory {
    fn get_file_handle(
        &self,
        _: &std::path::Path,
    ) -> std::result::Result<
        Arc<dyn tantivy::directory::FileHandle>,
        tantivy::directory::error::OpenReadError,
    > {
        panic!("async open must not call synchronous lookup")
    }

    fn get_file_handle_async<'a>(
        &'a self,
        path: &'a std::path::Path,
    ) -> tantivy::directory::DirectoryFuture<
        'a,
        std::result::Result<
            Arc<dyn tantivy::directory::FileHandle>,
            tantivy::directory::error::OpenReadError,
        >,
    > {
        Box::pin(async move {
            self.gate.read_bytes_async(0..1).await.map_err(|error| {
                tantivy::directory::error::OpenReadError::wrap_io_error(error, path.to_path_buf())
            })?;
            self.inner.get_file_handle(path)
        })
    }

    fn delete(
        &self,
        path: &std::path::Path,
    ) -> std::result::Result<(), tantivy::directory::error::DeleteError> {
        self.inner.delete(path)
    }
    fn exists(
        &self,
        path: &std::path::Path,
    ) -> std::result::Result<bool, tantivy::directory::error::OpenReadError> {
        self.inner.exists(path)
    }
    fn open_write(
        &self,
        path: &std::path::Path,
    ) -> std::result::Result<tantivy::directory::WritePtr, tantivy::directory::error::OpenWriteError>
    {
        self.inner.open_write(path)
    }
    fn atomic_read(
        &self,
        _: &std::path::Path,
    ) -> std::result::Result<Vec<u8>, tantivy::directory::error::OpenReadError> {
        panic!("async metadata reads must not use synchronous storage")
    }
    fn atomic_read_async<'a>(
        &'a self,
        path: &'a std::path::Path,
    ) -> tantivy::directory::DirectoryFuture<
        'a,
        std::result::Result<Vec<u8>, tantivy::directory::error::OpenReadError>,
    > {
        Box::pin(async move {
            self.gate.read_bytes_async(0..1).await.map_err(|error| {
                tantivy::directory::error::OpenReadError::wrap_io_error(error, path.to_path_buf())
            })?;
            self.inner.atomic_read(path)
        })
    }
    fn atomic_write(&self, path: &std::path::Path, data: &[u8]) -> std::io::Result<()> {
        self.inner.atomic_write(path, data)
    }
    fn sync_directory(&self) -> std::io::Result<()> {
        self.inner.sync_directory()
    }
    fn watch(
        &self,
        callback: tantivy::directory::WatchCallback,
    ) -> tantivy::Result<tantivy::directory::WatchHandle> {
        self.inner.watch(callback)
    }
}

fn drive_queued_future<T>(
    future: impl std::future::Future<Output = T>,
    queue: &tantivy::directory::ReadQueue,
    source: &HashMap<String, Arc<[u8]>>,
    requests: &mut Vec<(String, std::ops::Range<usize>)>,
) -> T {
    use tantivy::directory::OwnedBytes;
    let mut future = std::pin::pin!(future);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    loop {
        if let std::task::Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
        let request = queue.pop().expect("future must wait on injected I/O");
        for _ in 0..3 {
            assert!(future.as_mut().poll(&mut cx).is_pending());
            assert!(queue.pop().is_none(), "pending read was submitted twice");
        }
        requests.push((request.name().into(), request.range()));
        let bytes = OwnedBytes::new(Arc::clone(&source[request.name()])).slice(request.range());
        let response = Mutex::new(Some((request, bytes)));
        let completion = crate::Completion::new_write(move |_| {
            let (request, bytes) = response.lock().take().expect("completion called once");
            request.complete(Ok(bytes));
        });
        assert!(!completion.finished());
        completion.complete(0);
        assert!(completion.succeeded());
    }
}
