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
fn chunk_assembly_rejects_stray_chunk_numbers_without_panicking() {
    let path = std::path::Path::new("x.term");
    let mut chunks: HashMap<i64, Vec<u8>> = HashMap::default();
    chunks.insert(0, vec![1, 2, 3]);
    chunks.insert(1, vec![4, 5]);
    assert_eq!(
        &*assemble_chunks(path, chunks.clone()).unwrap(),
        &[1, 2, 3, 4, 5]
    );

    // A negative chunk number next to valid ones: it is counted but never
    // written, so assembly must error rather than hand out uninitialized
    // bytes or trip an assert.
    chunks.insert(-1, vec![9]);
    assert!(matches!(
        assemble_chunks(path, chunks.clone()),
        Err(LimboError::Corrupt(_))
    ));

    // A hole is reported as the missing chunk.
    chunks.remove(&-1);
    chunks.remove(&0);
    assert!(matches!(
        assemble_chunks(path, chunks),
        Err(LimboError::Corrupt(_))
    ));
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
fn tombstoned_docs_are_invisible_at_the_reader_level() {
    let attachment = test_attachment();
    let (mut segment, _) = build_and_load_segment(
        &attachment,
        &[(1, "hello turso"), (2, "hello world"), (3, "goodbye")],
    );

    let mut cursor = FtsCursor::new(&attachment);
    cursor.segments = vec![segment.clone()];
    cursor.snapshot_loaded = true;
    let postings = cursor.live_postings_for_rowid(2).unwrap();
    assert_eq!(postings.len(), 1);
    let (segment_id, doc_id) = postings[0];
    assert_eq!(segment_id, segment.id());

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
    assert!(cursor.live_postings_for_rowid(2).unwrap().is_empty());
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
