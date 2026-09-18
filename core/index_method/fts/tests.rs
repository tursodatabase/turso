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

#[test]
fn field_weights_reject_non_finite_and_non_positive_values() {
    let mut accepted = Vec::new();
    for weight in [
        "NaN",
        "nan",
        "+NaN",
        "-NaN",
        "inf",
        "+inf",
        "-inf",
        "Infinity",
        "+INFINITY",
        "-infinity",
        "1e39",
        "-1e39",
        "0",
        "-0",
        "-1",
        "1e-46",
    ] {
        let result = FtsIndexAttachment::new(IndexMethodConfiguration {
            table_name: "w".to_string(),
            index_name: "wx".to_string(),
            columns: vec![IndexColumn::new("title", 0), IndexColumn::new("body", 1)],
            parameters: FxHashMap::from_iter([(
                "weights".to_string(),
                Value::from_text(format!("title={weight},body=1")),
            )]),
        });
        match result {
            Ok(_) => accepted.push(weight),
            Err(error) => assert!(
                matches!(error, LimboError::ParseError(_)),
                "{weight}: {error}"
            ),
        }
    }
    assert!(
        accepted.is_empty(),
        "accepted invalid weights: {accepted:?}"
    );
}

#[test]
fn field_weights_accept_finite_positive_boundaries() {
    let columns = [IndexColumn::new("title", 0), IndexColumn::new("body", 1)];
    for (input, expected) in [
        ("1e-45", f32::from_bits(1)),
        ("1.17549435e-38", f32::MIN_POSITIVE),
        ("0.5", 0.5),
        ("+1", 1.0),
        ("3.4028235e38", f32::MAX),
    ] {
        let weights = parse_field_weights(&format!("title={input},body=2"), &columns).unwrap();
        assert_eq!(weights["title"], expected, "{input}");
        assert_eq!(weights["body"], 2.0);
    }
}

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
    let (rekeyed, _) = segment_rows_from_files(
        minted,
        segment.descriptor.max_doc,
        renamed,
        segment.data.identities.clone(),
    )
    .unwrap();
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
fn rowid_lookup_uses_current_deletes_with_reordered_segments() {
    let attachment = test_attachment();
    let (first, _) = build_and_load_segment(&attachment, &[(7, "first"), (9, "other")]);
    let (second, _) = build_and_load_segment(&attachment, &[(11, "other"), (7, "second")]);
    let first_id = first.id();
    let second_id = second.id();
    let mut cursor = FtsCursor::new(&attachment);
    cursor.segments = vec![first, second];
    cursor.snapshot_loaded = true;
    cursor.ensure_searcher().unwrap();
    cursor.segments.reverse();
    let hits: HashSet<_> = cursor
        .live_postings_for_rowid(7)
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(hits, HashSet::from_iter([(first_id, 0), (second_id, 1)]));
    cursor.segments[0].deleted.insert(1);
    assert_eq!(
        cursor.live_postings_for_rowid(7).unwrap(),
        vec![(first_id, 0)]
    );
    assert!(cursor.live_postings_for_rowid(99).unwrap().is_empty());
}

fn identities_of(segment: &LoadedSegment) -> Vec<DocumentIdentity> {
    (0..segment.descriptor.max_doc)
        .map(|position| segment.data.identities.identity_of(position).unwrap())
        .collect()
}

#[test]
fn segment_load_reads_the_identities_the_build_wrote() {
    let attachment = test_attachment();
    let (segment, _) = build_and_load_segment(
        &attachment,
        &[(1, "hello turso"), (2, "hello world"), (3, "goodbye")],
    );
    let written = identities_of(&segment);
    assert_eq!(written.len(), 3);
    assert!(written
        .iter()
        .all(|identity| identity.raw() > u128::from(u64::MAX)));
    assert!(
        written.windows(2).all(|pair| pair[0] != pair[1]),
        "every document gets its own identity"
    );

    // A segment loaded from storage reads its identities from the fast
    // field. A merged segment and every cache miss do the same.
    let files: HashMap<PathBuf, Arc<[u8]>> = segment
        .data
        .files
        .iter()
        .map(|(name, bytes)| (PathBuf::from(name), Arc::clone(bytes)))
        .collect();
    let scratch = attachment.shared.scratch_index(&attachment.schema).unwrap();
    let read_back = read_segment_identities(
        &scratch,
        &attachment.schema,
        segment.id(),
        segment.descriptor.max_doc,
        files,
    )
    .unwrap();
    assert_eq!(read_back, segment.data.identities);

    let mut cursor = FtsCursor::new(&attachment);
    cursor.segments = vec![segment];
    cursor.ensure_searcher().unwrap();
    let reader = &cursor.searcher.as_ref().unwrap().segment_readers()[0];
    let hi = reader.fast_fields().u64(IDENTITY_HI_FIELD).unwrap();
    let lo = reader.fast_fields().u64(IDENTITY_LO_FIELD).unwrap();
    for (position, identity) in written.iter().enumerate() {
        assert_eq!(
            hi.first(position as u32),
            Some((identity.raw() >> 64) as u64)
        );
        assert_eq!(lo.first(position as u32), Some(identity.raw() as u64));
    }

    let (other, _) = build_and_load_segment(&attachment, &[(4, "other")]);
    assert!(
        !written.contains(&other.data.identities.identity_of(0).unwrap()),
        "identities of different builds must not collide"
    );
}

#[test]
fn segment_load_rejects_the_old_identity_field() {
    let attachment = test_attachment();
    let mut schema = Schema::builder();
    let rowid = schema.add_i64_field(
        ROWID_FIELD,
        tantivy::schema::INDEXED | tantivy::schema::FAST,
    );
    let identity = schema.add_u64_field("doc_identity", tantivy::schema::FAST);
    let directory = BuildDirectory::default();
    let index = Index::create(directory.clone(), schema.build(), IndexSettings::default()).unwrap();
    let id = SegmentId::generate_random();
    let mut writer = SegmentWriter::for_segment(
        DEFAULT_MEMORY_BUDGET_BYTES,
        index.segment(index.new_segment_meta(id, 0)),
    )
    .unwrap();
    let mut document = TantivyDocument::default();
    document.add_i64(rowid, 7);
    document.add_u64(identity, 902);
    writer
        .add_document(AddOperation {
            opstamp: 0,
            document,
        })
        .unwrap();
    writer.finalize().unwrap();

    let scratch = attachment.shared.scratch_index(&attachment.schema).unwrap();
    let error = read_segment_identities(
        &scratch,
        &attachment.schema,
        id,
        1,
        directory.captured_files(),
    )
    .unwrap_err();
    assert!(matches!(&error, LimboError::Corrupt(_)));
    assert!(error.to_string().contains("rebuild the index"), "{error}");
}

#[test]
fn merge_keeps_document_identities_and_retires_only_dropped_tombstones() {
    let attachment = test_attachment();
    let (mut first, _) = build_and_load_segment(&attachment, &[(1, "alpha one"), (2, "alpha two")]);
    let (mut second, _) =
        build_and_load_segment(&attachment, &[(3, "alpha three"), (4, "alpha four")]);
    let first_ids = identities_of(&first);
    let second_ids = identities_of(&second);

    // Delete rowids 2 and 3: one document in each input segment.
    first.deleted.insert(1);
    second.deleted.insert(0);
    let dropped = [first_ids[1], second_ids[0]];
    let kept = [first_ids[0], second_ids[1]];

    let mut cursor = FtsCursor::new(&attachment);
    cursor.segments = vec![first.clone(), second.clone()];
    cursor.snapshot_loaded = true;
    let candidates: HashSet<SegmentId> = [first.id(), second.id()].into_iter().collect();
    cursor.stage_merge_of_segments(&candidates).unwrap();
    let publish = cursor.publish.take().expect("merge staged a publication");

    let PublishApply::ReplaceSegments(merged) = publish.apply else {
        panic!("a merge replaces the visible segment set");
    };
    assert_eq!(merged.len(), 1);
    let merged = merged.into_iter().next().unwrap();
    assert_eq!(merged.descriptor.max_doc, 2);
    assert_eq!(
        identities_of(&merged).into_iter().collect::<BTreeSet<_>>(),
        kept.into_iter().collect::<BTreeSet<_>>(),
        "surviving documents keep the identity they were indexed with"
    );

    // The rowid of each survivor still maps to its original identity.
    cursor.segments = vec![merged.clone()];
    cursor.invalidate_snapshot_view();
    for (rowid, identity) in [(1, kept[0]), (4, kept[1])] {
        let postings = cursor.live_postings_for_rowid(rowid).unwrap();
        assert_eq!(postings.len(), 1);
        assert_eq!(
            merged.data.identities.identity_of(postings[0].1),
            Some(identity)
        );
    }
    // A tombstone written against an input segment still hides the
    // document in the merged one.
    assert_eq!(
        merged
            .data
            .identities
            .tombstoned_positions(&HashSet::from_iter([kept[1]]))
            .len(),
        1
    );

    // The merge deletes only the tombstone rows of the dropped documents.
    // It also deletes the chunk rows of both inputs. The registry rows are
    // not its job: the claim that runs before it deleted them already.
    let targets = publish
        .deleter
        .expect("merge retires rows")
        .targets()
        .to_vec();
    let tombstone_targets: Vec<&PathTarget> = targets
        .iter()
        .filter(|target| matches!(target, PathTarget::Exact(path) if path.starts_with(FTS2_TOMB_PREFIX)))
        .collect();
    assert_eq!(
        tombstone_targets,
        dropped
            .iter()
            .map(|identity| PathTarget::Exact(document_tombstone_path(*identity)))
            .collect::<Vec<_>>()
            .iter()
            .collect::<Vec<_>>()
    );
    for input in [&first, &second] {
        assert!(!targets.contains(&PathTarget::Exact(segment_registry_path(&input.id()))));
        assert!(targets.contains(&PathTarget::Prefix(segment_chunk_prefix(&input.id()))));
    }
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
        Arc::new(SegmentData::new(files, SegmentIdentities::new(Vec::new())))
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
fn query_rowid_readers_follow_cached_searcher_order_and_snapshot() {
    let attachment = test_attachment();
    let (first, _) = build_and_load_segment(
        &attachment,
        &[(91, "alpha alpha"), (-7, "alpha beta gamma")],
    );
    let (second, _) =
        build_and_load_segment(&attachment, &[(400, "beta alpha"), (13, "beta delta")]);
    let mut original = FtsCursor::new(&attachment);
    original.segments = vec![first.clone(), second.clone()];
    original.ensure_searcher().unwrap();

    let mut cached = FtsCursor::new(&attachment);
    cached.segments = vec![second, first];
    cached.ensure_searcher().unwrap();
    assert!(Arc::ptr_eq(&original.rowid_readers, &cached.rowid_readers));

    for cursor in [&mut original, &mut cached] {
        for pattern in [FTS_PATTERN_MATCH, FTS_PATTERN_COMBINED] {
            let mut hits = query_hits(cursor, pattern, "alpha", -1);
            hits.sort_by_key(|hit| hit.0);
            assert_eq!(
                hits.iter().map(|hit| hit.0).collect::<Vec<_>>(),
                [-7, 91, 400]
            );
            if pattern == FTS_PATTERN_COMBINED {
                assert!(hits.iter().all(|hit| hit.1 != Value::from_f64(0.0)));
            }
        }
        for query in ["\"alpha beta\"", "alpha AND gamma"] {
            assert_eq!(
                query_hits(cursor, FTS_PATTERN_MATCH, query, -1),
                vec![(-7, Value::from_i64(1))]
            );
        }
        assert!(query_hits(cursor, FTS_PATTERN_MATCH_LIMIT, "alpha", 0).is_empty());
        assert_eq!(
            query_hits(cursor, FTS_PATTERN_MATCH_LIMIT, "alpha", 1).len(),
            1
        );
    }

    let ranked = query_hits(&mut original, FTS_PATTERN_COMBINED_ORDERED, "alpha", -1);
    assert_eq!(
        ranked.iter().map(|hit| hit.0).collect::<Vec<_>>(),
        [91, 400, -7]
    );
    assert_eq!(
        query_hits(&mut cached, FTS_PATTERN_COMBINED_ORDERED_LIMIT, "alpha", 2),
        ranked[..2]
    );

    cached.segments[1].deleted.insert(0);
    cached.invalidate_snapshot_view();
    assert!(cached.rowid_readers.is_empty());
    let deleted = query_hits(&mut cached, FTS_PATTERN_COMBINED_ORDERED_LIMIT, "alpha", 1);
    assert_eq!(deleted[0].0, 400);
    assert!(!Arc::ptr_eq(&original.rowid_readers, &cached.rowid_readers));
    assert_eq!(
        query_hits(&mut original, FTS_PATTERN_COMBINED_ORDERED, "alpha", -1),
        ranked
    );

    let (replacement, _) = build_and_load_segment(&attachment, &[(999, "alpha")]);
    cached.segments = vec![replacement];
    cached.invalidate_snapshot_view();
    cached.build_snapshot_view(false).unwrap();
    assert_eq!(
        query_hits(&mut cached, FTS_PATTERN_MATCH, "alpha", -1),
        vec![(999, Value::from_i64(1))]
    );
}

fn query_hits(cursor: &mut FtsCursor, pattern: i64, query: &str, limit: i64) -> Vec<(i64, Value)> {
    let values = [
        Register::Value(Value::from_i64(pattern)),
        Register::Value(Value::from_text(query.to_owned())),
        Register::Value(Value::from_i64(limit)),
    ];
    let mut hits = Vec::new();
    let mut next = cursor.query_start(&values).unwrap();
    while let IOResult::Done(true) = next {
        let IOResult::Done(Some(rowid)) = cursor.query_rowid().unwrap() else {
            panic!("query must have a rowid");
        };
        let IOResult::Done(score) = cursor.query_column(0).unwrap() else {
            panic!("query column must not yield");
        };
        hits.push((rowid, score));
        next = cursor.query_next().unwrap();
    }
    assert!(matches!(next, IOResult::Done(false)));
    hits
}
