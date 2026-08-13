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

// ===================== D3(a): decoder corruption fuzz =====================
//
// Every persisted FTS blob must parse to `Err` (never panic, never OOM) no
// matter how it is mutated. Two batteries per decoder:
// - raw mutations: the record checksum must reject them;
// - checksum-fixed mutations: the structural validation behind the checksum
//   must hold on its own (an attacker-shaped or torn write can have a valid
//   checksum over garbage).

/// Deterministic xorshift64* so failures reproduce without a seed printout.
struct XorShift64(u64);

impl XorShift64 {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound.max(1) as u64) as usize
    }
}

/// Re-checksum a mutated payload so it reaches the structural parser.
fn with_valid_checksum(payload: &[u8]) -> Vec<u8> {
    super::format::append_checksum(payload.to_vec())
}

fn assert_all_mutations_rejected(what: &str, original: &[u8], decode: &dyn Fn(&[u8]) -> bool) {
    // Every single-bit flip must be rejected by the checksum.
    for byte in 0..original.len() {
        for bit in 0..8 {
            let mut mutated = original.to_vec();
            mutated[byte] ^= 1 << bit;
            assert!(
                decode(&mutated),
                "{what}: single-bit flip at byte {byte} bit {bit} must be rejected"
            );
        }
    }
    // Every truncation must be rejected.
    for len in 0..original.len() {
        assert!(
            decode(&original[..len]),
            "{what}: truncation to {len} bytes must be rejected"
        );
    }
    // Trailing junk must be rejected.
    let mut extended = original.to_vec();
    extended.extend_from_slice(b"junk");
    assert!(decode(&extended), "{what}: trailing bytes must be rejected");

    // Random multi-byte splats (raw): checksum must reject.
    let mut rng = XorShift64(0x5eed_0d3a);
    for _ in 0..2_000 {
        let mut mutated = original.to_vec();
        for _ in 0..=rng.below(8) {
            let pos = rng.below(mutated.len());
            mutated[pos] = rng.next() as u8;
        }
        if mutated != original {
            assert!(decode(&mutated), "{what}: random splat must be rejected");
        }
    }
}

/// Checksum-fixed payload mutations must never panic; structural validation
/// decides Ok/Err on its own.
fn splat_payloads_never_panic(original: &[u8], decode: &dyn Fn(&[u8]) -> bool) {
    let payload_len = original.len() - 8;
    let payload = &original[..payload_len];
    let mut rng = XorShift64(0xdead_beef_cafe);
    for _ in 0..2_000 {
        let mut mutated = payload.to_vec();
        match rng.below(3) {
            // byte splats
            0 => {
                for _ in 0..=rng.below(8) {
                    let pos = rng.below(mutated.len());
                    mutated[pos] = rng.next() as u8;
                }
            }
            // truncation
            1 => {
                mutated.truncate(rng.below(mutated.len() + 1));
            }
            // length-field-shaped inflation: overwrite an aligned u32 with
            // a huge value
            _ => {
                if mutated.len() >= 4 {
                    let pos = rng.below(mutated.len() - 3);
                    let inflated = (u32::MAX - rng.below(1024) as u32).to_le_bytes();
                    mutated[pos..pos + 4].copy_from_slice(&inflated);
                }
            }
        }
        // Must return (Ok or Err), never panic or hang.
        let _ = decode(&with_valid_checksum(&mutated));
    }
}

#[test]
fn corrupted_control_records_always_error_and_never_panic() {
    let control = FtsControlV2::new(42, 0x1234_5678);
    let encoded = control.encode();
    assert!(FtsControlV2::decode(&encoded).is_ok());

    let rejects = |bytes: &[u8]| FtsControlV2::decode(bytes).is_err();
    assert_all_mutations_rejected("control", &encoded, &rejects);
    splat_payloads_never_panic(&encoded, &rejects);

    // Targeted structural cases behind a valid checksum.
    let payload = &encoded[..encoded.len() - 8];
    // Wrong magic.
    let mut wrong_magic = payload.to_vec();
    wrong_magic[0] ^= 0xff;
    assert!(FtsControlV2::decode(&with_valid_checksum(&wrong_magic)).is_err());
    // Unsupported version.
    let mut wrong_version = payload.to_vec();
    wrong_version[8] = 0xff;
    assert!(FtsControlV2::decode(&with_valid_checksum(&wrong_version)).is_err());
    // Trailing payload bytes.
    let mut trailing = payload.to_vec();
    trailing.push(0);
    assert!(FtsControlV2::decode(&with_valid_checksum(&trailing)).is_err());
}

#[test]
fn schema_hash_mismatch_is_rejected_with_a_rebuild_hint() {
    let attachment = test_attachment();
    let cursor = FtsCursor::new(&attachment);

    // Matching hash and pre-hash (0) stores open fine.
    assert!(cursor
        .check_schema_hash(&FtsControlV2::new(1, cursor.schema_hash))
        .is_ok());
    assert!(cursor.check_schema_hash(&FtsControlV2::new(1, 0)).is_ok());

    // A store built under any other schema is refused, naming the remedy.
    let err = cursor
        .check_schema_hash(&FtsControlV2::new(1, cursor.schema_hash ^ 1))
        .unwrap_err();
    assert!(
        err.to_string().contains("DROP INDEX"),
        "error must name the rebuild remedy, got: {err}"
    );

    // The hash actually varies with the indexed schema (different column
    // set), so cross-schema opens cannot collide into acceptance.
    let other = FtsIndexAttachment::new(IndexMethodConfiguration {
        table_name: "docs".to_string(),
        index_name: "docs_other".to_string(),
        columns: vec![IndexColumn::new("body", 1)],
        parameters: FxHashMap::<String, Value>::default(),
    })
    .unwrap();
    assert_ne!(attachment.schema_hash, other.schema_hash);
}

#[test]
fn corrupted_segment_descriptors_always_error_and_never_panic() {
    let attachment = test_attachment();
    let (segment, _) = build_and_load_segment(
        &attachment,
        &[(1, "hello corruption"), (2, "goodbye corruption")],
    );
    let descriptor = &segment.descriptor;
    let encoded = descriptor.encode().unwrap();
    let id = descriptor.segment_id;
    assert!(SegmentDescriptor::decode(id, &encoded).is_ok());

    let rejects = |bytes: &[u8]| SegmentDescriptor::decode(id, bytes).is_err();
    assert_all_mutations_rejected("descriptor", &encoded, &rejects);
    splat_payloads_never_panic(&encoded, &rejects);

    // Length-field inflation behind a valid checksum must be rejected by
    // bounds checks, not by allocating file_count/name_len bytes.
    let payload = &encoded[..encoded.len() - 8];
    let mut inflated_count = payload.to_vec();
    // Layout: magic(8) max_doc(4) file_count(4) ...
    inflated_count[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(SegmentDescriptor::decode(id, &with_valid_checksum(&inflated_count)).is_err());

    let mut inflated_name = payload.to_vec();
    // First file entry: name_len at offset 16.
    inflated_name[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(SegmentDescriptor::decode(id, &with_valid_checksum(&inflated_name)).is_err());

    // Zero-chunk file entries are structurally invalid.
    let zero_chunks = SegmentDescriptor {
        segment_id: id,
        max_doc: 1,
        files: vec![SegmentFileEntry {
            name: "f.term".to_string(),
            size: 10,
            num_chunks: 0,
        }],
    };
    let encoded_zero = zero_chunks.encode().unwrap();
    assert!(SegmentDescriptor::decode(id, &encoded_zero).is_err());
}

#[test]
fn corrupted_chunk_layouts_always_error() {
    // Real v2 segment files end in a Tantivy footer; build the fixture bytes
    // the same way so the valid layout passes the crc check.
    let file_a = with_tantivy_footer(vec![1, 2, 3, 4, 5, 6]).unwrap();
    let file_b = with_tantivy_footer(vec![7, 8, 9]).unwrap();
    let a_split = file_a.len() / 2;
    let descriptor = SegmentDescriptor {
        segment_id: SegmentId::generate_random(),
        max_doc: 1,
        files: vec![
            SegmentFileEntry {
                name: "a.term".to_string(),
                size: file_a.len() as u64,
                num_chunks: 2,
            },
            SegmentFileEntry {
                name: "b.store".to_string(),
                size: file_b.len() as u64,
                num_chunks: 1,
            },
        ],
    };
    let valid_chunks = || {
        let mut chunks: HashMap<u32, HashMap<i64, Vec<u8>>> = HashMap::default();
        let mut a: HashMap<i64, Vec<u8>> = HashMap::default();
        a.insert(0, file_a[..a_split].to_vec());
        a.insert(1, file_a[a_split..].to_vec());
        chunks.insert(0, a);
        let mut b: HashMap<i64, Vec<u8>> = HashMap::default();
        b.insert(0, file_b.clone());
        chunks.insert(1, b);
        chunks
    };
    assert!(assemble_segment_data(&descriptor, valid_chunks()).is_ok());

    // A same-length bit flip in chunk bytes passes every structural check;
    // only the footer crc catches it.
    let mut flipped = valid_chunks();
    flipped.get_mut(&0).unwrap().get_mut(&0).unwrap()[1] ^= 0x01;
    assert!(assemble_segment_data(&descriptor, flipped).is_err());

    // Missing file.
    let mut missing_file = valid_chunks();
    missing_file.remove(&1);
    assert!(assemble_segment_data(&descriptor, missing_file).is_err());

    // Missing chunk within a file.
    let mut missing_chunk = valid_chunks();
    missing_chunk.get_mut(&0).unwrap().remove(&1);
    assert!(assemble_segment_data(&descriptor, missing_chunk).is_err());

    // Chunk-count mismatch with a hole (still 2 entries, wrong numbers).
    let mut hole = valid_chunks();
    let moved = hole.get_mut(&0).unwrap().remove(&1).unwrap();
    hole.get_mut(&0).unwrap().insert(5, moved);
    assert!(assemble_segment_data(&descriptor, hole).is_err());

    // Negative chunk number.
    let mut negative = valid_chunks();
    let moved = negative.get_mut(&0).unwrap().remove(&1).unwrap();
    negative.get_mut(&0).unwrap().insert(-1, moved);
    assert!(assemble_segment_data(&descriptor, negative).is_err());

    // Assembled size differs from the descriptor.
    let mut resized = valid_chunks();
    let mut longer = file_b.clone();
    longer.push(10);
    resized.get_mut(&1).unwrap().insert(0, longer);
    assert!(assemble_segment_data(&descriptor, resized).is_err());

    // Orphan chunks for a file the descriptor does not list.
    let mut orphan = valid_chunks();
    let mut extra: HashMap<i64, Vec<u8>> = HashMap::default();
    extra.insert(0, vec![0]);
    orphan.insert(9, extra);
    assert!(assemble_segment_data(&descriptor, orphan).is_err());
}
