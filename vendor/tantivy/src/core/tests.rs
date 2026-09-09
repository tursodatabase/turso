use crate::collector::Count;
use crate::directory::{RamDirectory, WatchCallback};
use crate::index::SegmentId;
use crate::indexer::{LogMergePolicy, NoMergePolicy};
use crate::postings::Postings;
use crate::query::TermQuery;
use crate::schema::{Field, IndexRecordOption, Schema, INDEXED, STRING, TEXT};
use crate::tokenizer::TokenizerManager;
use crate::{
    Directory, DocSet, Index, IndexBuilder, IndexReader, IndexSettings, IndexWriter, ReloadPolicy,
    TantivyDocument, Term,
};

#[test]
fn test_indexer_for_field() {
    let mut schema_builder = Schema::builder();
    let num_likes_field = schema_builder.add_u64_field("num_likes", INDEXED);
    let body_field = schema_builder.add_text_field("body", TEXT);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);
    assert!(index.tokenizer_for_field(body_field).is_ok());
    assert_eq!(
        format!("{:?}", index.tokenizer_for_field(num_likes_field).err()),
        "Some(SchemaError(\"\\\"num_likes\\\" is not a text field.\"))"
    );
}

#[test]
fn test_set_tokenizer_manager() {
    let mut schema_builder = Schema::builder();
    schema_builder.add_u64_field("num_likes", INDEXED);
    schema_builder.add_text_field("body", TEXT);
    let schema = schema_builder.build();
    let index = IndexBuilder::new()
        // set empty tokenizer manager
        .tokenizers(TokenizerManager::new())
        .schema(schema)
        .create_in_ram()
        .unwrap();
    assert!(index.tokenizers().get("raw").is_none());
}

#[test]
fn test_index_exists() {
    let directory: Box<dyn Directory> = Box::new(RamDirectory::create());
    assert!(!Index::exists(directory.as_ref()).unwrap());
    assert!(Index::create(
        directory.clone(),
        throw_away_schema(),
        IndexSettings::default()
    )
    .is_ok());
    assert!(Index::exists(directory.as_ref()).unwrap());
}

#[test]
fn open_or_create_should_create() {
    let directory = RamDirectory::create();
    assert!(!Index::exists(&directory).unwrap());
    assert!(Index::open_or_create(directory.clone(), throw_away_schema()).is_ok());
    assert!(Index::exists(&directory).unwrap());
}

#[test]
fn open_or_create_should_open() {
    let directory: Box<dyn Directory> = Box::new(RamDirectory::create());
    assert!(Index::create(
        directory.clone(),
        throw_away_schema(),
        IndexSettings::default()
    )
    .is_ok());
    assert!(Index::exists(directory.as_ref()).unwrap());
    assert!(Index::open_or_create(directory, throw_away_schema()).is_ok());
}

#[test]
fn create_should_wipeoff_existing() {
    let directory: Box<dyn Directory> = Box::new(RamDirectory::create());
    assert!(Index::create(
        directory.clone(),
        throw_away_schema(),
        IndexSettings::default()
    )
    .is_ok());
    assert!(Index::exists(directory.as_ref()).unwrap());
    assert!(Index::create(
        directory,
        Schema::builder().build(),
        IndexSettings::default()
    )
    .is_ok());
}

#[test]
fn open_or_create_exists_but_schema_does_not_match() {
    let directory = RamDirectory::create();
    assert!(Index::create(
        directory.clone(),
        throw_away_schema(),
        IndexSettings::default()
    )
    .is_ok());
    assert!(Index::exists(&directory).unwrap());
    assert!(Index::open_or_create(directory.clone(), throw_away_schema()).is_ok());
    let err = Index::open_or_create(directory, Schema::builder().build());
    assert_eq!(
        format!("{:?}", err.unwrap_err()),
        "SchemaError(\"An index exists but the schema does not match.\")"
    );
}

fn throw_away_schema() -> Schema {
    let mut schema_builder = Schema::builder();
    let _ = schema_builder.add_u64_field("num_likes", INDEXED);
    schema_builder.build()
}

#[test]
fn test_index_on_commit_reload_policy() -> crate::Result<()> {
    let schema = throw_away_schema();
    let field = schema.get_field("num_likes").unwrap();
    let index = Index::create_in_ram(schema);
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()
        .unwrap();
    assert_eq!(reader.searcher().num_docs(), 0);
    test_index_on_commit_reload_policy_aux(field, &index, &reader)
}

#[cfg(feature = "mmap")]
mod mmap_specific {

    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_index_on_commit_reload_policy_mmap() -> crate::Result<()> {
        let schema = throw_away_schema();
        let field = schema.get_field("num_likes").unwrap();
        let tempdir = TempDir::new().unwrap();
        let tempdir_path = PathBuf::from(tempdir.path());
        let index = Index::create_in_dir(tempdir_path, schema).unwrap();
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()
            .unwrap();
        assert_eq!(reader.searcher().num_docs(), 0);
        test_index_on_commit_reload_policy_aux(field, &index, &reader)
    }

    #[test]
    fn test_index_manual_policy_mmap() -> crate::Result<()> {
        let schema = throw_away_schema();
        let field = schema.get_field("num_likes").unwrap();
        let mut index = Index::create_from_tempdir(schema)?;
        let mut writer: IndexWriter = index.writer_for_tests()?;
        writer.commit()?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        assert_eq!(reader.searcher().num_docs(), 0);
        writer.add_document(doc!(field=>1u64))?;
        let (sender, receiver) = crossbeam_channel::unbounded();
        let _handle = index.directory_mut().watch(WatchCallback::new(move || {
            let _ = sender.send(());
        }));
        writer.commit()?;
        assert!(receiver.recv().is_ok());
        assert_eq!(reader.searcher().num_docs(), 0);
        reader.reload()?;
        assert_eq!(reader.searcher().num_docs(), 1);
        Ok(())
    }

    #[test]
    fn test_index_on_commit_reload_policy_different_directories() -> crate::Result<()> {
        let schema = throw_away_schema();
        let field = schema.get_field("num_likes").unwrap();
        let tempdir = TempDir::new().unwrap();
        let tempdir_path = PathBuf::from(tempdir.path());
        let write_index = Index::create_in_dir(&tempdir_path, schema).unwrap();
        let read_index = Index::open_in_dir(&tempdir_path).unwrap();
        let reader = read_index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()
            .unwrap();
        assert_eq!(reader.searcher().num_docs(), 0);
        test_index_on_commit_reload_policy_aux(field, &write_index, &reader)
    }
}
fn test_index_on_commit_reload_policy_aux(
    field: Field,
    index: &Index,
    reader: &IndexReader,
) -> crate::Result<()> {
    let mut reader_index = reader.index();
    let (sender, receiver) = crossbeam_channel::unbounded();
    let _watch_handle = reader_index
        .directory_mut()
        .watch(WatchCallback::new(move || {
            let _ = sender.send(());
        }));
    let mut writer: IndexWriter = index.writer_for_tests()?;
    assert_eq!(reader.searcher().num_docs(), 0);
    writer.add_document(doc!(field=>1u64))?;
    writer.commit().unwrap();
    // We need a loop here because it is possible for notify to send more than
    // one modify event. It was observed on CI on MacOS.
    loop {
        assert!(receiver.recv().is_ok());
        if reader.searcher().num_docs() == 1 {
            break;
        }
    }
    writer.add_document(doc!(field=>2u64))?;
    writer.commit().unwrap();
    // ... Same as above
    loop {
        assert!(receiver.recv().is_ok());
        if reader.searcher().num_docs() == 2 {
            break;
        }
    }
    Ok(())
}

// This test will not pass on windows, because windows
// prevent deleting files that are MMapped.
#[cfg(not(target_os = "windows"))]
#[test]
fn garbage_collect_works_as_intended() -> crate::Result<()> {
    let directory = RamDirectory::create();
    let schema = throw_away_schema();
    let field = schema.get_field("num_likes").unwrap();
    let index = Index::create(directory.clone(), schema, IndexSettings::default())?;

    let mut writer: IndexWriter = index.writer_with_num_threads(1, 32_000_000).unwrap();
    for _seg in 0..8 {
        for i in 0u64..1_000u64 {
            writer.add_document(doc!(field => i))?;
        }
        writer.commit()?;
    }

    let mem_right_after_commit = directory.total_mem_usage();

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    assert_eq!(reader.searcher().num_docs(), 8_000);
    assert_eq!(reader.searcher().segment_readers().len(), 8);

    writer.wait_merging_threads()?;

    let mem_right_after_merge_finished = directory.total_mem_usage();

    reader.reload().unwrap();
    let searcher = reader.searcher();
    assert_eq!(searcher.segment_readers().len(), 1);
    assert_eq!(searcher.num_docs(), 8_000);
    assert!(
        mem_right_after_merge_finished < mem_right_after_commit,
        "(mem after merge){mem_right_after_merge_finished} is expected < (mem before \
         merge){mem_right_after_commit}"
    );
    Ok(())
}

#[test]
fn test_single_segment_index_writer() -> crate::Result<()> {
    let mut schema_builder = Schema::builder();
    let text_field = schema_builder.add_text_field("text", TEXT);
    let schema = schema_builder.build();
    let directory = RamDirectory::default();
    let mut single_segment_index_writer = Index::builder()
        .schema(schema)
        .single_segment_index_writer(directory, 15_000_000)?;
    for _ in 0..10 {
        let doc = doc!(text_field=>"hello");
        single_segment_index_writer.add_document(doc)?;
    }
    let index = single_segment_index_writer.finalize()?;
    let searcher = index.reader()?.searcher();
    let term_query = TermQuery::new(
        Term::from_field_text(text_field, "hello"),
        IndexRecordOption::Basic,
    );
    let count = searcher.search(&term_query, &Count)?;
    assert_eq!(count, 10);
    Ok(())
}

#[test]
fn test_merging_segment_update_docfreq() {
    let mut schema_builder = Schema::builder();
    let text_field = schema_builder.add_text_field("text", TEXT);
    let id_field = schema_builder.add_text_field("id", STRING);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);
    let mut writer: IndexWriter = index.writer_for_tests().unwrap();
    writer.set_merge_policy(Box::new(NoMergePolicy));
    for _ in 0..5 {
        writer.add_document(doc!(text_field=>"hello")).unwrap();
    }
    writer
        .add_document(doc!(text_field=>"hello", id_field=>"TO_BE_DELETED"))
        .unwrap();
    writer
        .add_document(doc!(text_field=>"hello", id_field=>"TO_BE_DELETED"))
        .unwrap();
    writer.add_document(TantivyDocument::default()).unwrap();
    writer.commit().unwrap();
    for _ in 0..7 {
        writer.add_document(doc!(text_field=>"hello")).unwrap();
    }
    writer.add_document(TantivyDocument::default()).unwrap();
    writer.add_document(TantivyDocument::default()).unwrap();
    writer.delete_term(Term::from_field_text(id_field, "TO_BE_DELETED"));
    writer.commit().unwrap();

    let segment_ids: Vec<SegmentId> = index
        .list_all_segment_metas()
        .into_iter()
        .map(|reader| reader.id())
        .collect();
    writer.merge(&segment_ids[..]).wait().unwrap();
    let index_reader = index.reader().unwrap();
    let searcher = index_reader.searcher();
    assert_eq!(searcher.segment_readers().len(), 1);
    assert_eq!(searcher.num_docs(), 15);
    let segment_reader = searcher.segment_reader(0);
    assert_eq!(segment_reader.max_doc(), 15);
    let inv_index = segment_reader.inverted_index(text_field).unwrap();
    let term = Term::from_field_text(text_field, "hello");
    let term_info = inv_index.get_term_info(&term).unwrap().unwrap();
    assert_eq!(term_info.doc_freq, 12);
}

// motivated by https://github.com/quickwit-oss/quickwit/issues/4130
#[test]
fn test_positions_merge_bug_non_text_json_vint() {
    let mut schema_builder = Schema::builder();
    let field = schema_builder.add_json_field("dynamic", TEXT);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer_for_tests().unwrap();
    let mut merge_policy = LogMergePolicy::default();
    merge_policy.set_min_num_segments(2);
    writer.set_merge_policy(Box::new(merge_policy));
    // Here a string would work.
    let doc_json = r#"{"tenant_id":75}"#;
    let vals = serde_json::from_str(doc_json).unwrap();
    let mut doc = TantivyDocument::default();
    doc.add_object(field, vals);
    writer.add_document(doc.clone()).unwrap();
    writer.commit().unwrap();
    writer.add_document(doc.clone()).unwrap();
    writer.commit().unwrap();
    writer.wait_merging_threads().unwrap();
    let reader = index.reader().unwrap();
    assert_eq!(reader.searcher().segment_readers().len(), 1);
}

// Same as above but with bitpacked blocks
#[test]
fn test_positions_merge_bug_non_text_json_bitpacked_block() {
    let mut schema_builder = Schema::builder();
    let field = schema_builder.add_json_field("dynamic", TEXT);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer_for_tests().unwrap();
    let mut merge_policy = LogMergePolicy::default();
    merge_policy.set_min_num_segments(2);
    writer.set_merge_policy(Box::new(merge_policy));
    // Here a string would work.
    let doc_json = r#"{"tenant_id":75}"#;
    let vals = serde_json::from_str(doc_json).unwrap();
    let mut doc = TantivyDocument::default();
    doc.add_object(field, vals);
    for _ in 0..128 {
        writer.add_document(doc.clone()).unwrap();
    }
    writer.commit().unwrap();
    writer.add_document(doc.clone()).unwrap();
    writer.commit().unwrap();
    writer.wait_merging_threads().unwrap();
    let reader = index.reader().unwrap();
    assert_eq!(reader.searcher().segment_readers().len(), 1);
}

#[test]
fn test_non_text_json_term_freq() {
    let mut schema_builder = Schema::builder();
    let field = schema_builder.add_json_field("dynamic", TEXT);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer_for_tests().unwrap();
    // Here a string would work.
    let doc_json = r#"{"tenant_id":75}"#;
    let vals = serde_json::from_str(doc_json).unwrap();
    let mut doc = TantivyDocument::default();
    doc.add_object(field, vals);
    writer.add_document(doc.clone()).unwrap();
    writer.commit().unwrap();
    let reader = index.reader().unwrap();
    assert_eq!(reader.searcher().segment_readers().len(), 1);
    let searcher = reader.searcher();
    let segment_reader = searcher.segment_reader(0u32);
    let inv_idx = segment_reader.inverted_index(field).unwrap();

    let mut term = Term::from_field_json_path(field, "tenant_id", false);
    term.append_type_and_fast_value(75i64);

    let postings = inv_idx
        .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)
        .unwrap()
        .unwrap();
    assert_eq!(postings.doc(), 0);
    assert_eq!(postings.term_freq(), 1u32);
}

#[test]
fn test_non_text_json_term_freq_bitpacked() {
    let mut schema_builder = Schema::builder();
    let field = schema_builder.add_json_field("dynamic", TEXT);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer_for_tests().unwrap();
    // Here a string would work.
    let doc_json = r#"{"tenant_id":75}"#;
    let vals = serde_json::from_str(doc_json).unwrap();
    let mut doc = TantivyDocument::default();
    doc.add_object(field, vals);
    let num_docs = 132;
    for _ in 0..num_docs {
        writer.add_document(doc.clone()).unwrap();
    }
    writer.commit().unwrap();
    let reader = index.reader().unwrap();
    assert_eq!(reader.searcher().segment_readers().len(), 1);
    let searcher = reader.searcher();
    let segment_reader = searcher.segment_reader(0u32);
    let inv_idx = segment_reader.inverted_index(field).unwrap();

    let mut term = Term::from_field_json_path(field, "tenant_id", false);
    term.append_type_and_fast_value(75i64);

    let mut postings = inv_idx
        .read_postings(&term, IndexRecordOption::WithFreqsAndPositions)
        .unwrap()
        .unwrap();
    assert_eq!(postings.doc(), 0);
    assert_eq!(postings.term_freq(), 1u32);
    for i in 1..num_docs {
        assert_eq!(postings.advance(), i);
        assert_eq!(postings.term_freq(), 1u32);
    }
}
