use std::path::Path;

use tantivy::directory::{Directory, ManagedDirectory, RamDirectory, TerminatingWrite};
use tantivy::schema::{Schema, TEXT};
use tantivy::{doc, Index, IndexWriter, Term};

#[test]
fn test_failpoints_managed_directory_gc_if_delete_fails() {
    let _scenario = fail::FailScenario::setup();

    let test_path: &'static Path = Path::new("some_path_for_test");

    let ram_directory = Box::new(RamDirectory::create());
    let mut managed_directory = ManagedDirectory::wrap(ram_directory).unwrap();
    managed_directory
        .open_write(test_path)
        .unwrap()
        .terminate()
        .unwrap();
    assert!(managed_directory.exists(test_path).unwrap());
    // triggering gc and setting the delete operation to fail.
    //
    // We are checking that the gc operation is not removing the
    // file from managed.json to ensure that the file will be removed
    // in the next gc.
    //
    // The initial 1*off is there to allow for the removal of the
    // lock file.
    fail::cfg("RamDirectory::delete", "1*off->1*return").unwrap();
    assert!(managed_directory.garbage_collect(Default::default).is_ok());
    assert!(managed_directory.exists(test_path).unwrap());

    // running the gc a second time should remove the file.
    assert!(managed_directory.garbage_collect(Default::default).is_ok());
    assert!(
        !managed_directory.exists(test_path).unwrap(),
        "The file should have been deleted"
    );
}

#[test]
fn test_write_commit_fails() -> tantivy::Result<()> {
    let _fail_scenario_guard = fail::FailScenario::setup();
    let mut schema_builder = Schema::builder();
    let text_field = schema_builder.add_text_field("text", TEXT);
    let index = Index::create_in_ram(schema_builder.build());

    let mut index_writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000)?;
    for _ in 0..100 {
        index_writer.add_document(doc!(text_field => "a"))?;
    }
    index_writer.commit()?;
    fail::cfg("save_metas", "return(error_write_failed)").unwrap();
    for _ in 0..100 {
        index_writer.add_document(doc!(text_field => "b"))?;
    }
    assert!(index_writer.commit().is_err());

    let num_docs_containing = |s: &str| {
        let term_a = Term::from_field_text(text_field, s);
        index.reader()?.searcher().doc_freq(&term_a)
    };
    assert_eq!(num_docs_containing("a")?, 100);
    assert_eq!(num_docs_containing("b")?, 0);
    Ok(())
}

// Motivated by
// - https://github.com/quickwit-oss/quickwit/issues/730
// Details at
// - https://github.com/quickwit-oss/tantivy/issues/1198
#[test]
fn test_fail_on_flush_segment() -> tantivy::Result<()> {
    let _fail_scenario_guard = fail::FailScenario::setup();
    let mut schema_builder = Schema::builder();
    let text_field = schema_builder.add_text_field("text", TEXT);
    let index = Index::create_in_ram(schema_builder.build());
    let index_writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000)?;
    fail::cfg("FieldSerializer::close_term", "return(simulatederror)").unwrap();
    for i in 0..100_000 {
        if index_writer
            .add_document(doc!(text_field => format!("hellohappytaxpayerlongtokenblabla{}", i)))
            .is_err()
        {
            return Ok(());
        }
    }
    panic!("add_document should have returned an error");
}

#[test]
fn test_fail_on_flush_segment_but_one_worker_remains() -> tantivy::Result<()> {
    let _fail_scenario_guard = fail::FailScenario::setup();
    let mut schema_builder = Schema::builder();
    let text_field = schema_builder.add_text_field("text", TEXT);
    let index = Index::create_in_ram(schema_builder.build());
    let index_writer: IndexWriter = index.writer_with_num_threads(2, 30_000_000)?;
    fail::cfg("FieldSerializer::close_term", "1*return(simulatederror)").unwrap();
    for i in 0..100_000 {
        if index_writer
            .add_document(doc!(text_field => format!("hellohappytaxpayerlongtokenblabla{}", i)))
            .is_err()
        {
            return Ok(());
        }
    }
    panic!("add_document should have returned an error");
}

#[test]
fn test_fail_on_commit_segment() -> tantivy::Result<()> {
    let _fail_scenario_guard = fail::FailScenario::setup();
    let mut schema_builder = Schema::builder();
    let text_field = schema_builder.add_text_field("text", TEXT);
    let index = Index::create_in_ram(schema_builder.build());
    let mut index_writer: IndexWriter = index.writer_with_num_threads(1, 15_000_000)?;
    fail::cfg("FieldSerializer::close_term", "return(simulatederror)").unwrap();
    for i in 0..10 {
        index_writer
            .add_document(doc!(text_field => format!("hellohappytaxpayerlongtokenblabla{}", i)))?;
    }
    assert!(index_writer.commit().is_err());
    Ok(())
}
