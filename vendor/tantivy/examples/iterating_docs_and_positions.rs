// # Iterating docs and positions.
//
// At its core of tantivy, relies on a data structure
// called an inverted index.
//
// This example shows how to manually iterate through
// the list of documents containing a term, getting
// its term frequency, and accessing its positions.

use tantivy::postings::Postings;
// ---
// Importing tantivy...
use tantivy::schema::*;
use tantivy::{doc, DocSet, Index, IndexWriter, TERMINATED};

fn main() -> tantivy::Result<()> {
    // We first create a schema for the sake of the
    // example. Check the `basic_search` example for more information.
    let mut schema_builder = Schema::builder();

    // For this example, we need to make sure to index positions for our title
    // field. `TEXT` precisely does this.
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema);

    let mut index_writer: IndexWriter = index.writer_with_num_threads(1, 50_000_000)?;
    index_writer.add_document(doc!(title => "The Old Man and the Sea"))?;
    index_writer.add_document(doc!(title => "Of Mice and Men"))?;
    index_writer.add_document(doc!(title => "The modern Prometheus"))?;
    index_writer.commit()?;

    let reader = index.reader()?;

    let searcher = reader.searcher();

    // A tantivy index is actually a collection of segments.
    // Similarly, a searcher just wraps a list `segment_reader`.
    //
    // (Because we indexed a very small number of documents over one thread
    // there is actually only one segment here, but let's iterate through the list
    // anyway)
    for segment_reader in searcher.segment_readers() {
        // A segment contains different data structure.
        // Inverted index stands for the combination of
        // - the term dictionary
        // - the inverted lists associated with each terms and their positions
        let inverted_index = segment_reader.inverted_index(title)?;

        // A `Term` is a text token associated with a field.
        // Let's go through all docs containing the term `title:the` and access their position
        let term_the = Term::from_field_text(title, "the");

        // This segment posting object is like a cursor over the documents matching the term.
        // The `IndexRecordOption` arguments tells tantivy we will be interested in both term
        // frequencies and positions.
        //
        // If you don't need all this information, you may get better performance by decompressing
        // less information.
        if let Some(mut segment_postings) =
            inverted_index.read_postings(&term_the, IndexRecordOption::WithFreqsAndPositions)?
        {
            // this buffer will be used to request for positions
            let mut positions: Vec<u32> = Vec::with_capacity(100);
            let mut doc_id = segment_postings.doc();
            while doc_id != TERMINATED {
                // This MAY contains deleted documents as well.
                if segment_reader.is_deleted(doc_id) {
                    doc_id = segment_postings.advance();
                    continue;
                }

                // the number of time the term appears in the document.
                let term_freq: u32 = segment_postings.term_freq();
                // accessing positions is slightly expensive and lazy, do not request
                // for them if you don't need them for some documents.
                segment_postings.positions(&mut positions);

                // By definition we should have `term_freq` positions.
                assert_eq!(positions.len(), term_freq as usize);

                // This prints:
                // ```
                // Doc 0: TermFreq 2: [0, 4]
                // Doc 2: TermFreq 1: [0]
                // ```
                println!("Doc {doc_id}: TermFreq {term_freq}: {positions:?}");
                doc_id = segment_postings.advance();
            }
        }
    }

    // A `Term` is a text token associated with a field.
    // Let's go through all docs containing the term `title:the` and access their position
    let term_the = Term::from_field_text(title, "the");

    // Some other powerful operations (especially `.skip_to`) may be useful to consume these
    // posting lists rapidly.
    // You can check for them in the [`DocSet`](https://docs.rs/tantivy/~0/tantivy/trait.DocSet.html) trait
    // and the [`Postings`](https://docs.rs/tantivy/~0/tantivy/trait.Postings.html) trait

    // Also, for some VERY specific high performance use case like an OLAP analysis of logs,
    // you can get better performance by accessing directly the blocks of doc ids.
    for segment_reader in searcher.segment_readers() {
        // A segment contains different data structure.
        // Inverted index stands for the combination of
        // - the term dictionary
        // - the inverted lists associated with each terms and their positions
        let inverted_index = segment_reader.inverted_index(title)?;

        // This segment posting object is like a cursor over the documents matching the term.
        // The `IndexRecordOption` arguments tells tantivy we will be interested in both term
        // frequencies and positions.
        //
        // If you don't need all this information, you may get better performance by decompressing
        // less information.
        if let Some(mut block_segment_postings) =
            inverted_index.read_block_postings(&term_the, IndexRecordOption::Basic)?
        {
            loop {
                let docs = block_segment_postings.docs();
                if docs.is_empty() {
                    break;
                }
                // Once again these docs MAY contains deleted documents as well.
                let docs = block_segment_postings.docs();
                // Prints `Docs [0, 2].`
                println!("Docs {docs:?}");
                block_segment_postings.advance();
            }
        }
    }

    Ok(())
}
