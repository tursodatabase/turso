// # Basic Example
//
// This example covers the basic functionalities of
// tantivy.
//
// We will :
// - define our schema
// - create an index in a directory
// - index a few documents into our index
// - search for the best document matching a basic query
// - retrieve the best document's original content.
// ---
// Importing tantivy...
use tantivy::collector::{Count, TopDocs};
use tantivy::query::FuzzyTermQuery;
use tantivy::schema::*;
use tantivy::{doc, Index, IndexWriter, ReloadPolicy};
use tempfile::TempDir;

fn main() -> tantivy::Result<()> {
    // Let's create a temporary directory for the
    // sake of this example
    let index_path = TempDir::new()?;

    // # Defining the schema
    //
    // The Tantivy index requires a very strict schema.
    // The schema declares which fields are in the index,
    // and for each field, its type and "the way it should
    // be indexed".

    // First we need to define a schema ...
    let mut schema_builder = Schema::builder();

    // Our first field is title.
    // We want full-text search for it, and we also want
    // to be able to retrieve the document after the search.
    //
    // `TEXT | STORED` is some syntactic sugar to describe
    // that.
    //
    // `TEXT` means the field should be tokenized and indexed,
    // along with its term frequency and term positions.
    //
    // `STORED` means that the field will also be saved
    // in a compressed, row-oriented key-value store.
    // This store is useful for reconstructing the
    // documents that were selected during the search phase.
    let title = schema_builder.add_text_field("title", TEXT | STORED);

    let schema = schema_builder.build();

    // # Indexing documents
    //
    // Let's create a brand new index.
    //
    // This will actually just save a meta.json
    // with our schema in the directory.
    let index = Index::create_in_dir(&index_path, schema.clone())?;

    // To insert a document we will need an index writer.
    // There must be only one writer at a time.
    // This single `IndexWriter` is already
    // multithreaded.
    //
    // Here we give tantivy a budget of `50MB`.
    // Using a bigger memory_arena for the indexer may increase
    // throughput, but 50 MB is already plenty.
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // Let's index our documents!
    // We first need a handle on the title and the body field.

    // ### Adding documents
    //
    index_writer.add_document(doc!(
        title => "The Name of the Wind",
    ))?;
    index_writer.add_document(doc!(
        title => "The Diary of Muadib",
    ))?;
    index_writer.add_document(doc!(
        title => "A Dairy Cow",
    ))?;
    index_writer.add_document(doc!(
        title => "The Diary of a Young Girl",
    ))?;

    // ### Committing
    //
    // At this point our documents are not searchable.
    //
    //
    // We need to call `.commit()` explicitly to force the
    // `index_writer` to finish processing the documents in the queue,
    // flush the current index to the disk, and advertise
    // the existence of new documents.
    //
    // This call is blocking.
    index_writer.commit()?;

    // If `.commit()` returns correctly, then all of the
    // documents that have been added are guaranteed to be
    // persistently indexed.
    //
    // In the scenario of a crash or a power failure,
    // tantivy behaves as if it has rolled back to its last
    // commit.

    // # Searching
    //
    // ### Searcher
    //
    // A reader is required first in order to search an index.
    // It acts as a `Searcher` pool that reloads itself,
    // depending on a `ReloadPolicy`.
    //
    // For a search server you will typically create one reader for the entire lifetime of your
    // program, and acquire a new searcher for every single request.
    //
    // In the code below, we rely on the 'ON_COMMIT' policy: the reader
    // will reload the index automatically after each commit.
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()?;

    // We now need to acquire a searcher.
    //
    // A searcher points to a snapshotted, immutable version of the index.
    //
    // Some search experience might require more than
    // one query. Using the same searcher ensures that all of these queries will run on the
    // same version of the index.
    //
    // Acquiring a `searcher` is very cheap.
    //
    // You should acquire a searcher every time you start processing a request and
    // and release it right after your query is finished.
    let searcher = reader.searcher();

    // ### FuzzyTermQuery
    {
        let term = Term::from_field_text(title, "Diary");
        let query = FuzzyTermQuery::new(term, 2, true);

        let (top_docs, count) = searcher
            .search(&query, &(TopDocs::with_limit(5).order_by_score(), Count))
            .unwrap();
        assert_eq!(count, 3);
        assert_eq!(top_docs.len(), 3);
        for (score, doc_address) in top_docs {
            // Note that the score is not lower for the fuzzy hit.
            // There's an issue open for that: https://github.com/quickwit-oss/tantivy/issues/563
            let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;
            println!("score {score:?} doc {}", retrieved_doc.to_json(&schema));
            // score 1.0 doc {"title":["The Diary of Muadib"]}
            //
            // score 1.0 doc {"title":["The Diary of a Young Girl"]}
            //
            // score 1.0 doc {"title":["A Dairy Cow"]}
        }
    }

    Ok(())
}
