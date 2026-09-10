// # Deleting and Updating (?) documents
//
// This example explains how to delete and update documents.
// In fact there is actually no such thing as an update in tantivy.
//
// To update a document, you need to delete a document and then reinsert
// its new version.
//
// ---
// Importing tantivy...
use tantivy::collector::TopDocs;
use tantivy::query::TermQuery;
use tantivy::schema::*;
use tantivy::{doc, Index, IndexReader, IndexWriter};

// A simple helper function to fetch a single document
// given its id from our index.
// It will be helpful to check our work.
fn extract_doc_given_isbn(
    reader: &IndexReader,
    isbn_term: &Term,
) -> tantivy::Result<Option<TantivyDocument>> {
    let searcher = reader.searcher();

    // This is the simplest query you can think of.
    // It matches all of the documents containing a specific term.
    //
    // The second argument is here to tell we don't care about decoding positions,
    // or term frequencies.
    let term_query = TermQuery::new(isbn_term.clone(), IndexRecordOption::Basic);
    let top_docs = searcher.search(&term_query, &TopDocs::with_limit(1).order_by_score())?;

    if let Some((_score, doc_address)) = top_docs.first() {
        let doc = searcher.doc(*doc_address)?;
        Ok(Some(doc))
    } else {
        // no doc matching this ID.
        Ok(None)
    }
}

fn main() -> tantivy::Result<()> {
    // # Defining the schema
    //
    // Check out the *basic_search* example if this makes
    // small sense to you.
    let mut schema_builder = Schema::builder();

    // Tantivy does not really have a notion of primary id.
    // This may change in the future.
    //
    // Still, we can create a `isbn` field and use it as an id. This
    // field can be `u64` or a `text`, depending on your use case.
    // It just needs to be indexed.
    //
    // If it is `text`, let's make sure to keep it `raw` and let's avoid
    // running any text processing on it.
    // This is done by associating this field to the tokenizer named `raw`.
    // Rather than building our
    // [`TextOptions`](//docs.rs/tantivy/~0/tantivy/schema/struct.TextOptions.html) manually, We
    // use the `STRING` shortcut. `STRING` stands for indexed (without term frequency or positions)
    // and untokenized.
    //
    // Because we also want to be able to see this `id` in our returned documents,
    // we also mark the field as stored.
    let isbn = schema_builder.add_text_field("isbn", STRING | STORED);
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema.clone());

    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    // Let's add a couple of documents, for the sake of the example.
    let mut old_man_doc = TantivyDocument::default();
    old_man_doc.add_text(title, "The Old Man and the Sea");
    index_writer.add_document(doc!(
        isbn => "978-0099908401",
        title => "The old Man and the see"
    ))?;
    index_writer.add_document(doc!(
        isbn => "978-0140177398",
        title => "Of Mice and Men",
    ))?;
    index_writer.add_document(doc!(
       title => "Frankentein", //< Oops there is a typo here.
       isbn => "978-9176370711",
    ))?;
    index_writer.commit()?;
    let reader = index.reader()?;

    let frankenstein_isbn = Term::from_field_text(isbn, "978-9176370711");

    // Oops our frankenstein doc seems misspelled
    let frankenstein_doc_misspelled = extract_doc_given_isbn(&reader, &frankenstein_isbn)?.unwrap();
    assert_eq!(
        frankenstein_doc_misspelled.to_json(&schema),
        r#"{"isbn":["978-9176370711"],"title":["Frankentein"]}"#,
    );

    // # Update = Delete + Insert
    //
    // Here we will want to update the typo in the `Frankenstein` book.
    //
    // Tantivy does not handle updates directly, we need to delete
    // and reinsert the document.
    //
    // This can be complicated as it means you need to have access
    // to the entire document. It is good practise to integrate tantivy
    // with a key value store for this reason.
    //
    // To remove one of the document, we just call `delete_term`
    // on its id.
    //
    // Note that `tantivy` does nothing to enforce the idea that
    // there is only one document associated with this id.
    //
    // Also you might have noticed that we apply the delete before
    // having committed. This does not matter really...
    index_writer.delete_term(frankenstein_isbn.clone());

    // We now need to reinsert our document without the typo.
    index_writer.add_document(doc!(
       title => "Frankenstein",
       isbn => "978-9176370711",
    ))?;

    // You are guaranteed that your clients will only observe your index in
    // the state it was in after a commit.
    // In this example, your search engine will at no point be missing the *Frankenstein* document.
    // Everything happened as if the document was updated.
    index_writer.commit()?;
    // We reload our searcher to make our change available to clients.
    reader.reload()?;

    // No more typo!
    let frankenstein_new_doc = extract_doc_given_isbn(&reader, &frankenstein_isbn)?.unwrap();
    assert_eq!(
        frankenstein_new_doc.to_json(&schema),
        r#"{"isbn":["978-9176370711"],"title":["Frankenstein"]}"#,
    );

    Ok(())
}
