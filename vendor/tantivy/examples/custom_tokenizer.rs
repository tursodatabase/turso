// # Defining a tokenizer pipeline
//
// In this example, we'll see how to define a tokenizer
// by creating a custom `NgramTokenizer`.
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::tokenizer::NgramTokenizer;
use tantivy::{doc, Index, IndexWriter};

fn main() -> tantivy::Result<()> {
    // # Defining the schema
    //
    // The Tantivy index requires a very strict schema.
    // The schema declares which fields are in the index,
    // and for each field, its type and "the way it should
    // be indexed".

    // first we need to define a schema ...
    let mut schema_builder = Schema::builder();

    // Our first field is title.
    // In this example we want to use NGram searching
    // we will set that to 3 characters, so any three
    // char in the title should be findable.
    let text_field_indexing = TextFieldIndexing::default()
        .set_tokenizer("ngram3")
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let text_options = TextOptions::default()
        .set_indexing_options(text_field_indexing)
        .set_stored();
    let title = schema_builder.add_text_field("title", text_options);

    // Our second field is body.
    // We want full-text search for it, but we do not
    // need to be able to be able to retrieve it
    // for our application.
    //
    // We can make our index lighter by omitting the `STORED` flag.
    let body = schema_builder.add_text_field("body", TEXT);

    let schema = schema_builder.build();

    // # Indexing documents
    //
    // Let's create a brand new index.
    // To simplify we will work entirely in RAM.
    // This is not what you want in reality, but it is very useful
    // for your unit tests... Or this example.
    let index = Index::create_in_ram(schema.clone());

    // here we are registering our custom tokenizer
    // this will store tokens of 3 characters each
    index
        .tokenizers()
        .register("ngram3", NgramTokenizer::new(3, 3, false).unwrap());

    // To insert document we need an index writer.
    // There must be only one writer at a time.
    // This single `IndexWriter` is already
    // multithreaded.
    //
    // Here we use a buffer of 50MB per thread. Using a bigger
    // memory arena for the indexer can increase its throughput.
    let mut index_writer: IndexWriter = index.writer(50_000_000)?;
    index_writer.add_document(doc!(
    title => "The Old Man and the Sea",
    body => "He was an old man who fished alone in a skiff in the Gulf Stream and \
     he had gone eighty-four days now without taking a fish."
    ))?;
    index_writer.add_document(doc!(
    title => "Of Mice and Men",
       body => r#"A few miles south of Soledad, the Salinas River drops in close to the hillside
                bank and runs deep and green. The water is warm too, for it has slipped twinkling
                over the yellow sands in the sunlight before reaching the narrow pool. On one
                side of the river the golden foothill slopes curve up to the strong and rocky
                Gabilan Mountains, but on the valley side the water is lined with trees—willows
                fresh and green with every spring, carrying in their lower leaf junctures the
                debris of the winter’s flooding; and sycamores with mottled, white, recumbent
                limbs and branches that arch over the pool"#
    ))?;
    index_writer.add_document(doc!(
    title => "Frankenstein",
        body => r#"You will rejoice to hear that no disaster has accompanied the commencement of an
                enterprise which you have regarded with such evil forebodings.  I arrived here
                yesterday, and my first task is to assure my dear sister of my welfare and
                increasing confidence in the success of my undertaking."#
    ))?;
    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    // The query parser can interpret human queries.
    // Here, if the user does not specify which
    // field they want to search, tantivy will search
    // in both title and body.
    let query_parser = QueryParser::for_index(&index, vec![title, body]);

    // here we want to get a hit on the 'ken' in Frankenstein
    let query = query_parser.parse_query("ken")?;

    let top_docs = searcher.search(&query, &TopDocs::with_limit(10).order_by_score())?;

    for (_, doc_address) in top_docs {
        let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;
        println!("{}", retrieved_doc.to_json(&schema));
    }

    Ok(())
}
