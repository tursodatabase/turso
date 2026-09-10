// # Stop Words Example
//
// This example covers the basic usage of stop words
// with tantivy
//
// We will :
// - define our schema
// - create an index in a directory
// - add a few stop words
// - index few documents in our index

// ---
// Importing tantivy...
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::tokenizer::*;
use tantivy::{doc, Index, IndexWriter};

fn main() -> tantivy::Result<()> {
    // this example assumes you understand the content in `basic_search`
    let mut schema_builder = Schema::builder();

    // This configures your custom options for how tantivy will
    // store and process your content in the index; The key
    // to note is that we are setting the tokenizer to `stoppy`
    // which will be defined and registered below.
    let text_field_indexing = TextFieldIndexing::default()
        .set_tokenizer("stoppy")
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let text_options = TextOptions::default()
        .set_indexing_options(text_field_indexing)
        .set_stored();

    // Our first field is title.
    schema_builder.add_text_field("title", text_options);

    // Our second field is body.
    let text_field_indexing = TextFieldIndexing::default()
        .set_tokenizer("stoppy")
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let text_options = TextOptions::default()
        .set_indexing_options(text_field_indexing)
        .set_stored();
    schema_builder.add_text_field("body", text_options);

    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema.clone());

    // This tokenizer lowers all of the text (to help with stop word matching)
    // then removes all instances of `the` and `and` from the corpus
    let tokenizer = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(LowerCaser)
        .filter(StopWordFilter::remove(vec![
            "the".to_string(),
            "and".to_string(),
        ]))
        .build();

    index.tokenizers().register("stoppy", tokenizer);

    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    let title = schema.get_field("title").unwrap();
    let body = schema.get_field("body").unwrap();

    index_writer.add_document(doc!(
    title => "The Old Man and the Sea",
    body => "He was an old man who fished alone in a skiff in the Gulf Stream and \
     he had gone eighty-four days now without taking a fish."
    ))?;

    index_writer.add_document(doc!(
    title => "Of Mice and Men",
    body => "A few miles south of Soledad, the Salinas River drops in close to the hillside \
            bank and runs deep and green. The water is warm too, for it has slipped twinkling \
            over the yellow sands in the sunlight before reaching the narrow pool. On one \
            side of the river the golden foothill slopes curve up to the strong and rocky \
            Gabilan Mountains, but on the valley side the water is lined with trees—willows \
            fresh and green with every spring, carrying in their lower leaf junctures the \
            debris of the winter’s flooding; and sycamores with mottled, white, recumbent \
            limbs and branches that arch over the pool"
    ))?;

    index_writer.add_document(doc!(
    title => "Frankenstein",
    body => "You will rejoice to hear that no disaster has accompanied the commencement of an \
             enterprise which you have regarded with such evil forebodings.  I arrived here \
             yesterday, and my first task is to assure my dear sister of my welfare and \
             increasing confidence in the success of my undertaking."
    ))?;

    index_writer.commit()?;

    let reader = index.reader()?;

    let searcher = reader.searcher();

    let query_parser = QueryParser::for_index(&index, vec![title, body]);

    // stop words are applied on the query as well.
    // The following will be equivalent to `title:frankenstein`
    let query = query_parser.parse_query("title:\"the Frankenstein\"")?;
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10).order_by_score())?;

    for (score, doc_address) in top_docs {
        let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;
        println!("\n==\nDocument score {score}:");
        println!("{}", retrieved_doc.to_json(&schema));
    }

    Ok(())
}
