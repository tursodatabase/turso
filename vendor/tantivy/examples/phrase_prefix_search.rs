use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::{doc, Index, IndexWriter, ReloadPolicy, Result};
use tempfile::TempDir;

fn main() -> Result<()> {
    let index_path = TempDir::new()?;

    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("title", TEXT | STORED);
    schema_builder.add_text_field("body", TEXT);
    let schema = schema_builder.build();

    let title = schema.get_field("title").unwrap();
    let body = schema.get_field("body").unwrap();

    let index = Index::create_in_dir(&index_path, schema)?;

    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    index_writer.add_document(doc!(
    title => "The Old Man and the Sea",
    body => "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone \
            eighty-four days now without taking a fish.",
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

    // Multivalued field just need to be repeated.
    index_writer.add_document(doc!(
    title => "Frankenstein",
    title => "The Modern Prometheus",
    body => "You will rejoice to hear that no disaster has accompanied the commencement of an \
             enterprise which you have regarded with such evil forebodings.  I arrived here \
             yesterday, and my first task is to assure my dear sister of my welfare and \
             increasing confidence in the success of my undertaking."
    ))?;

    index_writer.commit()?;

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()?;

    let searcher = reader.searcher();

    let query_parser = QueryParser::for_index(&index, vec![title, body]);
    // This will match documents containing the phrase "in the"
    // followed by some word starting with "su",
    // i.e. it will match "in the sunlight" and "in the success",
    // but not "in the Gulf Stream".
    let query = query_parser.parse_query("\"in the su\"*")?;

    let top_docs = searcher.search(&query, &TopDocs::with_limit(10).order_by_score())?;
    let mut titles = top_docs
        .into_iter()
        .map(|(_score, doc_address)| {
            let doc = searcher.doc::<TantivyDocument>(doc_address)?;
            let title = doc
                .get_first(title)
                .and_then(|v| v.as_str())
                .unwrap()
                .to_owned();
            Ok(title)
        })
        .collect::<Result<Vec<_>>>()?;
    titles.sort_unstable();
    assert_eq!(titles, ["Frankenstein", "Of Mice and Men"]);

    Ok(())
}
