// # Faceted Search With Tweak Score
//
// This example covers the faceted search functionalities of
// tantivy.
//
// We will :
// - define a text field "name" in our schema
// - define a facet field "classification" in our schema

use std::collections::HashSet;

use tantivy::collector::TopDocs;
use tantivy::query::BooleanQuery;
use tantivy::schema::*;
use tantivy::{doc, DocId, Index, IndexWriter, Score, SegmentReader};

fn main() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();

    let title = schema_builder.add_text_field("title", STORED);
    let ingredient = schema_builder.add_facet_field("ingredient", FacetOptions::default());

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);

    let mut index_writer: IndexWriter = index.writer(30_000_000)?;

    index_writer.add_document(doc!(
        title => "Fried egg",
        ingredient => Facet::from("/ingredient/egg"),
        ingredient => Facet::from("/ingredient/oil"),
    ))?;
    index_writer.add_document(doc!(
        title => "Scrambled egg",
        ingredient => Facet::from("/ingredient/egg"),
        ingredient => Facet::from("/ingredient/butter"),
        ingredient => Facet::from("/ingredient/milk"),
        ingredient => Facet::from("/ingredient/salt"),
    ))?;
    index_writer.add_document(doc!(
        title => "Egg rolls",
        ingredient => Facet::from("/ingredient/egg"),
        ingredient => Facet::from("/ingredient/garlic"),
        ingredient => Facet::from("/ingredient/salt"),
        ingredient => Facet::from("/ingredient/oil"),
        ingredient => Facet::from("/ingredient/tortilla-wrap"),
        ingredient => Facet::from("/ingredient/mushroom"),
    ))?;
    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();
    {
        let facets = [
            Facet::from("/ingredient/egg"),
            Facet::from("/ingredient/oil"),
            Facet::from("/ingredient/garlic"),
            Facet::from("/ingredient/mushroom"),
        ];
        let query = BooleanQuery::new_multiterms_query(
            facets
                .iter()
                .map(|key| Term::from_facet(ingredient, key))
                .collect(),
        );
        let top_docs_by_custom_score =
            // Call TopDocs with a custom tweak score
            TopDocs::with_limit(2).tweak_score(move |segment_reader: &SegmentReader| {
                let ingredient_reader = segment_reader.facet_reader("ingredient").unwrap();
                let facet_dict = ingredient_reader.facet_dict();

                let query_ords: HashSet<u64> = facets
                    .iter()
                    .filter_map(|key| facet_dict.term_ord(key.encoded_str()).unwrap())
                    .collect();

                move |doc: DocId, original_score: Score| {
                    // Update the original score with a tweaked score
                    let missing_ingredients = ingredient_reader
                        .facet_ords(doc)
                        .filter(|ord| !query_ords.contains(ord))
                        .count();
                    let tweak = 1.0 / 4_f32.powi(missing_ingredients as i32);

                    original_score * tweak
                }
            });
        let top_docs = searcher.search(&query, &top_docs_by_custom_score)?;

        let titles: Vec<String> = top_docs
            .iter()
            .map(|(_, doc_id)| {
                searcher
                    .doc::<TantivyDocument>(*doc_id)
                    .unwrap()
                    .get_first(title)
                    .and_then(|v| v.as_str().map(|el| el.to_string()))
                    .unwrap()
            })
            .collect();
        assert_eq!(titles, vec!["Fried egg", "Egg rolls"]);
    }
    Ok(())
}
