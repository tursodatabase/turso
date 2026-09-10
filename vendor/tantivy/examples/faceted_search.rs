// # Faceted Search
//
// This example covers the faceted search functionalities of
// tantivy.
//
// We will :
// - define a text field "name" in our schema
// - define a facet field "classification" in our schema
// - create an index in memory
// - index few documents with respective facets in our index
// - search and count the number of documents that the classifications start the facet "/Felidae"
// - Search the facet "/Felidae/Pantherinae" and count the number of documents that the
//   classifications include the facet.
//
// ---
// Importing tantivy...
use tantivy::collector::FacetCollector;
use tantivy::query::{AllQuery, TermQuery};
use tantivy::schema::*;
use tantivy::{doc, Index, IndexWriter};

fn main() -> tantivy::Result<()> {
    // Let's create a temporary directory for the sake of this example
    let mut schema_builder = Schema::builder();

    let name = schema_builder.add_text_field("name", TEXT | STORED);
    // this is our faceted field: its scientific classification
    let classification = schema_builder.add_facet_field("classification", FacetOptions::default());

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);

    let mut index_writer: IndexWriter = index.writer(30_000_000)?;

    // For convenience, tantivy also comes with a macro to
    // reduce the boilerplate above.
    index_writer.add_document(doc!(
        name => "Cat",
        classification => Facet::from("/Felidae/Felinae/Felis")
    ))?;
    index_writer.add_document(doc!(
        name => "Canada lynx",
        classification => Facet::from("/Felidae/Felinae/Lynx")
    ))?;
    index_writer.add_document(doc!(
        name => "Cheetah",
        classification => Facet::from("/Felidae/Felinae/Acinonyx")
    ))?;
    index_writer.add_document(doc!(
        name => "Tiger",
        classification => Facet::from("/Felidae/Pantherinae/Panthera")
    ))?;
    index_writer.add_document(doc!(
        name => "Lion",
        classification => Facet::from("/Felidae/Pantherinae/Panthera")
    ))?;
    index_writer.add_document(doc!(
        name => "Jaguar",
        classification => Facet::from("/Felidae/Pantherinae/Panthera")
    ))?;
    index_writer.add_document(doc!(
        name => "Sunda clouded leopard",
        classification => Facet::from("/Felidae/Pantherinae/Neofelis")
    ))?;
    index_writer.add_document(doc!(
        name => "Fossa",
        classification => Facet::from("/Eupleridae/Cryptoprocta")
    ))?;
    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();
    {
        let mut facet_collector = FacetCollector::for_field("classification");
        facet_collector.add_facet("/Felidae");
        let facet_counts = searcher.search(&AllQuery, &facet_collector)?;
        // This lists all of the facet counts, right below "/Felidae".
        let facets: Vec<(&Facet, u64)> = facet_counts.get("/Felidae").collect();
        assert_eq!(
            facets,
            vec![
                (&Facet::from("/Felidae/Felinae"), 3),
                (&Facet::from("/Felidae/Pantherinae"), 4),
            ]
        );
    }

    // Facets are also searchable.
    //
    // For instance a common UI pattern is to allow the user someone to click on a facet link
    // (e.g: `Pantherinae`) to drill down and filter the current result set with this subfacet.
    //
    // The search would then look as follows.

    // Check the reference doc for different ways to create a `Facet` object.
    {
        let facet = Facet::from("/Felidae/Pantherinae");
        let facet_term = Term::from_facet(classification, &facet);
        let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
        let mut facet_collector = FacetCollector::for_field("classification");
        facet_collector.add_facet("/Felidae/Pantherinae");
        let facet_counts = searcher.search(&facet_term_query, &facet_collector)?;
        let facets: Vec<(&Facet, u64)> = facet_counts.get("/Felidae/Pantherinae").collect();
        assert_eq!(
            facets,
            vec![
                (&Facet::from("/Felidae/Pantherinae/Neofelis"), 1),
                (&Facet::from("/Felidae/Pantherinae/Panthera"), 3),
            ]
        );
    }

    Ok(())
}
