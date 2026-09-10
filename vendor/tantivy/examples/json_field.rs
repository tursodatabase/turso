// # Json field example
//
// This example shows how the json field can be used
// to make tantivy partially schemaless by setting it as
// default query parser field.

use tantivy::collector::{Count, TopDocs};
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, FAST, STORED, STRING, TEXT};
use tantivy::{Index, IndexWriter, TantivyDocument};

fn main() -> tantivy::Result<()> {
    // # Defining the schema
    let mut schema_builder = Schema::builder();
    schema_builder.add_date_field("timestamp", FAST | STORED);
    let event_type = schema_builder.add_text_field("event_type", STRING | STORED);
    let attributes = schema_builder.add_json_field("attributes", STORED | TEXT);
    let schema = schema_builder.build();

    // # Indexing documents
    let index = Index::create_in_ram(schema.clone());

    let mut index_writer: IndexWriter = index.writer(50_000_000)?;
    let doc = TantivyDocument::parse_json(
        &schema,
        r#"{
        "timestamp": "2022-02-22T23:20:50.53Z",
        "event_type": "click",
        "attributes": {
            "target": "submit-button",
            "cart": {"product_id": 103},
            "description": "the best vacuum cleaner ever"
        }
    }"#,
    )?;
    index_writer.add_document(doc)?;
    let doc = TantivyDocument::parse_json(
        &schema,
        r#"{
        "timestamp": "2022-02-22T23:20:51.53Z",
        "event_type": "click",
        "attributes": {
            "target": "submit-button",
            "cart": {"product_id": 133},
            "description": "das keyboard",
            "event_type": "holiday-sale"
        }
    }"#,
    )?;
    index_writer.add_document(doc)?;
    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    // # Default fields: event_type and attributes
    // By setting attributes as a default field it allows omitting attributes itself, e.g. "target",
    // instead of "attributes.target"
    let query_parser = QueryParser::for_index(&index, vec![event_type, attributes]);
    {
        let query = query_parser.parse_query("target:submit-button")?;
        let count_docs = searcher.search(&*query, &TopDocs::with_limit(2).order_by_score())?;
        assert_eq!(count_docs.len(), 2);
    }
    {
        let query = query_parser.parse_query("target:submit")?;
        let count_docs = searcher.search(&*query, &TopDocs::with_limit(2).order_by_score())?;
        assert_eq!(count_docs.len(), 2);
    }
    {
        let query = query_parser.parse_query("cart.product_id:103")?;
        let count_docs = searcher.search(&*query, &Count)?;
        assert_eq!(count_docs, 1);
    }
    {
        let query = query_parser.parse_query("click AND cart.product_id:133")?;
        let hits = searcher.search(&*query, &TopDocs::with_limit(2).order_by_score())?;
        assert_eq!(hits.len(), 1);
    }
    {
        // The sub-fields in the json field marked as default field still need to be explicitly
        // addressed
        let query = query_parser.parse_query("click AND 133")?;
        let hits = searcher.search(&*query, &TopDocs::with_limit(2).order_by_score())?;
        assert_eq!(hits.len(), 0);
    }
    {
        // Default json fields are ignored if they collide with the schema
        let query = query_parser.parse_query("event_type:holiday-sale")?;
        let hits = searcher.search(&*query, &TopDocs::with_limit(2).order_by_score())?;
        assert_eq!(hits.len(), 0);
    }
    // # Query via full attribute path
    {
        // This only searches in our schema's `event_type` field
        let query = query_parser.parse_query("event_type:click")?;
        let hits = searcher.search(&*query, &TopDocs::with_limit(2).order_by_score())?;
        assert_eq!(hits.len(), 2);
    }
    {
        // Default json fields can still be accessed by full path
        let query = query_parser.parse_query("attributes.event_type:holiday-sale")?;
        let hits = searcher.search(&*query, &TopDocs::with_limit(2).order_by_score())?;
        assert_eq!(hits.len(), 1);
    }
    Ok(())
}
