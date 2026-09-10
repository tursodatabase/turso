use tantivy::schema::*;

// # Document from json
//
// For convenience, `Document` can be parsed directly from json.
fn main() -> tantivy::Result<()> {
    // Let's first define a schema and an index.
    // Check out the basic example if this is confusing to you.
    //
    // first we need to define a schema ...
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("title", TEXT | STORED);
    schema_builder.add_text_field("body", TEXT);
    schema_builder.add_u64_field("year", INDEXED);
    let schema = schema_builder.build();

    // Let's assume we have a json-serialized document.
    let mice_and_men_doc_json = r#"{
       "title": "Of Mice and Men",
       "year": 1937
    }"#;

    // We can parse our document
    let _mice_and_men_doc = TantivyDocument::parse_json(&schema, mice_and_men_doc_json)?;

    // Multi-valued field are allowed, they are
    // expressed in JSON by an array.
    // The following document has two titles.
    let frankenstein_json = r#"{
       "title": ["Frankenstein", "The Modern Prometheus"],
       "year": 1818
    }"#;
    let _frankenstein_doc = TantivyDocument::parse_json(&schema, frankenstein_json)?;

    // Note that the schema is saved in your index directory.
    //
    // As a result, Indexes are aware of their schema, and you can use this feature
    // just by opening an existing `Index`, and calling `index.schema()..parse_document(json)`.
    Ok(())
}
