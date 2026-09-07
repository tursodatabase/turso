// # Aggregation example
//
// This example shows how you can use built-in aggregations.
// We will use nested aggregations with buckets and metrics:
// - Range buckets and compute the average in each bucket.
// - Term aggregation and compute the min price in each bucket
// ---

use serde_json::{Deserializer, Value};
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{self, IndexRecordOption, Schema, TextFieldIndexing, FAST};
use tantivy::{Index, IndexWriter, TantivyDocument};

fn main() -> tantivy::Result<()> {
    // # Create Schema
    //
    // Lets create a schema for a footwear shop, with 4 fields: name, category, stock and price.
    // category, stock and price will be fast fields as that's the requirement
    // for aggregation queries.
    //

    let mut schema_builder = Schema::builder();
    // In preparation of the `TermsAggregation`, the category field is configured with:
    // - `set_fast`
    // - `raw` tokenizer
    //
    // The tokenizer is set to "raw", because the fast field uses the same dictionary as the
    // inverted index. (This behaviour will change in tantivy 0.20, where the fast field will
    // always be raw tokenized independent from the regular tokenizing)
    //
    let text_fieldtype = schema::TextOptions::default()
        .set_indexing_options(
            TextFieldIndexing::default()
                .set_index_option(IndexRecordOption::WithFreqs)
                .set_tokenizer("raw"),
        )
        .set_fast(None)
        .set_stored();
    schema_builder.add_text_field("category", text_fieldtype);
    schema_builder.add_f64_field("stock", FAST);
    schema_builder.add_f64_field("price", FAST);

    let schema = schema_builder.build();

    // # Indexing documents
    //
    // Lets index a bunch of documents for this example.
    let index = Index::create_in_ram(schema.clone());

    let data = r#"{
        "name": "Almond Toe Court Shoes, Patent Black",
        "category": "Womens Footwear",
        "price": 99.00,
        "stock": 5
    }
    {
        "name": "Suede Shoes, Blue",
        "category": "Womens Footwear",
        "price": 42.00,
        "stock": 4
    }
    {
        "name": "Leather Driver Saddle Loafers, Tan",
        "category": "Mens Footwear",
        "price": 34.00,
        "stock": 12
    }
    {
        "name": "Flip Flops, Red",
        "category": "Mens Footwear",
        "price": 19.00,
        "stock": 6
    }
    {
        "name": "Flip Flops, Blue",
        "category": "Mens Footwear",
        "price": 19.00,
        "stock": 0
    }
    {
        "name": "Gold Button Cardigan, Black",
        "category": "Womens Casualwear",
        "price": 167.00,
        "stock": 6
    }
    {
        "name": "Cotton Shorts, Medium Red",
        "category": "Womens Casualwear",
        "price": 30.00,
        "stock": 5
    }
    {
        "name": "Fine Stripe Short Sleeve￼Shirt, Grey",
        "category": "Mens Casualwear",
        "price": 49.99,
        "stock": 9
    }
    {
        "name": "Fine Stripe Short Sleeve￼Shirt, Green",
        "category": "Mens Casualwear",
        "price": 49.99,
        "offer": 39.99,
        "stock": 9
    }
    {
        "name": "Sharkskin Waistcoat, Charcoal",
        "category": "Mens Formalwear",
        "price": 75.00,
        "stock": 2
    }
    {
        "name": "Lightweight Patch Pocket￼Blazer, Deer",
        "category": "Mens Formalwear",
        "price": 175.50,
        "stock": 1
    }
    {
        "name": "Bird Print Dress, Black",
        "category": "Womens Formalwear",
        "price": 270.00,
        "stock": 10
    }
    {
        "name": "Mid Twist Cut-Out Dress, Pink",
        "category": "Womens Formalwear",
        "price": 540.00,
        "stock": 5
    }"#;

    let stream = Deserializer::from_str(data).into_iter::<Value>();

    let mut index_writer: IndexWriter = index.writer(50_000_000)?;
    let mut num_indexed = 0;
    for value in stream {
        let doc = TantivyDocument::parse_json(&schema, &serde_json::to_string(&value.unwrap())?)?;
        index_writer.add_document(doc)?;
        num_indexed += 1;
        if num_indexed > 4 {
            // Writing the first segment
            index_writer.commit()?;
        }
    }

    // Writing the second segment
    index_writer.commit()?;

    // We have two segments now. The `AggregationCollector` will run the aggregation on each
    // segment and then merge the results into an `IntermediateAggregationResult`.

    let reader = index.reader()?;
    let searcher = reader.searcher();
    // ---
    // # Aggregation Query
    //
    //
    // We can construct the query by building the request structure or by deserializing from JSON.
    // The JSON API is more stable and therefore recommended.
    //
    // ## Request 1

    let agg_req_str = r#"
    {
      "group_by_stock": {
        "aggs": {
          "average_price": { "avg": { "field": "price" } }
        },
        "range": {
          "field": "stock",
          "ranges": [
            { "key": "few", "to": 1.0 },
            { "key": "some", "from": 1.0, "to": 10.0 },
            { "key": "many", "from": 10.0 }
          ]
        }
      }
    } "#;

    // In this Aggregation we want to get the average price for different groups, depending on how
    // many items are in stock. We define custom ranges `few`, `some`, `many` via the
    // range aggregation.
    // For every bucket we want the average price, so we create a nested metric aggregation on the
    // range bucket aggregation. Only buckets support nested aggregations.
    // ### Request JSON API
    //

    let agg_req: Aggregations = serde_json::from_str(agg_req_str)?;
    let collector = AggregationCollector::from_aggs(agg_req, Default::default());

    // We use the `AllQuery` which will pass all documents to the AggregationCollector.
    let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();
    let res: Value = serde_json::to_value(agg_res)?;

    // ### Aggregation Result
    //
    // The resulting structure deserializes in the same JSON format as elastic search.
    //
    let expected_res = r#"
    {
        "group_by_stock":{
            "buckets":[
                {"average_price":{"value":19.0},"doc_count":1,"key":"few","to":1.0},
                {"average_price":{"value":124.748},"doc_count":10,"from":1.0,"key":"some","to":10.0},
                {"average_price":{"value":152.0},"doc_count":2,"from":10.0,"key":"many"}
            ]
        }
    }
    "#;
    let expected_json: Value = serde_json::from_str(expected_res)?;
    assert_eq!(expected_json, res);

    // ### Request 2
    //
    // Now we are interested in the minimum price per category, so we create a bucket per
    // category via `TermsAggregation`. We are interested in the highest minimum prices, and set the
    // order of the buckets `"order": { "min_price": "desc" }` to be sorted by the the metric of
    // the sub aggregation. (awesome)
    //
    let agg_req_str = r#"
    {
      "min_price_per_category": {
        "aggs": {
          "min_price": { "min": { "field": "price" } }
        },
        "terms": {
          "field": "category",
          "min_doc_count": 1,
          "order": { "min_price": "desc" }
        }
      }
    } "#;

    let agg_req: Aggregations = serde_json::from_str(agg_req_str)?;

    let collector = AggregationCollector::from_aggs(agg_req, Default::default());

    let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();
    let res: Value = serde_json::to_value(agg_res)?;

    // Minimum price per category, sorted by minimum price descending
    //
    // As you can see, the starting prices for `Formalwear` are higher than `Casualwear`.
    //
    let expected_res = r#"
    {
      "min_price_per_category": {
        "buckets": [
          { "doc_count": 2, "key": "Womens Formalwear", "min_price": { "value": 270.0 } },
          { "doc_count": 2, "key": "Mens Formalwear", "min_price": { "value": 75.0 } },
          { "doc_count": 2, "key": "Mens Casualwear", "min_price": { "value": 49.99 } },
          { "doc_count": 2, "key": "Womens Footwear", "min_price": { "value": 42.0 } },
          { "doc_count": 2, "key": "Womens Casualwear", "min_price": { "value": 30.0 } },
          { "doc_count": 3, "key": "Mens Footwear", "min_price": { "value": 19.0 } }
        ],
        "sum_other_doc_count": 0
      }
    }
    "#;
    let expected_json: Value = serde_json::from_str(expected_res)?;

    assert_eq!(expected_json, res);

    Ok(())
}
