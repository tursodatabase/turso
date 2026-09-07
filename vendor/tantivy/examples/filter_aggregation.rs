// # Filter Aggregation Example
//
// This example demonstrates filter aggregations - creating buckets of documents
// matching specific queries, with nested aggregations computed on each bucket.
//
// Filter aggregations are useful for computing metrics on different subsets of
// your data in a single query, like "average price overall + average price for
// electronics + count of in-stock items".

use serde_json::json;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::AggregationCollector;
use tantivy::query::AllQuery;
use tantivy::schema::{Schema, FAST, INDEXED, TEXT};
use tantivy::{doc, Index};

fn main() -> tantivy::Result<()> {
    // Create a simple product schema
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("category", TEXT | FAST);
    schema_builder.add_text_field("brand", TEXT | FAST);
    schema_builder.add_u64_field("price", FAST);
    schema_builder.add_f64_field("rating", FAST);
    schema_builder.add_bool_field("in_stock", FAST | INDEXED);
    let schema = schema_builder.build();

    // Create index and add sample products
    let index = Index::create_in_ram(schema.clone());
    let mut writer = index.writer(50_000_000)?;

    writer.add_document(doc!(
        schema.get_field("category")? => "electronics",
        schema.get_field("brand")? => "apple",
        schema.get_field("price")? => 999u64,
        schema.get_field("rating")? => 4.5f64,
        schema.get_field("in_stock")? => true
    ))?;
    writer.add_document(doc!(
        schema.get_field("category")? => "electronics",
        schema.get_field("brand")? => "samsung",
        schema.get_field("price")? => 799u64,
        schema.get_field("rating")? => 4.2f64,
        schema.get_field("in_stock")? => true
    ))?;
    writer.add_document(doc!(
        schema.get_field("category")? => "clothing",
        schema.get_field("brand")? => "nike",
        schema.get_field("price")? => 120u64,
        schema.get_field("rating")? => 4.1f64,
        schema.get_field("in_stock")? => false
    ))?;
    writer.add_document(doc!(
        schema.get_field("category")? => "books",
        schema.get_field("brand")? => "penguin",
        schema.get_field("price")? => 25u64,
        schema.get_field("rating")? => 4.8f64,
        schema.get_field("in_stock")? => true
    ))?;

    writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Example 1: Basic filter with metric aggregation
    println!("=== Example 1: Electronics average price ===");
    let agg_req = json!({
        "electronics": {
            "filter": "category:electronics",
            "aggs": {
                "avg_price": { "avg": { "field": "price" } }
            }
        }
    });

    let agg: Aggregations = serde_json::from_value(agg_req)?;
    let collector = AggregationCollector::from_aggs(agg, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    let expected = json!({
        "electronics": {
            "doc_count": 2,
            "avg_price": { "value": 899.0 }
        }
    });
    assert_eq!(serde_json::to_value(&result)?, expected);
    println!("{}\n", serde_json::to_string_pretty(&result)?);

    // Example 2: Multiple independent filters
    println!("=== Example 2: Multiple filters in one query ===");
    let agg_req = json!({
        "electronics": {
            "filter": "category:electronics",
            "aggs": { "avg_price": { "avg": { "field": "price" } } }
        },
        "in_stock": {
            "filter": "in_stock:true",
            "aggs": { "count": { "value_count": { "field": "brand" } } }
        },
        "high_rated": {
            "filter": "rating:[4.5 TO *]",
            "aggs": { "count": { "value_count": { "field": "brand" } } }
        }
    });

    let agg: Aggregations = serde_json::from_value(agg_req)?;
    let collector = AggregationCollector::from_aggs(agg, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    let expected = json!({
        "electronics": {
            "doc_count": 2,
            "avg_price": { "value": 899.0 }
        },
        "in_stock": {
            "doc_count": 3,
            "count": { "value": 3.0 }
        },
        "high_rated": {
            "doc_count": 2,
            "count": { "value": 2.0 }
        }
    });
    assert_eq!(serde_json::to_value(&result)?, expected);
    println!("{}\n", serde_json::to_string_pretty(&result)?);

    // Example 3: Nested filters - progressive refinement
    println!("=== Example 3: Nested filters ===");
    let agg_req = json!({
        "in_stock": {
            "filter": "in_stock:true",
            "aggs": {
                "electronics": {
                    "filter": "category:electronics",
                    "aggs": {
                        "expensive": {
                            "filter": "price:[800 TO *]",
                            "aggs": {
                                "avg_rating": { "avg": { "field": "rating" } }
                            }
                        }
                    }
                }
            }
        }
    });

    let agg: Aggregations = serde_json::from_value(agg_req)?;
    let collector = AggregationCollector::from_aggs(agg, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    let expected = json!({
        "in_stock": {
            "doc_count": 3,  // apple, samsung, penguin
            "electronics": {
                "doc_count": 2,  // apple, samsung
                "expensive": {
                    "doc_count": 1,  // only apple (999)
                    "avg_rating": { "value": 4.5 }
                }
            }
        }
    });
    assert_eq!(serde_json::to_value(&result)?, expected);
    println!("{}\n", serde_json::to_string_pretty(&result)?);

    // Example 4: Filter with sub-aggregation (terms)
    println!("=== Example 4: Filter with terms sub-aggregation ===");
    let agg_req = json!({
        "electronics": {
            "filter": "category:electronics",
            "aggs": {
                "by_brand": {
                    "terms": { "field": "brand" },
                    "aggs": {
                        "avg_price": { "avg": { "field": "price" } }
                    }
                }
            }
        }
    });

    let agg: Aggregations = serde_json::from_value(agg_req)?;
    let collector = AggregationCollector::from_aggs(agg, Default::default());
    let result = searcher.search(&AllQuery, &collector)?;

    let expected = json!({
        "electronics": {
            "doc_count": 2,
            "by_brand": {
                "buckets": [
                    {
                        "key": "samsung",
                        "doc_count": 1,
                        "avg_price": { "value": 799.0 }
                    },
                    {
                        "key": "apple",
                        "doc_count": 1,
                        "avg_price": { "value": 999.0 }
                    }
                ],
                "sum_other_doc_count": 0,
                "doc_count_error_upper_bound": 0
            }
        }
    });
    assert_eq!(serde_json::to_value(&result)?, expected);
    println!("{}", serde_json::to_string_pretty(&result)?);

    Ok(())
}
