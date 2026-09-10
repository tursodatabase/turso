use serde_json::Value;

use crate::aggregation::agg_req::{Aggregation, Aggregations};
use crate::aggregation::agg_result::AggregationResults;
use crate::aggregation::collector::AggregationCollector;
use crate::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use crate::aggregation::tests::{get_test_index_2_segments, get_test_index_from_values_and_terms};
use crate::aggregation::DistributedAggregationCollector;
use crate::docset::COLLECT_BLOCK_BUFFER_LEN;
use crate::query::{AllQuery, TermQuery};
use crate::schema::{IndexRecordOption, Schema, FAST};
use crate::{Index, IndexWriter, Term};

// The following tests ensure that each bucket aggregation type correctly functions as a
// sub-aggregation of another bucket aggregation in two scenarios:
// 1) The parent has more buckets than the child sub-aggregation
// 2) The child sub-aggregation has more buckets than the parent
//
// These scenarios exercise the bucket id mapping and sub-aggregation routing logic.

#[test]
fn test_terms_as_subagg_parent_more_vs_child_more() -> crate::Result<()> {
    let index = get_test_index_2_segments(false)?;

    // Case A: parent has more buckets than child
    // Parent: range with 4 buckets
    // Child: terms on text -> 2 buckets
    let agg_parent_more: Aggregations = serde_json::from_value(json!({
        "parent_range": {
            "range": {
                "field": "score",
                "ranges": [
                    {"to": 3.0},
                    {"from": 3.0, "to": 7.0},
                    {"from": 7.0, "to": 20.0},
                    {"from": 20.0}
                ]
            },
            "aggs": {
                "child_terms": {"terms": {"field": "text", "order": {"_key": "asc"}}}
            }
        }
    }))
    .unwrap();

    let res = crate::aggregation::tests::exec_request(agg_parent_more, &index)?;
    // Exact expected structure and counts
    assert_eq!(
        res["parent_range"]["buckets"],
        json!([
            {
                "key": "*-3",
                "doc_count": 1,
                "to": 3.0,
                "child_terms": {
                    "buckets": [
                        {"doc_count": 1, "key": "cool"}
                    ],
                    "sum_other_doc_count": 0
                }
            },
            {
                "key": "3-7",
                "doc_count": 3,
                "from": 3.0,
                "to": 7.0,
                "child_terms": {
                    "buckets": [
                        {"doc_count": 2, "key": "cool"},
                        {"doc_count": 1, "key": "nohit"}
                    ],
                    "sum_other_doc_count": 0
                }
            },
            {
                "key": "7-20",
                "doc_count": 3,
                "from": 7.0,
                "to": 20.0,
                "child_terms": {
                    "buckets": [
                        {"doc_count": 3, "key": "cool"}
                    ],
                    "sum_other_doc_count": 0
                }
            },
            {
                "key": "20-*",
                "doc_count": 2,
                "from": 20.0,
                "child_terms": {
                    "buckets": [
                        {"doc_count": 1, "key": "cool"},
                        {"doc_count": 1, "key": "nohit"}
                    ],
                    "sum_other_doc_count": 0
                }
            }
        ])
    );

    // Case B: child has more buckets than parent
    // Parent: histogram on score with large interval -> 1 bucket
    // Child: terms on text -> 2 buckets (cool/nohit)
    let agg_child_more: Aggregations = serde_json::from_value(json!({
        "parent_hist": {
            "histogram": {"field": "score", "interval": 100.0},
            "aggs": {
                "child_terms": {"terms": {"field": "text", "order": {"_key": "asc"}}}
            }
        }
    }))
    .unwrap();

    let res = crate::aggregation::tests::exec_request(agg_child_more, &index)?;
    assert_eq!(
        res["parent_hist"],
        json!({
            "buckets": [
                {
                    "key": 0.0,
                    "doc_count": 9,
                    "child_terms": {
                        "buckets": [
                            {"doc_count": 7, "key": "cool"},
                            {"doc_count": 2, "key": "nohit"}
                        ],
                        "sum_other_doc_count": 0
                    }
                }
            ]
        })
    );

    Ok(())
}

#[test]
fn test_range_as_subagg_parent_more_vs_child_more() -> crate::Result<()> {
    let index = get_test_index_2_segments(false)?;

    // Case A: parent has more buckets than child
    // Parent: range with 5 buckets
    // Child: coarse range with 3 buckets
    let agg_parent_more: Aggregations = serde_json::from_value(json!({
        "parent_range": {
            "range": {
                "field": "score",
                "ranges": [
                    {"to": 3.0},
                    {"from": 3.0, "to": 7.0},
                    {"from": 7.0, "to": 11.0},
                    {"from": 11.0, "to": 20.0},
                    {"from": 20.0}
                ]
            },
            "aggs": {
                "child_range": {
                    "range": {
                        "field": "score",
                        "ranges": [
                            {"to": 3.0},
                            {"from": 3.0, "to": 20.0}
                        ]
                    }
                }
            }
        }
    }))
    .unwrap();
    let res = crate::aggregation::tests::exec_request(agg_parent_more, &index)?;
    assert_eq!(
        res["parent_range"]["buckets"],
        json!([
            {"key": "*-3", "doc_count": 1, "to": 3.0,
                "child_range": {"buckets": [
                    {"key": "*-3", "doc_count": 1, "to": 3.0},
                    {"key": "3-20", "doc_count": 0, "from": 3.0, "to": 20.0},
                    {"key": "20-*", "doc_count": 0, "from": 20.0}
                ]}
            },
            {"key": "3-7", "doc_count": 3, "from": 3.0, "to": 7.0,
                "child_range": {"buckets": [
                    {"key": "*-3", "doc_count": 0, "to": 3.0},
                    {"key": "3-20", "doc_count": 3, "from": 3.0, "to": 20.0},
                    {"key": "20-*", "doc_count": 0, "from": 20.0}
                ]}
            },
            {"key": "7-11", "doc_count": 1, "from": 7.0, "to": 11.0,
                "child_range": {"buckets": [
                    {"key": "*-3", "doc_count": 0, "to": 3.0},
                    {"key": "3-20", "doc_count": 1, "from": 3.0, "to": 20.0},
                    {"key": "20-*", "doc_count": 0, "from": 20.0}
                ]}
            },
            {"key": "11-20", "doc_count": 2, "from": 11.0, "to": 20.0,
                "child_range": {"buckets": [
                    {"key": "*-3", "doc_count": 0, "to": 3.0},
                    {"key": "3-20", "doc_count": 2, "from": 3.0, "to": 20.0},
                    {"key": "20-*", "doc_count": 0, "from": 20.0}
                ]}
            },
            {"key": "20-*", "doc_count": 2, "from": 20.0,
                "child_range": {"buckets": [
                    {"key": "*-3", "doc_count": 0, "to": 3.0},
                    {"key": "3-20", "doc_count": 0, "from": 3.0, "to": 20.0},
                    {"key": "20-*", "doc_count": 2, "from": 20.0}
                ]}
            }
        ])
    );

    // Case B: child has more buckets than parent
    // Parent: terms on text (2 buckets)
    // Child: range with 4 buckets
    let agg_child_more: Aggregations = serde_json::from_value(json!({
        "parent_terms": {
            "terms": {"field": "text"},
            "aggs": {
                "child_range": {
                    "range": {
                        "field": "score",
                        "ranges": [
                            {"to": 3.0},
                            {"from": 3.0, "to": 7.0},
                            {"from": 7.0, "to": 20.0}
                        ]
                    }
                }
            }
        }
    }))
    .unwrap();
    let res = crate::aggregation::tests::exec_request(agg_child_more, &index)?;

    assert_eq!(
        res["parent_terms"],
        json!({
            "buckets": [
                {
                    "key": "cool",
                    "doc_count": 7,
                    "child_range": {
                        "buckets": [
                            {"key": "*-3", "doc_count": 1, "to": 3.0},
                            {"key": "3-7", "doc_count": 2, "from": 3.0, "to": 7.0},
                            {"key": "7-20", "doc_count": 3, "from": 7.0, "to": 20.0},
                            {"key": "20-*", "doc_count": 1, "from": 20.0}
                        ]
                    }
                },
                {
                    "key": "nohit",
                    "doc_count": 2,
                    "child_range": {
                        "buckets": [
                            {"key": "*-3", "doc_count": 0, "to": 3.0},
                            {"key": "3-7", "doc_count": 1, "from": 3.0, "to": 7.0},
                            {"key": "7-20", "doc_count": 0, "from": 7.0, "to": 20.0},
                            {"key": "20-*", "doc_count": 1, "from": 20.0}
                        ]
                    }
                }
            ],
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0
        })
    );

    Ok(())
}

#[test]
fn test_histogram_as_subagg_parent_more_vs_child_more() -> crate::Result<()> {
    let index = get_test_index_2_segments(false)?;

    // Case A: parent has more buckets than child
    // Parent: range with several ranges
    // Child: histogram with large interval (single bucket per parent)
    let agg_parent_more: Aggregations = serde_json::from_value(json!({
        "parent_range": {
            "range": {
                "field": "score",
                "ranges": [
                    {"to": 3.0},
                    {"from": 3.0, "to": 7.0},
                    {"from": 7.0, "to": 11.0},
                    {"from": 11.0, "to": 20.0},
                    {"from": 20.0}
                ]
            },
            "aggs": {
                "child_hist": {"histogram": {"field": "score", "interval": 100.0}}
            }
        }
    }))
    .unwrap();
    let res = crate::aggregation::tests::exec_request(agg_parent_more, &index)?;
    assert_eq!(
        res["parent_range"]["buckets"],
        json!([
            {"key": "*-3", "doc_count": 1, "to": 3.0,
                "child_hist": {"buckets": [ {"key": 0.0, "doc_count": 1} ]}
            },
            {"key": "3-7", "doc_count": 3, "from": 3.0, "to": 7.0,
                "child_hist": {"buckets": [ {"key": 0.0, "doc_count": 3} ]}
            },
            {"key": "7-11", "doc_count": 1, "from": 7.0, "to": 11.0,
                "child_hist": {"buckets": [ {"key": 0.0, "doc_count": 1} ]}
            },
            {"key": "11-20", "doc_count": 2, "from": 11.0, "to": 20.0,
                "child_hist": {"buckets": [ {"key": 0.0, "doc_count": 2} ]}
            },
            {"key": "20-*", "doc_count": 2, "from": 20.0,
                "child_hist": {"buckets": [ {"key": 0.0, "doc_count": 2} ]}
            }
        ])
    );

    // Case B: child has more buckets than parent
    // Parent: terms on text -> 2 buckets
    // Child: histogram with small interval -> multiple buckets including empties
    let agg_child_more: Aggregations = serde_json::from_value(json!({
        "parent_terms": {
            "terms": {"field": "text"},
            "aggs": {
                "child_hist": {"histogram": {"field": "score", "interval": 10.0}}
            }
        }
    }))
    .unwrap();
    let res = crate::aggregation::tests::exec_request(agg_child_more, &index)?;
    assert_eq!(
        res["parent_terms"],
        json!({
            "buckets": [
                {
                    "key": "cool",
                    "doc_count": 7,
                    "child_hist": {
                        "buckets": [
                            {"key": 0.0, "doc_count": 4},
                            {"key": 10.0, "doc_count": 2},
                            {"key": 20.0, "doc_count": 0},
                            {"key": 30.0, "doc_count": 0},
                            {"key": 40.0, "doc_count": 1}
                        ]
                    }
                },
                {
                    "key": "nohit",
                    "doc_count": 2,
                    "child_hist": {
                        "buckets": [
                            {"key": 0.0, "doc_count": 1},
                            {"key": 10.0, "doc_count": 0},
                            {"key": 20.0, "doc_count": 0},
                            {"key": 30.0, "doc_count": 0},
                            {"key": 40.0, "doc_count": 1}
                        ]
                    }
                }
            ],
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0
        })
    );

    Ok(())
}

#[test]
fn test_date_histogram_as_subagg_parent_more_vs_child_more() -> crate::Result<()> {
    let index = get_test_index_2_segments(false)?;

    // Case A: parent has more buckets than child
    // Parent: range with several buckets
    // Child: date_histogram with 30d -> single bucket per parent
    let agg_parent_more: Aggregations = serde_json::from_value(json!({
        "parent_range": {
            "range": {
                "field": "score",
                "ranges": [
                    {"to": 3.0},
                    {"from": 3.0, "to": 7.0},
                    {"from": 7.0, "to": 11.0},
                    {"from": 11.0, "to": 20.0},
                    {"from": 20.0}
                ]
            },
            "aggs": {
                "child_date_hist": {"date_histogram": {"field": "date", "fixed_interval": "30d"}}
            }
        }
    }))
    .unwrap();
    let res = crate::aggregation::tests::exec_request(agg_parent_more, &index)?;
    let buckets = res["parent_range"]["buckets"].as_array().unwrap();
    // Verify each parent bucket has exactly one child date bucket with matching doc_count
    for bucket in buckets {
        let parent_count = bucket["doc_count"].as_u64().unwrap();
        let child_buckets = bucket["child_date_hist"]["buckets"].as_array().unwrap();
        assert_eq!(child_buckets.len(), 1);
        assert_eq!(child_buckets[0]["doc_count"], parent_count);
    }

    // Case B: child has more buckets than parent
    // Parent: terms on text (2 buckets)
    // Child: date_histogram with 1d -> multiple buckets
    let agg_child_more: Aggregations = serde_json::from_value(json!({
        "parent_terms": {
            "terms": {"field": "text"},
            "aggs": {
                "child_date_hist": {"date_histogram": {"field": "date", "fixed_interval": "1d"}}
            }
        }
    }))
    .unwrap();
    let res = crate::aggregation::tests::exec_request(agg_child_more, &index)?;
    let buckets = res["parent_terms"]["buckets"].as_array().unwrap();

    // cool bucket
    assert_eq!(buckets[0]["key"], "cool");
    let cool_buckets = buckets[0]["child_date_hist"]["buckets"].as_array().unwrap();
    assert_eq!(cool_buckets.len(), 3);
    assert_eq!(cool_buckets[0]["doc_count"], 1); // day 0
    assert_eq!(cool_buckets[1]["doc_count"], 4); // day 1
    assert_eq!(cool_buckets[2]["doc_count"], 2); // day 2

    // nohit bucket
    assert_eq!(buckets[1]["key"], "nohit");
    let nohit_buckets = buckets[1]["child_date_hist"]["buckets"].as_array().unwrap();
    assert_eq!(nohit_buckets.len(), 2);
    assert_eq!(nohit_buckets[0]["doc_count"], 1); // day 1
    assert_eq!(nohit_buckets[1]["doc_count"], 1); // day 2

    Ok(())
}

fn get_avg_req(field_name: &str) -> Aggregation {
    serde_json::from_value(json!({
        "avg": {
            "field": field_name,
        }
    }))
    .unwrap()
}

fn get_collector(agg_req: Aggregations) -> AggregationCollector {
    AggregationCollector::from_aggs(agg_req, Default::default())
}

// *** EVERY BUCKET-TYPE SHOULD BE TESTED HERE ***
// Note: The flushng part of these  tests are outdated, since the buffering change after converting
// the collection into one collector per request instead of per bucket.
//
// However they are useful as they test a complex aggregation requests.
fn test_aggregation_flushing(
    merge_segments: bool,
    use_distributed_collector: bool,
) -> crate::Result<()> {
    let mut values_and_terms = (0..80)
        .map(|val| vec![(val as f64, "terma".to_string())])
        .collect::<Vec<_>>();
    values_and_terms.last_mut().unwrap()[0].1 = "termb".to_string();
    let index = get_test_index_from_values_and_terms(merge_segments, &values_and_terms)?;

    let reader = index.reader()?;

    assert_eq!(COLLECT_BLOCK_BUFFER_LEN, 64);
    // In the tree we cache documents of COLLECT_BLOCK_BUFFER_LEN before passing them down as one
    // block.
    //
    // Build a request so that on the first level we have one full cache, which is then flushed.
    // The same cache should have some residue docs at the end, which are flushed (Range 0-70)
    // -> 70 docs
    //
    // The second level should also have some residue docs in the cache that are flushed at the
    // end.
    //
    // A second bucket on the first level should have the cache unfilled

    // let elasticsearch_compatible_json_req = r#"
    let elasticsearch_compatible_json = json!(
    {
    "bucketsL1": {
        "range": {
            "field": "score",
            "ranges": [ { "to": 3.0f64 }, { "from": 3.0f64, "to": 70.0f64 }, { "from": 70.0f64 } ]
        },
        "aggs": {
            "bucketsL2": {
                "range": {
                    "field": "score",
                    "ranges": [ { "to": 30.0f64 }, { "from": 30.0f64, "to": 70.0f64 }, { "from": 70.0f64 } ]
                }
            }
        }
    },
    "top_hits_test":{
        "terms": {
            "field": "string_id"
        },
        "aggs": {
            "bucketsL2": {
                "top_hits": {
                    "size": 2,
                    "sort": [
                        { "score": "asc" }
                    ],
                    "docvalue_fields": ["score"]
                }
            }
        }
    },
    "histogram_test":{
        "histogram": {
            "field": "score",
            "interval":  70.0,
            "offset": 3.0
        },
        "aggs": {
            "bucketsL2": {
                "histogram": {
                    "field": "score",
                    "interval":  70.0
                }
            }
        }
    },
    "term_agg_test":{
        "terms": {
            "field": "string_id"
        },
        "aggs": {
            "bucketsL2": {
                "histogram": {
                    "field": "score",
                    "interval":  70.0
                }
            }
        }
    },
    "cardinality_string_id":{
        "cardinality": {
            "field": "string_id"
        }
    },
    "cardinality_score":{
        "cardinality": {
            "field": "score"
        }
    }
    });

    let agg_req: Aggregations =
        serde_json::from_str(&serde_json::to_string(&elasticsearch_compatible_json).unwrap())
            .unwrap();

    let agg_res: AggregationResults = if use_distributed_collector {
        let collector =
            DistributedAggregationCollector::from_aggs(agg_req.clone(), Default::default());

        let searcher = reader.searcher();
        let intermediate_agg_result = searcher.search(&AllQuery, &collector).unwrap();

        // Test postcard roundtrip serialization
        let intermediate_agg_result_bytes = postcard::to_allocvec(&intermediate_agg_result).expect(
            "Postcard Serialization failed, flatten etc. is not supported in the intermediate \
             result",
        );
        let intermediate_agg_result: IntermediateAggregationResults =
            postcard::from_bytes(&intermediate_agg_result_bytes)
                .expect("Post deserialization failed");

        intermediate_agg_result
            .into_final_result(agg_req, Default::default())
            .unwrap()
    } else {
        let collector = get_collector(agg_req);

        let searcher = reader.searcher();
        searcher.search(&AllQuery, &collector).unwrap()
    };

    let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;

    assert_eq!(res["bucketsL1"]["buckets"][0]["doc_count"], 3);
    assert_eq!(
        res["bucketsL1"]["buckets"][0]["bucketsL2"]["buckets"][0]["doc_count"],
        3
    );
    assert_eq!(res["bucketsL1"]["buckets"][1]["key"], "3-70");
    assert_eq!(res["bucketsL1"]["buckets"][1]["doc_count"], 70 - 3);
    assert_eq!(
        res["bucketsL1"]["buckets"][1]["bucketsL2"]["buckets"][0]["doc_count"],
        27
    );
    assert_eq!(
        res["bucketsL1"]["buckets"][1]["bucketsL2"]["buckets"][1]["doc_count"],
        40
    );
    assert_eq!(
        res["bucketsL1"]["buckets"][1]["bucketsL2"]["buckets"][2]["doc_count"],
        0
    );
    assert_eq!(
        res["bucketsL1"]["buckets"][2]["bucketsL2"]["buckets"][2]["doc_count"],
        80 - 70
    );
    assert_eq!(res["bucketsL1"]["buckets"][2]["doc_count"], 80 - 70);

    assert_eq!(
        res["term_agg_test"],
        json!(
        {
            "buckets": [
              {
                "bucketsL2": {
                  "buckets": [
                    {
                      "doc_count": 70,
                      "key": 0.0
                    },
                    {
                      "doc_count": 9,
                      "key": 70.0
                    }
                  ]
                },
                "doc_count": 79,
                "key": "terma"
              },
              {
                "bucketsL2": {
                  "buckets": [
                    {
                      "doc_count": 1,
                      "key": 70.0
                    }
                  ]
                },
                "doc_count": 1,
                "key": "termb"
              }
            ],
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0
          }
        )
    );

    assert_eq!(res["cardinality_string_id"]["value"], 2.0);
    assert_eq!(res["cardinality_score"]["value"], 80.0);

    Ok(())
}

#[test]
fn test_aggregation_flushing_variants() {
    test_aggregation_flushing(false, false).unwrap();
    test_aggregation_flushing(false, true).unwrap();
    test_aggregation_flushing(true, false).unwrap();
    test_aggregation_flushing(true, true).unwrap();
}

// Regression test for https://github.com/quickwit-oss/tantivy/issues/2992
//
// A skewed terms bucket over <100 terms uses the low-cardinality (Vec) sub-agg buffer. A dominant
// term keeps crossing the periodic flush threshold (every 2048 docs), which used to drop the
// cached doc ids of the minority buckets before collecting them — corrupting their metric
// sub-aggregations while their doc counts stayed exact.
#[test]
fn test_terms_sub_agg_flushing_skewed_buckets() -> crate::Result<()> {
    use std::collections::HashMap;

    // 89 minority terms + 1 dominant term = 90 distinct terms, staying below
    // MAX_NUM_TERMS_FOR_VEC (100) so the low-cardinality Vec sub-agg buffer is used.
    const NUM_MINORITY_TERMS: usize = 89;

    let mut values: Vec<(f64, String)> = Vec::new();
    let mut minority_idx = 0usize;
    // Enough docs to cross the 2048 flush threshold multiple times. The minority docs are
    // sprinkled among the dominant ones so they land in different flush windows.
    for i in 0..5000u64 {
        if i % 25 == 0 {
            let term = format!("minority_{:02}", minority_idx % NUM_MINORITY_TERMS);
            minority_idx += 1;
            values.push(((i % 13 + 1) as f64, term));
        } else {
            values.push((7.0, "dominant".to_string()));
        }
    }

    let mut truth: HashMap<String, (u64, f64)> = HashMap::new();
    for (score, term) in &values {
        let entry = truth.entry(term.clone()).or_insert((0, 0.0));
        entry.0 += 1;
        entry.1 += *score;
    }
    // Sanity check on the shape of the generated data.
    assert_eq!(truth.len(), NUM_MINORITY_TERMS + 1);

    let index = get_test_index_from_values_and_terms(false, &[values])?;
    let reader = index.reader()?;

    let agg_req: Aggregations = serde_json::from_value(json!({
        "my_terms": {
            "terms": { "field": "string_id", "size": 100 },
            "aggs": {
                "sum_score": { "sum": { "field": "score" } }
            }
        }
    }))
    .unwrap();

    let collector = get_collector(agg_req);
    let searcher = reader.searcher();
    let agg_res = searcher.search(&AllQuery, &collector)?;
    let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;

    let buckets = res["my_terms"]["buckets"].as_array().unwrap();
    // size 100 >= 90 distinct terms, so every bucket is returned.
    assert_eq!(buckets.len(), truth.len());
    for bucket in buckets {
        let key = bucket["key"].as_str().unwrap();
        let (true_count, true_sum) = truth[key];
        assert_eq!(
            bucket["doc_count"].as_u64().unwrap(),
            true_count,
            "doc_count mismatch for {key}"
        );
        assert_eq!(
            bucket["sum_score"]["value"].as_f64().unwrap(),
            true_sum,
            "sum sub-agg mismatch for {key}"
        );
    }

    Ok(())
}

#[test]
fn test_aggregation_level1_simple() -> crate::Result<()> {
    let index = get_test_index_2_segments(true)?;

    let reader = index.reader()?;
    let text_field = reader.searcher().schema().get_field("text").unwrap();

    let term_query = TermQuery::new(
        Term::from_field_text(text_field, "cool"),
        IndexRecordOption::Basic,
    );

    let range_agg = |field_name: &str| -> Aggregation {
        serde_json::from_value(json!({
            "range": {
                "field": field_name,
                "ranges": [ { "from": 3.0f64, "to": 7.0f64 }, { "from": 7.0f64, "to": 20.0f64 } ]
            }
        }))
        .unwrap()
    };

    let agg_req_1: Aggregations = vec![
        ("average".to_string(), get_avg_req("score")),
        ("range".to_string(), range_agg("score")),
    ]
    .into_iter()
    .collect();

    let collector = get_collector(agg_req_1);

    let searcher = reader.searcher();
    let agg_res: AggregationResults = searcher.search(&term_query, &collector).unwrap();

    let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
    assert_eq!(res["average"]["value"], 12.142857142857142);
    assert_eq!(
        res["range"]["buckets"],
        json!(
        [
        {
          "key": "*-3",
          "doc_count": 1,
          "to": 3.0
        },
        {
          "key": "3-7",
          "doc_count": 2,
          "from": 3.0,
          "to": 7.0
        },
        {
          "key": "7-20",
          "doc_count": 3,
          "from": 7.0,
          "to": 20.0
        },
        {
          "key": "20-*",
          "doc_count": 1,
          "from": 20.0
        }
        ])
    );

    Ok(())
}

#[test]
fn test_aggregation_level1() -> crate::Result<()> {
    let index = get_test_index_2_segments(true)?;

    let reader = index.reader()?;
    let text_field = reader.searcher().schema().get_field("text").unwrap();

    let term_query = TermQuery::new(
        Term::from_field_text(text_field, "cool"),
        IndexRecordOption::Basic,
    );

    let range_agg = |field_name: &str| -> Aggregation {
        serde_json::from_value(json!({
            "range": {
                "field": field_name,
                "ranges": [ { "from": 3.0f64, "to": 7.0f64 }, { "from": 7.0f64, "to": 20.0f64 } ]
            }
        }))
        .unwrap()
    };

    let agg_req_1: Aggregations = vec![
        ("average_i64".to_string(), get_avg_req("score_i64")),
        ("average_f64".to_string(), get_avg_req("score_f64")),
        ("average".to_string(), get_avg_req("score")),
        ("range".to_string(), range_agg("score")),
        ("rangef64".to_string(), range_agg("score_f64")),
        ("rangei64".to_string(), range_agg("score_i64")),
    ]
    .into_iter()
    .collect();

    let collector = get_collector(agg_req_1);

    let searcher = reader.searcher();
    let agg_res: AggregationResults = searcher.search(&term_query, &collector).unwrap();

    let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;
    assert_eq!(res["average"]["value"], 12.142857142857142);
    assert_eq!(res["average_f64"]["value"], 12.214285714285714);
    assert_eq!(res["average_i64"]["value"], 12.142857142857142);
    assert_eq!(
        res["range"]["buckets"],
        json!(
        [
        {
          "key": "*-3",
          "doc_count": 1,
          "to": 3.0
        },
        {
          "key": "3-7",
          "doc_count": 2,
          "from": 3.0,
          "to": 7.0
        },
        {
          "key": "7-20",
          "doc_count": 3,
          "from": 7.0,
          "to": 20.0
        },
        {
          "key": "20-*",
          "doc_count": 1,
          "from": 20.0
        }
        ])
    );

    Ok(())
}

fn test_aggregation_level2(
    merge_segments: bool,
    use_distributed_collector: bool,
) -> crate::Result<()> {
    let index = get_test_index_2_segments(merge_segments)?;

    let reader = index.reader()?;
    let text_field = reader.searcher().schema().get_field("text").unwrap();

    let term_query = TermQuery::new(
        Term::from_field_text(text_field, "cool"),
        IndexRecordOption::Basic,
    );

    let query_with_no_hits = TermQuery::new(
        Term::from_field_text(text_field, "thistermdoesnotexist"),
        IndexRecordOption::Basic,
    );

    let elasticsearch_compatible_json_req = r#"
{
  "rangef64": {
    "range": {
      "field": "score_f64",
      "ranges": [
        { "to": 3.0 },
        { "from": 3.0, "to": 7.0 },
        { "from": 7.0, "to": 19.0 },
        { "from": 19.0, "to": 20.0 },
        { "from": 20.0 }
      ]
    },
    "aggs": {
      "average_in_range": { "avg": { "field": "score" } },
      "term_agg": { "terms": { "field": "text" } }
    }
  },
  "rangei64": {
    "range": {
      "field": "score_i64",
      "ranges": [
        { "to": 3.0 },
        { "from": 3.0, "to": 7.0 },
        { "from": 7.0, "to": 19.0 },
        { "from": 19.0, "to": 20.0 },
        { "from": 20.0 }
      ]
    },
    "aggs": {
      "average_in_range": { "avg": { "field": "score" } },
      "term_agg": { "terms": { "field": "text" } }
    }
  },
  "average": {
    "avg": { "field": "score" }
  },
  "range": {
    "range": {
      "field": "score",
      "ranges": [
        { "to": 3.0 },
        { "from": 3.0, "to": 7.0 },
        { "from": 7.0, "to": 19.0 },
        { "from": 19.0, "to": 20.0 },
        { "from": 20.0 }
      ]
    },
    "aggs": {
      "average_in_range": { "avg": { "field": "score" } },
      "term_agg": { "terms": { "field": "text" } }
    }
  }
}
"#;
    let agg_req: Aggregations = serde_json::from_str(elasticsearch_compatible_json_req).unwrap();

    let agg_res: AggregationResults = if use_distributed_collector {
        let collector =
            DistributedAggregationCollector::from_aggs(agg_req.clone(), Default::default());

        let searcher = reader.searcher();
        let res = searcher.search(&term_query, &collector).unwrap();
        res.into_final_result(agg_req.clone(), Default::default())
            .unwrap()
    } else {
        let collector = get_collector(agg_req.clone());

        let searcher = reader.searcher();
        searcher.search(&term_query, &collector).unwrap()
    };

    let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;

    assert_eq!(res["range"]["buckets"][1]["key"], "3-7");
    assert_eq!(res["range"]["buckets"][1]["doc_count"], 2u64);
    assert_eq!(res["rangef64"]["buckets"][1]["doc_count"], 2u64);
    assert_eq!(res["rangei64"]["buckets"][1]["doc_count"], 2u64);

    assert_eq!(res["average"]["value"], 12.142857142857142f64);
    assert_eq!(res["range"]["buckets"][2]["key"], "7-19");
    assert_eq!(res["range"]["buckets"][2]["doc_count"], 3u64);
    assert_eq!(res["rangef64"]["buckets"][2]["doc_count"], 3u64);
    assert_eq!(res["rangei64"]["buckets"][2]["doc_count"], 3u64);
    assert_eq!(res["rangei64"]["buckets"][5], serde_json::Value::Null);

    assert_eq!(res["range"]["buckets"][4]["key"], "20-*");
    assert_eq!(res["range"]["buckets"][4]["doc_count"], 1u64);
    assert_eq!(res["rangef64"]["buckets"][4]["doc_count"], 1u64);
    assert_eq!(res["rangei64"]["buckets"][4]["doc_count"], 1u64);

    assert_eq!(res["range"]["buckets"][3]["key"], "19-20");
    assert_eq!(res["range"]["buckets"][3]["doc_count"], 0u64);
    assert_eq!(res["rangef64"]["buckets"][3]["doc_count"], 0u64);
    assert_eq!(res["rangei64"]["buckets"][3]["doc_count"], 0u64);

    assert_eq!(
        res["range"]["buckets"][3]["average_in_range"]["value"],
        serde_json::Value::Null
    );

    assert_eq!(
        res["range"]["buckets"][4]["average_in_range"]["value"],
        44.0f64
    );
    assert_eq!(
        res["rangef64"]["buckets"][4]["average_in_range"]["value"],
        44.0f64
    );
    assert_eq!(
        res["rangei64"]["buckets"][4]["average_in_range"]["value"],
        44.0f64
    );

    assert_eq!(
        res["range"]["7-19"]["average_in_range"]["value"],
        res["rangef64"]["7-19"]["average_in_range"]["value"]
    );
    assert_eq!(
        res["range"]["7-19"]["average_in_range"]["value"],
        res["rangei64"]["7-19"]["average_in_range"]["value"]
    );

    // Test empty result set
    let collector = get_collector(agg_req);
    let searcher = reader.searcher();
    searcher.search(&query_with_no_hits, &collector).unwrap();

    Ok(())
}

#[test]
fn test_aggregation_level2_multi_segments() -> crate::Result<()> {
    test_aggregation_level2(false, false)
}

#[test]
fn test_aggregation_level2_single_segment() -> crate::Result<()> {
    test_aggregation_level2(true, false)
}

#[test]
fn test_aggregation_level2_multi_segments_distributed_collector() -> crate::Result<()> {
    test_aggregation_level2(false, true)
}

#[test]
fn test_aggregation_level2_single_segment_distributed_collector() -> crate::Result<()> {
    test_aggregation_level2(true, true)
}

#[test]
fn test_aggregation_invalid_requests() -> crate::Result<()> {
    let index = get_test_index_2_segments(false)?;

    let reader = index.reader()?;

    let avg_on_field = |field_name: &str| {
        let agg_req_1: Aggregations = serde_json::from_value(json!({
            "average": {
                "avg": {
                    "field": field_name,
                },
            }
        }))
        .unwrap();

        let collector = get_collector(agg_req_1);

        let searcher = reader.searcher();

        searcher.search(&AllQuery, &collector)
    };

    let agg_res = avg_on_field("dummy_text").unwrap_err();
    assert_eq!(
        format!("{agg_res:?}"),
        r#"InvalidArgument("Field \"dummy_text\" is not configured as fast field")"#
    );

    let agg_req_1: Result<Aggregations, serde_json::Error> = serde_json::from_value(json!({
        "average": {
            "avg": {
                "fieldd": "a",
            },
        }
    }));

    assert_eq!(agg_req_1.is_err(), true);
    assert_eq!(agg_req_1.unwrap_err().to_string(), "missing field `field`");

    let agg_req_1: Result<Aggregations, serde_json::Error> = serde_json::from_value(json!({
        "average": {
            "doesnotmatchanyagg": {
                "field": "a",
            },
        }
    }));

    assert_eq!(agg_req_1.is_err(), true);
    // TODO: This should list valid values
    assert!(agg_req_1
        .unwrap_err()
        .to_string()
        .contains("unknown variant `doesnotmatchanyagg`, expected one of"));

    // TODO: This should return an error
    // let agg_res = avg_on_field("not_exist_field").unwrap_err();
    // assert_eq!(
    // format!("{:?}", agg_res),
    // r#"InvalidArgument("No fast field found for field: not_exist_field")"#
    //);

    // TODO: This should return an error
    // let agg_res = avg_on_field("ip_addr").unwrap_err();
    // assert_eq!(
    // format!("{:?}", agg_res),
    // r#"InvalidArgument("No fast field found for field: ip_addr")"#
    //);

    Ok(())
}

#[test]
fn test_aggregation_on_json_object() {
    let mut schema_builder = Schema::builder();
    let json = schema_builder.add_json_field("json", FAST);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);
    let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
    index_writer
        .add_document(doc!(json => json!({"color": "red"})))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"color": "red"})))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"color": "blue"})))
        .unwrap();
    index_writer.commit().unwrap();
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();

    let agg: Aggregations = serde_json::from_value(json!({
        "jsonagg": {
            "terms": {
                "field": "json.color",
            }
        }
    }))
    .unwrap();

    let aggregation_collector = get_collector(agg);
    let aggregation_results = searcher.search(&AllQuery, &aggregation_collector).unwrap();
    let aggregation_res_json = serde_json::to_value(aggregation_results).unwrap();
    assert_eq!(
        &aggregation_res_json,
        &serde_json::json!({
            "jsonagg": {
                "buckets": [
                    {"doc_count": 2, "key": "red"},
                    {"doc_count": 1, "key": "blue"},
                ],
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0
            }
        })
    );
}

#[test]
fn test_aggregation_on_nested_json_object() {
    let mut schema_builder = Schema::builder();
    let json = schema_builder.add_json_field("json.blub", FAST);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);
    let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
    index_writer
        .add_document(doc!(json => json!({"color.dot": "red", "color": {"nested":"red"} })))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"color.dot": "blue", "color": {"nested":"blue"} })))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"color.dot": "blue", "color": {"nested":"blue"} })))
        .unwrap();
    index_writer.commit().unwrap();
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();

    let agg: Aggregations = serde_json::from_value(json!({
        "jsonagg1": {
            "terms": {
                "field": "json\\.blub.color\\.dot",
            }
        },
        "jsonagg2": {
            "terms": {
                "field": "json\\.blub.color.nested",
            }
        }

    }))
    .unwrap();

    let aggregation_collector = get_collector(agg);
    let aggregation_results = searcher.search(&AllQuery, &aggregation_collector).unwrap();
    let aggregation_res_json = serde_json::to_value(aggregation_results).unwrap();
    assert_eq!(
        &aggregation_res_json,
        &serde_json::json!({
            "jsonagg1": {
                "buckets": [
                    {"doc_count": 2, "key": "blue"},
                    {"doc_count": 1, "key": "red"}
                ],
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0
            },
            "jsonagg2": {
                "buckets": [
                    {"doc_count": 2, "key": "blue"},
                    {"doc_count": 1, "key": "red"}
                ],
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0
            }

        })
    );
}

#[test]
fn test_aggregation_on_json_object_empty_columns() {
    let mut schema_builder = Schema::builder();
    let json = schema_builder.add_json_field("json", FAST);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);
    let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
    // => Empty column when accessing color
    index_writer
        .add_document(doc!(json => json!({"price": 10.0})))
        .unwrap();
    index_writer.commit().unwrap();
    // => Empty column when accessing price
    index_writer
        .add_document(doc!(json => json!({"color": "blue"})))
        .unwrap();
    index_writer.commit().unwrap();

    // => Non Empty columns
    index_writer
        .add_document(doc!(json => json!({"color": "red", "price": 10.0})))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"color": "red", "price": 10.0})))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"color": "green", "price": 20.0})))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"color": "green", "price": 20.0})))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"color": "green", "price": 20.0})))
        .unwrap();

    index_writer.commit().unwrap();

    let reader = index.reader().unwrap();
    let searcher = reader.searcher();

    let agg: Aggregations = serde_json::from_value(json!({
        "jsonagg": {
            "terms": {
                "field": "json.color",
            }
        }
    }))
    .unwrap();

    let aggregation_collector = get_collector(agg);
    let aggregation_results = searcher.search(&AllQuery, &aggregation_collector).unwrap();
    let aggregation_res_json = serde_json::to_value(aggregation_results).unwrap();
    assert_eq!(
        &aggregation_res_json,
        &serde_json::json!({
            "jsonagg": {
                "buckets": [
                    {"doc_count": 3, "key": "green"},
                    {"doc_count": 2, "key": "red"},
                    {"doc_count": 1, "key": "blue"}
                ],
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0
            }
        })
    );

    let agg_req_str = r#"
    {
      "jsonagg": {
        "aggs": {
          "min_price": { "min": { "field": "json.price" } }
        },
        "terms": {
          "field": "json.color",
          "order": { "min_price": "desc" }
        }
      }
    } "#;
    let agg: Aggregations = serde_json::from_str(agg_req_str).unwrap();
    let aggregation_collector = get_collector(agg);
    let aggregation_results = searcher.search(&AllQuery, &aggregation_collector).unwrap();
    let aggregation_res_json = serde_json::to_value(aggregation_results).unwrap();
    assert_eq!(
        &aggregation_res_json,
        &serde_json::json!(
            {
              "jsonagg": {
                "buckets": [
                  {
                    "key": "green",
                    "doc_count": 3,
                    "min_price": {
                      "value": 20.0
                    }
                  },
                  {
                    "key": "red",
                    "doc_count": 2,
                    "min_price": {
                      "value": 10.0
                    }
                  },
                  {
                    "key": "blue",
                    "doc_count": 1,
                    "min_price": {
                      "value": null
                    }
                  }
                ],
                "sum_other_doc_count": 0
              }
            }
        )
    );
}

#[test]
fn test_aggregation_on_json_object_mixed_types() {
    let mut schema_builder = Schema::builder();
    let json = schema_builder.add_json_field("json", FAST);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);
    let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
    // => Segment with all values numeric
    index_writer
        .add_document(doc!(json => json!({"mixed_type": 10.0, "mixed_price": 10.0})))
        .unwrap();
    index_writer.commit().unwrap();
    // => Segment with all values text
    index_writer
        .add_document(doc!(json => json!({"mixed_type": "blue", "mixed_price": 5.0})))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"mixed_type": "blue", "mixed_price": 5.0})))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"mixed_type": "blue", "mixed_price": 5.0})))
        .unwrap();
    index_writer.commit().unwrap();
    // => Segment with all boolean
    index_writer
        .add_document(doc!(json => json!({"mixed_type": true, "mixed_price": "no_price"})))
        .unwrap();
    index_writer.commit().unwrap();

    // => Segment with mixed values
    index_writer
        .add_document(doc!(json => json!({"mixed_type": "red", "mixed_price": 1.0})))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"mixed_type": "red", "mixed_price": 1.0})))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"mixed_type": -20.5, "mixed_price": -20.5})))
        .unwrap();
    index_writer
        .add_document(doc!(json => json!({"mixed_type": true, "mixed_price": "no_price"})))
        .unwrap();

    index_writer.commit().unwrap();

    // All bucket types
    let agg_req_str = r#"
    {
        "termagg": {
            "terms": {
                "field": "json.mixed_type",
                "order": { "min_price": "desc" }
            },
            "aggs": {
                "min_price": { "min": { "field": "json.mixed_price" } }
            }
        },
        "rangeagg": {
            "range": {
                "field": "json.mixed_type",
                "ranges": [
                    { "to": 3.0 },
                    { "from": 19.0, "to": 20.0 },
                    { "from": 20.0 }
                ]
            },
            "aggs": {
                "average_in_range": { "avg": { "field": "json.mixed_type" } }
            }
        }
    } "#;
    let agg: Aggregations = serde_json::from_str(agg_req_str).unwrap();
    let aggregation_collector = get_collector(agg);
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();

    let aggregation_results = searcher.search(&AllQuery, &aggregation_collector).unwrap();
    let aggregation_res_json = serde_json::to_value(aggregation_results).unwrap();
    use pretty_assertions::assert_eq;
    assert_eq!(
        &aggregation_res_json,
        &serde_json::json!({
          "rangeagg": {
            "buckets": [
              { "average_in_range": { "value": -20.5 }, "doc_count": 1, "key": "*-3", "to": 3.0 },
              { "average_in_range": { "value": 10.0 }, "doc_count": 1, "from": 3.0, "key": "3-19", "to": 19.0 },
              { "average_in_range": { "value": null }, "doc_count": 0, "from": 19.0, "key": "19-20", "to": 20.0 },
              { "average_in_range": { "value": null }, "doc_count": 0, "from": 20.0, "key": "20-*" }
            ]
          },
          "termagg": {
            "buckets": [
              { "doc_count": 1, "key": 10, "min_price": { "value": 10.0 } },
              { "doc_count": 3, "key": "blue", "min_price": { "value": 5.0 } },
              { "doc_count": 2, "key": "red", "min_price": { "value": 1.0 } },
              { "doc_count": 1, "key": -20.5, "min_price": { "value": -20.5 } },
              { "doc_count": 2, "key": 1, "key_as_string": "true", "min_price": { "value": null } },
            ],
            "sum_other_doc_count": 0
          }
        }
        )
    );
}

#[test]
fn test_aggregation_on_json_object_mixed_numerical_segments() {
    let mut schema_builder = Schema::builder();
    let json = schema_builder.add_json_field("json", FAST);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);
    let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
    // => Segment with all values f64 numeric
    index_writer
        .add_document(doc!(json => json!({"mixed_price": 10.5})))
        .unwrap();
    // Gets converted to f64!
    index_writer
        .add_document(doc!(json => json!({"mixed_price": 10})))
        .unwrap();
    index_writer.commit().unwrap();
    // => Segment with all values i64 numeric
    index_writer
        .add_document(doc!(json => json!({"mixed_price": 10})))
        .unwrap();
    index_writer.commit().unwrap();

    index_writer.commit().unwrap();

    // All bucket types
    let agg_req_str = r#"
    {
        "termagg": {
            "terms": {
                "field": "json.mixed_price"
            }
        }
    } "#;
    let agg: Aggregations = serde_json::from_str(agg_req_str).unwrap();
    let aggregation_collector = get_collector(agg);
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();

    let aggregation_results = searcher.search(&AllQuery, &aggregation_collector).unwrap();
    let aggregation_res_json = serde_json::to_value(aggregation_results).unwrap();
    use pretty_assertions::assert_eq;
    assert_eq!(
        &aggregation_res_json,
        &serde_json::json!({
          "termagg": {
            "buckets": [
              { "doc_count": 2, "key": 10},
              { "doc_count": 1, "key": 10.5},
            ],
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0
          }
        }
        )
    );
}
