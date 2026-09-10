# Contributing

When adding new bucket aggregation make sure to extend the "test_aggregation_flushing" test for at least 2 levels.



# Code Organization

Tantivy's aggregations have been designed to mimic the 
[aggregations of elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html).

The code is organized in submodules:

## bucket
Contains all bucket aggregations, like range aggregation. These bucket aggregations group documents into buckets and can contain sub-aggregations.

## metric
Contains all metric aggregations, like average aggregation. Metric aggregations do not have sub aggregations.

#### agg_req
agg_req contains the users aggregation request. Deserialization from json is compatible with elasticsearch aggregation requests.

#### agg_data
agg_data contains the users aggregation request enriched with fast field accessors etc, which are
used during collection.

#### segment_agg_result
segment_agg_result contains the aggregation result tree, which is used for collection of a segment.
agg_data is passed during collection.

#### intermediate_agg_result
intermediate_agg_result contains the aggregation tree for merging with other trees.

#### agg_result
agg_result contains the final aggregation tree.
