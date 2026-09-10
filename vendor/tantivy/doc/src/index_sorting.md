
- [Index Sorting](#index-sorting)
  - [Why Sorting](#why-sorting)
    - [Compression](#compression)
    - [Top-N Optimization](#top-n-optimization)
    - [Pruning](#pruning)
    - [Other](#other)
  - [Usage](#usage)

# Index Sorting has been removed!
More infos here:

https://github.com/quickwit-oss/tantivy/issues/2352

# Index Sorting

Tantivy allows you to sort the index according to a property.

## Why Sorting

Presorting an index has several advantages:

### Compression

When data is sorted it is easier to compress the data. E.g. the numbers sequence [5, 2, 3, 1, 4] would be sorted to [1, 2, 3, 4, 5].
If we apply delta encoding this list would be unsorted [5, -3, 1, -2, 3] vs. [1, 1, 1, 1, 1].
Compression ratio is mainly affected on the fast field of the sorted property, every thing else is likely unaffected.

### Top-N Optimization

When data is presorted by a field and search queries request sorting by the same field, we can leverage the natural order of the documents.
E.g. if the data is sorted by timestamp and want the top n newest docs containing a term, we can simply leveraging the order of the docids.

Note: tantivy 0.16 does not do this optimization yet.

### Pruning

Let's say we want all documents and want to apply the filter `>= 2010-08-11`. When the data is sorted, we could make a lookup in the fast field to find the docid range and use this as the filter.

Note: tantivy 0.16 does not do this optimization yet.

### Other?

In principle there are many algorithms possible that exploit the monotonically increasing nature. (aggregations maybe?)

## Usage

The index sorting can be configured setting [`sort_by_field`](https://github.com/quickwit-oss/tantivy/blob/000d76b11a139a84b16b9b95060a1c93e8b9851c/src/core/index_meta.rs#L238) on `IndexSettings` and passing it to a `IndexBuilder`. As of tantivy 0.16 only fast fields are allowed to be used.

```rust
let settings = IndexSettings {
    sort_by_field: Some(IndexSortByField {
        field: "intval".to_string(),
        order: Order::Desc,
    }),
    ..Default::default()
};
let mut index_builder = Index::builder().schema(schema);
index_builder = index_builder.settings(settings);
let index = index_builder.create_in_ram().unwrap();
```

## Implementation details

Sorting an index is applied in the serialization step. In general there are two serialization steps: [Finishing a single segment](https://github.com/quickwit-oss/tantivy/blob/000d76b11a139a84b16b9b95060a1c93e8b9851c/src/indexer/segment_writer.rs#L338) and [merging multiple segments](https://github.com/quickwit-oss/tantivy/blob/000d76b11a139a84b16b9b95060a1c93e8b9851c/src/indexer/merger.rs#L1073).

In both cases we generate a docid mapping reflecting the sort. This mapping is used when serializing the different components (doc store, fastfields, posting list, normfield, facets).
