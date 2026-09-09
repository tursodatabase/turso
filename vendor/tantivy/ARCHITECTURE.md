# Tantivy

## What is tantivy?

Tantivy is a library that is meant to build search engines. Although it is by no means a port of Lucene, its architecture is strongly inspired by it. If you are familiar with Lucene, you may be struck by the overlapping vocabulary.
This is not fortuitous.

Tantivy's bread and butter is to address the problem of full-text search :

Given a large set of textual documents, and a text query, return the K-most relevant documents in a very efficient way. To execute these queries rapidly, the tantivy needs to build an index beforehand. The relevance score implemented in the tantivy is not configurable. Tantivy uses the same score as the default similarity used in Lucene / Elasticsearch, called [BM25](https://en.wikipedia.org/wiki/Okapi_BM25).

But tantivy's scope does not stop there. Numerous features are required to power rich-search applications. For instance, one may want to:

- compute the count of documents matching a query in the different section of an e-commerce website,
- display an average price per meter square for a real estate search engine,
- take into account historical user data to rank documents in a specific way,
- or even use tantivy to power an OLAP database.

A more abstract description of the problem space tantivy is trying to address is the following.

Ingest a large set of documents, create an index that makes it possible to
rapidly select all documents matching a given predicate (also known as a query) and
collect some information about them ([See collector](#collector-define-what-to-do-with-matched-documents)).

Roughly speaking the design is following these guiding principles:

- Search should be O(1) in memory.
- Indexing should be O(1) in memory. (In practice it is just sublinear)
- Search should be as fast as possible

This comes at the cost of the dynamicity of the index: while it is possible to add, and delete documents from our corpus, the tantivy is designed to handle these updates in large batches.

## [core/](src/core): Index, segments, searchers

Core contains all of the high-level code to make it possible to create an index, add documents, delete documents and commit.

This is both the most high-level part of tantivy, the least performance-sensitive one, the seemingly most mundane code... And paradoxically the most complicated part.

### Index and Segments

A tantivy index is a collection of smaller independent immutable segments.
Each segment contains its own independent set of data structures.

A segment is identified by a segment id that is in fact a UUID.
The file of a segment has the format

 ```segment-id . ext```

The extension signals which data structure (or [`SegmentComponent`](src/index/segment_component.rs)) is stored in the file.

A small `meta.json` file is in charge of keeping track of the list of segments, as well as the schema.

On commit, one segment per indexing thread is written to disk, and the `meta.json` is then updated atomically.

For a better idea of how indexing works, you may read the [following blog post](https://fulmicoton.com/posts/behold-tantivy-part2/).

### Deletes

Deletes happen by deleting a "term". Tantivy does not offer any notion of primary id, so it is up to the user to use a field in their schema as if it was a primary id, and delete the associated term if they want to delete only one specific document.

On commit, tantivy will find all of the segments with documents matching this existing term and remove from [alive bitset file](src/fastfield/alive_bitset.rs) that represents the bitset of the alive document ids.
Like all segment files, this file is immutable. Because it is possible to have more than one alive bitset file at a given instant, the alive bitset filename has the format ```segment_id . commit_opstamp . del```.

An opstamp is simply an incremental id that identifies any operation applied to the index. For instance, performing a commit or adding a document.

### DocId

Within a segment, all documents are identified by a DocId that ranges within `[0, max_doc)`.
where `max_doc` is the number of documents in the segment, (deleted or not). Having such a compact `DocId` space is key to the compression of our data structures.

The DocIds are simply allocated in the order documents are added to the index.

### Merges

In separate threads, tantivy's index writer search for opportunities to merge segments.
The point of segment merge is to:

- eventually get rid of tombstoned documents
- reduce the otherwise ever-growing number of segments.

Indeed, while having several segments instead of one does not hurt search too much, having hundreds can have a measurable impact on the search performance.

### Searcher

The user of the library usually does not need to know about the existence of Segments.
Searching is done through an object called a [`Searcher`](src/core/searcher.rs), that captures a
snapshot of the index at one point of time, by holding a list of [SegmentReader](src/core/segment_reader.rs).

In other words, regardless of commits, file garbage collection, or segment merge that might happen, as long as the user holds and reuse the same [Searcher](src/core/searcher.rs), search will happen on an immutable snapshot of the index.

## [directory/](src/directory): Where should the data be stored?

Tantivy, like Lucene, abstracts the place where the data should be stored in a key-trait
called [`Directory`](src/directory/directory.rs).
Contrary to Lucene however, "files" are quite different from some kind of `io::Read` object.
Check out [`src/directory/directory.rs`](src/directory/directory.rs) trait for more details.

Tantivy ships two main directory implementation: the `MmapDirectory` and the `RamDirectory`,
but users can extend tantivy with their own implementation.

## [schema/](src/schema): What are documents?

Tantivy's document follows a very strict schema, decided before building any index.

The schema defines all of the fields that the indexes [`Document`](src/schema/document/mod.rs) may and should contain, their types (`text`, `i64`, `u64`, `Date`, ...) as well as how it should be indexed / represented in tantivy.

Depending on the type of the field, you can decide to

- put it in the docstore
- store it as a fast field
- index it

Practically, tantivy will push values associated with this type to up to 3 respective
data structures.

*Limitations*

As of today, tantivy's schema imposes a 1:1 relationship between a field that is being ingested and a field represented in the search index. In sophisticated search application, it is fairly common to want to index a field twice using different tokenizers, or to index the concatenation of several fields together into one field.

This is not something tantivy supports, and it is up to the user to duplicate field / concatenate fields before feeding them to tantivy.

## General information about these data structures

All data structures in tantivy, have:

- a writer
- a serializer
- a reader

The writer builds an in-memory representation of a batch of documents. This representation is not searchable. It is just meant as an intermediary mutable representation, to which we can sequentially add
the document of a batch. At the end of the batch (or if a memory limit is reached), this representation
is then converted into an on-disk immutable representation, that is extremely compact.
This conversion is done by the serializer.

Finally, the reader is in charge of offering an API to read on this on-disk read-only representation.
In tantivy, readers are designed to require very little anonymous memory. The data is read straight from an mmapped file, and loading an index is as fast as mmapping its files.

## [store/](src/store): Here is my DocId, Gimme my document

The docstore is a row-oriented storage that, for each document, stores a subset of the fields
that are marked as stored in the schema. The docstore is compressed using a general-purpose algorithm
like LZ4.

**Useful for**

In search engines, it is often used to display search results.
Once the top 10 documents have been identified, we fetch them from the store, and display them or their snippet on the search result page (aka SERP).

**Not useful for**

Fetching a document from the store is typically a "slow" operation. It usually consists in

- searching into a compact tree-like data structure to find the position of the right block.
- decompressing a small block
- returning the document from this block.

It is NOT meant to be called for every document matching a query.

As a rule of thumb, if you hit the docstore more than 100 times per search query, you are probably misusing tantivy.

## [fastfield/](src/fastfield): Here is my DocId, Gimme my value

Fast fields are stored in a column-oriented storage that allows for random access.
The only compression applied is bitpacking. The column comes with two meta data.
The minimum value in the column and the number of bits per doc.

Fetching a value for a `DocId` is then as simple as computing

```rust
min_value + fetch_bits(num_bits * doc_id..num_bits * (doc_id+1))
```

This operation just requires one memory fetch.
Because, DocSets are scanned through in order (DocId are iterated in a sorted manner) which
also help locality.

In Lucene's jargon, fast fields are called DocValues.

**Useful for**

They are typically integer values that are useful to either rank or compute aggregate over
all of the documents matching a query (aka [DocSet](src/docset.rs)).

For instance, one could define a function to combine upvotes with tantivy's internal relevancy score.
This can be done by fetching a fast field during scoring.
One could also compute the mean price of the items matching a query in an e-commerce website.
This can be done by fetching a fast field in a collector.
Finally one could decide to post-filter a docset to remove docset with a price within a specific range.
If the ratio of filtered out documents is not too low, an efficient way to do this is to fetch the price and apply the filter on the collector side.

Aside from integer values, it is also possible to store an actual byte payload.
For advanced search engine, it is possible to store all of the features required for learning-to-rank in a byte payload, access it during search, and apply the learning-to-rank model.

Finally facets are a specific kind of fast field, and the associated source code is in [`fastfield/facet_reader.rs`](src/fastfield/facet_reader.rs).

# The inverted search index

The inverted index is the core part of full-text search.
When presented a new document with the text field "Hello, happy tax payer!", tantivy breaks it into a list of so-called tokens. In addition to just splitting these strings into tokens, it might also do different kinds of operations like dropping the punctuation, converting the character to lowercase, apply stemming, etc. Tantivy makes it possible to configure the operations to be applied in the schema (tokenizer/ is the place where these operations are implemented).

For instance, the default tokenizer of tantivy would break our text into: `[hello, happy, tax, payer]`.
The document will therefore be registered in the inverted index as containing the terms
`[text:hello, text:happy, text:tax, text:payer]`.

The role of the inverted index is, when given a term, gives us in return a very fast iterator over the sorted doc ids that match the term.

Such an iterator is called a posting list. In addition to giving us `DocId`, they can also give us optionally the number of occurrence of the term for each document, also called term frequency or TF.

These iterators being sorted by DocId, one can create an iterator over the document containing `text:tax AND text:payer`, `(text:tax AND text:payer) OR (text:contribuable)` or any boolean expression.

In order to represent the function
```Term ⟶ Posting```

The inverted index actually consists of two data structures chained together.

- [Term](src/schema/term.rs) ⟶ [TermInfo](src/postings/term_info.rs) is addressed by the term dictionary.
- [TermInfo](src/postings/term_info.rs) ⟶ [Posting](src/postings/postings.rs) is addressed by the posting lists.

Where [TermInfo](src/postings/term_info.rs) is an object containing some meta data about a term.

## [termdict/](src/termdict): Here is a term, give me the [TermInfo](src/postings/term_info.rs)

Tantivy's term dictionary is mainly in charge of supplying the function

[Term](src/schema/term.rs) ⟶ [TermInfo](src/postings/term_info.rs)

It is itself broken into two parts.

- [Term](src/schema/term.rs) ⟶ [TermOrdinal](src/termdict/mod.rs) is addressed by a finite state transducer, implemented by the fst crate.
- [TermOrdinal](src/termdict/mod.rs) ⟶ [TermInfo](src/postings/term_info.rs) is addressed by the term info store.

## [postings/](src/postings): Iterate over documents... very fast

A posting list makes it possible to store a sorted list of doc ids and for each doc store
a term frequency as well.

The posting lists are stored in a separate file. The [TermInfo](src/postings/term_info.rs) contains an offset into that file and a number of documents for the given posting list. Both are required and sufficient to read the posting list.

The posting list is organized in block of 128 documents.
One block of doc ids is followed by one block of term frequencies.

The doc ids are delta encoded and bitpacked.
The term frequencies are bitpacked.

Because the number of docs is rarely a multiple of 128, the last block may contain an arbitrary number of docs between 1 and 127 documents. We then use variable int encoding instead of bitpacking.

## [positions/](src/positions): Where are my terms within the documents?

Phrase queries make it possible to search for documents containing a specific sequence of terms.
For instance, when the phrase query "the art of war" does not match "the war of art".
To make it possible, it is possible to specify in the schema that a field should store positions in addition to being indexed.

The token positions of all of the terms are then stored in a separate file with the extension `.pos`.
The [TermInfo](src/postings/term_info.rs) gives an offset (expressed in position this time) in this file. As we iterate through the docset,
we advance the position reader by the number of term frequencies of the current document.

## [fieldnorm/](src/fieldnorm): Here is my doc, how many tokens in this field?

The [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) formula also requires to know the number of tokens stored in a specific field for a given document. We store this information on one byte per document in the fieldnorm.
The fieldnorm is therefore compressed. Values up to 40 are encoded unchanged.

## [tokenizer/](src/tokenizer): How should we process text?

Text processing is key to a good search experience.
Splits or normalize your text too much, and the search results will have a less precision and a higher recall.
Do not normalize, or under split your text, you will end up with a higher precision and a lesser recall.

Text processing can be configured by selecting an off-the-shelf [`Tokenizer`](./src/tokenizer/tokenizer.rs) or implementing your own to first split the text into tokens, and then chain different [`TokenFilter`](src/tokenizer/tokenizer.rs)'s to it.

Tantivy's comes with few tokenizers, but external crates are offering advanced tokenizers, such as [Lindera](https://crates.io/crates/lindera) for Japanese.

## [query/](src/query): Define and compose queries

The [Query](src/query/query.rs) trait defines what a query is.
Due to the necessity for some queries to compute some statistics over the entire index, and because the
index is composed of several `SegmentReader`, the path from transforming a `Query` to an iterator over documents is slightly convoluted, but fundamentally, this is what a Query is.

The iterator over a document comes with some scoring function. The resulting trait is called a
[Scorer](src/query/scorer.rs) and is specific to a segment.

Different queries can be combined using the [BooleanQuery](src/query/boolean_query/).
Tantivy comes with different types of queries and can be extended by implementing
the `Query`, `Weight`, and `Scorer` traits.

## [collector](src/collector): Define what to do with matched documents

Collectors define how to aggregate the documents matching a query, in the broadest sense possible.
The search will push matched documents one by one, calling their
`fn collect(doc: DocId, score: Score);` method.

Users may implement their own collectors by implementing the [Collector](src/collector/mod.rs) trait.

## [query-grammar](query-grammar): Defines the grammar of the query parser

While the [QueryParser](src/query/query_parser/query_parser.rs) struct is located in the `query/` directory, the actual parser combinator used to convert user queries into an AST is in an external crate called `query-grammar`. This part was externalized to lighten the work of the compiler.
