# Foreword, what is the scope of tantivy?

> Tantivy is a **search** engine **library** for Rust.

If you are familiar with Lucene, it's an excellent approximation to consider tantivy as Lucene for Rust. Tantivy is heavily inspired by Lucene's design and
they both have the same scope and targeted use cases.

If you are not familiar with Lucene, let's break down our little tagline.

- **Search** here means full-text search : fundamentally, tantivy is here to help you
identify efficiently what are the documents matching a given query in your corpus.
But modern search UI are so much more : text processing, facetting, autocomplete, fuzzy search, good
relevancy, collapsing, highlighting, spatial search.

  While some of these features are not available in tantivy yet, all of these are relevant
  feature requests. Tantivy's objective is to offer a solid toolbox to create the best search
  experience. But keep in mind this is just a toolbox.
  Which bring us to the second keyword...

- **Library** means that you will have to write code. Tantivy is not an *all-in-one* server solution like Elasticsearch for instance.

  Sometimes a functionality will not be available in tantivy because it is too
  specific to your use case. By design, tantivy should make it possible to extend
  the available set of features using the existing rock-solid datastructures.

  Most frequently this will mean writing your own `Collector`, your own `Scorer` or your own
  `TokenFilter`... Some of your requirements may also be related to
  something closer to architecture or operations. For instance, you may
  want to build a large corpus on Hadoop, fine-tune the merge policy to keep your
  index sharded in a time-wise fashion, or you may want to convert and existing
  index from a different format.

  Tantivy exposes a lot of low level API to do all of these things.
  
