# Anatomy of an index

## Straight from disk

Tantivy accesses its data using an abstracting trait called `Directory`.
In theory, one can come and override the data access logic. In practise, the
trait somewhat assumes that your data can be mapped to memory, and tantivy
seems deeply married to using `mmap` for its io [^1], and the only persisting
directory shipped with tantivy is the `MmapDirectory`.

While this design has some downsides, this greatly simplifies the source code of
tantivy. Caching is also entirely delegated to the OS.

Tantivy works entirely (or almost) by directly reading the datastructures as they are laid on disk. As a result, the act of opening an indexing does not involve loading different datastructures from the disk into random access memory : starting a process, opening an index, and performing your first query can typically be done in a matter of milliseconds.

This is an interesting property for a command line search engine, or for some multi-tenant log search engine : spawning a new process for each new query can be a perfectly sensible solution in some use case.

In later chapters, we will discuss tantivy's inverted index data layout.
One key take away is that to achieve great performance, search indexes are extremely compact.
Of course this is crucial to reduce IO, and ensure that as much of our index can sit in RAM.

Also, whenever possible its data is accessed sequentially. Of course, this is an amazing property when tantivy needs to access the data from your spinning hard disk, but this is also
critical for performance, if your data is read from and an `SSD` or even already in your pagecache.

## Segments, and the log method

That kind of compact layout comes at one cost: it prevents our datastructures from being dynamic.
In fact, the `Directory` trait does not even allow you to modify part of a file.

To allow the addition / deletion of documents, and create the illusion that
your index is dynamic (i.e.: adding and deleting documents), tantivy uses a common database trick sometimes referred to as the *log method*.

Let's forget about deletes for a moment.

As you add documents, these documents are processed and stored in a dedicated datastructure, in a `RAM` buffer. This datastructure is not ready for search, but it is useful to receive your data and rearrange it very rapidly.

As you add documents, this buffer will reach its capacity and tantivy will transparently stop adding document to it and start converting this datastructure to its final read-only format on disk. Once written, an brand empty buffer is available to resume adding documents.

The resulting chunk of index obtained after this serialization is called a `Segment`.

> A segment is a self-contained atomic piece of index. It is identified with a UUID, and all of its files are identified using the naming scheme : `<UUID>.*`.

Which brings us to the nature of a tantivy `Index`.

> A tantivy `Index` is a collection of `Segments`.

Physically, this really just means and index is a bunch of segment files in a given `Directory`,
linked together by a `meta.json` file. This transparency can become extremely handy
to get tantivy to fit your use case:

*Example 1* You could for instance use hadoop to build a very large search index in a timely manner, copy all of the resulting segment files in the same directory and edit the `meta.json` to get a functional index.[^2]

*Example 2* You could also disable your merge policy and enforce daily segments. Removing data after one week can then be done very efficiently by just editing the `meta.json` and deleting the files associated with segment `D-7`.

## Merging

As you index more and more data, your index will accumulate more and more segments.
Having a lot of small segments is not really optimal. There is a bit of redundancy in having
all these term dictionary. Also when searching, we will need to do term lookups as many times as we have segments.  It can hurt search performance a bit.

That's where merging or compacting comes into place. Tantivy will continuously consider merge
opportunities and start merging segments in the background.

## Indexing throughput, number of indexing threads

[^1]: This may eventually change.

[^2]: Be careful however. By default these files will not be considered as *managed* by tantivy. This means they will never be garbage collected by tantivy, regardless of whether they become obsolete or not.
