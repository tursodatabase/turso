/// Streamer describes a "streaming iterator."
///
/// It provides a mechanism for writing code that is generic over streams
/// produced by this crate.
///
/// Note that this is strictly less useful than `Iterator` because the item
/// associated type is bound to a specific lifetime. However, this does permit
/// us to write *some* generic code over streams that produce values tied
/// to the lifetime of the stream.
///
/// Some form of stream abstraction is inherently required for this crate
/// because elements in a finite state transducer are produced *by iterating*
/// over the structure. The alternative would be to create a new allocation
/// for each element iterated over, which would be prohibitively expensive.
///
/// # Usage & motivation
///
/// Streams are hard to use because they don't fit into Rust's current type
/// system very well. They are so hard to use that this author loathes having a
/// publically defined trait for it. Nevertheless, they do just barely provide
/// a means for composing multiple stream abstractions with different concrete
/// types. For example, one might want to take the union of a range query
/// stream with a stream that has been filtered by a regex. These streams have
/// different concrete types. A `Streamer` trait allows us to write code that
/// is generic over these concrete types. (All of the set operations are
/// implemented this way.)
///
/// A problem with streams is that the trait is itself parameterized by a
/// lifetime. In practice, this makes them very unergonomic because specifying
/// a `Streamer` bound generally requires a higher-ranked trait bound. This is
/// necessary because the lifetime can't actually be named in the enclosing
/// function; instead, the lifetime is local to iteration itself. Therefore,
/// one must assert that the bound is valid for *any particular* lifetime.
/// This is the essence of higher-rank trait bounds.
///
/// Because of this, you might expect to see lots of bounds that look like
/// this:
///
/// ```ignore
/// fn takes_stream<T, S>(s: S)
///     where S: for<'a> Streamer<'a, Item=T>
/// {
/// }
/// ```
///
/// There are *three* different problems with this declaration:
///
/// 1. `S` is not bound by any particular lifetime itself, and most streams
///    probably contain a reference to an underlying finite state transducer.
/// 2. It is often convenient to separate the notion of "stream" with
///    "stream constructor." This represents a similar split found in the
///    standard library for `Iterator` and `IntoIterator`, respectively.
/// 3. The `Item=T` is invalid because `Streamer`'s associated type is
///    parameterized by a lifetime and there is no way to parameterize an
///    arbitrary type constructor. (In this context, `T` is the type
///    constructor, because it will invariably require a lifetime to become
///    a concrete type.)
///
/// With that said, we must revise our possibly-workable bounds to a giant
/// scary monster:
///
/// ```ignore
/// fn takes_stream<'f, I, S>(s: I)
///     where I: for<'a> IntoStreamer<'a, Into=S, Item=(&'a [u8], Output)>,
///           S: 'f + for<'a> Streamer<'a, Item=(&'a [u8], Output)>
/// {
/// }
/// ```
///
/// We addressed the above points correspondingly:
///
/// 1. `S` is now bound by `'f`, which corresponds to the lifetime (possibly
///     `'static`) of the underlying stream.
/// 2. The `I` type parameter has been added to refer to a type that knows how
///    to build a stream. Notice that neither of the bounds for `I` or `S`
///    share a lifetime parameter. This is because the higher rank trait bound
///    specifies it works for *any* particular lifetime.
/// 3. `T` has been replaced with specific concrete types. Note that these
///    concrete types are duplicated. With iterators, we could use
///    `Item=S::Item` in the bound for `I`, but one cannot access an associated
///    type through a higher-ranked trait bound. Therefore, we must duplicate
///    the item type.
///
/// As you can see, streams offer little flexibility, little ergonomics and a
/// lot of hard to read trait bounds. The situation is lamentable, but
/// nevertheless, without them, we would not be able to compose streams by
/// leveraging the type system.
///
/// A redeemable quality is that these *same exact* trait bounds (modulo some
/// tweaks in the `Item` associated type) appear in many places in this crate
/// without much variation. Therefore, once you grok it, it's mostly easy to
/// pattern match it with "oh I need a stream." My hope is that clear
/// documentation and examples make these complex bounds easier to burden.
///
/// Stretching this abstraction further with Rust's current type system is not
/// advised.
pub trait Streamer<'a> {
    /// The type of the item emitted by this stream.
    type Item: 'a;

    /// Emits the next element in this stream, or `None` to indicate the stream
    /// has been exhausted.
    ///
    /// It is not specified what a stream does after `None` is emitted. In most
    /// cases, `None` should be emitted on every subsequent call.
    fn next(&'a mut self) -> Option<Self::Item>;
}

/// IntoStreamer describes types that can be converted to streams.
///
/// This is analogous to the `IntoIterator` trait for `Iterator` in
/// `std::iter`.
pub trait IntoStreamer<'a> {
    /// The type of the item emitted by the stream.
    type Item: 'a;
    /// The type of the stream to be constructed.
    type Into: Streamer<'a, Item = Self::Item>;

    /// Construct a stream from `Self`.
    fn into_stream(self) -> Self::Into;
}

impl<'a, S: Streamer<'a>> IntoStreamer<'a> for S {
    type Item = S::Item;
    type Into = S;

    fn into_stream(self) -> S {
        self
    }
}
