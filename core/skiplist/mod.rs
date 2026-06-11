//! Concurrent maps and sets based on [skip lists].
//!
//! This crate provides the types [`SkipMap`] and [`SkipSet`].
//! These data structures provide an interface similar to [`BTreeMap`] and [`BTreeSet`],
//! respectively, except they support safe concurrent access across
//! multiple threads.
//!
//! # Concurrent access
//! [`SkipMap`] and [`SkipSet`] implement [`Send`] and [`Sync`],
//! so they can be shared across threads with ease.
//!
//! Methods which mutate the map, such as [`insert`],
//! take `&self` rather than `&mut self`. This allows
//! them to be invoked concurrently.
//!
//! ```
//! use turso_core::skiplist::SkipMap;
//! use crossbeam_utils::thread::scope;
//!
//! let person_ages = SkipMap::new();
//!
//! scope(|s| {
//!     // Insert entries into the map from multiple threads.
//!     s.spawn(|_| {
//!         person_ages.insert("Spike Garrett", 22);
//!         person_ages.insert("Stan Hancock", 47);
//!         person_ages.insert("Rea Bryan", 234);
//!
//!         assert_eq!(person_ages.get("Spike Garrett").unwrap().value(), &22);
//!     });
//!     s.spawn(|_| {
//!         person_ages.insert("Bryon Conroy", 65);
//!         person_ages.insert("Lauren Reilly", 2);
//!     });
//! }).unwrap();
//!
//! assert!(person_ages.contains_key("Spike Garrett"));
//! person_ages.remove("Rea Bryan");
//! assert!(!person_ages.contains_key("Rea Bryan"));
//!
//! ```
//!
//! Concurrent access to skip lists is lock-free and sound.
//! Threads won't get blocked waiting for other threads to finish operating
//! on the map.
//!
//! Be warned that, because of this lock-freedom, it's easy to introduce
//! race conditions into your code. For example:
//! ```no_run
//! use turso_core::skiplist::SkipSet;
//! use crossbeam_utils::thread::scope;
//!
//! let numbers = SkipSet::new();
//! scope(|s| {
//!     // Spawn a thread which will remove 5 from the set.
//!     s.spawn(|_| {
//!         numbers.remove(&5);
//!     });
//!
//!     // While the thread above is running, insert a value into the set.
//!     numbers.insert(5);
//!
//!     // This check can fail!
//!     // The other thread may remove the value
//!     // before we perform this check.
//!     assert!(numbers.contains(&5));
//! }).unwrap();
//! ```
//!
//! In effect, a _single_ operation on the map, such as [`insert`],
//! operates atomically: race conditions are impossible. However,
//! concurrent calls to functions can become interleaved across
//! threads, introducing non-determinism.
//!
//! To avoid this sort of race condition, never assume that a collection's
//! state will remain the same across multiple lines of code. For instance,
//! in the example above, the problem arises from the assumption that
//! the map won't be mutated between the calls to `insert` and `contains`.
//! In sequential code, this would be correct. But when multiple
//! threads are introduced, more care is needed.
//!
//! Note that race conditions do not violate Rust's memory safety rules.
//! A race between multiple threads can never cause memory errors or
//! segfaults. A race condition is a _logic error_ in its entirety.
//!
//! # Mutable access to elements
//! [`SkipMap`] and [`SkipSet`] provide no way to retrieve a mutable reference
//! to a value. Since access methods can be called concurrently, providing
//! e.g. a `get_mut` function could cause data races.
//!
//! A solution to the above is to have the implementation wrap
//! each value in a lock. However, this has some repercussions:
//! * The map would no longer be lock-free, inhibiting scalability
//!   and allowing for deadlocks.
//! * If a user of the map doesn't need mutable access, then they pay
//!   the price of locks without actually needing them.
//!
//! Instead, the approach taken by this crate gives more control to the user.
//! If mutable access is needed, then you can use interior mutability,
//! such as [`RwLock`]: `SkipMap<Key, RwLock<Value>>`.
//!
//! # Garbage collection
//! A problem faced by many concurrent data structures
//! is choosing when to free unused memory. Care must be
//! taken to prevent use-after-frees and double-frees, both
//! of which cause undefined behavior.
//!
//! Consider the following sequence of events operating on a [`SkipMap`]:
//! * Thread A calls [`get`] and holds a reference to a value in the map.
//! * Thread B removes that key from the map.
//! * Thread A now attempts to access the value.
//!
//! What happens here? If the map implementation frees the memory
//! belonging to a value when it is
//! removed, then a use-after-free occurs, resulting in memory corruption.
//!
//! To solve the above, this crate uses the _epoch-based memory reclamation_ mechanism
//! implemented in [`crossbeam-epoch`]. Simplified, a value removed from the map
//! is not freed until after all references to it have been dropped. This mechanism
//! is similar to the garbage collection found in some languages, such as Java, except
//! it operates solely on the values inside the map.
//!
//! This garbage collection scheme functions automatically; users don't have to worry about it.
//! However, keep in mind that holding [`Entry`] handles to entries in the map will prevent
//! that memory from being freed until at least after the handles are dropped.
//!
//! # Performance versus B-trees
//! In general, when you need concurrent writes
//! to an ordered collection, skip lists are a reasonable choice.
//! However, they can be substantially slower than B-trees
//! in some scenarios.
//!
//! The main benefit of a skip list over a `RwLock<BTreeMap>`
//! is that it allows concurrent writes to progress without
//! mutual exclusion. However, when the frequency
//! of writes is low, this benefit isn't as useful.
//! In these cases, a shared [`BTreeMap`] may be a faster option.
//!
//! These guidelines should be taken with a grain of salt—performance
//! in practice varies depending on your use case.
//! In the end, the best way to choose between [`BTreeMap`] and [`SkipMap`]
//! is to benchmark them in your own application.
//!
//! # Alternatives
//! This crate implements _ordered_ maps and sets, akin to [`BTreeMap`] and [`BTreeSet`].
//! In many situations, however, a defined order on elements is not required. For these
//! purposes, unordered maps will suffice. In addition, unordered maps
//! often have better performance characteristics than their ordered alternatives.
//!
//! Crossbeam [does not currently provide a concurrent unordered map](https://github.com/crossbeam-rs/rfcs/issues/32).
//! That said, here are some other crates which may suit you:
//! * [`DashMap`](https://docs.rs/dashmap) implements a novel concurrent hash map
//!   with good performance characteristics.
//! * [`flurry`](https://docs.rs/flurry) is a Rust port of Java's `ConcurrentHashMap`.
//!
//! [`insert`]: SkipMap::insert
//! [`get`]: SkipMap::get
//! [`Entry`]: map::Entry
//! [skip lists]: https://en.wikipedia.org/wiki/Skip_list
//! [`crossbeam-epoch`]: https://docs.rs/crossbeam-epoch
//! [`BTreeMap`]: std::collections::BTreeMap
//! [`BTreeSet`]: std::collections::BTreeSet
//! [`RwLock`]: std::sync::RwLock
//!
//! # Examples
//! [`SkipMap`] basic usage:
//! ```
//! use turso_core::skiplist::SkipMap;
//!
//! // Note that the variable doesn't have to be mutable:
//! // SkipMap methods take &self to support concurrent access.
//! let movie_reviews = SkipMap::new();
//!
//! // Insert some key-value pairs.
//! movie_reviews.insert("Office Space",       "Deals with real issues in the workplace.");
//! movie_reviews.insert("Pulp Fiction",       "Masterpiece.");
//! movie_reviews.insert("The Godfather",      "Very enjoyable.");
//! movie_reviews.insert("The Blues Brothers", "Eye lyked it a lot.");
//!
//! // Get the value associated with a key.
//! // get() returns an Entry, which gives
//! // references to the key and value.
//! let pulp_fiction = movie_reviews.get("Pulp Fiction").unwrap();
//! assert_eq!(*pulp_fiction.key(), "Pulp Fiction");
//! assert_eq!(*pulp_fiction.value(), "Masterpiece.");
//!
//! // Remove a key-value pair.
//! movie_reviews.remove("The Blues Brothers");
//! assert!(movie_reviews.get("The Blues Brothers").is_none());
//!
//! // Iterate over the reviews. Since SkipMap
//! // is an ordered map, the iterator will yield
//! // keys in lexicographical order.
//! for entry in &movie_reviews {
//!     let movie = entry.key();
//!     let review = entry.value();
//!     println!("{}: \"{}\"", movie, review);
//! }
//! ```
//!
//! [`SkipSet`] basic usage:
//! ```
//! use turso_core::skiplist::SkipSet;
//!
//! let books = SkipSet::new();
//!
//! // Add some books to the set.
//! books.insert("A Dance With Dragons");
//! books.insert("To Kill a Mockingbird");
//! books.insert("The Odyssey");
//! books.insert("The Great Gatsby");
//!
//! // Check for a specific one.
//! if !books.contains("The Winds of Winter") {
//!    println!("We have {} books, but The Winds of Winter ain't one.",
//!             books.len());
//! }
//!
//! // Remove a book from the set.
//! books.remove("To Kill a Mockingbird");
//! assert!(!books.contains("To Kill a Mockingbird"));
//!
//! // Iterate over the books in the set.
//! // Values are returned in lexicographical order.
//! for entry in &books {
//!     let book = entry.value();
//!     println!("{}", book);
//! }
//! ```
//!
//! # Provenance
//! Vendored from the `crossbeam-skiplist` crate at crossbeam git master,
//! commit `05f9478b333ead58c0bf8e5a37d9ef9bd3b5bf17`
//! (<https://github.com/crossbeam-rs/crossbeam>), licensed under MIT OR
//! Apache-2.0 (see `licenses/core/`). Local modifications are kept minimal;
//! prefer upstreaming fixes.

#![warn(missing_docs, unsafe_op_in_unsafe_fn)]

mod alloc_helper;

pub mod base;

#[doc(inline)]
pub use base::SkipList;

pub mod map;
pub mod set;

#[doc(inline)]
pub use self::{map::SkipMap, set::SkipSet};

pub mod comparator;
pub mod equivalent;

#[cfg(test)]
mod base_tests;
#[cfg(test)]
mod map_tests;
#[cfg(test)]
mod set_tests;
