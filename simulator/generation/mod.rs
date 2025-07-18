use std::{iter::Sum, ops::SubAssign};

use anarchist_readable_name_generator_lib::readable_name_custom;
use rand::{distributions::uniform::SampleUniform, Rng};

use crate::runner::env::SimulatorTables;

mod expr;
pub mod plan;
mod predicate;
pub mod property;
pub mod query;
pub mod table;

type ArbitraryFromFunc<'a, R, T> = Box<dyn Fn(&mut R) -> T + 'a>;
type Choice<'a, R, T> = (usize, Box<dyn Fn(&mut R) -> Option<T> + 'a>);

/// Arbitrary trait for generating random values
/// An implementation of arbitrary is assumed to be a uniform sampling of
/// the possible values of the type, with a bias towards smaller values for
/// practicality.
pub trait Arbitrary {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self;
}

/// ArbitrarySized trait for generating random values of a specific size
/// An implementation of arbitrary_sized is assumed to be a uniform sampling of
/// the possible values of the type, with a bias towards smaller values for
/// practicality, but with the additional constraint that the generated value
/// must fit in the given size. This is useful for generating values that are
/// constrained by a specific size, such as integers or strings.
pub trait ArbitrarySized {
    fn arbitrary_sized<R: Rng>(rng: &mut R, size: usize) -> Self;
}

/// ArbitraryFrom trait for generating random values from a given value
/// ArbitraryFrom allows for constructing relations, where the generated
/// value is dependent on the given value. These relations could be constraints
/// such as generating an integer within an interval, or a value that fits in a table,
/// or a predicate satisfying a given table row.
pub trait ArbitraryFrom<T> {
    fn arbitrary_from<R: Rng>(rng: &mut R, t: T) -> Self;
}

/// ArbitrarySizedFrom trait for generating random values from a given value
/// ArbitrarySizedFrom allows for constructing relations, where the generated
/// value is dependent on the given value and a size constraint. These relations
/// could be constraints such as generating an integer within an interval,
/// or a value that fits in a table, or a predicate satisfying a given table row,
/// but with the additional constraint that the generated value must fit in the given size.
/// This is useful for generating values that are constrained by a specific size,
/// such as integers or strings, while still being dependent on the given value.
pub trait ArbitrarySizedFrom<T> {
    fn arbitrary_sized_from<R: Rng>(rng: &mut R, t: T, size: usize) -> Self;
}

/// ArbitraryFromMaybe trait for fallibally generating random values from a given value
pub trait ArbitraryFromMaybe<T> {
    fn arbitrary_from_maybe<R: Rng>(rng: &mut R, t: T) -> Option<Self>
    where
        Self: Sized;
}

/// Shadow trait for types that can be "shadowed" in the simulator environment.
/// Shadowing is a process of applying a transformation to the simulator environment
/// that reflects the changes made by the query or operation represented by the type.
/// The result of the shadowing is typically a vector of rows, which can be used to
/// update the simulator environment or to verify the correctness of the operation.
/// The `Result` type is used to indicate the type of the result of the shadowing
/// operation, which can vary depending on the type of the operation being shadowed.
/// For example, a `Create` operation might return an empty vector, while an `Insert` operation
/// might return a vector of rows that were inserted into the table.
pub(crate) trait Shadow {
    type Result;
    fn shadow(&self, tables: &mut SimulatorTables) -> Self::Result;
}

/// Frequency is a helper function for composing different generators with different frequency
/// of occurrences.
/// The type signature for the `N` parameter is a bit complex, but it
/// roughly corresponds to a type that can be summed, compared, subtracted and sampled, which are
/// the operations we require for the implementation.
// todo: switch to a simpler type signature that can accommodate all integer and float types, which
//       should be enough for our purposes.
pub(crate) fn frequency<
    T,
    R: Rng,
    N: Sum + PartialOrd + Copy + Default + SampleUniform + SubAssign,
>(
    choices: Vec<(N, ArbitraryFromFunc<R, T>)>,
    rng: &mut R,
) -> T {
    let total = choices.iter().map(|(weight, _)| *weight).sum::<N>();
    let mut choice = rng.gen_range(N::default()..total);

    for (weight, f) in choices {
        if choice < weight {
            return f(rng);
        }
        choice -= weight;
    }

    unreachable!()
}

/// one_of is a helper function for composing different generators with equal probability of occurrence.
pub(crate) fn one_of<T, R: Rng>(choices: Vec<ArbitraryFromFunc<R, T>>, rng: &mut R) -> T {
    let index = rng.gen_range(0..choices.len());
    choices[index](rng)
}

/// backtrack is a helper function for composing different "failable" generators.
/// The function takes a list of functions that return an Option<T>, along with number of retries
/// to make before giving up.
pub(crate) fn backtrack<T, R: Rng>(mut choices: Vec<Choice<R, T>>, rng: &mut R) -> Option<T> {
    loop {
        // If there are no more choices left, we give up
        let choices_ = choices
            .iter()
            .enumerate()
            .filter(|(_, (retries, _))| *retries > 0)
            .collect::<Vec<_>>();
        if choices_.is_empty() {
            tracing::trace!("backtrack: no more choices left");
            return None;
        }
        // Run a one_of on the remaining choices
        let (choice_index, choice) = pick(&choices_, rng);
        let choice_index = *choice_index;
        // If the choice returns None, we decrement the number of retries and try again
        let result = choice.1(rng);
        if result.is_some() {
            return result;
        } else {
            choices[choice_index].0 -= 1;
        }
    }
}

/// pick is a helper function for uniformly picking a random element from a slice
pub(crate) fn pick<'a, T, R: Rng>(choices: &'a [T], rng: &mut R) -> &'a T {
    let index = rng.gen_range(0..choices.len());
    &choices[index]
}

/// pick_index is typically used for picking an index from a slice to later refer to the element
/// at that index.
pub(crate) fn pick_index<R: Rng>(choices: usize, rng: &mut R) -> usize {
    rng.gen_range(0..choices)
}

/// pick_n_unique is a helper function for uniformly picking N unique elements from a range.
/// The elements themselves are usize, typically representing indices.
pub(crate) fn pick_n_unique<R: Rng>(
    range: std::ops::Range<usize>,
    n: usize,
    rng: &mut R,
) -> Vec<usize> {
    use rand::seq::SliceRandom;
    let mut items: Vec<usize> = range.collect();
    items.shuffle(rng);
    items.into_iter().take(n).collect()
}

/// gen_random_text uses `anarchist_readable_name_generator_lib` to generate random
/// readable names for tables, columns, text values etc.
pub(crate) fn gen_random_text<T: Rng>(rng: &mut T) -> String {
    let big_text = rng.gen_ratio(1, 1000);
    if big_text {
        // let max_size: u64 = 2 * 1024 * 1024 * 1024;
        let max_size: u64 = 2 * 1024;
        let size = rng.gen_range(1024..max_size);
        let mut name = String::with_capacity(size as usize);
        for i in 0..size {
            name.push(((i % 26) as u8 + b'A') as char);
        }
        name
    } else {
        let name = readable_name_custom("_", rng);
        name.replace("-", "_")
    }
}
