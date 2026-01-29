use std::{iter::Sum, ops::SubAssign};

use anarchist_readable_name_generator_lib::readable_name_custom;
use rand::{distr::uniform::SampleUniform, Rng};

pub mod expr;
pub mod generated_expr;
pub mod opts;
pub mod predicate;
pub mod query;
pub mod table;
pub mod value;

pub use opts::*;

type ArbitraryFromFunc<'a, R, T> = Box<dyn Fn(&mut R) -> T + 'a>;
type Choice<'a, R, T> = (usize, Box<dyn Fn(&mut R) -> Option<T> + 'a>);

/// Arbitrary trait for generating random values
/// An implementation of arbitrary is assumed to be a uniform sampling of
/// the possible values of the type, with a bias towards smaller values for
/// practicality.
pub trait Arbitrary {
    fn arbitrary<R: Rng + ?Sized, C: GenerationContext>(rng: &mut R, context: &C) -> Self;
}

/// ArbitrarySized trait for generating random values of a specific size
/// An implementation of arbitrary_sized is assumed to be a uniform sampling of
/// the possible values of the type, with a bias towards smaller values for
/// practicality, but with the additional constraint that the generated value
/// must fit in the given size. This is useful for generating values that are
/// constrained by a specific size, such as integers or strings.
pub trait ArbitrarySized {
    fn arbitrary_sized<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        size: usize,
    ) -> Self;
}

/// ArbitraryFrom trait for generating random values from a given value
/// ArbitraryFrom allows for constructing relations, where the generated
/// value is dependent on the given value. These relations could be constraints
/// such as generating an integer within an interval, or a value that fits in a table,
/// or a predicate satisfying a given table row.
pub trait ArbitraryFrom<T> {
    fn arbitrary_from<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        t: T,
    ) -> Self;
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
    fn arbitrary_sized_from<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        t: T,
        size: usize,
    ) -> Self;
}

/// ArbitraryFromMaybe trait for fallibally generating random values from a given value
pub trait ArbitraryFromMaybe<T> {
    fn arbitrary_from_maybe<R: Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        t: T,
    ) -> Option<Self>
    where
        Self: Sized;
}

/// Frequency is a helper function for composing different generators with different frequency
/// of occurrences.
/// The type signature for the `N` parameter is a bit complex, but it
/// roughly corresponds to a type that can be summed, compared, subtracted and sampled, which are
/// the operations we require for the implementation.
// todo: switch to a simpler type signature that can accommodate all integer and float types, which
//       should be enough for our purposes.
pub fn frequency<
    T,
    R: Rng + ?Sized,
    N: Sum + PartialOrd + Copy + Default + SampleUniform + SubAssign,
>(
    choices: Vec<(N, ArbitraryFromFunc<R, T>)>,
    rng: &mut R,
) -> T {
    let total = choices.iter().map(|(weight, _)| *weight).sum::<N>();
    let mut choice = rng.random_range(N::default()..total);

    for (weight, f) in choices {
        if choice < weight {
            return f(rng);
        }
        choice -= weight;
    }

    unreachable!()
}

/// one_of is a helper function for composing different generators with equal probability of occurrence.
pub fn one_of<T, R: Rng + ?Sized>(choices: Vec<ArbitraryFromFunc<R, T>>, rng: &mut R) -> T {
    let index = rng.random_range(0..choices.len());
    choices[index](rng)
}

/// backtrack is a helper function for composing different "failable" generators.
/// The function takes a list of functions that return an Option<T>, along with number of retries
/// to make before giving up.
pub fn backtrack<T, R: Rng + ?Sized>(mut choices: Vec<Choice<R, T>>, rng: &mut R) -> Option<T> {
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
pub fn pick<'a, T, R: Rng + ?Sized>(choices: &'a [T], rng: &mut R) -> &'a T {
    let index = rng.random_range(0..choices.len());
    &choices[index]
}

/// pick_index is typically used for picking an index from a slice to later refer to the element
/// at that index.
pub fn pick_index<R: Rng + ?Sized>(choices: usize, rng: &mut R) -> usize {
    rng.random_range(0..choices)
}

/// pick_n_unique is a helper function for uniformly picking N unique elements from a range.
/// The elements themselves are usize, typically representing indices.
pub fn pick_n_unique<R: Rng + ?Sized>(
    range: std::ops::Range<usize>,
    n: usize,
    rng: &mut R,
) -> impl Iterator<Item = usize> {
    use rand::seq::SliceRandom;
    let mut items: Vec<usize> = range.collect();
    items.shuffle(rng);
    items.into_iter().take(n)
}

/// gen_random_text uses `anarchist_readable_name_generator_lib` to generate random
/// readable names for tables, columns, text values etc.
pub fn gen_random_text<R: Rng + ?Sized>(rng: &mut R) -> String {
    let big_text = rng.random_ratio(1, 1000);
    if big_text {
        // let max_size: u64 = 2 * 1024 * 1024 * 1024;
        let max_size: u64 = 2 * 1024;
        let size = rng.random_range(1024..max_size);
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

//FIXME this can hang if count > items.len() or if there are duplicates
pub fn pick_unique<'a, T: PartialEq, R: Rng + ?Sized>(
    items: &'a [T],
    count: usize,
    rng: &mut R,
) -> impl Iterator<Item = &'a T> {
    let mut picked: Vec<&T> = Vec::new();
    while picked.len() < count {
        let item = pick(items, rng);
        if !picked.contains(&item) {
            picked.push(item);
        }
    }
    picked.into_iter()
}

#[cfg(test)]
pub mod tests {
    use crate::{
        generation::{GenerationContext, Opts},
        model::table::Table,
    };

    #[derive(Debug, Default, Clone)]
    pub struct TestContext {
        pub opts: Opts,
        pub tables: Vec<Table>,
    }

    impl TestContext {
        /// Create a test context with generated columns disabled.
        /// These tests create random values for all columns, which doesn't work correctly
        /// with generated columns since their values should be computed from expressions.
        pub fn no_gencol() -> Self {
            let mut ctx = Self::default();
            ctx.opts.table.generated_columns.enable = false;
            ctx
        }
    }

    impl GenerationContext for TestContext {
        fn tables(&self) -> &Vec<Table> {
            &self.tables
        }

        fn opts(&self) -> &Opts {
            &self.opts
        }
    }
}
