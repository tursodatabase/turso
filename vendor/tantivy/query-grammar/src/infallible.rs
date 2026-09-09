//! nom combinators for infallible operations

use std::convert::Infallible;

use nom::{AsChar, IResult, InputLength, InputTakeAtPosition};
use serde::Serialize;

pub(crate) type ErrorList = Vec<LenientErrorInternal>;
pub(crate) type JResult<I, O> = IResult<I, (O, ErrorList), Infallible>;

/// An error, with an end-of-string based offset
#[derive(Debug)]
pub(crate) struct LenientErrorInternal {
    pub pos: usize,
    pub message: String,
}

/// A recoverable error and the position it happened at
#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct LenientError {
    pub pos: usize,
    pub message: String,
}

impl LenientError {
    pub(crate) fn from_internal(internal: LenientErrorInternal, str_len: usize) -> LenientError {
        LenientError {
            pos: str_len - internal.pos,
            message: internal.message,
        }
    }
}

fn unwrap_infallible<T>(res: Result<T, nom::Err<Infallible>>) -> T {
    match res {
        Ok(val) => val,
        Err(_) => unreachable!(),
    }
}

// when rfcs#1733 get stabilized, this can make things clearer
// trait InfallibleParser<I, O> = nom::Parser<I, (O, ErrorList), std::convert::Infallible>;

/// A variant of the classical `opt` parser, except it returns an infallible error type.
///
/// It's less generic than the original to ease type resolution in the rest of the code.
pub(crate) fn opt_i<I: Clone, O, F>(mut f: F) -> impl FnMut(I) -> JResult<I, Option<O>>
where F: nom::Parser<I, O, nom::error::Error<I>> {
    move |input: I| {
        let i = input.clone();
        match f.parse(input) {
            Ok((i, o)) => Ok((i, (Some(o), Vec::new()))),
            Err(_) => Ok((i, (None, Vec::new()))),
        }
    }
}

pub(crate) fn opt_i_err<'a, I: Clone + InputLength, O, F>(
    mut f: F,
    message: impl ToString + 'a,
) -> impl FnMut(I) -> JResult<I, Option<O>> + 'a
where
    F: nom::Parser<I, O, nom::error::Error<I>> + 'a,
{
    move |input: I| {
        let i = input.clone();
        match f.parse(input) {
            Ok((i, o)) => Ok((i, (Some(o), Vec::new()))),
            Err(_) => {
                let errs = vec![LenientErrorInternal {
                    pos: i.input_len(),
                    message: message.to_string(),
                }];
                Ok((i, (None, errs)))
            }
        }
    }
}

pub(crate) fn space0_infallible<T>(input: T) -> JResult<T, T>
where
    T: InputTakeAtPosition + Clone,
    <T as InputTakeAtPosition>::Item: AsChar + Clone,
{
    opt_i(nom::character::complete::multispace0)(input)
        .map(|(left, (spaces, errors))| (left, (spaces.expect("multispace0 can't fail"), errors)))
}

pub(crate) fn space1_infallible<T>(input: T) -> JResult<T, Option<T>>
where
    T: InputTakeAtPosition + Clone + InputLength,
    <T as InputTakeAtPosition>::Item: AsChar + Clone,
{
    opt_i(nom::character::complete::multispace1)(input).map(|(left, (spaces, mut errors))| {
        if spaces.is_none() {
            errors.push(LenientErrorInternal {
                pos: left.input_len(),
                message: "missing space".to_string(),
            })
        }
        (left, (spaces, errors))
    })
}

pub(crate) fn fallible<I, O, E: nom::error::ParseError<I>, F>(
    mut f: F,
) -> impl FnMut(I) -> IResult<I, O, E>
where F: nom::Parser<I, (O, ErrorList), Infallible> {
    use nom::Err;
    move |input: I| match f.parse(input) {
        Ok((input, (output, _err))) => Ok((input, output)),
        Err(Err::Incomplete(needed)) => Err(Err::Incomplete(needed)),
        // old versions don't understand this is uninhabited and need the empty match to help,
        // newer versions warn because this arm is unreachable (which it is indeed).
        Err(Err::Error(val)) | Err(Err::Failure(val)) => match val {},
    }
}

pub(crate) fn terminated_infallible<I, O1, O2, F, G>(
    mut first: F,
    mut second: G,
) -> impl FnMut(I) -> JResult<I, O1>
where
    F: nom::Parser<I, (O1, ErrorList), Infallible>,
    G: nom::Parser<I, (O2, ErrorList), Infallible>,
{
    move |input: I| {
        let (input, (o1, mut err)) = first.parse(input)?;
        let (input, (_, mut err2)) = second.parse(input)?;
        err.append(&mut err2);
        Ok((input, (o1, err)))
    }
}

pub(crate) fn delimited_infallible<I, O1, O2, O3, F, G, H>(
    mut first: F,
    mut second: G,
    mut third: H,
) -> impl FnMut(I) -> JResult<I, O2>
where
    F: nom::Parser<I, (O1, ErrorList), Infallible>,
    G: nom::Parser<I, (O2, ErrorList), Infallible>,
    H: nom::Parser<I, (O3, ErrorList), Infallible>,
{
    move |input: I| {
        let (input, (_, mut err)) = first.parse(input)?;
        let (input, (o2, mut err2)) = second.parse(input)?;
        err.append(&mut err2);
        let (input, (_, mut err3)) = third.parse(input)?;
        err.append(&mut err3);
        Ok((input, (o2, err)))
    }
}

// Parse nothing. Just a lazy way to not implement terminated/preceded and use delimited instead
pub(crate) fn nothing(i: &str) -> JResult<&str, ()> {
    Ok((i, ((), Vec::new())))
}

pub(crate) trait TupleInfallible<I, O> {
    /// Parses the input and returns a tuple of results of each parser.
    fn parse(&mut self, input: I) -> JResult<I, O>;
}

impl<Input, Output, F: nom::Parser<Input, (Output, ErrorList), Infallible>>
    TupleInfallible<Input, (Output,)> for (F,)
{
    fn parse(&mut self, input: Input) -> JResult<Input, (Output,)> {
        self.0.parse(input).map(|(i, (o, e))| (i, ((o,), e)))
    }
}

// these macros are heavily copied from nom, with some minor adaptations for our type
macro_rules! tuple_trait(
  ($name1:ident $ty1:ident, $name2: ident $ty2:ident, $($name:ident $ty:ident),*) => (
    tuple_trait!(__impl $name1 $ty1, $name2 $ty2; $($name $ty),*);
  );
  (__impl $($name:ident $ty: ident),+; $name1:ident $ty1:ident, $($name2:ident $ty2:ident),*) => (
    tuple_trait_impl!($($name $ty),+);
    tuple_trait!(__impl $($name $ty),+ , $name1 $ty1; $($name2 $ty2),*);
  );
  (__impl $($name:ident $ty: ident),+; $name1:ident $ty1:ident) => (
    tuple_trait_impl!($($name $ty),+);
    tuple_trait_impl!($($name $ty),+, $name1 $ty1);
  );
);

macro_rules! tuple_trait_impl(
  ($($name:ident $ty: ident),+) => (
    impl<
      Input: Clone, $($ty),+ ,
      $($name: nom::Parser<Input, ($ty, ErrorList), Infallible>),+
    > TupleInfallible<Input, ( $($ty),+ )> for ( $($name),+ ) {

      fn parse(&mut self, input: Input) -> JResult<Input, ( $($ty),+ )> {
        let mut error_list = Vec::new();
        tuple_trait_inner!(0, self, input, (), error_list, $($name)+)
      }
    }
  );
);

macro_rules! tuple_trait_inner(
  ($it:tt, $self:expr_2021, $input:expr_2021, (), $error_list:expr_2021, $head:ident $($id:ident)+) => ({
    let (i, (o, mut err)) = $self.$it.parse($input.clone())?;
    $error_list.append(&mut err);

    succ!($it, tuple_trait_inner!($self, i, ( o ), $error_list, $($id)+))
  });
  ($it:tt, $self:expr_2021, $input:expr_2021, ($($parsed:tt)*), $error_list:expr_2021, $head:ident $($id:ident)+) => ({
    let (i, (o, mut err)) = $self.$it.parse($input.clone())?;
    $error_list.append(&mut err);

    succ!($it, tuple_trait_inner!($self, i, ($($parsed)* , o), $error_list, $($id)+))
  });
  ($it:tt, $self:expr_2021, $input:expr_2021, ($($parsed:tt)*), $error_list:expr_2021, $head:ident) => ({
    let (i, (o, mut err)) = $self.$it.parse($input.clone())?;
    $error_list.append(&mut err);

    Ok((i, (($($parsed)* , o), $error_list)))
  });
);

macro_rules! succ (
  (0, $submac:ident ! ($($rest:tt)*)) => ($submac!(1, $($rest)*));
  (1, $submac:ident ! ($($rest:tt)*)) => ($submac!(2, $($rest)*));
  (2, $submac:ident ! ($($rest:tt)*)) => ($submac!(3, $($rest)*));
  (3, $submac:ident ! ($($rest:tt)*)) => ($submac!(4, $($rest)*));
  (4, $submac:ident ! ($($rest:tt)*)) => ($submac!(5, $($rest)*));
  (5, $submac:ident ! ($($rest:tt)*)) => ($submac!(6, $($rest)*));
  (6, $submac:ident ! ($($rest:tt)*)) => ($submac!(7, $($rest)*));
  (7, $submac:ident ! ($($rest:tt)*)) => ($submac!(8, $($rest)*));
  (8, $submac:ident ! ($($rest:tt)*)) => ($submac!(9, $($rest)*));
  (9, $submac:ident ! ($($rest:tt)*)) => ($submac!(10, $($rest)*));
  (10, $submac:ident ! ($($rest:tt)*)) => ($submac!(11, $($rest)*));
  (11, $submac:ident ! ($($rest:tt)*)) => ($submac!(12, $($rest)*));
  (12, $submac:ident ! ($($rest:tt)*)) => ($submac!(13, $($rest)*));
  (13, $submac:ident ! ($($rest:tt)*)) => ($submac!(14, $($rest)*));
  (14, $submac:ident ! ($($rest:tt)*)) => ($submac!(15, $($rest)*));
  (15, $submac:ident ! ($($rest:tt)*)) => ($submac!(16, $($rest)*));
  (16, $submac:ident ! ($($rest:tt)*)) => ($submac!(17, $($rest)*));
  (17, $submac:ident ! ($($rest:tt)*)) => ($submac!(18, $($rest)*));
  (18, $submac:ident ! ($($rest:tt)*)) => ($submac!(19, $($rest)*));
  (19, $submac:ident ! ($($rest:tt)*)) => ($submac!(20, $($rest)*));
  (20, $submac:ident ! ($($rest:tt)*)) => ($submac!(21, $($rest)*));
);

tuple_trait!(FnA A, FnB B, FnC C, FnD D, FnE E, FnF F, FnG G, FnH H, FnI I, FnJ J, FnK K, FnL L,
  FnM M, FnN N, FnO O, FnP P, FnQ Q, FnR R, FnS S, FnT T, FnU U);

// Special case: implement `TupleInfallible` for `()`, the unit type.
// This can come up in macros which accept a variable number of arguments.
// Literally, `()` is an empty tuple, so it should simply parse nothing.
impl<I> TupleInfallible<I, ()> for () {
    fn parse(&mut self, input: I) -> JResult<I, ()> {
        Ok((input, ((), Vec::new())))
    }
}

pub(crate) fn tuple_infallible<I, O, List: TupleInfallible<I, O>>(
    mut l: List,
) -> impl FnMut(I) -> JResult<I, O> {
    move |i: I| l.parse(i)
}

pub(crate) fn separated_list_infallible<I, O, O2, F, G>(
    mut sep: G,
    mut f: F,
) -> impl FnMut(I) -> JResult<I, Vec<O>>
where
    I: Clone + InputLength,
    F: nom::Parser<I, (O, ErrorList), Infallible>,
    G: nom::Parser<I, (O2, ErrorList), Infallible>,
{
    move |i: I| {
        let mut res: Vec<O> = Vec::new();
        let mut errors: ErrorList = Vec::new();

        let (mut i, (o, mut err)) = unwrap_infallible(f.parse(i.clone()));
        errors.append(&mut err);
        res.push(o);

        loop {
            let (i_sep_parsed, (_, mut err_sep)) = unwrap_infallible(sep.parse(i.clone()));
            let len_before = i_sep_parsed.input_len();

            let (i_elem_parsed, (o, mut err_elem)) =
                unwrap_infallible(f.parse(i_sep_parsed.clone()));

            // infinite loop check: the parser must always consume
            // if we consumed nothing here, don't produce an element.
            if i_elem_parsed.input_len() == len_before {
                return Ok((i, (res, errors)));
            }
            res.push(o);
            errors.append(&mut err_sep);
            errors.append(&mut err_elem);
            i = i_elem_parsed;
        }
    }
}

pub(crate) trait Alt<I, O> {
    /// Tests each parser in the tuple and returns the result of the first one that succeeds
    fn choice(&mut self, input: I) -> Option<JResult<I, O>>;
}

macro_rules! alt_trait(
  ($first_cond:ident $first:ident, $($id_cond:ident $id: ident),+) => (
    alt_trait!(__impl $first_cond $first; $($id_cond $id),+);
  );
  (__impl $($current_cond:ident $current:ident),*; $head_cond:ident $head:ident, $($id_cond:ident $id:ident),+) => (
    alt_trait_impl!($($current_cond $current),*);

    alt_trait!(__impl $($current_cond $current,)* $head_cond $head; $($id_cond $id),+);
  );
  (__impl $($current_cond:ident $current:ident),*; $head_cond:ident $head:ident) => (
    alt_trait_impl!($($current_cond $current),*);
    alt_trait_impl!($($current_cond $current,)* $head_cond $head);
  );
);

macro_rules! alt_trait_impl(
  ($($id_cond:ident $id:ident),+) => (
    impl<
      Input: Clone, Output,
      $(
          // () are to make things easier on me, but I'm not entirely sure whether we can do better
          // with rule E0207
          $id_cond: nom::Parser<Input, (), ()>,
          $id: nom::Parser<Input, (Output, ErrorList), Infallible>
      ),+
    > Alt<Input, Output> for ( $(($id_cond, $id),)+ ) {

      fn choice(&mut self, input: Input) -> Option<JResult<Input, Output>> {
        match self.0.0.parse(input.clone()) {
          Err(_) => alt_trait_inner!(1, self, input, $($id_cond $id),+),
          Ok((input_left, _)) => Some(self.0.1.parse(input_left)),
        }
      }
    }
  );
);

macro_rules! alt_trait_inner(
  ($it:tt, $self:expr_2021, $input:expr_2021, $head_cond:ident $head:ident, $($id_cond:ident $id:ident),+) => (
    match $self.$it.0.parse($input.clone()) {
      Err(_) => succ!($it, alt_trait_inner!($self, $input, $($id_cond $id),+)),
      Ok((input_left, _)) => Some($self.$it.1.parse(input_left)),
    }
  );
  ($it:tt, $self:expr_2021, $input:expr_2021, $head_cond:ident $head:ident) => (
    None
  );
);

alt_trait!(A1 A, B1 B, C1 C, D1 D, E1 E, F1 F, G1 G, H1 H, I1 I, J1 J, K1 K,
           L1 L, M1 M, N1 N, O1 O, P1 P, Q1 Q, R1 R, S1 S, T1 T, U1 U);

/// An alt() like combinator. For each branch, it first tries a fallible parser, which commits to
/// this branch, or tells to check next branch, and the execute the infallible parser which follow.
///
/// In case no branch match, the default (fallible) parser is executed.
pub(crate) fn alt_infallible<I: Clone, O, F, List: Alt<I, O>>(
    mut l: List,
    mut default: F,
) -> impl FnMut(I) -> JResult<I, O>
where
    F: nom::Parser<I, (O, ErrorList), Infallible>,
{
    move |i: I| l.choice(i.clone()).unwrap_or_else(|| default.parse(i))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lenient_error_serialization() {
        let error = LenientError {
            pos: 42,
            message: "test error message".to_string(),
        };

        assert_eq!(
            serde_json::to_string(&error).unwrap(),
            "{\"pos\":42,\"message\":\"test error message\"}"
        );
    }
}
