use std::io;
use std::ops::Bound;

#[derive(Clone, Debug)]
pub struct BoundsRange<T> {
    pub lower_bound: Bound<T>,
    pub upper_bound: Bound<T>,
}
impl<T> BoundsRange<T> {
    pub fn new(lower_bound: Bound<T>, upper_bound: Bound<T>) -> Self {
        BoundsRange {
            lower_bound,
            upper_bound,
        }
    }
    pub fn is_unbounded(&self) -> bool {
        matches!(self.lower_bound, Bound::Unbounded) && matches!(self.upper_bound, Bound::Unbounded)
    }
    pub fn map_bound<TTo>(&self, transform: impl Fn(&T) -> TTo) -> BoundsRange<TTo> {
        BoundsRange {
            lower_bound: map_bound(&self.lower_bound, &transform),
            upper_bound: map_bound(&self.upper_bound, &transform),
        }
    }

    pub fn map_bound_res<TTo, Err>(
        &self,
        transform: impl Fn(&T) -> Result<TTo, Err>,
    ) -> Result<BoundsRange<TTo>, Err> {
        Ok(BoundsRange {
            lower_bound: map_bound_res(&self.lower_bound, &transform)?,
            upper_bound: map_bound_res(&self.upper_bound, &transform)?,
        })
    }

    pub fn transform_inner<TTo>(
        &self,
        transform_lower: impl Fn(&T) -> TransformBound<TTo>,
        transform_upper: impl Fn(&T) -> TransformBound<TTo>,
    ) -> BoundsRange<TTo> {
        BoundsRange {
            lower_bound: transform_bound_inner(&self.lower_bound, &transform_lower),
            upper_bound: transform_bound_inner(&self.upper_bound, &transform_upper),
        }
    }

    /// Returns the first set inner value
    pub fn get_inner(&self) -> Option<&T> {
        inner_bound(&self.lower_bound).or(inner_bound(&self.upper_bound))
    }
}

pub enum TransformBound<T> {
    /// Overwrite the bounds
    NewBound(Bound<T>),
    /// Use Existing bounds with new value
    Existing(T),
}

/// Takes a bound and transforms the inner value into a new bound via a closure.
/// The bound variant may change by the value returned value from the closure.
pub fn transform_bound_inner_res<TFrom, TTo>(
    bound: &Bound<TFrom>,
    transform: impl Fn(&TFrom) -> io::Result<TransformBound<TTo>>,
) -> io::Result<Bound<TTo>> {
    use self::Bound::*;
    Ok(match bound {
        Excluded(from_val) => match transform(from_val)? {
            TransformBound::NewBound(new_val) => new_val,
            TransformBound::Existing(new_val) => Excluded(new_val),
        },
        Included(from_val) => match transform(from_val)? {
            TransformBound::NewBound(new_val) => new_val,
            TransformBound::Existing(new_val) => Included(new_val),
        },
        Unbounded => Unbounded,
    })
}

/// Takes a bound and transforms the inner value into a new bound via a closure.
/// The bound variant may change by the value returned value from the closure.
pub fn transform_bound_inner<TFrom, TTo>(
    bound: &Bound<TFrom>,
    transform: impl Fn(&TFrom) -> TransformBound<TTo>,
) -> Bound<TTo> {
    use self::Bound::*;
    match bound {
        Excluded(from_val) => match transform(from_val) {
            TransformBound::NewBound(new_val) => new_val,
            TransformBound::Existing(new_val) => Excluded(new_val),
        },
        Included(from_val) => match transform(from_val) {
            TransformBound::NewBound(new_val) => new_val,
            TransformBound::Existing(new_val) => Included(new_val),
        },
        Unbounded => Unbounded,
    }
}

/// Returns the inner value of a `Bound`
pub fn inner_bound<T>(val: &Bound<T>) -> Option<&T> {
    match val {
        Bound::Included(term) | Bound::Excluded(term) => Some(term),
        Bound::Unbounded => None,
    }
}

pub fn map_bound<TFrom, TTo>(
    bound: &Bound<TFrom>,
    transform: impl Fn(&TFrom) -> TTo,
) -> Bound<TTo> {
    use self::Bound::*;
    match bound {
        Excluded(from_val) => Bound::Excluded(transform(from_val)),
        Included(from_val) => Bound::Included(transform(from_val)),
        Unbounded => Unbounded,
    }
}

pub fn map_bound_res<TFrom, TTo, Err>(
    bound: &Bound<TFrom>,
    transform: impl Fn(&TFrom) -> Result<TTo, Err>,
) -> Result<Bound<TTo>, Err> {
    use self::Bound::*;
    Ok(match bound {
        Excluded(from_val) => Excluded(transform(from_val)?),
        Included(from_val) => Included(transform(from_val)?),
        Unbounded => Unbounded,
    })
}
