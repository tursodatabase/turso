use super::{TursoFromIterator, TursoIteratorExt};
use crate::alloc::TryReserveError;
#[cfg(nightly)]
use {
    super::{trusted_len, TursoTryWithCapacityExt},
    crate::alloc::Vec,
    std::iter::TrustedLen,
};

fn try_from_result_iter<T, E, F, C, I>(iter: I) -> Result<Result<C, F>, TryReserveError>
where
    C: TursoFromIterator<T>,
    F: From<E>,
    I: Iterator<Item = Result<T, E>>,
{
    let mut error = None;
    let values = C::try_from_iter(iter.map_while(|item| match item {
        Ok(value) => Some(value),
        Err(err) => {
            error = Some(F::from(err));
            None
        }
    }))?;
    if let Some(error) = error {
        Ok(Err(error))
    } else {
        Ok(Ok(values))
    }
}

#[cfg(nightly)]
trait TrySpecFromResultIter<T, E, I>: Sized {
    fn try_from_result_iter(iter: I) -> Result<Self, TryReserveError>;
}

#[cfg(nightly)]
impl<T, E, F, C, I> TrySpecFromResultIter<T, E, I> for Result<C, F>
where
    C: TursoFromIterator<T>,
    F: From<E>,
    I: Iterator<Item = Result<T, E>>,
{
    default fn try_from_result_iter(iter: I) -> Result<Self, TryReserveError> {
        try_from_result_iter(iter)
    }
}

#[cfg(nightly)]
impl<T, E, F, I> TrySpecFromResultIter<T, E, I> for Result<Vec<T>, F>
where
    F: From<E>,
    I: TrustedLen<Item = Result<T, E>>,
{
    fn try_from_result_iter(iter: I) -> Result<Self, TryReserveError> {
        // Specialize before the fallback's `map_while`, which cannot retain
        // `TrustedLen` because an error may stop collection early.
        let additional = trusted_len(iter.size_hint())?;
        let mut values = <Vec<T> as TursoTryWithCapacityExt>::try_with_capacity_ext(additional)?;

        for item in iter {
            match item {
                Ok(value) => {
                    let _ = values.push_within_capacity(value);
                }
                Err(error) => return Ok(Err(F::from(error))),
            }
        }
        Ok(Ok(values))
    }
}

impl<T, C> TursoFromIterator<Option<T>> for Option<C>
where
    C: TursoFromIterator<T>,
{
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = Option<T>>,
    {
        let mut saw_none = false;
        let values = C::try_from_iter(iter.into_iter().map_while(|item| match item {
            Some(value) => Some(value),
            None => {
                saw_none = true;
                None
            }
        }))?;
        if saw_none {
            Ok(None)
        } else {
            Ok(Some(values))
        }
    }

    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = Option<T>>,
    {
        let Some(values) = self else {
            return Ok(());
        };
        let mut saw_none = false;
        values.try_extend(iter.into_iter().map_while(|item| match item {
            Some(value) => Some(value),
            None => {
                saw_none = true;
                None
            }
        }))?;
        if saw_none {
            *self = None;
        }
        Ok(())
    }
}

impl<T, E, F, C> TursoFromIterator<Result<T, E>> for Result<C, F>
where
    C: TursoFromIterator<T>,
    F: From<E>,
{
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = Result<T, E>>,
    {
        let iter = iter.into_iter();
        #[cfg(nightly)]
        {
            <Self as TrySpecFromResultIter<T, E, I::IntoIter>>::try_from_result_iter(iter)
        }
        #[cfg(not(nightly))]
        {
            try_from_result_iter(iter)
        }
    }

    fn try_extend<I>(&mut self, iter: I) -> Result<(), TryReserveError>
    where
        I: IntoIterator<Item = Result<T, E>>,
    {
        let Ok(values) = self else {
            return Ok(());
        };
        let mut error = None;
        values.try_extend(iter.into_iter().map_while(|item| match item {
            Ok(value) => Some(value),
            Err(err) => {
                error = Some(F::from(err));
                None
            }
        }))?;
        if let Some(error) = error {
            *self = Err(error);
        }
        Ok(())
    }
}

impl<I: Iterator> TursoIteratorExt for I {}

macro_rules! impl_tuple_from_iterator {
    ($(($ty:ident, $from_ty:ident, $item:ident, $collection:ident)),+) => {
        impl<$($ty,)+ $($from_ty,)+> TursoFromIterator<($($ty,)+)> for ($($from_ty,)+)
        where
            $($from_ty: TursoFromIterator<$ty>,)+
        {
            fn try_from_iter<Iter>(iter: Iter) -> Result<Self, TryReserveError>
            where
                Iter: IntoIterator<Item = ($($ty,)+)>,
            {
                let mut values = ($($from_ty::try_from_iter(std::iter::empty())?,)+);
                values.try_extend(iter)?;
                Ok(values)
            }

            fn try_extend<Iter>(&mut self, iter: Iter) -> Result<(), TryReserveError>
            where
                Iter: IntoIterator<Item = ($($ty,)+)>,
            {
                let ($($collection,)+) = self;
                for ($($item,)+) in iter {
                    $($collection.try_extend(std::iter::once($item))?;)+
                }
                Ok(())
            }
        }
    };
}

impl_tuple_from_iterator!((A, FromA, a, from_a));
impl_tuple_from_iterator!((A, FromA, a, from_a), (B, FromB, b, from_b));
impl_tuple_from_iterator!(
    (A, FromA, a, from_a),
    (B, FromB, b, from_b),
    (C, FromC, c, from_c)
);
impl_tuple_from_iterator!(
    (A, FromA, a, from_a),
    (B, FromB, b, from_b),
    (C, FromC, c, from_c),
    (D, FromD, d, from_d)
);
impl_tuple_from_iterator!(
    (A, FromA, a, from_a),
    (B, FromB, b, from_b),
    (C, FromC, c, from_c),
    (D, FromD, d, from_d),
    (E, FromE, e, from_e)
);
impl_tuple_from_iterator!(
    (A, FromA, a, from_a),
    (B, FromB, b, from_b),
    (C, FromC, c, from_c),
    (D, FromD, d, from_d),
    (E, FromE, e, from_e),
    (F, FromF, f, from_f)
);
impl_tuple_from_iterator!(
    (A, FromA, a, from_a),
    (B, FromB, b, from_b),
    (C, FromC, c, from_c),
    (D, FromD, d, from_d),
    (E, FromE, e, from_e),
    (F, FromF, f, from_f),
    (G, FromG, g, from_g)
);
impl_tuple_from_iterator!(
    (A, FromA, a, from_a),
    (B, FromB, b, from_b),
    (C, FromC, c, from_c),
    (D, FromD, d, from_d),
    (E, FromE, e, from_e),
    (F, FromF, f, from_f),
    (G, FromG, g, from_g),
    (H, FromH, h, from_h)
);
impl_tuple_from_iterator!(
    (A, FromA, a, from_a),
    (B, FromB, b, from_b),
    (C, FromC, c, from_c),
    (D, FromD, d, from_d),
    (E, FromE, e, from_e),
    (F, FromF, f, from_f),
    (G, FromG, g, from_g),
    (H, FromH, h, from_h),
    (I, FromI, i, from_i)
);
impl_tuple_from_iterator!(
    (A, FromA, a, from_a),
    (B, FromB, b, from_b),
    (C, FromC, c, from_c),
    (D, FromD, d, from_d),
    (E, FromE, e, from_e),
    (F, FromF, f, from_f),
    (G, FromG, g, from_g),
    (H, FromH, h, from_h),
    (I, FromI, i, from_i),
    (J, FromJ, j, from_j)
);
impl_tuple_from_iterator!(
    (A, FromA, a, from_a),
    (B, FromB, b, from_b),
    (C, FromC, c, from_c),
    (D, FromD, d, from_d),
    (E, FromE, e, from_e),
    (F, FromF, f, from_f),
    (G, FromG, g, from_g),
    (H, FromH, h, from_h),
    (I, FromI, i, from_i),
    (J, FromJ, j, from_j),
    (K, FromK, k, from_k)
);
impl_tuple_from_iterator!(
    (A, FromA, a, from_a),
    (B, FromB, b, from_b),
    (C, FromC, c, from_c),
    (D, FromD, d, from_d),
    (E, FromE, e, from_e),
    (F, FromF, f, from_f),
    (G, FromG, g, from_g),
    (H, FromH, h, from_h),
    (I, FromI, i, from_i),
    (J, FromJ, j, from_j),
    (K, FromK, k, from_k),
    (L, FromL, l, from_l)
);
