use super::{TursoFromIterator, TursoIteratorExt};
use crate::TryReserveError;

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
        let mut error = None;
        let values = C::try_from_iter(iter.into_iter().map_while(|item| match item {
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
