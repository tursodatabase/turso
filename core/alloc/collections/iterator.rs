use super::{TursoFromIterator, TursoIteratorExt};
use crate::alloc::TryReserveError;

impl<T, C> TursoFromIterator<Option<T>> for Option<C>
where
    C: TursoFromIterator<T>,
{
    fn try_from_iter<I>(iter: I) -> Result<Self, TryReserveError>
    where
        I: IntoIterator<Item = Option<T>>,
    {
        let mut saw_none = false;
        let values = C::try_from_iter(iter.into_iter().scan((), |(), item| match item {
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
        let values = C::try_from_iter(iter.into_iter().scan((), |(), item| match item {
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
}

impl<I: Iterator> TursoIteratorExt for I {}
