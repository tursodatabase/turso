use super::{
    AllocError, BinaryHeap, Box, TryReserveError, TursoBinaryHeapExt, TursoTryNewExt,
    TursoTryWithCapacityExt, TursoVecDequeExt, TursoVecExt, Vec, VecDeque,
};

pub trait TryClone: Sized {
    type Error;

    fn try_clone(&self) -> Result<Self, Self::Error>;
}

impl<T: Clone> TryClone for Box<T> {
    type Error = AllocError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        <Self as TursoTryNewExt<T>>::try_new((**self).clone())
    }
}

impl<T: Clone> TryClone for Vec<T> {
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        let mut cloned = <Self as TursoTryWithCapacityExt>::try_with_capacity(self.len())?;
        cloned.try_extend(self.iter().cloned())?;
        Ok(cloned)
    }
}

impl<T: Clone> TryClone for VecDeque<T> {
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        let mut cloned = <Self as TursoTryWithCapacityExt>::try_with_capacity(self.len())?;
        cloned.try_extend(self.iter().cloned())?;
        Ok(cloned)
    }
}

impl<T: Clone + Ord> TryClone for BinaryHeap<T> {
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        let mut cloned = <Self as TursoTryWithCapacityExt>::try_with_capacity(self.len())?;
        cloned.try_extend(self.iter().cloned())?;
        Ok(cloned)
    }
}
