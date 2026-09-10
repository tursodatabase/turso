pub struct FlatMapWithBuffer<T, F, Iter> {
    buffer: Vec<T>,
    fill_buffer: F,
    underlying_it: Iter,
}

impl<T, F, Iter, I> Iterator for FlatMapWithBuffer<T, F, Iter>
where
    Iter: Iterator<Item = I>,
    F: Fn(I, &mut Vec<T>),
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        while self.buffer.is_empty() {
            let next_el = self.underlying_it.next()?;
            (self.fill_buffer)(next_el, &mut self.buffer);
            // We will pop elements, so we reverse the buffer first.
            self.buffer.reverse();
        }
        self.buffer.pop()
    }
}

#[allow(dead_code)]
pub trait FlatMapWithBufferIter: Iterator {
    /// Function similar to `flat_map`, but allows reusing a shared `Vec`.
    fn flat_map_with_buffer<F, T>(self, fill_buffer: F) -> FlatMapWithBuffer<T, F, Self>
    where
        F: Fn(Self::Item, &mut Vec<T>),
        Self: Sized,
    {
        FlatMapWithBuffer {
            buffer: Vec::with_capacity(10),
            fill_buffer,
            underlying_it: self,
        }
    }
}

impl<T: ?Sized> FlatMapWithBufferIter for T where T: Iterator {}

#[cfg(test)]
mod tests {
    use crate::indexer::flat_map_with_buffer::FlatMapWithBufferIter;

    #[test]
    fn test_flat_map_with_buffer_empty() {
        let mut empty_iter = std::iter::empty::<usize>()
            .flat_map_with_buffer(|_val: usize, _buffer: &mut Vec<usize>| {});
        assert!(empty_iter.next().is_none());
    }

    #[test]
    fn test_flat_map_with_buffer_simple() {
        let vals: Vec<usize> = (1..5)
            .flat_map_with_buffer(|val: usize, buffer: &mut Vec<usize>| buffer.extend(0..val))
            .collect();
        assert_eq!(&[0, 0, 1, 0, 1, 2, 0, 1, 2, 3], &vals[..]);
    }

    #[test]
    fn test_flat_map_filling_no_elements_does_not_stop_iterator() {
        let vals: Vec<usize> = [2, 0, 0, 3]
            .into_iter()
            .flat_map_with_buffer(|val: usize, buffer: &mut Vec<usize>| buffer.extend(0..val))
            .collect();
        assert_eq!(&[0, 1, 0, 1, 2], &vals[..]);
    }
}
