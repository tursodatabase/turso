use std::ops::Deref;

#[derive(Debug)]
pub struct Wrapper<T> {
    pub inner: T,
}

impl<T> Deref for Wrapper<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

unsafe impl<T> Send for Wrapper<T> {}
unsafe impl<T> Sync for Wrapper<T> {}
