/// Assert that a type implements Send at compile time.
/// Usage: assert_send!(MyType);
/// Usage: assert_send!(Type1, Type2, Type3);
macro_rules! assert_send {
    ($($t:ty),+ $(,)?) => {
        #[cfg(test)]
        $(const _: () = {
            const fn _assert_send<T: ?Sized + Send>() {}
            _assert_send::<$t>();
        };)+
    };
}

pub(crate) use assert_send;

/// Assert that a type implements Sync at compile time.
/// Usage: assert_sync!(MyType);
/// Usage: assert_sync!(Type1, Type2, Type3);
macro_rules! assert_sync {
    ($($t:ty),+ $(,)?) => {
        #[cfg(test)]
        $(const _: () = {
            const fn _assert_sync<T: ?Sized + Sync>() {}
            _assert_sync::<$t>();
        };)+
    };
}
pub(crate) use assert_sync;

/// Assert that a type implements both Send and Sync at compile time.
/// Usage: assert_send_sync!(MyType);
/// Usage: assert_send_sync!(Type1, Type2, Type3);
macro_rules! assert_send_sync {
    ($($t:ty),+ $(,)?) => {
        #[cfg(test)]
        $(const _: () = {
            const fn _assert_send<T: ?Sized + Send>() {}
            const fn _assert_sync<T: ?Sized + Sync>() {}
            _assert_send::<$t>();
            _assert_sync::<$t>();
        };)+
    };
}
pub(crate) use assert_send_sync;
