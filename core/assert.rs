/// turso_assert! is a direct replacement for assert! builtin macros which under the hood
/// uses Antithesis SDK to guide Antithesis simulator if --cfg antithesis is enabled
#[cfg(not(antithesis))]
#[macro_export]
macro_rules! turso_assert {
    ($cond:expr, $msg:literal, $($optional:tt)+) => {
        assert!($cond, $msg, $($optional)+);
    };
    ($cond:expr, $msg:literal) => {
        assert!($cond, $msg);
    };
}

#[cfg(antithesis)]
#[macro_export]
macro_rules! turso_assert {
    ($cond:expr, $msg:literal, $($optional:tt)+) => {
        {
            let __cond = $cond;
            antithesis_sdk::assert_always_or_unreachable!(__cond, $msg);
            assert!(__cond, $msg, $($optional)+);
        }
    };
    ($cond:expr, $msg:literal) => {
        {
            let __cond = $cond;
            antithesis_sdk::assert_always_or_unreachable!(__cond, $msg);
            assert!(__cond, $msg);
        }
    };
}

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
