pub mod capi;
pub mod rsapi;

#[macro_export]
macro_rules! assert_send {
    ($($ty:ty),+ $(,)?) => {
        const _: fn() = || {
            fn check<T: Send>() {}
            $( check::<$ty>(); )+
        };
    };
}

#[macro_export]
macro_rules! assert_sync {
    ($($ty:ty),+ $(,)?) => {
        const _: fn() = || {
            fn check<T: Sync>() {}
            $( check::<$ty>(); )+
        };
    };
}
