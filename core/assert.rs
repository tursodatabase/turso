/// turso_assert! is a direct replacement for assert! builtin macros which under the hood
/// uses Antithesis SDK to guide Antithesis simulator if --features antithesis is enabled
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! turso_assert {
    ($cond:expr, $msg:literal, $($optional:tt)+) => {
        assert!($cond, $msg, $($optional)+);
    };
    ($cond:expr, $msg:literal) => {
        assert!($cond, $msg);
    };
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! turso_assert {
    ($cond:expr, $msg:literal, $($optional:tt)+) => {
        antithesis_sdk::assert_always_or_unreachable!($cond, $msg);
        assert!($cond, $msg, $($optional)+);
    };
    ($cond:expr, $msg:literal) => {
        antithesis_sdk::assert_always_or_unreachable!($cond, $msg);
        assert!($cond, $msg);
    };
}

/// Assert that a condition is true at least once during testing.
/// This helps Antithesis identify rare conditions that should be triggered.
///
/// # Parameters
/// - `$cond` - The condition expression to evaluate
/// - `$msg` - A static string literal describing the assertion
/// - details (optional) - Additional context as `{ "key": value, ... }` JSON-like syntax
///
/// # Example
/// ```ignore
/// turso_assert_sometimes!(rare_condition, "Rare condition triggered");
/// turso_assert_sometimes!(rare_condition, "Rare condition triggered", { "key": value, "other": 42 });
/// ```
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! turso_assert_sometimes {
    ($cond:expr, $msg:literal) => {
        // When not using Antithesis, this is a no-op
        let _ = $cond;
    };
    ($cond:expr, $msg:literal, { $($details:tt)* }) => {
        // When not using Antithesis, this is a no-op
        let _ = $cond;
    };
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! turso_assert_sometimes {
    ($cond:expr, $msg:literal) => {
        antithesis_sdk::assert_sometimes!($cond, $msg);
    };
    ($cond:expr, $msg:literal, { $($details:tt)* }) => {
        antithesis_sdk::assert_sometimes!(
            $cond,
            $msg,
            &serde_json::json!({ $($details)* })
        );
    };
}

/// Assert that a code path is reached at least once during testing.
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! turso_assert_reachable {
    ($msg:literal) => {
        // When not using Antithesis, this is a no-op
    };
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! turso_assert_reachable {
    ($msg:literal) => {
        antithesis_sdk::assert_reachable!($msg);
    };
}

/// Assert that a code path is never reached during testing.
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! turso_assert_unreachable {
    ($msg:literal) => {
        unreachable!($msg)
    };
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! turso_assert_unreachable {
    ($msg:literal) => {{
        antithesis_sdk::assert_unreachable!($msg);
        unreachable!($msg)
    }};
}

/// Assert that left > right with more visibility for Antithesis.
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! turso_assert_greater_than {
    ($left:expr, $right:expr, $msg:literal) => {
        assert!($left > $right, $msg);
    };
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! turso_assert_greater_than {
    ($left:expr, $right:expr, $msg:literal) => {
        antithesis_sdk::assert_always_greater_than!($left, $right, $msg);
        assert!($left > $right, $msg);
    };
}

/// Assert that left >= right with more visibility for Antithesis.
/// Note: assert_always_greater_than_or_equal_to has a bug in antithesis_sdk 0.2.5,
/// so we can't use it yet. Use turso_assert! instead for >= comparisons.
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! turso_assert_greater_than_or_equal {
    ($left:expr, $right:expr, $msg:literal) => {
        assert!($left >= $right, $msg);
    };
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! turso_assert_greater_than_or_equal {
    ($left:expr, $right:expr, $msg:literal) => {
        // Bug in SDK: antithesis_sdk::assert_always_greater_than_or_equal_to!($left, $right, $msg);
        assert!($left >= $right, $msg);
    };
}

/// Assert that left < right with more visibility for Antithesis.
/// Note: assert_always_less_than has a bug in antithesis_sdk 0.2.5,
/// so we can't use it yet. Use turso_assert! instead for < comparisons.
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! turso_assert_less_than {
    ($left:expr, $right:expr, $msg:literal) => {
        assert!($left < $right, $msg);
    };
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! turso_assert_less_than {
    ($left:expr, $right:expr, $msg:literal) => {
        // Bug in SDK: antithesis_sdk::assert_always_less_than!($left, $right, $msg);
        assert!($left < $right, $msg);
    };
}

/// Assert that left <= right with more visibility for Antithesis.
/// Note: assert_always_less_than_or_equal_to has a bug in antithesis_sdk 0.2.5,
/// so we can't use it yet. Use turso_assert! instead for <= comparisons.
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! turso_assert_less_than_or_equal {
    ($left:expr, $right:expr, $msg:literal) => {
        assert!($left <= $right, $msg);
    };
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! turso_assert_less_than_or_equal {
    ($left:expr, $right:expr, $msg:literal) => {
        // Bug in SDK: antithesis_sdk::assert_always_less_than_or_equal_to!($left, $right, $msg);
        assert!($left <= $right, $msg);
    };
}

/// Assert that left == right with more visibility for Antithesis.
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! turso_assert_eq {
    ($left:expr, $right:expr, $msg:literal) => {
        assert_eq!($left, $right, $msg);
    };
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! turso_assert_eq {
    ($left:expr, $right:expr, $msg:literal) => {
        antithesis_sdk::assert_always!($left == $right, $msg);
        assert_eq!($left, $right, $msg);
    };
}

/// Assert that left != right with more visibility for Antithesis.
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! turso_assert_ne {
    ($left:expr, $right:expr, $msg:literal) => {
        assert_ne!($left, $right, $msg);
    };
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! turso_assert_ne {
    ($left:expr, $right:expr, $msg:literal) => {
        antithesis_sdk::assert_always!($left != $right, $msg);
        assert_ne!($left, $right, $msg);
    };
}

/// Assert that left > right at least once during testing.
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! turso_assert_sometimes_greater_than {
    ($left:expr, $right:expr, $msg:literal) => {
        let _ = ($left, $right);
    };
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! turso_assert_sometimes_greater_than {
    ($left:expr, $right:expr, $msg:literal) => {
        antithesis_sdk::assert_sometimes_greater_than!($left, $right, $msg);
    };
}

/// Assert that left < right at least once during testing.
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! turso_assert_sometimes_less_than {
    ($left:expr, $right:expr, $msg:literal) => {
        let _ = ($left, $right);
    };
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! turso_assert_sometimes_less_than {
    ($left:expr, $right:expr, $msg:literal) => {
        antithesis_sdk::assert_sometimes_less_than!($left, $right, $msg);
    };
}

/// Assert that left >= right at least once during testing.
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! turso_assert_sometimes_greater_than_or_equal {
    ($left:expr, $right:expr, $msg:literal) => {
        let _ = ($left, $right);
    };
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! turso_assert_sometimes_greater_than_or_equal {
    ($left:expr, $right:expr, $msg:literal) => {
        antithesis_sdk::assert_sometimes_greater_than_or_equal_to!($left, $right, $msg);
    };
}

/// Assert that left <= right at least once during testing.
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! turso_assert_sometimes_less_than_or_equal {
    ($left:expr, $right:expr, $msg:literal) => {
        let _ = ($left, $right);
    };
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! turso_assert_sometimes_less_than_or_equal {
    ($left:expr, $right:expr, $msg:literal) => {
        antithesis_sdk::assert_sometimes_less_than_or_equal_to!($left, $right, $msg);
    };
}

/// Assert that a condition is always true when evaluated, but only when it has been evaluated
/// at least once. Unlike `turso_assert!`, this won't fail if the code path is never reached.
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! turso_assert_always_some {
    ($cond:expr, $msg:literal) => {
        // When not using Antithesis, this is a no-op
        let _ = $cond;
    };
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! turso_assert_always_some {
    ($cond:expr, $msg:literal) => {
        antithesis_sdk::assert_always_some!($cond, $msg);
    };
}

/// Assert that a condition is true across all evaluations, at least once during testing.
/// Combines "always true" with "sometimes reached".
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! turso_assert_sometimes_all {
    ($cond:expr, $msg:literal) => {
        // When not using Antithesis, this is a no-op
        let _ = $cond;
    };
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! turso_assert_sometimes_all {
    ($cond:expr, $msg:literal) => {
        antithesis_sdk::assert_sometimes_all!($cond, $msg);
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
