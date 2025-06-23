//! Assertion macros that abstract the antithesis layer below it
//! Currently we are ignoring the details field for the assertions as
//! forcing users to use `serde_json` is very restrictive
//!
//! TODO: One idea to mitigate this is to use a similar macro syntax to tracing.
//! We could then convert all the structured bits of information to json as a final tranformation step
//! E.g assert_always!(true, "message", val, ?val2, %test_val)
//! this would all convert to something like {val: <val value>, val2: "<val2 debug value>", test_val: "<test_val display value>"}

#[macro_export]
#[cfg(not(feature = "antithesis"))]
macro_rules! assert_always {
    ($condition:expr, $message:literal) => {
        ::core::assert!($condition, $message);
    };
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! assert_always {
    ($condition:expr, $message:literal) => {
        ::antithesis_sdk::assert_always!($condition, $message);
    };
}

#[macro_export]
#[cfg(not(feature = "antithesis"))]
macro_rules! assert_always_greater_than {
    ($left:expr, $right:expr, $message:literal) => {
        ::core::assert!($left > $right, $message);
    };
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! assert_always_greater_than {
    ($left:expr, $right:expr, $message:literal) => {
        ::antithesis_sdk::assert_always_greater_than!($left, $right, $message);
    };
}

#[macro_export]
#[cfg(not(feature = "antithesis"))]
macro_rules! assert_always_greater_than_or_equal_to {
    ($left:expr, $right:expr, $message:literal) => {
        ::core::assert!($left >= $right, $message);
    };
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! assert_always_greater_than_or_equal_to {
    ($left:expr, $right:expr, $message:literal) => {
        ::antithesis_sdk::assert_always_greater_than_or_equal_to!($left, $right, $message);
    };
}

#[cfg(not(feature = "antithesis"))]
macro_rules! assert_always_less_than {
    ($left:expr, $right:expr, $message:literal) => {
        ::core::assert!($left < $right, $message);
    };
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! assert_always_less_than {
    ($left:expr, $right:expr, $message:literal) => {
        ::antithesis_sdk::assert_always_less_than!($left, $right, $message);
    };
}

#[cfg(not(feature = "antithesis"))]
macro_rules! assert_always_less_than_or_equal_to {
    ($left:expr, $right:expr, $message:literal) => {
        ::core::assert!($left <= $right, $message);
    };
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! assert_always_less_than_or_equal_to {
    ($left:expr, $right:expr, $message:literal) => {
        ::antithesis_sdk::assert_always_less_than_or_equal_to!($left, $right, $message);
    };
}

#[macro_export]
#[cfg(not(feature = "antithesis"))]
macro_rules! assert_always_or_unreachable {
    ($condition:expr, $message:literal) => {
        ::core::assert!($condition, $message);
    };
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! assert_always_or_unreachable {
    ($condition:expr, $message:literal) => {
        ::antithesis_sdk::assert_always_or_unreachable!($condition, $message);
    };
}

#[macro_export]
#[cfg(not(feature = "antithesis"))]
macro_rules! assert_always_some {
    ({$($($name:ident: $cond:expr),+ $(,)?)?}, $message:literal) => {
        ::core::assert!(($($($cond)||+)?), $message);
    };
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! assert_always_some {
    ({$($($name:ident: $cond:expr),+ $(,)?)?}, $message:literal) => {
        ::antithesis_sdk::assert_always_some!({$($($name: $cond),+)?}, $message);
    };
}

#[macro_export]
#[cfg(not(feature = "antithesis"))]
macro_rules! assert_reachable {
    ($message:literal) => {};
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! assert_reachable {
    ($message:literal) => {
        ::antithesis_sdk::assert_reachable!($message);
    };
}

#[macro_export]
#[cfg(not(feature = "antithesis"))]
macro_rules! assert_sometimes {
    ($condition:expr, $message:literal) => {};
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! assert_sometimes {
    ($condition:expr, $message:literal) => {
        ::antithesis_sdk::assert_sometimes!($condition, $message);
    };
}

#[macro_export]
#[cfg(not(feature = "antithesis"))]
macro_rules! assert_sometimes_all {
    ({$($($name:ident: $cond:expr),+ $(,)?)?}, $message:literal) => {};
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! assert_sometimes_all {
    ({$($($name:ident: $cond:expr),+ $(,)?)?}, $message:literal) => {
        ::antithesis_sdk::assert_sometimes_all!({$($($name: $cond),+)?}, $message);
    };
}

#[macro_export]
#[cfg(not(feature = "antithesis"))]
macro_rules! assert_sometimes_greater_than {
    ($left:expr, $right:expr, $message:literal) => {};
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! assert_sometimes_greater_than {
    ($left:expr, $right:expr, $message:literal) => {
        ::antithesis_sdk::assert_sometimes_greater_than!($left, $right, $message);
    };
}

#[macro_export]
#[cfg(not(feature = "antithesis"))]
macro_rules! assert_sometimes_greater_than_or_equal_to {
    ($left:expr, $right:expr, $message:literal) => {};
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! assert_sometimes_greater_than_or_equal_to {
    ($left:expr, $right:expr, $message:literal) => {
        ::antithesis_sdk::assert_sometimes_greater_than_or_equal_to!($left, $right, $message);
    };
}

#[macro_export]
#[cfg(not(feature = "antithesis"))]
macro_rules! assert_sometimes_less_than {
    ($left:expr, $right:expr, $message:literal) => {};
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! assert_sometimes_less_than {
    ($left:expr, $right:expr, $message:literal) => {
        ::antithesis_sdk::assert_sometimes_less_than!($left, $right, $message);
    };
}

#[macro_export]
#[cfg(not(feature = "antithesis"))]
macro_rules! assert_sometimes_less_than_or_equal_to {
    ($left:expr, $right:expr, $message:literal) => {};
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! assert_sometimes_less_than_or_equal_to {
    ($left:expr, $right:expr, $message:literal) => {
        ::antithesis_sdk::assert_sometimes_less_than_or_equal_to!($left, $right, $message);
    };
}

#[macro_export]
#[cfg(not(feature = "antithesis"))]
macro_rules! assert_unreachable {
    ($message:literal) => {
        ::core::unreachable!($message);
    };
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! assert_unreachable {
    ($message:literal) => {
        ::antithesis_sdk::assert_unreachable!($message);
    };
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_compiles() {
        assert_always!(true, "hi");
        assert_always_greater_than!(2, 1, "always greater than");
        assert_always_greater_than_or_equal_to!(2, 2, "always greater than or equal to");
        assert_always_less_than!(1, 2, "always less than");
        assert_always_less_than_or_equal_to!(2, 2, "always less than or equal to");
        assert_always_or_unreachable!(true, "always or unreachable");
        assert_always_some!({a: 1 == 1, b: 2 < 1}, "always some");
        assert_reachable!("reachable");
        assert_sometimes!(true, "sometimes");
        assert_sometimes_all!({a: 1 == 1, b: 2 > 1}, "sometimes all");
        assert_sometimes_greater_than!(2, 1, "sometimes greater than");
        assert_sometimes_greater_than_or_equal_to!(2, 2, "sometimes greater than or equal to");
        assert_sometimes_less_than!(1, 2, "sometimes less than");
        assert_sometimes_less_than_or_equal_to!(2, 2, "sometimes less than or equal to");
    }

    #[test]
    #[should_panic]
    #[cfg(not(feature = "antithesis"))]
    fn test_unreachable() {
        assert_unreachable!("unreachable");
    }

    #[test]
    #[cfg(feature = "antithesis")]
    fn test_unreachable() {
        assert_unreachable!("unreachable");
    }
}
