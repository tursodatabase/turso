/// turso_assert! is a direct replacement for assert! builtin macros which under the hood
/// uses Antithesis SDK to guide Antithesis simulator if --features antithesis is enabled
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! turso_assert {
    ($cond:expr, $msg:literal, $($optional:tt)+) => {
        ::core::assert!($cond, $msg, $($optional)+);
    };
    ($cond:expr, $msg:literal) => {
        ::core::assert!($cond, $msg);
    };
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! turso_assert {
    ($cond:expr, $msg:literal, $($optional:tt)+) => {
        $crate::assert::antithesis_sdk::assert_always_or_unreachable!($cond, $msg);
        ::core::assert!($cond, $msg, $($optional)+);
    };
    ($cond:expr, $msg:literal) => {
        $crate::assert::antithesis_sdk::assert_always_or_unreachable!($cond, $msg);
        ::core::assert!($cond, $msg);
    };
}

#[macro_export]
#[cfg(not(feature = "antithesis"))]
macro_rules! turso_assert_eq {
    ($left:expr, $right:expr, $message:literal) => {
        ::core::assert_eq!($left, $right, $message);
    };
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! turso_assert_eq {
    ($left:expr, $right:expr, $message:literal) => {
        $crate::assert::antithesis_sdk::assert_always!($left == $right, $message);
    };
}

#[macro_export]
#[cfg(not(feature = "antithesis"))]
macro_rules! turso_assert_ne {
    ($left:expr, $right:expr, $message:literal) => {
        ::core::assert_ne!($left, $right, $message);
    };
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! turso_assert_ne {
    ($left:expr, $right:expr, $message:literal) => {
        $crate::assert::antithesis_sdk::assert_always!($left != $right, $message);
    };
}

#[macro_export]
#[cfg(not(feature = "antithesis"))]
macro_rules! turso_assert_reachable {
    ($message:literal) => {};
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! turso_assert_reachable {
    ($message:literal) => {
        $crate::assert::antithesis_sdk::assert_reachable!($message);
    };
}

#[macro_export]
#[cfg(not(feature = "antithesis"))]
macro_rules! turso_assert_sometimes {
    ($condition:expr, $message:literal) => {};
}

#[macro_export]
#[cfg(feature = "antithesis")]
macro_rules! turso_assert_sometimes {
    ($condition:expr, $message:literal) => {
        $crate::assert::antithesis_sdk::assert_sometimes!($condition, $message);
    };
}

#[cfg(feature = "antithesis")]
pub use antithesis_sdk;

#[cfg(test)]
mod tests {

    #[test]
    fn test_compiles() {
        turso_assert_eq!(100, 100, "always eq");
        turso_assert_ne!(1, 2, "always not eq");
        turso_assert!(true, "hi");

        turso_assert_sometimes!(true, "sometimes");
        turso_assert_reachable!("reachable");
    }
}
