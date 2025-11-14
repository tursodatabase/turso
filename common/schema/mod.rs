pub mod affinity;
pub mod collation;
pub mod column;

#[macro_export]
macro_rules! eq_ignore_ascii_case {
    ( $var:expr, $value:literal ) => {{
        ::turso_macros::match_ignore_ascii_case!(match $var {
            $value => true,
            _ => false,
        })
    }};
}

#[macro_export]
macro_rules! contains_ignore_ascii_case {
    ( $var:expr, $value:literal ) => {{
        let compare_to_idx = $var.len().saturating_sub($value.len());
        if $var.len() < $value.len() {
            false
        } else {
            let mut result = false;
            for i in 0..=compare_to_idx {
                if eq_ignore_ascii_case!(&$var[i..i + $value.len()], $value) {
                    result = true;
                    break;
                }
            }

            result
        }
    }};
}

#[macro_export]
macro_rules! starts_with_ignore_ascii_case {
    ( $var:expr, $value:literal ) => {{
        if $var.len() < $value.len() {
            false
        } else {
            eq_ignore_ascii_case!(&$var[..$value.len()], $value)
        }
    }};
}

#[macro_export]
macro_rules! ends_with_ignore_ascii_case {
    ( $var:expr, $value:literal ) => {{
        if $var.len() < $value.len() {
            false
        } else {
            eq_ignore_ascii_case!(&$var[$var.len() - $value.len()..], $value)
        }
    }};
}
