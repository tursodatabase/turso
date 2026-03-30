#[cfg(test)]
#[allow(unexpected_cfgs)]
mod tests {
    fn panic_message(f: impl FnOnce() + std::panic::UnwindSafe) -> String {
        let err = std::panic::catch_unwind(f).unwrap_err();
        if let Some(s) = err.downcast_ref::<String>() {
            s.clone()
        } else if let Some(s) = err.downcast_ref::<&str>() {
            s.to_string()
        } else {
            panic!("unexpected panic payload type");
        }
    }

    #[test]
    fn test_turso_assert_details_in_panic_message() {
        let msg = panic_message(|| {
            let page_id = 42;
            turso_macros::turso_assert!(false, "page must be dirty", { "page_id": page_id });
        });
        assert!(
            msg.contains("page must be dirty") && msg.contains("page_id=42"),
            "expected details in panic message, got: {msg}"
        );
    }

    #[test]
    fn test_turso_assert_multiple_details() {
        let msg = panic_message(|| {
            let x = 1;
            let y = 2;
            turso_macros::turso_assert!(false, "check failed", { "x": x, "y": y });
        });
        assert!(
            msg.contains("check failed") && msg.contains("x=1") && msg.contains("y=2"),
            "expected all details in panic message, got: {msg}"
        );
    }

    #[test]
    fn test_turso_assert_no_details_still_works() {
        let msg = panic_message(|| {
            turso_macros::turso_assert!(false, "simple message");
        });
        assert!(
            msg.contains("simple message"),
            "expected message in panic, got: {msg}"
        );
    }

    #[test]
    fn test_turso_assert_eq_details_in_panic_message() {
        let msg = panic_message(|| {
            let expected = 10;
            turso_macros::turso_assert_eq!(1, 2, "values must match", { "expected": expected });
        });
        assert!(
            msg.contains("values must match") && msg.contains("expected=10"),
            "expected details in assert_eq panic message, got: {msg}"
        );
    }

    #[test]
    fn test_turso_assert_greater_than_details() {
        let msg = panic_message(|| {
            let limit = 100;
            turso_macros::turso_assert_greater_than!(5, 10, "must be greater", { "limit": limit });
        });
        assert!(
            msg.contains("must be greater") && msg.contains("limit=100"),
            "expected details in assert_greater_than panic message, got: {msg}"
        );
    }

    #[test]
    fn test_turso_assert_string_detail_values() {
        let msg = panic_message(|| {
            let state = format!("{:?}", vec![1, 2, 3]);
            turso_macros::turso_assert!(false, "bad state", { "state": state });
        });
        assert!(
            msg.contains("bad state") && msg.contains("state="),
            "expected string detail in panic message, got: {msg}"
        );
    }

    #[test]
    #[allow(clippy::nonminimal_bool)]
    fn test_turso_assert_some_passes_when_one_true() {
        turso_macros::turso_assert_some!(
            {a: true, b: false},
            "at least one should be true"
        );
    }

    #[test]
    #[allow(clippy::nonminimal_bool)]
    fn test_turso_assert_some_passes_when_all_true() {
        turso_macros::turso_assert_some!(
            {a: true, b: true},
            "at least one should be true"
        );
    }

    #[test]
    #[allow(clippy::nonminimal_bool)]
    fn test_turso_assert_some_panics_when_none_true() {
        let msg = panic_message(|| {
            turso_macros::turso_assert_some!(
                {a: false, b: false},
                "at least one should be true"
            );
        });
        assert!(
            msg.contains("at least one should be true"),
            "expected message in panic, got: {msg}"
        );
    }

    #[test]
    #[allow(clippy::nonminimal_bool)]
    fn test_turso_assert_some_details_in_panic() {
        let msg = panic_message(|| {
            let table = "users";
            turso_macros::turso_assert_some!(
                {a: false, b: false},
                "row must have data",
                { "table": table }
            );
        });
        assert!(
            msg.contains("row must have data") && msg.contains("table="),
            "expected details in panic message, got: {msg}"
        );
    }

    #[test]
    #[allow(clippy::nonminimal_bool)]
    fn test_turso_assert_all_passes_when_all_true() {
        turso_macros::turso_assert_all!(
            {a: true, b: true},
            "all should be true"
        );
    }

    #[test]
    #[allow(clippy::nonminimal_bool)]
    fn test_turso_assert_all_panics_when_one_false() {
        let msg = panic_message(|| {
            turso_macros::turso_assert_all!(
                {a: true, b: false},
                "all should be true"
            );
        });
        assert!(
            msg.contains("all should be true"),
            "expected message in panic, got: {msg}"
        );
    }

    #[test]
    #[allow(clippy::nonminimal_bool)]
    fn test_turso_assert_all_panics_when_all_false() {
        let msg = panic_message(|| {
            turso_macros::turso_assert_all!(
                {a: false, b: false},
                "all should be true"
            );
        });
        assert!(
            msg.contains("all should be true"),
            "expected message in panic, got: {msg}"
        );
    }

    #[test]
    #[allow(clippy::nonminimal_bool)]
    fn test_turso_assert_all_details_in_panic() {
        let msg = panic_message(|| {
            let magic = 0xDEAD;
            turso_macros::turso_assert_all!(
                {a: true, b: false},
                "file must be well-formed",
                { "magic": magic }
            );
        });
        assert!(
            msg.contains("file must be well-formed") && msg.contains("magic="),
            "expected details in panic message, got: {msg}"
        );
    }
}
