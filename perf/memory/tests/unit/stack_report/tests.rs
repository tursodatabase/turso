use super::*;

#[test]
fn statement_filter_matches_one_based_indexes() {
    let args = args_with_filters(vec![2, 4], Vec::new());

    assert!(!statement_matches_report_filters(1, "SELECT 1", &args));
    assert!(statement_matches_report_filters(2, "SELECT 2", &args));
    assert!(!statement_matches_report_filters(3, "SELECT 3", &args));
    assert!(statement_matches_report_filters(4, "SELECT 4", &args));
}

#[test]
fn sql_contains_filter_matches_any_substring() {
    let args = args_with_filters(Vec::new(), vec!["trigger", "GENERATED"]);

    assert!(statement_matches_report_filters(
        1,
        "CREATE TRIGGER cleanup AFTER INSERT ON t BEGIN SELECT 1; END",
        &args
    ));
    assert!(statement_matches_report_filters(
        2,
        "CREATE TABLE t(x INT GENERATED ALWAYS AS (1))",
        &args
    ));
    assert!(!statement_matches_report_filters(
        3,
        "CREATE TABLE t(x)",
        &args
    ));
}

#[test]
fn combined_filters_require_index_and_sql_match() {
    let args = args_with_filters(vec![3], vec!["trigger"]);

    assert!(!statement_matches_report_filters(
        2,
        "CREATE TRIGGER cleanup AFTER INSERT ON t BEGIN SELECT 1; END",
        &args
    ));
    assert!(!statement_matches_report_filters(3, "SELECT 1", &args));
    assert!(statement_matches_report_filters(
        3,
        "CREATE TRIGGER cleanup AFTER INSERT ON t BEGIN SELECT 1; END",
        &args
    ));
}

#[test]
fn stack_path_does_not_repeat_same_module_for_nested_spans() {
    let scope = vec![
        StackPathSegment {
            module_path: Some("turso_core::connection".to_string()),
            name: "prepare_with_origin".to_string(),
        },
        StackPathSegment {
            module_path: Some("turso_core::connection".to_string()),
            name: "compile_cmd".to_string(),
        },
    ];
    let event = StackPathSegment {
        module_path: Some("turso_core::connection".to_string()),
        name: "compile".to_string(),
    };

    assert_eq!(
        format_stack_path(&scope, &event),
        "turso_core::connection::prepare_with_origin::compile_cmd::compile"
    );
}

#[test]
fn stack_path_does_not_append_event_when_it_matches_current_span() {
    let scope = vec![StackPathSegment {
        module_path: Some("turso_core::connection".to_string()),
        name: "prepare_with_origin".to_string(),
    }];
    let event = StackPathSegment {
        module_path: Some("turso_core::connection".to_string()),
        name: "prepare_with_origin".to_string(),
    };

    assert_eq!(
        format_stack_path(&scope, &event),
        "turso_core::connection::prepare_with_origin"
    );
}

#[test]
fn stack_path_uses_tracing_style_names_when_scope_moves_to_another_module() {
    let scope = vec![
        StackPathSegment {
            module_path: Some("turso_core::connection".to_string()),
            name: "prepare_with_origin".to_string(),
        },
        StackPathSegment {
            module_path: Some("turso_core::translate".to_string()),
            name: "translate".to_string(),
        },
    ];
    let event = StackPathSegment {
        module_path: Some("turso_core::translate".to_string()),
        name: "dispatch".to_string(),
    };

    assert_eq!(
        format_stack_path(&scope, &event),
        "turso_core::connection::prepare_with_origin::translate::dispatch"
    );
}

#[test]
fn statement_stack_used_is_peak_delta_not_sum() {
    let report = build_statement_report(
        1,
        "SELECT 1",
        10_000,
        vec![
            StackSample {
                label: "outer".to_string(),
                detail: None,
                phase: StackPhase::Enter,
                remaining_stack: 9_000,
            },
            StackSample {
                label: "inner".to_string(),
                detail: None,
                phase: StackPhase::Enter,
                remaining_stack: 7_500,
            },
            StackSample {
                label: "inner:end".to_string(),
                detail: None,
                phase: StackPhase::Exit,
                remaining_stack: 9_000,
            },
            StackSample {
                label: "outer:end".to_string(),
                detail: None,
                phase: StackPhase::Exit,
                remaining_stack: 9_100,
            },
        ],
    );

    assert_eq!(report.stack_used, 2_500);
    assert_eq!(report.min_remaining_stack, 7_500);
    assert_eq!(report.spans[0].label, "inner");
    assert_eq!(report.spans[0].stack_used, 1_500);
    assert_eq!(report.spans[0].inclusive_stack_used, 1_500);
    assert_eq!(report.spans[0].cumulative_stack_used, 2_500);
    let outer = report
        .span_aggregates
        .iter()
        .find(|aggregate| aggregate.label == "outer")
        .expect("outer aggregate should exist");
    assert_eq!(outer.total_self_stack_used, 1_000);
    assert_eq!(outer.total_inclusive_stack_used, 2_500);
    assert_eq!(outer.peak_path_hits, 1);
    let inner = report
        .span_aggregates
        .iter()
        .find(|aggregate| aggregate.label == "inner")
        .expect("inner aggregate should exist");
    assert_eq!(inner.total_self_stack_used, 1_500);
    assert_eq!(inner.total_inclusive_stack_used, 1_500);
    assert_eq!(inner.peak_path_hits, 1);
    assert_eq!(
        report
            .spans
            .iter()
            .map(|span| span.stack_used)
            .sum::<usize>(),
        2_500
    );
}

#[test]
fn span_sort_keeps_trace_sequence_for_ties() {
    let report = build_statement_report(
        1,
        "SELECT 1",
        10_000,
        vec![
            StackSample {
                label: "first".to_string(),
                detail: None,
                phase: StackPhase::Sample,
                remaining_stack: 8_000,
            },
            StackSample {
                label: "second".to_string(),
                detail: None,
                phase: StackPhase::Sample,
                remaining_stack: 8_000,
            },
        ],
    );

    assert_eq!(report.spans[0].trace_sequence, 1);
    assert_eq!(report.spans[1].trace_sequence, 2);
}

#[test]
fn aggregate_spans_count_repeated_calls() {
    let report = build_statement_report(
        1,
        "SELECT 1",
        10_000,
        vec![
            StackSample {
                label: "leaf".to_string(),
                detail: Some("expr".to_string()),
                phase: StackPhase::Sample,
                remaining_stack: 9_000,
            },
            StackSample {
                label: "leaf".to_string(),
                detail: Some("expr".to_string()),
                phase: StackPhase::Sample,
                remaining_stack: 8_500,
            },
        ],
    );

    assert_eq!(report.span_aggregates.len(), 1);
    let aggregate = &report.span_aggregates[0];
    assert_eq!(aggregate.label, "leaf");
    assert_eq!(aggregate.detail.as_deref(), Some("expr"));
    assert_eq!(aggregate.calls, 2);
    assert_eq!(aggregate.total_self_stack_used, 2_500);
    assert_eq!(aggregate.max_self_stack_used, 1_500);
    assert_eq!(aggregate.total_inclusive_stack_used, 2_500);
    assert_eq!(aggregate.max_inclusive_stack_used, 1_500);
    assert_eq!(aggregate.peak_path_hits, 1);
}

#[test]
fn exit_samples_contribute_to_active_span_inclusive_usage() {
    let report = build_statement_report(
        1,
        "SELECT 1",
        10_000,
        vec![
            StackSample {
                label: "scope".to_string(),
                detail: None,
                phase: StackPhase::Enter,
                remaining_stack: 9_500,
            },
            StackSample {
                label: "scope:end".to_string(),
                detail: None,
                phase: StackPhase::Exit,
                remaining_stack: 9_000,
            },
        ],
    );

    assert_eq!(report.stack_used, 1_000);
    assert_eq!(report.spans.len(), 1);
    assert_eq!(report.spans[0].stack_used, 500);
    assert_eq!(report.spans[0].inclusive_stack_used, 1_000);
    assert_eq!(report.spans[0].peak_path_hits, 1);
}

#[test]
fn stacker_remaining_stack_tracks_real_stack_growth() {
    let before = stacker::remaining_stack().expect("remaining stack should be available");
    let inside = consume_stack_frame();
    assert!(
        before > inside,
        "remaining stack should shrink inside a stack-consuming frame: before={before}, inside={inside}"
    );
    assert!(
        before - inside >= 8 * 1024,
        "expected at least an 8KiB observed stack delta, got {} bytes",
        before - inside
    );
}

#[inline(never)]
fn consume_stack_frame() -> usize {
    let buffer = [0_u8; 16 * 1024];
    std::hint::black_box(&buffer);
    stacker::remaining_stack().expect("remaining stack should be available")
}

fn args_with_filters(statements: Vec<usize>, sql_contains: Vec<&str>) -> Args {
    Args {
        sql: PathBuf::from("-"),
        top: 40,
        format: OutputFormat::Human,
        statements,
        sql_contains: sql_contains.into_iter().map(str::to_string).collect(),
    }
}
