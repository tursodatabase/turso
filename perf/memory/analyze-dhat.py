#!/usr/bin/env python3
"""
Analyze dhat-heap.json and produce a human-readable allocation report.

Usage:
    python3 analyze-dhat.py [dhat-heap.json] [--top N] [--sort-by METRIC] [--filter PATTERN]

Metrics:
    tb   - total bytes allocated (cumulative, includes freed)
    tbk  - total blocks (allocation count)
    mb   - max bytes live at once (high-water mark for this site)
    gb   - bytes live at global peak (t-gmax snapshot)
    eb   - bytes still live at exit (potential leaks)

Examples:
    python3 analyze-dhat.py dhat-heap.json --top 20 --sort-by gb
    python3 analyze-dhat.py dhat-heap.json --sort-by eb --filter turso_core
    python3 analyze-dhat.py dhat-heap.json --sort-by tb --top 30
"""

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

METRICS = {
    "tb": ("Total bytes allocated", "tb", "tbk"),
    "tbk": ("Total allocation count", "tb", "tbk"),
    "mb": ("Max bytes live at once", "mb", "mbk"),
    "gb": ("Bytes live at global peak", "gb", "gbk"),
    "eb": ("Bytes live at exit", "eb", "ebk"),
}

TRACE_OVERHEAD_PATTERNS = (
    "memory_benchmark::trace",
    "memory/src/trace.rs",
)


def format_bytes(b: int) -> str:
    if b >= 1 << 30:
        return f"{b / (1 << 30):.2f} GB"
    if b >= 1 << 20:
        return f"{b / (1 << 20):.2f} MB"
    if b >= 1 << 10:
        return f"{b / (1 << 10):.2f} KB"
    return f"{b} B"


def parse_frame(frame: str) -> str:
    """Extract the meaningful part of a frame string."""
    # Format: "0xADDR: symbol (file:line:col)"
    if ": " in frame:
        # Drop the address prefix
        return frame.split(": ", 1)[1]
    return frame


def first_relevant_frame(frames: list[str]) -> str:
    """Find the first frame that's in project code (not alloc internals)."""
    skip_prefixes = (
        "alloc::",
        "core::",
        "std::",
        "<dhat::",
        "__rustc::",
        "<alloc::",
        "[root]",
        "hashbrown::",
        "<hashbrown::",
    )
    for f in frames:
        parsed = parse_frame(f)
        if not any(parsed.startswith(p) for p in skip_prefixes) and "???" not in f:
            return parsed
    # Fallback: return the deepest non-root frame
    for f in reversed(frames):
        if f != "[root]":
            return parse_frame(f)
    return "<unknown>"


def build_callstack(pp: dict, ftbl: list[str], max_depth: int = 8) -> list[str]:
    """Build a human-readable callstack from frame indices."""
    frames = []
    for idx in pp["fs"]:
        if idx < len(ftbl):
            frame = ftbl[idx]
            if frame == "[root]":
                continue
            parsed = parse_frame(frame)
            frames.append(parsed)
            if len(frames) >= max_depth:
                break
    return frames


def is_trace_overhead_pp(pp: dict, ftbl: list[str]) -> bool:
    frames = [ftbl[i] for i in pp["fs"] if i < len(ftbl)]
    return any(any(pattern in frame for pattern in TRACE_OVERHEAD_PATTERNS) for frame in frames)


def split_trace_overhead(pps: list[dict], ftbl: list[str]) -> tuple[list[dict], list[dict]]:
    kept = []
    ignored = []
    for pp in pps:
        if is_trace_overhead_pp(pp, ftbl):
            ignored.append(pp)
        else:
            kept.append(pp)
    return kept, ignored


def sum_pps(pps: list[dict]) -> dict:
    return {
        "tb": sum(pp["tb"] for pp in pps),
        "tbk": sum(pp["tbk"] for pp in pps),
        "gb": sum(pp["gb"] for pp in pps),
        "gbk": sum(pp["gbk"] for pp in pps),
        "eb": sum(pp["eb"] for pp in pps),
        "ebk": sum(pp["ebk"] for pp in pps),
    }


def aggregate_by_source(pps: list[dict], ftbl: list[str]) -> dict:
    """Aggregate allocation stats by source location (first relevant frame)."""
    agg = {}
    for pp in pps:
        frames = [ftbl[i] for i in pp["fs"] if i < len(ftbl)]
        key = first_relevant_frame(frames)
        if key not in agg:
            agg[key] = {"tb": 0, "tbk": 0, "mb": 0, "mbk": 0, "gb": 0, "gbk": 0, "eb": 0, "ebk": 0, "pps": []}
        for metric in ("tb", "tbk", "mb", "mbk", "gb", "gbk", "eb", "ebk"):
            agg[key][metric] += pp[metric]
        agg[key]["pps"].append(pp)
    return agg


def print_summary(data: dict, pps: list[dict], ignored_trace_pps: list[dict]):
    """Print the global summary from the dhat file."""
    te = data.get("te", 0)
    tg = data.get("tg", 0)
    totals = sum_pps(pps)

    print("=" * 70)
    print("DHAT HEAP ANALYSIS REPORT")
    print("=" * 70)
    print(f"Command:         {data.get('cmd', 'N/A')}")
    print(f"Allocation sites: {len(pps)}")
    if ignored_trace_pps:
        ignored = sum_pps(ignored_trace_pps)
        print(
            f"Trace overhead:   ignored {len(ignored_trace_pps)} sites "
            f"({format_bytes(ignored['tb'])} total, {format_bytes(ignored['gb'])} at t-gmax)"
        )
    print(f"Total runtime:   {te / 1_000_000:.2f}s")
    print(f"Global peak at:  {tg / 1_000_000:.2f}s")
    print()
    print(f"Total allocated:        {format_bytes(totals['tb'])} in {totals['tbk']:,} blocks")
    print(f"At global peak (t-gmax): {format_bytes(totals['gb'])} in {totals['gbk']:,} blocks")
    print(f"At exit (t-end):         {format_bytes(totals['eb'])} in {totals['ebk']:,} blocks")
    print()


def format_time_us(us: int) -> str:
    return f"{us / 1_000_000:.6f}s"


def event_start_us(event: dict) -> int:
    return event.get("at_us", event.get("begin_us", 0))


def event_end_us(event: dict) -> int:
    return event.get("at_us", event.get("end_us", event_start_us(event)))


def overlaps_time(event: dict, target_us: int, window_us: int) -> bool:
    return event_start_us(event) - window_us <= target_us <= event_end_us(event) + window_us


def summarize_sql_templates(event: dict, limit: int = 5) -> list[tuple[str, int]]:
    if "sql_summary" in event:
        items = [(row["sql"], row["count"]) for row in event["sql_summary"]]
        items.sort(key=lambda item: item[1], reverse=True)
        return items[:limit]
    return Counter(event.get("sql", [])).most_common(limit)


def print_trace_correlation(trace: dict, target_us: int, window_us: int):  # noqa: C901
    events = trace.get("events", [])
    phase_markers = sorted(
        (event for event in events if event.get("kind") == "phase_marker"),
        key=event_start_us,
    )
    current_phase = None
    for event in phase_markers:
        if event["at_us"] <= target_us:
            current_phase = event
        else:
            break

    active_batches = [
        event for event in events if event.get("kind") == "batch" and overlaps_time(event, target_us, window_us)
    ]
    active_statements = [
        event for event in events if event.get("kind") == "statement" and overlaps_time(event, target_us, window_us)
    ]
    active_batches.sort(key=lambda event: (event.get("connection", 0), event.get("batch", 0)))
    active_statements.sort(
        key=lambda event: (event.get("connection", 0), event.get("batch", 0), event.get("statement", 0))
    )

    print("-" * 70)
    print(f"WORKLOAD TRACE AT DHAT GLOBAL PEAK ({format_time_us(target_us)})")
    if window_us:
        print(f"Window: +/- {window_us}us")
    print("-" * 70)

    if current_phase:
        print(f"Phase: {current_phase['phase']} (entered at {format_time_us(current_phase['at_us'])})")
    else:
        print("Phase: <no phase marker before peak>")

    if active_batches:
        print()
        print(f"Active batches: {len(active_batches)}")
        for event in active_batches[:10]:
            print(
                f"  conn={event['connection']} batch={event['batch']} phase={event['phase']} "
                f"statements={event['statement_count']} "
                f"[{format_time_us(event['begin_us'])}, {format_time_us(event['end_us'])}]"
            )
            for sql, count in summarize_sql_templates(event):
                print(f"    {count:>5}x {sql}")
        if len(active_batches) > 10:
            print(f"  ... {len(active_batches) - 10} more active batches")
    else:
        print()
        print("Active batches: none")

    if active_statements:
        print()
        print(f"Active statements: {len(active_statements)}")
        for event in active_statements[:20]:
            print(
                f"  conn={event['connection']} batch={event['batch']} stmt={event['statement']} "
                f"phase={event['phase']} [{format_time_us(event['begin_us'])}, {format_time_us(event['end_us'])}]"
            )
            print(f"    {event['sql']}")
        if len(active_statements) > 20:
            print(f"  ... {len(active_statements) - 20} more active statements")
    else:
        print()
        print("Active statements: none")

    if not active_batches and not active_statements:
        nearby = sorted(
            (event for event in events if event.get("kind") in ("batch", "statement")),
            key=lambda event: min(
                abs(target_us - event_start_us(event)),
                abs(target_us - event_end_us(event)),
            ),
        )[:5]
        if nearby:
            print()
            print("Nearest workload events:")
            for event in nearby:
                print(
                    f"  {event['kind']} conn={event.get('connection')} batch={event.get('batch')} "
                    f"phase={event.get('phase')} "
                    f"[{format_time_us(event_start_us(event))}, {format_time_us(event_end_us(event))}]"
                )
    print()


def print_top_sites(agg: dict, sort_by: str, top: int, filter_pat: str | None):
    """Print the top allocation sites sorted by the chosen metric."""
    metric_desc, bytes_key, blocks_key = METRICS[sort_by]

    items = list(agg.items())
    if filter_pat:
        items = [(k, v) for k, v in items if filter_pat.lower() in k.lower()]

    items.sort(key=lambda x: x[1][sort_by], reverse=True)
    items = items[:top]

    print("-" * 70)
    print(f"TOP {len(items)} ALLOCATION SITES (sorted by: {metric_desc})")
    if filter_pat:
        print(f"Filter: {filter_pat}")
    print("-" * 70)

    for rank, (site, stats) in enumerate(items, 1):
        print(f"\n#{rank}  {site}")
        print(f"     Total:    {format_bytes(stats['tb']):>12} ({stats['tbk']:>8,} allocs)")
        print(f"     Max live: {format_bytes(stats['mb']):>12} ({stats['mbk']:>8,} blocks)")
        print(f"     At peak:  {format_bytes(stats['gb']):>12} ({stats['gbk']:>8,} blocks)")
        print(f"     At exit:  {format_bytes(stats['eb']):>12} ({stats['ebk']:>8,} blocks)")


def print_detailed_stacks(pps: list[dict], ftbl: list[str], sort_by: str, top: int, filter_pat: str | None):
    """Print top allocation sites with full callstacks."""
    filtered = pps
    if filter_pat:

        def matches(pp):
            frames = [ftbl[i] for i in pp["fs"] if i < len(ftbl)]
            return any(filter_pat.lower() in f.lower() for f in frames)

        filtered = [pp for pp in pps if matches(pp)]

    filtered.sort(key=lambda pp: pp[sort_by], reverse=True)
    filtered = filtered[:top]

    print()
    print("-" * 70)
    print(f"TOP {len(filtered)} ALLOCATION STACKS (sorted by: {METRICS[sort_by][0]})")
    if filter_pat:
        print(f"Filter: {filter_pat}")
    print("-" * 70)

    for rank, pp in enumerate(filtered, 1):
        stack = build_callstack(pp, ftbl)
        print(
            f"\n#{rank}  Total: {format_bytes(pp['tb'])} ({pp['tbk']:,} allocs) | "
            f"Peak: {format_bytes(pp['gb'])} | Exit: {format_bytes(pp['eb'])}"
        )
        for i, frame in enumerate(stack):
            prefix = "  -> " if i == 0 else "     "
            print(f"{prefix}{frame}")


def print_module_summary(agg: dict, sort_by: str):
    """Aggregate and print stats by top-level module/crate."""
    modules = {}
    for site, stats in agg.items():
        # Extract module name: "turso_core::vdbe::..." -> "turso_core"
        if "::" in site:
            mod_name = site.split("::")[0]
            # Strip leading < for trait impls like "<turso_core::..."
            mod_name = mod_name.lstrip("<")
        else:
            mod_name = "<other>"
        if mod_name not in modules:
            modules[mod_name] = {"tb": 0, "tbk": 0, "mb": 0, "mbk": 0, "gb": 0, "gbk": 0, "eb": 0, "ebk": 0}
        for metric in ("tb", "tbk", "mb", "mbk", "gb", "gbk", "eb", "ebk"):
            modules[mod_name][metric] += stats[metric]

    items = sorted(modules.items(), key=lambda x: x[1][sort_by], reverse=True)

    print()
    print("-" * 70)
    print(f"MODULE SUMMARY (sorted by: {METRICS[sort_by][0]})")
    print("-" * 70)
    print(f"{'Module':<30} {'Total':>12} {'Allocs':>10} {'At Peak':>12} {'At Exit':>12}")
    print(f"{'':<30} {'':>12} {'':>10} {'':>12} {'':>12}")
    for mod_name, stats in items:
        if stats[sort_by] == 0:
            continue
        print(
            f"{mod_name:<30} {format_bytes(stats['tb']):>12} {stats['tbk']:>10,} "
            f"{format_bytes(stats['gb']):>12} {format_bytes(stats['eb']):>12}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Analyze dhat-heap.json and produce allocation reports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "file", nargs="?", default="dhat-heap.json", help="Path to dhat-heap.json (default: dhat-heap.json)"
    )
    parser.add_argument("--top", type=int, default=15, help="Number of top entries to show (default: 15)")
    parser.add_argument(
        "--sort-by",
        choices=list(METRICS.keys()),
        default="gb",
        help="Metric to sort by (default: gb = bytes at global peak)",
    )
    parser.add_argument(
        "--filter",
        type=str,
        default=None,
        help="Filter sites/stacks containing this substring (e.g. 'turso_core', 'btree', 'mvcc')",
    )
    parser.add_argument("--stacks", action="store_true", help="Show full callstacks for top allocation sites")
    parser.add_argument("--modules", action="store_true", help="Show per-module/crate summary")
    parser.add_argument(
        "--trace",
        type=str,
        default=None,
        help="Path to memory-benchmark workload trace JSON for t-gmax correlation",
    )
    parser.add_argument(
        "--trace-window-us",
        type=int,
        default=0,
        help="Include trace events within this many microseconds of t-gmax (default: 0)",
    )
    parser.add_argument(
        "--include-trace-overhead",
        action="store_true",
        help="Keep memory-benchmark trace bookkeeping allocation sites in allocation reports",
    )
    parser.add_argument(
        "--json", action="store_true", help="Output aggregated data as JSON (for programmatic consumption)"
    )
    args = parser.parse_args()

    path = Path(args.file)
    if not path.exists():
        print(f"Error: {path} not found. Run the benchmark first to generate it.", file=sys.stderr)
        sys.exit(1)

    with open(path) as f:
        data = json.load(f)

    pps = data["pps"]
    ftbl = data["ftbl"]
    ignored_trace_pps = []

    if args.trace and not args.include_trace_overhead:
        pps, ignored_trace_pps = split_trace_overhead(pps, ftbl)

    if args.json:
        agg = aggregate_by_source(pps, ftbl)
        # Strip the raw pps from the output
        output = {}
        for site, stats in agg.items():
            output[site] = {k: v for k, v in stats.items() if k != "pps"}
        json.dump(output, sys.stdout, indent=2)
        print()
        return

    print_summary(data, pps, ignored_trace_pps)
    if args.trace:
        trace_path = Path(args.trace)
        if not trace_path.exists():
            print(f"Error: {trace_path} not found.", file=sys.stderr)
            sys.exit(1)
        if "tg" not in data:
            print("Error: dhat file does not contain a global peak timestamp.", file=sys.stderr)
            sys.exit(1)
        with open(trace_path) as f:
            trace = json.load(f)
        print_trace_correlation(trace, int(data["tg"]), args.trace_window_us)

    agg = aggregate_by_source(pps, ftbl)
    print_top_sites(agg, args.sort_by, args.top, args.filter)

    if args.modules:
        print_module_summary(agg, args.sort_by)

    if args.stacks:
        print_detailed_stacks(pps, ftbl, args.sort_by, args.top, args.filter)


if __name__ == "__main__":
    main()
