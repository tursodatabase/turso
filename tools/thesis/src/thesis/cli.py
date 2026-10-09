"""Command-line interface."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from . import __version__
from .composer import load_template
from .runner import Options, Runner, random_seed


def parse_duration(s: str) -> float:
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(s|m|h)?", s.strip())
    if not m:
        raise argparse.ArgumentTypeError(f"invalid duration: {s!r} (examples: 90, 30s, 5m, 1h)")
    return float(m.group(1)) * {"s": 1, "m": 60, "h": 3600, None: 1}[m.group(2)]


def parse_seed(s: str) -> int:
    try:
        seed = int(s, 0)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid seed: {s!r}") from None
    if seed < 0:
        raise argparse.ArgumentTypeError("seed must be non-negative")
    return seed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="thesis", description="Local, seeded runner for Antithesis Test Composer templates."
    )
    parser.add_argument("--version", action="version", version=f"thesis {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser(
        "run", help="run a test template", description="Run a Test Composer template as one seeded timeline."
    )
    run.add_argument("template", type=Path, help="test template directory (e.g. testing/antithesis/stress-composer)")
    run.add_argument("--seed", type=parse_seed, help="run seed (default: random)")
    run.add_argument(
        "--steps", type=int, default=None, help="number of driver steps (default: 100, or unbounded with --duration)"
    )
    run.add_argument("--duration", type=parse_duration, help="stop starting driver steps after this long (e.g. 5m)")
    run.add_argument("--forever", action="store_true", help="run driver steps until a failure or Ctrl-C")
    run.add_argument("--timeout", type=parse_duration, help="kill a command that runs longer than this")
    run.add_argument("--workdir", type=Path, help="empty directory to run commands in (default: a fresh temp dir)")
    run.add_argument("--keep-workdir", action="store_true", help="keep the workdir even if the run passes")
    run.add_argument("--keep-going", action="store_true", help="continue after a failing step")
    run.add_argument(
        "--strict", action="store_true", help="also fail if a Sometimes/Reachable property was never satisfied"
    )
    out = run.add_mutually_exclusive_group()
    out.add_argument("-v", "--verbose", action="store_true", help="stream command output")
    out.add_argument("-q", "--quiet", action="store_true", help="print only failures and the summary")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "run":
        return cmd_run(args)
    return 2


def cmd_run(args: argparse.Namespace) -> int:
    if args.steps is not None and args.steps < 0:
        print("thesis: --steps must be non-negative", file=sys.stderr)
        return 2
    if args.forever and args.steps is not None:
        print("thesis: --forever and --steps are mutually exclusive", file=sys.stderr)
        return 2
    steps = args.steps
    if steps is None and not args.forever and args.duration is None:
        steps = 100
    try:
        template = load_template(args.template)
        runner = Runner(
            Options(
                template=template,
                seed=args.seed if args.seed is not None else random_seed(),
                steps=steps,
                duration=args.duration,
                timeout=args.timeout,
                workdir=args.workdir,
                keep_workdir=args.keep_workdir,
                strict=args.strict,
                keep_going=args.keep_going,
                verbose=args.verbose,
                quiet=args.quiet,
            )
        )
    except ValueError as e:
        print(f"thesis: {e}", file=sys.stderr)
        return 2
    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
