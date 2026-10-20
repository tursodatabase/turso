# thesis

A local, seeded runner for [Antithesis Test Composer](https://antithesis.com/docs/test_templates/) templates.

From the repository root:

```console
$ uv run --group antithesis thesis run testing/antithesis/stress-composer --seed 42 --steps 500
```

The `antithesis` dependency group (in the root `pyproject.toml`) installs `thesis`, the Antithesis SDK, and `pyturso`
built from your checkout, so the templates run against your local changes.

`thesis` runs a test template the way Antithesis would run one timeline of it, but on your machine, one command at a
time, with all randomness derived from a single seed. Running again with the same seed replays the same commands
making the same random decisions, and a failure prints the exact command to reproduce it.

## How it works

**Randomness.** Outside Antithesis, the SDK's `get_random()` and `random_choice()` fall back to Python's
`random.getrandbits()`. `thesis` puts a `sitecustomize.py` at the front of `PYTHONPATH` that seeds Python's global RNG
from `THESIS_SEED` before the command starts. Each command gets its own seed, derived from the run seed and the
command's position in the timeline. Commands that are not Python still see `THESIS_SEED` and can use it themselves.
If you already have a `sitecustomize` module, it still runs after ours.

**Assertions.** The SDK's `always()` and friends do not raise; they emit a message. `thesis` sets
`ANTITHESIS_SDK_LOCAL_OUTPUT` to a per-command file, reads the messages back after each command and evaluates them:

| Assertion | Fails the run when |
| --- | --- |
| `always`, `always_or_unreachable` | the condition is false |
| `unreachable` | it is reached |
| `sometimes` | never true during the run (only with `--strict`) |
| `reachable` | never reached during the run (only with `--strict`) |

A command exiting non-zero or exceeding `--timeout` also fails the run, like on Antithesis.

**Timeline.** Commands are discovered by Test Composer prefix; files without one (like `helper_utils.py`) and
non-executable files are ignored.

1. All `first_*` commands, in name order.
2. Driver steps: each step runs one command chosen at random from `parallel_driver_*`, `serial_driver_*` and
   `anytime_*`. If the template has `singleton_driver_*` commands, the timeline may instead run exactly one singleton
   and no other drivers.
3. All `finally_*`, then all `eventually_*` commands.

Commands run with a fresh, empty working directory as their cwd, so state from a previous run can't leak in. The
workdir is deleted after a passing run and kept after a failing one.

## Usage

```
thesis run TEMPLATE [--seed N] [--steps N | --duration 5m | --forever]
                    [--timeout 60s] [--workdir DIR] [--keep-workdir]
                    [--keep-going] [--strict] [-v | -q]
```

Without `--seed`, a random seed is chosen and printed. The default is 100 driver steps.

On failure:

```
FAIL at step 58 (parallel_driver_integritycheck.py, driver step 57)
  Always "Integrity check failed" violated at .../parallel_driver_integritycheck.py:40
    details: {"row": ["*** in database main ***\nPage 12: ..."]}
  last output (/tmp/thesis-42-x1y2/.thesis/000058-parallel_driver_integritycheck.py.log):
    Running integrity check...

thesis: 59 commands (57 driver steps) in 14.2s, seed 42
thesis: properties: 3 passed, 1 failed, 0 unsatisfied
thesis: reproduce with: thesis run /path/to/stress-composer --seed 42 --steps 57
thesis: workdir kept at /tmp/thesis-42-x1y2
```

The workdir's `.thesis/` directory has each command's output (`*.log`), its raw SDK messages (`*.sdk.jsonl`) and
`timeline.jsonl`, which records every command with its seed, exit status and violations.

Commands inherit `thesis`'s environment, including `PATH`, so under `uv run` a template's `#!/usr/bin/env python3`
resolves to the workspace venv, where `antithesis` and `turso` are importable.

## What a seed does not control

A seed reproduces the test's decisions, not the whole execution. A replay diverges if the system under test has its
own sources of nondeterminism: its own RNG, wall-clock time, temp file names, or thread scheduling in a multi-threaded
engine. That is also why `thesis` runs commands one at a time: running `parallel_driver_*` commands concurrently would
make the result depend on the OS scheduler, which only the Antithesis hypervisor can control.

## Development

From the repository root:

```console
$ uv run --package thesis --group dev pytest tools/thesis
```

The tests use small fixture templates that call the real Antithesis SDK; they don't need `pyturso`.
