"""Seeded, sequential execution of a Test Composer template.

A run is a single timeline: first_ commands, then a sequence of randomly
chosen driver steps, then finally_ and eventually_ commands. Every choice the
runner makes comes from one RNG seeded with the run seed, and every command
gets its own seed derived from the run seed and its position in the timeline.
Running the same template with the same seed therefore replays the same
commands with the same randomness, and a failure at driver step K can be
reproduced with --steps K.

Commands run one at a time. Concurrency would make the outcome depend on the
OS scheduler, which is exactly the nondeterminism a seed cannot capture.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import os
import random
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from .assertions import Assertions, Violation
from .composer import Command, Kind, Template

BOOT_DIR = Path(__file__).resolve().parent / "_boot"


@dataclass
class Options:
    template: Template
    seed: int
    steps: int | None = 100
    duration: float | None = None
    timeout: float | None = None
    workdir: Path | None = None
    keep_workdir: bool = False
    strict: bool = False
    keep_going: bool = False
    verbose: bool = False
    quiet: bool = False


@dataclass
class StepResult:
    index: int
    command: Command
    seed: int
    returncode: int | None
    timed_out: bool
    elapsed: float
    log: Path
    violations: list[Violation]

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and not self.timed_out and not self.violations


def derive_seed(seed: int, index: int) -> int:
    """The seed for the command at position index in the timeline."""
    digest = hashlib.sha256(f"thesis:{seed}:{index}".encode()).digest()
    return int.from_bytes(digest[:8], "little")


def random_seed() -> int:
    return int.from_bytes(os.urandom(8), "little") >> 1


class Runner:
    def __init__(self, opts: Options):
        self.opts = opts
        self.rng = random.Random(opts.seed)
        self.assertions = Assertions()
        self.index = 0
        self.driver_steps = 0
        self.failures: list[StepResult] = []
        self.workdir = self._make_workdir()
        self.state_dir = self.workdir / ".thesis"
        self.state_dir.mkdir()
        self.timeline = open(self.state_dir / "timeline.jsonl", "w", encoding="utf-8")

    # -- output -------------------------------------------------------------

    def say(self, msg: str = "") -> None:
        if not self.opts.quiet:
            print(msg, flush=True)

    def err(self, msg: str = "") -> None:
        print(msg, file=sys.stderr, flush=True)

    # -- setup --------------------------------------------------------------

    def _make_workdir(self) -> Path:
        if self.opts.workdir is None:
            return Path(tempfile.mkdtemp(prefix=f"thesis-{self.opts.seed}-"))
        workdir = self.opts.workdir.resolve()
        workdir.mkdir(parents=True, exist_ok=True)
        if any(workdir.iterdir()):
            raise ValueError(f"{workdir}: working directory is not empty; a run must start from a clean state")
        return workdir

    def _env(self, seed: int, sdk_output: Path) -> dict[str, str]:
        env = dict(os.environ)
        pythonpath = env.get("PYTHONPATH")
        env["PYTHONPATH"] = str(BOOT_DIR) + (os.pathsep + pythonpath if pythonpath else "")
        env["PYTHONHASHSEED"] = str(seed % 2**32)
        env["THESIS_SEED"] = str(seed)
        env["THESIS_STEP"] = str(self.index)
        env["ANTITHESIS_SDK_LOCAL_OUTPUT"] = str(sdk_output)
        return env

    # -- execution ----------------------------------------------------------

    def _exec(self, cmd: Command, env: dict[str, str], log: Path) -> tuple[int | None, bool]:
        with open(log, "wb") as out:
            proc = subprocess.Popen(
                [str(cmd.path)],
                cwd=self.workdir,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            timed_out = threading.Event()

            def expire() -> None:
                timed_out.set()
                _kill(proc)

            timer = None
            if self.opts.timeout:
                timer = threading.Timer(self.opts.timeout, expire)
                timer.start()
            try:
                assert proc.stdout is not None
                for chunk in iter(lambda: proc.stdout.readline(), b""):
                    out.write(chunk)
                    if self.opts.verbose:
                        sys.stdout.buffer.write(b"       | " + chunk)
                        sys.stdout.flush()
                proc.wait()
            except BaseException:
                _kill(proc)
                raise
            finally:
                if timer is not None:
                    timer.cancel()
        return proc.returncode, timed_out.is_set()

    def step(self, cmd: Command) -> StepResult:
        index = self.index
        seed = derive_seed(self.opts.seed, index)
        stem = f"{index:06d}-{cmd.name}"
        log = self.state_dir / f"{stem}.log"
        sdk_output = self.state_dir / f"{stem}.sdk.jsonl"

        self.say(f"[{index:5d}] {cmd.name}")
        start = time.monotonic()
        returncode, timed_out = self._exec(cmd, self._env(seed, sdk_output), log)
        elapsed = time.monotonic() - start
        violations = self.assertions.ingest(sdk_output, index)
        self.index += 1

        result = StepResult(index, cmd, seed, returncode, timed_out, elapsed, log, violations)
        self.timeline.write(
            json.dumps(
                {
                    "index": index,
                    "command": cmd.name,
                    "kind": cmd.kind.value,
                    "seed": seed,
                    "returncode": returncode,
                    "timed_out": timed_out,
                    "elapsed": round(elapsed, 3),
                    "violations": [v.prop.message for v in violations],
                }
            )
            + "\n"
        )
        self.timeline.flush()
        if not result.ok:
            self.failures.append(result)
            self.report_failure(result)
        return result

    # -- timeline -----------------------------------------------------------

    def _drivers_remaining(self, deadline: float | None) -> bool:
        if self.opts.steps is not None and self.driver_steps >= self.opts.steps:
            return False
        if deadline is not None and time.monotonic() >= deadline:
            return False
        return True

    def _drivers(self) -> Iterator[Command]:
        """Yields the driver steps of the timeline, counting each one."""
        t = self.opts.template
        deadline = time.monotonic() + self.opts.duration if self.opts.duration else None
        pool = t.of(Kind.PARALLEL_DRIVER, Kind.SERIAL_DRIVER, Kind.ANYTIME)
        singletons = t.of(Kind.SINGLETON_DRIVER)

        # A timeline that runs a singleton driver runs no other drivers.
        if singletons:
            singleton = self.rng.choice(singletons + ([None] if pool else []))
            if singleton is not None:
                if self._drivers_remaining(deadline):
                    self.driver_steps += 1
                    yield singleton
                return
        while pool and self._drivers_remaining(deadline):
            cmd = self.rng.choice(pool)
            self.driver_steps += 1
            yield cmd

    def _run_timeline(self) -> bool:
        """Runs the timeline and returns False if it stopped on a failure."""
        t = self.opts.template
        timeline = itertools.chain(t.of(Kind.FIRST), self._drivers(), t.of(Kind.FINALLY), t.of(Kind.EVENTUALLY))
        for cmd in timeline:
            if not self.step(cmd).ok and not self.opts.keep_going:
                return False
        return True

    def run(self) -> int:
        o = self.opts
        self.say(f"thesis: template {o.template.path}")
        self.say(f"thesis: seed {o.seed}, workdir {self.workdir}")
        start = time.monotonic()
        interrupted = False
        try:
            self._run_timeline()
        except KeyboardInterrupt:
            interrupted = True
            self.err("\nthesis: interrupted")
        finally:
            self.timeline.close()
        elapsed = time.monotonic() - start

        unsatisfied = self.assertions.unsatisfied()
        failed = bool(self.failures) or (o.strict and bool(unsatisfied))
        self.summary(elapsed, unsatisfied)
        if self.failures or interrupted:
            self.err(f"thesis: reproduce with: {self.repro_command()}")

        if failed or interrupted or o.keep_workdir:
            self.err(f"thesis: workdir kept at {self.workdir}")
        else:
            shutil.rmtree(self.workdir, ignore_errors=True)
        if interrupted:
            return 130
        return 1 if failed else 0

    # -- reporting ----------------------------------------------------------

    def report_failure(self, r: StepResult) -> None:
        self.err(f"\nFAIL at step {r.index} ({r.command.name}, driver step {self.driver_steps})")
        if r.timed_out:
            self.err(f"  command timed out after {self.opts.timeout}s")
        elif r.returncode != 0:
            self.err(f"  command exited with status {r.returncode}")
        for v in r.violations:
            self.err(f'  {v.prop.display_type} "{v.prop.message}" violated at {v.prop.where}')
            if v.details:
                self.err(f"    details: {json.dumps(v.details, default=str)}")
        tail = _tail(r.log, 20)
        if tail:
            self.err(f"  last output ({r.log}):")
            for line in tail:
                self.err(f"    {line}")
        self.err()

    def summary(self, elapsed: float, unsatisfied: list) -> None:
        props = self.assertions.properties.values()
        failed = sum(1 for p in props if p.failed())
        passed = sum(1 for p in props if not p.failed() and not p.unsatisfied())
        self.err(
            f"thesis: {self.index} commands ({self.driver_steps} driver steps) in {elapsed:.1f}s, seed {self.opts.seed}"
        )
        self.err(f"thesis: properties: {passed} passed, {failed} failed, {len(unsatisfied)} unsatisfied")
        for p in unsatisfied:
            self.err(f'  unsatisfied: {p.display_type} "{p.message}" ({p.where})')
        exit_failures = sum(1 for f in self.failures if f.returncode != 0 or f.timed_out)
        if exit_failures:
            self.err(f"thesis: {exit_failures} command(s) exited abnormally")

    def repro_command(self) -> str:
        o = self.opts
        args = ["thesis", "run", str(o.template.path), "--seed", str(o.seed), "--steps", str(self.driver_steps)]
        if o.timeout:
            args += ["--timeout", str(o.timeout)]
        return shlex.join(args)


def _kill(proc: subprocess.Popen) -> None:
    try:
        os.killpg(proc.pid, 9)
    except (ProcessLookupError, PermissionError):
        pass


def _tail(path: Path, n: int) -> list[str]:
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return [line.rstrip("\n") for line in deque(f, maxlen=n)]
    except FileNotFoundError:
        return []
