import json
import os
import sys
from pathlib import Path

from thesis.cli import main
from thesis.composer import Kind, load_template

SHEBANG = f"#!{sys.executable} -u\n"

# Appends every random decision the command makes to trace.txt in the
# workdir, so tests can compare what two runs actually did.
RECORD = """
import os, sys
from antithesis.random import get_random, random_choice
name = os.path.basename(sys.argv[0])
values = [get_random() for _ in range(3)] + [random_choice(["a", "b", "c", "d"])]
with open("trace.txt", "a") as f:
    f.write(f"{name} {values}\\n")
"""


def write(dir: Path, name: str, body: str, executable: bool = True) -> Path:
    path = dir / name
    path.write_text(SHEBANG + body)
    if executable:
        path.chmod(0o755)
    return path


def template(tmp_path: Path, **commands: str) -> Path:
    t = tmp_path / "template"
    t.mkdir()
    for name, body in commands.items():
        write(t, name, body)
    return t


def run(t: Path, workdir: Path, *args: str) -> int:
    return main(["run", str(t), "--workdir", str(workdir), "--keep-workdir", "-q", *args])


def trace(workdir: Path) -> str:
    return (workdir / "trace.txt").read_text()


def basic(tmp_path: Path) -> Path:
    return template(
        tmp_path,
        first_setup=RECORD,
        parallel_driver_a=RECORD,
        parallel_driver_b=RECORD,
        serial_driver_c=RECORD,
        finally_check=RECORD,
    )


def test_same_seed_same_run(tmp_path):
    t = basic(tmp_path)
    assert run(t, tmp_path / "w1", "--seed", "42", "--steps", "30") == 0
    assert run(t, tmp_path / "w2", "--seed", "42", "--steps", "30") == 0
    assert trace(tmp_path / "w1") == trace(tmp_path / "w2")
    lines = trace(tmp_path / "w1").splitlines()
    assert len(lines) == 32
    assert lines[0].startswith("first_setup ")
    assert lines[-1].startswith("finally_check ")


def test_different_seed_different_run(tmp_path):
    t = basic(tmp_path)
    assert run(t, tmp_path / "w1", "--seed", "1", "--steps", "10") == 0
    assert run(t, tmp_path / "w2", "--seed", "2", "--steps", "10") == 0
    assert trace(tmp_path / "w1") != trace(tmp_path / "w2")


def test_prefix_is_stable(tmp_path):
    # A shorter run with the same seed is a prefix of a longer one, which is
    # what makes --steps K a replay of the first K steps.
    t = template(tmp_path, parallel_driver_a=RECORD, parallel_driver_b=RECORD)
    run(t, tmp_path / "short", "--seed", "7", "--steps", "5")
    run(t, tmp_path / "long", "--seed", "7", "--steps", "20")
    assert trace(tmp_path / "long").startswith(trace(tmp_path / "short"))


def test_steps_do_not_repeat_randomness(tmp_path):
    t = template(tmp_path, parallel_driver_a=RECORD)
    run(t, tmp_path / "w", "--seed", "3", "--steps", "5")
    lines = trace(tmp_path / "w").splitlines()
    assert len(set(lines)) == len(lines)


FAIL_SOMETIMES = """
from antithesis.assertions import always
from antithesis.random import get_random
v = get_random() % 8
always(v != 0, "value is never zero", {"v": v})
"""


def test_always_violation_fails_and_reproduces(tmp_path, capsys):
    t = template(tmp_path, parallel_driver_a=FAIL_SOMETIMES)
    assert run(t, tmp_path / "w1", "--seed", "5", "--steps", "1000") == 1
    err = capsys.readouterr().err
    assert 'Always "value is never zero" violated' in err
    repro = next(line for line in err.splitlines() if "reproduce with:" in line)
    steps = repro.split("--steps ")[1].split()[0]

    timeline = [json.loads(line) for line in (tmp_path / "w1/.thesis/timeline.jsonl").read_text().splitlines()]
    assert len(timeline) == int(steps)
    assert timeline[-1]["violations"] == ["value is never zero"]
    assert all(not e["violations"] for e in timeline[:-1])

    # Replaying exactly that many steps hits the same failure on the last step.
    assert run(t, tmp_path / "w2", "--seed", "5", "--steps", steps) == 1
    replay = [json.loads(line) for line in (tmp_path / "w2/.thesis/timeline.jsonl").read_text().splitlines()]
    assert replay == [{**e, "elapsed": replay[i]["elapsed"]} for i, e in enumerate(timeline)]


def test_keep_going(tmp_path):
    t = template(tmp_path, parallel_driver_a=FAIL_SOMETIMES)
    assert run(t, tmp_path / "w", "--seed", "5", "--steps", "200", "--keep-going") == 1
    lines = (tmp_path / "w/.thesis/timeline.jsonl").read_text().splitlines()
    assert len(lines) == 200


def test_nonzero_exit_fails(tmp_path, capsys):
    t = template(tmp_path, parallel_driver_a="import sys; print('boom'); sys.exit(3)\n")
    assert run(t, tmp_path / "w", "--seed", "1") == 1
    err = capsys.readouterr().err
    assert "exited with status 3" in err
    assert "boom" in err


def test_timeout(tmp_path, capsys):
    t = template(tmp_path, parallel_driver_a="import time; time.sleep(30)\n")
    assert run(t, tmp_path / "w", "--seed", "1", "--timeout", "0.5") == 1
    assert "timed out" in capsys.readouterr().err


def test_unreachable(tmp_path, capsys):
    t = template(
        tmp_path,
        parallel_driver_a='from antithesis.assertions import unreachable\nunreachable("never here", {})\n',
    )
    assert run(t, tmp_path / "w", "--seed", "1") == 1
    assert 'Unreachable "never here"' in capsys.readouterr().err


SOMETIMES = """
from antithesis.assertions import sometimes, reachable
sometimes(False, "rare event", {})
reachable("driver ran", {})
"""


def test_unsatisfied_sometimes(tmp_path, capsys):
    t = template(tmp_path, parallel_driver_a=SOMETIMES)
    assert run(t, tmp_path / "w1", "--seed", "1", "--steps", "3") == 0
    err = capsys.readouterr().err
    assert "1 passed, 0 failed, 1 unsatisfied" in err
    assert 'unsatisfied: Sometimes "rare event"' in err
    assert run(t, tmp_path / "w2", "--seed", "1", "--steps", "3", "--strict") == 1


def test_singleton_timeline(tmp_path):
    t = template(tmp_path, singleton_driver_only=RECORD)
    assert run(t, tmp_path / "w", "--seed", "1", "--steps", "50") == 0
    assert len(trace(tmp_path / "w").splitlines()) == 1


def test_workdir_must_be_empty(tmp_path):
    t = basic(tmp_path)
    w = tmp_path / "w"
    w.mkdir()
    (w / "stale.db").write_text("")
    assert run(t, w, "--seed", "1") == 2


def test_workdir_removed_on_success(tmp_path):
    t = basic(tmp_path)
    w = tmp_path / "w"
    assert main(["run", str(t), "--workdir", str(w), "-q", "--seed", "1", "--steps", "2"]) == 0
    assert not w.exists()


def test_discovery(tmp_path):
    t = tmp_path / "t"
    t.mkdir()
    write(t, "parallel_driver_x.py", "")
    write(t, "anytime_check.sh", "")
    write(t, "helper_utils.py", "")
    write(t, "serial_driver_not_executable.py", "", executable=False)
    kinds = {c.name: c.kind for c in load_template(t).commands}
    assert kinds == {"anytime_check.sh": Kind.ANYTIME, "parallel_driver_x.py": Kind.PARALLEL_DRIVER}


def test_existing_sitecustomize_still_runs(tmp_path):
    site = tmp_path / "site"
    site.mkdir()
    (site / "sitecustomize.py").write_text("import os\nos.environ['CHAINED'] = '1'\n")
    t = template(
        tmp_path,
        parallel_driver_a="import os\nopen('trace.txt', 'w').write(os.environ.get('CHAINED', '0'))\n",
    )
    old = os.environ.get("PYTHONPATH")
    os.environ["PYTHONPATH"] = str(site)
    try:
        assert run(t, tmp_path / "w", "--seed", "1", "--steps", "1") == 0
    finally:
        if old is None:
            del os.environ["PYTHONPATH"]
        else:
            os.environ["PYTHONPATH"] = old
    assert trace(tmp_path / "w") == "1"
