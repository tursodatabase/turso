# Contributing to Turso

We'd love to have you contribute to Turso!

This document is a quick helper to get you going.

## Getting Started

Turso is a rewrite of SQLite in Rust. If you are new to SQLite, the following articles and books are a good starting point:

* [Architecture of SQLite](https://www.sqlite.org/arch.html)
* Sibsankar Haldar. [SQLite Database System Design and Implementation (2nd Edition)](https://books.google.fi/books/?id=yWzwCwAAQBAJ&redir_esc=y). 2016
* Jay Kreibich. [Using SQLite: Small. Fast. Reliable. Choose Any Three. 1st Edition](https://www.oreilly.com/library/view/using-sqlite/9781449394592/). 2010

If you are new to Rust, the following books are recommended reading:

* Jim Blandy et al. [Programming Rust, 2nd Edition](https://www.oreilly.com/library/view/programming-rust-2nd/9781492052586/). 2021
* Steve Klabnik and Carol Nichols. [The Rust Programming Language](https://doc.rust-lang.org/book/#the-rust-programming-language). 2022

Examples of contributing

* [How to contribute a SQL function implementation](docs/contributing/contributing_functions.md)
* [Rickrolling Turso DB](https://avi.im/blag/2025/rickrolling-turso)

To build and run `tursodb` CLI: 

```shell 
cargo run --package turso_cli --bin tursodb database.db
```

Run tests:
```console
cargo test
```

### Running Tests On Linux
> [!NOTE]
> These steps have been tested on Ubuntu Noble 24.04.2 LTS

Running tests on Linux and getting them pass requires a few additional steps

1. Install [SQLite](https://www.sqlite.org/index.html) headers
```console
sudo apt install sqlite3 libsqlite3-dev
```
2. Install Python3 dev files
```console
sudo apt install python3.12 python3.12-dev
```
3. Set env var for Maturin
```console
export PYO3_PYTHON=$(which python3)
```
4. Build Cargo 
```console
cargo build -p turso_sqlite3 --features capi
```
5. Run tests
```console
cargo test
```

Test coverage report:

```
cargo tarpaulin -o html
```

> [!NOTE]
> Generation of coverage report requires [tarpaulin](https://github.com/xd009642/tarpaulin) binary to be installed.
> You can install it with `cargo install cargo-tarpaulin`

[//]: # (TODO remove the below tip when the bug is solved)

> [!TIP]
> If coverage fails with "Test failed during run" error and all of the tests passed it might be the result of tarpaulin [bug](https://github.com/xd009642/tarpaulin/issues/1642). You can temporarily set [dynamic libraries linking manually](https://doc.rust-lang.org/cargo/reference/environment-variables.html#dynamic-library-paths) as a workaround, e.g. for linux  `LD_LIBRARY_PATH="$(rustc --print=target-libdir)" cargo tarpaulin -o html`.

Run benchmarks:

```console
cargo bench
```

Run benchmarks and generate flamegraphs:

```console
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
cargo bench --bench benchmark -- --profile-time=5
```

## Finding things to work on

The issue tracker has issues tagged with [good first issue](https://github.com/tursodatabase/limbo/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22),
which are considered to be things to work on to get going. If you're interested in working on one of them, comment on the issue tracker, and we're happy to help you get going.

## Submitting your work

Fork the repository and open a pull request to submit your work.

The CI checks for formatting, Clippy warnings, and test failures so remember to run the following before submitting your pull request:

* `cargo fmt` and `cargo clippy` to keep the code formatting in check.
* `make` to run the test suite.

**Keep your pull requests focused and as small as possible, but not smaller.** IOW, when preparing a pull request, ensure it focuses on a single thing and that your commits align with that. For example, a good pull request might fix a specific bug or a group of related bugs. Or a good pull request might add a new feature and test for it. Conversely, a bad pull request might fix a bug, add a new feature, and refactor some code.

**The commits in your pull request tell the story of your change.** Break your pull request into multiple commits when needed to make it easier to review and ensure that future developers can also understand the change as they are in the middle of a `git bisect` run to debug a nasty bug. A developer should be able to reconstruct the intent of your change and how you got to the end-result by reading the commits. To keep a clean commit history, make sure the commits are _atomic_:

* **Keep commits as small as possible**. The smaller the commit, the easier it is to review, but also easier `git revert` when things go bad.
* **Don't mix logic and cleanups in same commit**. If you need to refactor the code, do it in a commit of its own. Mixing refactoring with logic changes makes it very hard to review a commit.
* **Don't mix logic and formatting changes in same commit**. Resist the urge to fix random formatting issues in the same commit as your logic changes, because it only makes it harder to review the commit.
* **Write a good commit message**. You know your commit is atomic when it's easy to write a short commit message that describes the intent of the change.

To produce pull requests like this, you should learn how to use Git's interactive rebase (`git rebase -i`).

For a longer discussion on good commits, see Al Tenhundfeld's [What makes a good git commit](https://www.simplethread.com/what-makes-a-good-git-commit/), for example.


## Debugging query execution

Turso aims towards SQLite compatibility. If you find a query that has different behavior than SQLite, the first step is to check what the generated bytecode looks like.

To do that, first run the `EXPLAIN` command in `sqlite3` shell:

```
sqlite> EXPLAIN SELECT first_name FROM users;
addr  opcode         p1    p2    p3    p4             p5  comment
----  -------------  ----  ----  ----  -------------  --  -------------
0     Init           0     7     0                    0   Start at 7
1     OpenRead       0     2     0     2              0   root=2 iDb=0; users
2     Rewind         0     6     0                    0
3       Column         0     1     1                    0   r[1]= cursor 0 column 1
4       ResultRow      1     1     0                    0   output=r[1]
5     Next           0     3     0                    1
6     Halt           0     0     0                    0
7     Transaction    0     0     1     0              1   usesStmtJournal=0
8     Goto           0     1     0                    0
```

and then run the same command in Turso's shell.

If the bytecode is different, that's the bug -- work towards fixing code generation.
If the bytecode is the same, but query results are different, then the bug is somewhere in the virtual machine interpreter or storage layer.

## Compatibility tests

The `testing/test.all` is a starting point for adding functional tests using a similar syntax to SQLite.
The purpose of these tests is to verify behavior matches with SQLite and Turso.

### Prerequisites

1. [Cargo-c](https://github.com/lu-zero/cargo-c) is needed for building C-ABI compatible library. You can get it via:
```console
cargo install cargo-c
```
2. [SQLite](https://www.sqlite.org/index.html) is needed for compatibility checking. You can install it using `brew` on macOS/Linux:
```console
brew install sqlite
```
Or using `choco` on Windows:
```console
choco install sqlite
```

### Running the tests
To run the test suite with Turso, simply run:

```
make test
```

To run the test suite with SQLite, type:

```
SQLITE_EXEC=sqlite3 SQLITE_FLAGS="" make test
```

When working on a new feature, please consider adding a test case for it.

## TPC-H

[TPC-H](https://www.tpc.org/tpch/) is a standard benchmark for testing database performance. To try out Turso's performance against a TPC-H compatible workload,
you can generate or download a TPC-H compatible SQLite database e.g. [here](https://github.com/lovasoa/TPCH-sqlite).

## Deterministic simulation tests

The `simulator` directory contains a deterministic simulator for testing.
What this means is that the behavior of a test run is deterministic based on the seed value.
If the simulator catches a bug, you can always reproduce the exact same sequence of events by passing the same seed.
The simulator also performs fault injection to discover interesting bugs.

### Whopper

Whopper is a DST that, unlike `simulator`, performs concurrent query execution.

To run Whopper for your local changes, run:

```console
./whopper/bin/run
```

The output of the simulation run looks as follows:

```
mode = fast
seed = 11621338508193870992
       .             I/U/D/C
       .             22/17/15/0
       .             41/34/20/3
       |             62/43/27/4
       |             88/55/30/5
      ╱|╲            97/58/30/6
     ╱╲|╱╲           108/62/30/7
    ╱╲╱|╲╱╲          115/67/32/7
   ╱╲╱╲|╱╲╱╲         121/74/35/7
  ╱╲╱╲╱|╲╱╲╱╲        125/80/38/7
 ╱╲╱╲╱╲|╱╲╱╲╱╲       141/94/43/8

real    0m1.250s
user    0m0.843s
sys     0m0.043s
```

The simulator prints ten progress indication lines, regardless of how long a run takes. The progress indicator line shows the following stats:

* `I` -- the number of `INSERT` statements executed
* `U` -- the number of `UPDATE` statements executed
* `D` -- the number of `DELETE` statements executed
* `C` -- the number of `PRAGMA integrity_check` statements executed

This will do a short sanity check run in using the `fast` mode.

If you need to reproduce a run, just defined the `SEED` environment variable as follows:

```console
SEED=1234 ./whopper/bin/run
```

You can also run Whopper in exploration mode to find more serious bugs:

```console
./whopper/bin/explore
```

Note that exploration uses the `chaos` mode so if you need to reproduce a run, use:

```console
SEED=1234 ./whopper/bin/run --mode chaos
```

Both `explore` and `run` accept the `--enable-checksums` and `--enable-encryption` flags for per page checksums and encryption respectively.

## Python Bindings

Turso provides Python bindings built on top of the [PyO3](https://pyo3.rs) project.
To compile the Python bindings locally, you first need to create and activate a Python virtual environment (for example, with Python `3.12`):

```bash
python3.12 -m venv venv
source venv/bin/activate
```

Then, install [Maturin](https://pypi.org/project/maturin/):

```bash
pip install maturin
```

Once Maturin is installed, you can build the crate and install it as a Python module directly into the current virtual environment by running:

```bash
cd bindings/python && maturin develop
```

## Fault injection with unreliable libc

First, build the unreliable libc:

```
cd testing/unreliable-libc
make
```

The run the stress testing tool with fault injection enabled:

```
RUST_BACKTRACE=1 LD_PRELOAD=./testing/unreliable-libc/unreliable-libc.so cargo run -p turso_stress -- --nr-iterations 10000
```

## Antithesis

Antithesis is a testing platform for finding bugs with reproducibility. In
Turso, we use Antithesis in addition to our own deterministic simulation
testing (DST) tool for the following:

- Discovering bugs that the DST did not catch (and improve the DST)
- Discovering bugs that the DST does not cover (for example, non-simulated I/O)

If you have an Antithesis account, you first need to configure some
environment variables:

```bash
export ANTITHESIS_USER=
export ANTITHESIS_TENANT=
export ANTITHESIS_PASSWD=
export ANTITHESIS_DOCKER_HOST=
export ANTITHESIS_DOCKER_REPO=
export ANTITHESIS_EMAIL=
```

You can then publish a new Antithesis workflow with:
 
```bash
scripts/antithesis/publish-workload.sh
```

And launch an Antithesis test run with:

```bash
scripts/antithesis/launch.sh
```

## Adding Third Party Dependencies

When you want to add third party dependencies, please follow these steps:

1. Add Licenses: Place the appropriate licenses for the third-party dependencies under the licenses directory. Ensure
   that each license is in a separate file and named appropriately.
2. Update NOTICE.md: Specify the licenses for the third-party dependencies in the NOTICE.md file. Include the name of
   the dependency, the license file path, and the homepage of the dependency.

By following these steps, you ensure that all third-party dependencies are properly documented and their licenses are
included in the project.
