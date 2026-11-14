# MVCC Passive Checkpoint Specification

Quint specifications of the passive checkpoint in MVCC mode. `DESIGN.md`
describes the design in prose, maps it to the Rust code, and lists what the
models found.

| Spec | Configurations | Design document |
|------|----------------|-----------------|
| `MvccPassiveCheckpoint.qnt` | `MvccPassiveCheckpointConfigs.qnt` | `DESIGN.md`, `DESIGN-rows.md` |
| `MvccPassiveSequence.qnt` | `MvccPassiveSequenceConfigs.qnt` | `DESIGN-sequences.md` |
| `MvccPassiveSchema.qnt` | `MvccPassiveSchemaConfigs.qnt` | `DESIGN-schema.md` |

A configurations file has one module for each configuration. The module sets
the constants of the spec. The module named `main` is the main configuration
of the spec. Each other module is a variant. A variant switches off one rule
of the design or tries a simpler rule. The comment above each module gives the
result to expect and the invariants to check. The Makefile reads this comment.

To add a spec, add `Spec.qnt` and `SpecConfigs.qnt` with a module `main`.
The Makefile finds every `Mvcc*.qnt` file that has a configurations file.

`SIMPLIFY.md` is a plan to make the passive checkpoint simpler with the same
guarantees, based on the model results. `MODEL-GAPS.md` lists what the
specs cover and what the code does around them. `SYNC.md` describes how sync depends
on table ids. The specs do not model sync.

## Install the tools

You need Node.js, Java and Quint. Quint downloads Apalache itself.

1. Install Quint 0.33.1 from npm:

   ```bash
   npm install -g @informalsystems/quint@0.33.1
   ```

   If you install it into another directory, give its path to make, for
   example `make check QUINT=~/.local/share/quint/node_modules/.bin/quint`.

2. Install Java 17 or later, for example Temurin 21. Quint runs Apalache and
   TLC with the `java` command on `PATH`. To use another Java, give its home
   directory to make, for example `make check JAVA_HOME=/opt/jdk-21`.

3. Run `quint verify` once with network access. Quint then downloads Apalache
   0.62.4 to `~/.quint`. The TLC backend also needs this download, because
   Quint uses Apalache to prepare the spec for TLC and runs the TLC that
   comes with Apalache. Any target that model checks does this step for you.

## Run the targets

```bash
# Check the main configuration of each spec with TLC
make check

# Check every variant and compare the result with the comment above it
make variants

# Run the Quint simulator on each main configuration
make simulate

# Write random traces of each main configuration to traces/
make traces

# Write one failing trace for each variant that must fail to traces/bugs/
make bug-traces
```

These make variables change the runs:

| Variable | Default | Meaning |
|----------|---------|---------|
| `QUINT` | `quint` | The Quint command |
| `JAVA_HOME` | empty | The Java to use. Empty means the `java` on `PATH` |
| `WORKERS` | 4 | TLC workers, and simulator threads |
| `ONLY` | empty | Module names to check, separated by spaces. Empty means all |
| `SERVER` | `localhost:8822` | The port of the Apalache server that Quint starts |
| `STATES` | `states` | The directory for TLC states and temporary files |
| `SPECS` | every `Mvcc*.qnt` with a configurations file | The specs to run |
| `SAMPLES` | 10000 | Simulator runs for `simulate` |
| `STEPS` | 40 | Steps in each simulator run and trace |
| `TRACES` | 10 | Traces for each spec for `traces` |
| `BUG_SAMPLES` | 200000 | Simulator runs for each variant for `bug-traces` |

For example, this command checks two variants with 6 workers:

```bash
make variants ONLY="noGcWait noSnapshotClamp" WORKERS=6
```

Two runs at the same time in this directory need their own `SERVER` and
`STATES`, for example:

```bash
make variants SPECS=MvccPassiveSequence SERVER=localhost:8826 STATES=$PWD/states2
```

`check` and `variants` use the TLC backend (`quint verify --backend tlc`).
TLC visits every reachable state, and it visits each order of the keys,
readers and connections. If Apalache or TLC fails, Quint can still exit with 0. For
this reason, the Makefile reads the output text to find the result.

The Java tools write their temporary files and TLC states to `states/`. The
Makefile deletes them after each run. To do it by hand, run `make clean`.

## How long the runs take

These times are for 3 TLC workers.

| Run | Distinct states | Time |
|-----|-----------------|------|
| `make check`, row spec (`MvccPassiveCheckpoint`) | 16,786,385 | 10 minutes |
| `make check`, sequence spec (`MvccPassiveSequence`) | 4,008,777 | 3.5 minutes |
| Largest row variant that ran in full (`dirtyKeys`) | 45,359,101 | 26 minutes |
| Other row and sequence variants | up to 20,771,050 | up to 13 minutes |

`make variants` for the row spec takes about two hours. `gcSample` alone
needs more than 40 minutes. To leave out the largest
variants, give the others with `ONLY`.

## Traces

`make traces` runs the simulator with `--mbt`. Each state in a trace file has
two more fields:

- `mbt::actionTaken`: the action of the step, for example `WritePrepare`.
- `mbt::nondetPicks`: the values that the action picked, under the names of
  the action parameters, for example `k` and `isDelete`.

A test can replay a trace against the Rust code with these two fields.
`HARNESS.md` describes that test. The test is not built yet.

`make bug-traces` runs the simulator on each variant that must fail, until it
finds the violation, and writes that trace to `traces/bugs/<Spec>-<module>.itf.json`.
These are the traces of the known bugs, for example `issue11502`. If the
simulator finds no violation in `BUG_SAMPLES` runs, the target prints `miss`.
The TLC backend finds every one of these violations, but it does not write
them as ITF.
