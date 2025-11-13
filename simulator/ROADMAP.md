# Simulator Roadmap

This document describes the current roadmap for the simulator. There
are 5 crucial aspects in the development of the simulator:

1. Coverage: The reliability provided by random testing
is as strong as the input space of the random generation. We keep a detailed
[`COVERAGE.md`](./COVERAGE.md) file that describes how each feature of Turso is
covered by the simulator testing.
2. Input generation: For an input space as large as SQL, and a system as complex as
Turso, we cannot naively generate the whole input space, we must be selective and smart.
3. Correctness: What does it mean for Turso to be correct? We primarily rely on
[*properties*](#properties) as well as several [oracles](#oracles).
4. Debugging: Finding bugs is only the first half of testing a system, we must also
aim to help fix them. This is why simulator provides input shrinking as well as interactive
debugging capabilities.
5. Fault Injection: The simulator natively supports fault injection into the system,
simulating low level failures in the network or file system layer.

## Coverage

For detailed information about how much of the input space we currently cover, please
check the [`COVERAGE.md`](./COVERAGE.md) file. Currently, most of SQLite query language
is either not supported or partial, and functions aren't supported.

Actionable items:

- [ ] Implement generation and/or shadowing for one of the languages features in [COVERAGE.md](./COVERAGE.md)
- [ ] At the moment, implementing a feature requires both adding a generation for it as well as
      a shadow operation for keeping track. For some oracles such as `doublecheck` or `differential`, we do not
      need the shadowing. So we should separate the generation into `shadowable/not shadowable` paths, and generate
      based on the current oracle.
- [ ] Run the simulator with coverage measurement, highlighting parts of Turso not touched by the simulator.

## Input Generation

- [x] Single client generation: In the current form, we generate a single client, although that may interact with Turso
from multiple connections.
- [ ] Multi-client generation: Instead of a single client making multiple connections, what would be more manageable is
having multiple clients, and the orchestrator picking a next client. This would also follow into concurrent client
generation in the future.
- [ ] Invalid Input generation: With the default oracle that relies on properties, invalid inputs did not make much sense.
For other oracles such as `differential` or `doublecheck`, they do make sense, because even in the case of errors the database
state should be equivalent with SQLite. It would make sense to deliberately break working queries to construct possibly
invalid inputs for testing unhappy paths.
- [ ] LLM-guided generation: There's quite a bit of potential in pushing an LLM to guide the generation. We had some
preliminary discussions on this, but a well-planned proposal will be necessary for implementing it.
- [ ] Custom feedback guided generation: SQLancer analyzes the `EXPLAIN PLAN`s to decide if a database state isn't diverse
enough for constructing interesting DQLs and instead starts to generate DMLs. We can use a similar approach.
- [ ] Coverage-guided generation: Coverage guidance has been a striking force in the last decade of random testing. Any
proposal on advancing the simulator in this area will be welcomed.

## Correctness

- [ ] External property language: TODO
- [ ] SQL with Contracts (SQL-C): TODO
- [ ] SQLancer Properties: TODO

### Properties

- [ ] InsertValuesSelect: TODO
- [ ] ReadYourUpdatesBack: TODO
- [ ] TableHasExpectedContent: TODO
- [ ] AllTableHaveExpectedContent: TODO
- [ ] DoubleCreateFailure: TODO
- [ ] SelectLimit: TODO
- [ ] DeleteSelect: TODO
- [ ] DropSelect: TODO
- [ ] SelectSelectOptimizer: TODO
- [ ] WhereTrueFalseNull: TODO
- [ ] UNIONAllPreservesCardinality: TODO
- [ ] FsyncNoWait: TODO
- [ ] FaultyQuery: TODO

### Oracles

- [ ] Default: TODO
- [ ] Differential: TODO
- [ ] Doublecheck: TODO

## Debugging

- [ ] Automated Shrinking
  - [x] Statement Removal
    - [ ] Heuristic removal
      - [x] Removing unused tables
      - [ ] Removing all DQLs
    - [x] Backtracking removal
  - [ ] Statement shrinking
    - [ ] Value removal
      - [ ] Removing inserted values from `INSERT` statements
    - [ ] Value shrinking
      - [ ] Removing parts of SQL expressions (e.g `x AND y` into `x` or `y`)
- [ ] Interactive debugging (currently broken)
- [ ] Fault localization

## Fault Injection

- [ ] TODO
