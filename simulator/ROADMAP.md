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
- [ ] Continuation Simulations: Simulations that load an existing DB file and start testing on top of that.
- [ ] Long-term simulations: Most of our simulations are bounded by minutes, why shouldn't we run OSS-Fuzz style continuous campaigns?
This requires implementing continuation simulations so we can preserve progress over time if the underlying machine fails.

## Correctness

- [ ] External property language: Currently, properties are hardcoded into the simulator as new variants that define
how to generate a sequence of database interactions and assertions that will be used by the simulator for testing
correctness properties defined in [#properties](#properties). There's a sketch of an external language for expressing
properties using [*generation actions*](https://alperenkeles.com/documents/d4.pdf) in our previous paper submission.
Ideally, we'll implement this language as a frontend for expressing properties, after which we'll be able to dynamically
include/exclude certain properties, change them in small ways based on the requirements.
- [ ] SQL with Contracts (SQL-C): There's a work-in-progress pull request ([https://github.com/tursodatabase/turso/pull/3715](https://github.com/tursodatabase/turso/pull/3715))
for a SQL extension that includes inline bindings and assertions reasoning about dataframes. As this is done, the simulator will
output SQL-C files, which can be consumed by any SQL-C executor, decoupling the generation/execution in the simulator.
- [ ] Shadow State Abstraction: We currently maintain a shadow Turso, a simple in-memory key-value store that mimics the semantics
of the SQL statements. We use this shadow state for both random input generation as well as correctness checking in some of the
properties. However, maintaining a shadow state for all of SQL is virtually impossible, hence we would like to have alternative
solutions that make it easier to extend our reach, because a shadow implementation of any feature is required in order to use the simulator.
In this direction, we shall abstract away the shadow state, allowing multiple implementation of shadowing, and the ability to switch between them.
- [ ] SQLite as shadow state: Currently, we have a custom shadow state implementation that mimics Turso's behavior.
Another approach would be to use SQLite as the shadow state, using SQLite to be the source of truth for the contents
of the database. We can generally rely on this fact because Turso binary format is compatible with SQLite, but
the ephemeral state that's in-process will be an issue.

### Properties

There are a number of properties currently implemented. We implement existing properties from literature (mainly SQLancer),
as well as bespoke properties that is specific and niche. We also have a number of properties that rely on fault injection,
which is a unique feature of the simulator.

#### SQLancer Properties

SQLancer has a number of properties implemented for testing various database systems.
We have ported some of them to the simulator, and we plan to port more in the future. The current list of
ported properties is as follows:

- [ ] [Pivoted Query Synthesis (PQS)](https://www.usenix.org/system/files/osdi20-rigger.pdf)
  - [ ] InsertSelect: This property is a variation of the Pivoted Query Synthesis paper, where we first
  insert some values into the database, and try to read them back by crafting a SELECT query that should return
  the values.
    - [x] `INSERT VALUES ...; SELECT ...:`: We can currently test inserting some literal values and reading them back.
    - [ ] `INSERT SELECT ...; SELECT ...;`: We cannot test using the result of another SELECT for picking the inserted values.
    - [ ] TODO: (there are other variants we currently do not support)
- [ ] [Non-optimizing Reference Engine Construction (NoREC)](https://arxiv.org/abs/2007.08292)
  - [x] SelectSelectOptimizer: This is another SQLancer-inspired property, where we check that `SELECT * from t WHERE p` is equivalent to
  `SELECT p from t` by asserting that the number of returned rows in the first one is equal to the number of `TRUE` values in the second one.
- [ ] [Ternary Logic Partitioning (TLP)](https://dl.acm.org/doi/pdf/10.1145/3428279)
  - [x] WhereTrueFalseNull: This property relies on the three-valued logic of SQL, where `p OR (NOT P) OR (p IS NULL)` should always be `TRUE`.
  - [x] UNIONAllPreservesCardinality: This property asserts that merging the results of multiple queries via `UNION ALL` is equivalent to
- [ ] [Differential Query Execution (DQE)](https://ieeexplore.ieee.org/document/10172736)
  - [ ] TODO: This paper is not open access, if anyone has access, please update this.
- [ ] [Cardinality Estimation Restriction Testing (CERT)](https://arxiv.org/pdf/2306.00355)
- [ ] [Differential Query Plans (DQP)](https://dl.acm.org/doi/pdf/10.1145/3654991)
- [ ] [Constant Optimization Driven Database System Testing (CODDTest)](https://arxiv.org/abs/2501.11252)

#### Bespoke Properties

- [x] ReadYourUpdatesBack: This property is similar to InsertSelect, the main difference being we use UPDATE for changing some existing
values in the database and then checking the result of the UPDATE.
- [x] DeleteSelect: This property is similar to ReadYourUpdatesBack, the main difference being we use DELETE for removing some existing
values in the database and then checking the result of the DELETE, mainly by checking that the deleted values are not present anymore.
- [x] DropSelect: This is a failure property, where we drop a table and then check that any SELECT queries on the dropped table fail as expected.
- [x] DoubleCreateFailure: This is a failure property, where we try to create a table that already exists and check that the operation fails as expected.
- [x] SelectLimit: This property checks that the LIMIT clause in SELECT statements is respected by checking the cardinality of the returned results.

#### Shadow State Properties

These properties are invariants that check the consistency between Turso and the shadow state.

- [x] TableHasExpectedContent: This property checks that a specific table in Turso has the same content as the shadow state.
- [x] AllTableHaveExpectedContent: This property checks that all tables in Turso have the same content as the shadow state.

#### Fault Injection Properties

- [x] FsyncNoWait: TODO
- [x] FaultyQuery: TODO

### Oracles

- [x] Property: The `property` oracle tests the properties presented above by generating random SQL statements and assertions that follow them.
- [x] Differential: The `differential` oracle tests Turso against SQLite.
- [x] Doublecheck: The `doublecheck` oracle tests Turso against itself for checking determinism across runs.
- [ ] Oracle-based Generation: Currently, we generate regardless the oracle, but `property` oracle actually poses limitations compared to `differential`
and `doublecheck` oracles, in those oracles we have much more freedom to generate random inputs. We must leverage this freedom.

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

## Data Collection

At the moment, we collect no data from the simulator, which makes it hard to make future decisions. Some examples are:

- Which properties are the most useful for finding bugs?
- What are the bottlenecks of the simulator speed, how can we optimize them?
- Which features/profiles are used, which are not, so we can focus on simplification by removing unused features or understanding why they aren't used.