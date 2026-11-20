# Limbo Simulator

Limbo simulator uses randomized deterministic simulations to test the Limbo database behaviors.

Each simulation begins with a random configurations:

- the database workload distribution(percentages of reads, writes, deletes...),
- database parameters(page size),
- number of reader or writers, etc.

Based on these parameters, we randomly generate **interaction plans**. Interaction plans consist of statements/queries, and assertions that will be executed in order. The building blocks of interaction plans are:

- Randomly generated SQL queries satisfying the workload distribution,
- Properties, which contain multiple matching queries with assertions indicating the expected result.

An example of a property is the following:

```sql
-- begin testing 'Select-Select-Optimizer'
-- ASSUME table marvelous_ideal exists;
SELECT ((devoted_ahmed = -9142609771.541502 AND loving_wicker = -1246708244.164486)) FROM marvelous_ideal WHERE TRUE;
SELECT * FROM marvelous_ideal WHERE (devoted_ahmed = -9142609771.541502 AND loving_wicker = -1246708244.164486);
-- ASSERT select queries should return the same amount of results;
-- end testing 'Select-Select-Optimizer'
```

The simulator starts from an initially empty database, adding random interactions based on the workload distribution. It can
add random queries unrelated to the properties without breaking the property invariants to reach more diverse states and respect the configured workload distribution.

The simulator executes the interaction plans in a loop, and checks the assertions. It can add random queries unrelated to the properties without
breaking the property invariants to reach more diverse states and respect the configured workload distribution.

The simulator code is broken into 4 main parts:

- **Simulator(main.rs)**: The main entry point of the simulator. It generates random configurations and interaction plans, and executes them.
- **Model(model.rs, model/table.rs, model/query.rs)**: A simpler model of the database, it contains atomic actions for insertion and selection, we use this model while deciding the next actions.
- **Generation(generation.rs, generation/table.rs, generation/query.rs, generation/plan.rs)**: Random generation functions for the database model and interaction plans.
- **Properties(properties.rs)**: Contains the properties that we want to test.

## Running the simulator

To run the simulator, you can use the following command:

```bash
RUST_LOG=limbo_sim=debug cargo run --bin limbo_sim
```

The simulator CLI has a few configuration options that you can explore via `--help` flag.

```txt
The Limbo deterministic simulator

Usage: limbo_sim [OPTIONS]

Options:
  -s, --seed <SEED>                    set seed for reproducible runs
  -d, --doublecheck                    enable doublechecking, run the simulator with the plan twice and check output equality
  -n, --maximum-tests <MAXIMUM_TESTS>  change the maximum size of the randomly generated sequence of interactions [default: 5000]
  -k, --minimum-tests <MINIMUM_TESTS>  change the minimum size of the randomly generated sequence of interactions [default: 1000]
  -t, --maximum-time <MAXIMUM_TIME>    change the maximum time of the simulation(in seconds) [default: 3600]
  -l, --load <LOAD>                    load plan from the bug base
  -w, --watch                          enable watch mode that reruns the simulation on file changes
      --differential                   run differential testing between sqlite and Limbo
  -h, --help                           Print help
  -V, --version                        Print version
```

## Adding new properties

The properties are defined in `simulator/generation/property.rs` in the `Property` enum. Each property is documented with
inline doc comments, an example is given below:

```rust
/// Insert-Select is a property in which the inserted row
/// must be in the resulting rows of a select query that has a
/// where clause that matches the inserted row.
/// The execution of the property is as follows
///     INSERT INTO <t> VALUES (...)
///     I_0
///     I_1
///     ...
///     I_n
///     SELECT * FROM <t> WHERE <predicate>
/// The interactions in the middle has the following constraints;
/// - There will be no errors in the middle interactions.
/// - The inserted row will not be deleted.
/// - The inserted row will not be updated.
/// - The table `t` will not be renamed, dropped, or altered.
InsertValuesSelect {
    /// The insert query
    insert: Insert,
    /// Selected row index
    row_index: usize,
    /// Additional interactions in the middle of the property
    queries: Vec<Query>,
    /// The select query
    select: Select,
},
```

If you would like to add a new property, you can add a new variant to the `Property` enum, and the corresponding
generation function in `simulator/generation/property.rs`. The generation function should return a `Property` instance, and
it should generate the necessary queries and assertions for the property.

## Automatic Compatibility Testing with SQLite

You can use the `--differential` flag to run the simulator in differential testing mode. This mode will run the same interaction plan on both Limbo and SQLite, and compare the results. It will also check for any panics or errors in either database.

## Simulator Profiles
A Simulator Profile allows you to influence query generation and I/O fault injection. You can run predefined profiles or you can create your own custom profile in a separate JSON file. You can select the profile you want by passing the `--profile` flag to he CLI. It will accept a predefined Profile name or a file path. 

For development purposes, you can run `make sim-schema` to generate a JsonSchema of the `Profile` struct. Then you can create profiles to test locally in a `configs/custom` folder that is gitignored and have editor integration by adding `$schema` tag to reference the generated JsonSchema:

```json
{
	"$schema": "./profile-schema.json",
  ...
}
```

## Swarm testing

A run with all statements and fixed weights cannot get to some states.
For example, DELETE keeps tables small, savepoint rollbacks discard writes, and frequent checkpoints keep the WAL short.
With `--swarm`, the seed of each run also picks these settings:

- The statements. Each kind of statement is off in about half of the runs. The weight of each other kind is 1/4, 1/2, 1, 2, or 4 times its weight in the profile.
- The properties that write, for example `Savepoint-Rollback` and `Delete-Select`. Each one is off in about half of the runs.
- The faults: database reopen and power loss, faulty queries, and `FsyncNoWait`. Each one is off in about half of the runs.
- The connections: 1 to the connection count of the profile. The default profile has 10 connections.
- The I/O latency probability: 0, 1, or 10 percent.

Three rules apply:

- SELECT and INSERT always stay on. The read-only properties take their weight from the SELECT weight. Without INSERT, the tables stay empty.
- The read-only properties always stay on, for example `Select-Limit`. They do not change the database, so a run without them finds fewer bugs.
- If DROP TABLE stays on, CREATE TABLE also stays on. Otherwise, a run can drop all of its tables.

The swarm changes the profile that you select with `--profile`.
It does not turn on a statement, a property, or a fault that the profile or a flag turns off.
For example, `--swarm --disable-reopen-database` never reopens the database, and `--swarm --profile faultless` injects no I/O faults.
If you give `--latency-prob`, it keeps its value.

The output shows the settings of the run:

```text
swarm: connections 9
swarm: --latency-prob 10 --disable-delete-select --disable-drop-select --disable-savepoint-rollback --disable-fsync-no-wait --disable-reopen-database
swarm: queries select:240 create_table:8 insert:30 drop_table:8 alter_table:1 drop_index:4 setval:2
swarm: queries off create_index update delete pragma create_sequence drop_sequence nextval
```

The names of the statements are the weight fields of the profile, without the `_weight` suffix.
To repeat a run, use the same seed and the same flags:

```bash
cargo run -p limbo_sim -- --swarm --seed 1
```

CI runs a swarm profile.
On pull requests, iteration N uses seed N, so each pull request tests the same settings.
On pushes to main, each iteration uses a new random seed.
If an iteration fails, the log ends with the command that repeats the run.

## Run simulator using the Miri interpreter

Miri is a deterministic Rust interpreter designed to identify undefined behavior. To run the simulator under Miri, use
```bash
MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-disable-stacked-borrows" RUST_LOG=limbo_sim=debug cargo +nightly miri run --bin limbo_sim -- --disable-integrity-check
````
Notes:
- `-Zmiri-disable-isolation` is needed for host access (like opening a file)
- `-Zmiri-disable-stacked-borrows` this alias checking is experimental, so disabled for now
- `--disable-integrity-check` is needed since we can't run sqlite via the FFI in Miri

## Resources

- [(reading) TigerBeetle Deterministic Simulation Testing](https://docs.tigerbeetle.com/about/vopr/)
- [(reading) sled simulation guide (jepsen-proof engineering)](https://sled.rs/simulation.html)
- [(video) "Testing Distributed Systems w/ Deterministic Simulation" by Will Wilson](https://www.youtube.com/watch?v=4fFDFbi3toc)
- [(video) FF meetup #4 - Deterministic simulation testing](https://www.youtube.com/live/29Vz5wkoUR8)
- [(code) Hiisi: a proof of concept libSQL written in Rust following TigerBeetle-style with deterministic simulation testing](https://github.com/penberg/hiisi)

## MVCC validation

MVCC generation uses explicit INTEGER PRIMARY KEY values in INSERTs and UPSERTs.
Its row model predicts implicit keys from the currently visible maximum, but
MVCC allocation can retain reservations after rollback. Comparing those
predicted keys would report a false failure. WAL generation retains implicit-key
generation.

The final integrity check uses Turso for MVCC and SQLite for WAL. Memory-I/O
exports include the MVCC logical log so retained failure artifacts can be
reopened with their committed state.
