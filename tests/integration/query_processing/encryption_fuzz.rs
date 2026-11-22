//! Seeded encryption authentication, API-misuse, and I/O-failure fuzzing.
//! Prints a random SEED, or uses SEED from the environment to reproduce a run.
//! Default campaigns sample six cases; ENCRYPTION_TEST_FULL=1 selects 48.
//! ENCRYPTION_FUZZ_CASES overrides the case count. Use the same test features
//! and case count when reproducing a seed.

use super::*;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeSet;
use turso_core::{CompletionError, Statement, StepResult};

#[path = "encryption_scenarios.rs"]
mod scenarios;

const GENERATED_DECISIONS: usize = 128;

pub(super) const CIPHERS: [CipherMode; 8] = [
    CipherMode::Aes128Gcm,
    CipherMode::Aes256Gcm,
    CipherMode::Aegis128L,
    CipherMode::Aegis128X2,
    CipherMode::Aegis128X4,
    CipherMode::Aegis256,
    CipherMode::Aegis256X2,
    CipherMode::Aegis256X4,
];
const PAGE_SIZES: [u32; 8] = [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536];

#[test]
fn seeded_encryption_state_machine() {
    let seed = std::env::var("SEED").map_or_else(
        |_| rand::random::<u64>(),
        |seed| seed.parse().expect("SEED must be an unsigned integer"),
    );
    let cases = env_number(
        "ENCRYPTION_FUZZ_CASES",
        if crate::common::full_encryption_tests() {
            48
        } else {
            6
        },
    );
    assert!(cases > 0);
    eprintln!("encryption fuzzer: SEED={seed} ENCRYPTION_FUZZ_CASES={cases}");
    campaign(seed, cases);
}

#[test]
fn encrypted_reload_with_retained_completions_regression() {
    campaign(11283090107835449216, 6);
}

fn campaign(seed: u64, cases: u64) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    for index in 0..cases {
        let mut case = Case::generate(&mut rng, seed, index);
        for faults in [false, true] {
            case.faults = faults;
            eprintln!("case {index}, faults={faults}: {case:?}");
            execute(
                &case,
                Box::new(RandomChooser {
                    rng: ChaCha8Rng::seed_from_u64(case.seeds.schedule),
                }),
            );
        }
    }
    eprintln!("completed encryption campaign: SEED={seed} ENCRYPTION_FUZZ_CASES={cases}");
}

fn execute(case: &Case, chooser: Box<dyn Chooser>) -> Trace {
    static RUNS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let run = RUNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut trace = Trace::new(
        format!(
            "{}-{}-{}-{run}",
            case.seeds.campaign, case.seeds.index, case.faults
        ),
        case.faults,
        chooser,
    );
    trace.generated = GENERATED_DECISIONS;

    match &case.family {
        Family::PageOne {
            cipher,
            page_size,
            credential,
        } => {
            page_one_schedule(&mut trace, *cipher, *page_size, *credential);
        }
        Family::Manager {
            existing,
            mvcc,
            credential,
            cancel_waiter,
            wait_for_bytes,
        } => {
            manager_schedule(
                &mut trace,
                *existing,
                *mvcc,
                *credential,
                *cancel_waiter,
                *wait_for_bytes,
            );
        }
        Family::Lifecycle(spec) => lifecycle(&mut trace, spec),
    }
    trace
}

impl Trace {
    pub(super) fn generating(&self) -> bool {
        self.actions.len() < self.generated
    }
}

fn fair(enabled: &[Action], decided: &[Action]) -> Action {
    if let Some(action) = enabled
        .iter()
        .find(|action| matches!(action, Action::Complete(_)))
    {
        return action.clone();
    }
    if enabled.contains(&Action::CompleteUnderlying) {
        return Action::CompleteUnderlying;
    }
    let last = decided.iter().rev().find_map(|action| match action {
        Action::Poll(actor) => Some(*actor),
        _ => None,
    });
    let polls: Vec<usize> = enabled
        .iter()
        .filter_map(|action| match action {
            Action::Poll(actor) => Some(*actor),
            _ => None,
        })
        .collect();
    if let Some(actor) = polls
        .iter()
        .find(|actor| last.is_none_or(|last| **actor > last))
        .or_else(|| polls.first())
    {
        return Action::Poll(*actor);
    }
    enabled
        .iter()
        .find(|action| matches!(action, Action::Start(_)))
        .or_else(|| {
            enabled
                .iter()
                .find(|action| matches!(action, Action::Drop(_)))
        })
        .cloned()
        .expect("only failure injections are enabled")
}
struct RandomChooser {
    rng: ChaCha8Rng,
}

impl Chooser for RandomChooser {
    fn choose(&mut self, enabled: &[Action], decided: &[Action]) -> Action {
        let action = if decided.len() < GENERATED_DECISIONS {
            let rng = &mut self.rng;
            let choices: Vec<_> = enabled
                .iter()
                .filter(|action| {
                    !matches!(action, Action::Drop(_) | Action::Fail(_)) || rng.random_ratio(1, 4)
                })
                .cloned()
                .collect();
            if choices.is_empty() {
                fair(enabled, decided)
            } else {
                choices[self.rng.random_range(0..choices.len())].clone()
            }
        } else {
            fair(enabled, decided)
        };
        action
    }
}

#[derive(Clone, Debug)]
struct Seeds {
    campaign: u64,
    index: u64,
    scenario: u64,
    operations: u64,
    schedule: u64,
}

#[derive(Clone, Debug)]
enum Family {
    PageOne {
        cipher: CipherMode,
        page_size: u32,
        credential: Credential,
    },
    Manager {
        existing: bool,
        mvcc: bool,
        credential: Credential,
        cancel_waiter: bool,
        wait_for_bytes: bool,
    },
    Lifecycle(LifecycleSpec),
}

#[derive(Clone, Debug)]
struct Case {
    seeds: Seeds,
    faults: bool,
    family: Family,
}

const CREDENTIALS: [Credential; 4] = [
    Credential::Missing,
    Credential::Wrong,
    Credential::WrongCipher,
    Credential::Correct,
];
impl Case {
    fn generate(rng: &mut ChaCha8Rng, campaign: u64, index: u64) -> Case {
        let seeds = Seeds {
            campaign,
            index,
            scenario: rng.random(),
            operations: rng.random(),
            schedule: rng.random(),
        };
        let mut scenario = ChaCha8Rng::seed_from_u64(seeds.scenario);
        let mut family = match index % 3 {
            0 => Family::PageOne {
                cipher: CIPHERS[scenario.random_range(0..CIPHERS.len())],
                page_size: PAGE_SIZES[scenario.random_range(0..PAGE_SIZES.len())],
                credential: CREDENTIALS[scenario.random_range(0..4)],
            },
            1 => {
                let existing = scenario.random::<bool>();
                Family::Manager {
                    existing,
                    // A new database opened without MVCC finishes before the
                    // registry owner yields, so it has no waiting window.
                    mvcc: !existing || scenario.random::<bool>(),
                    credential: CREDENTIALS[scenario.random_range(0..4)],
                    cancel_waiter: scenario.random::<bool>(),
                    wait_for_bytes: existing || scenario.random::<bool>(),
                }
            }
            _ => Family::Lifecycle(LifecycleSpec::generate(
                &mut scenario,
                &mut ChaCha8Rng::seed_from_u64(seeds.operations),
            )),
        };
        if index == 2 {
            let Family::Lifecycle(spec) = &mut family else {
                unreachable!()
            };
            spec.require_public_accessor_sweep();
        }
        Case {
            seeds,
            faults: false,
            family,
        }
    }
}

fn operation_owner(actor: usize, command: usize) -> String {
    format!("a{actor}.c{command}")
}

fn env_number(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |value| value.parse().unwrap())
}

/// Credentials for opening or connecting to the main database, relative to the
/// key that ends up established.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Cred {
    Missing,
    Bound,
    OtherKey,
    WrongCipher,
}

/// Credentials in an ATTACH URI for the separately encrypted target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AttachCred {
    Missing,
    PrimaryKey,
    TargetKey,
    WrongCipher,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CipherChoice {
    Main,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Setting {
    Cipher(CipherChoice),
    Key(u8),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OpenApi {
    Sync,
    Async,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Command {
    /// Execute an encryption PRAGMA.
    Set(Setting),
    Read,
    Schema,
    TableInfo,
    Insert(i64),
    Checkpoint,
    Connect(Cred),
    OpenCached(Cred),
    OpenDatabase(Cred, OpenApi),
    ReadDatabaseMvStore,
    ReadConnectionMvStore,
    ReadStatementMvStore,
    ReadConnectionSchema,
    ReadConnectionPager,
    ReadStatementPager,
    BeginWalInsert,
    PublishSchemaAfterRestore,
    ReloadWalAfterRestore,
    Attach(AttachCred),
}

impl std::fmt::Display for Command {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, formatter)
    }
}

/// How the scenario's actors first reach the database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Start {
    /// Actors connect to a new plaintext or encrypted database before its first write.
    Earlier,
    /// The encrypted database is created, filled and closed; actors then open
    /// it through the registry with their own credentials.
    Cold,
}

#[derive(Clone, Debug)]
pub(super) struct LifecycleSpec {
    pub(super) start: Start,
    pub(super) plaintext: bool,
    pub(super) cipher: CipherMode,
    pub(super) other: CipherMode,
    pub(super) page_size: u32,
    pub(super) mvcc: bool,
    pub(super) target_cached: bool,
    /// Commands run concurrently after the database holds protected data.
    pub(super) scripts: Vec<Vec<Command>>,
}

impl LifecycleSpec {
    fn generate(scenario: &mut ChaCha8Rng, operations: &mut ChaCha8Rng) -> LifecycleSpec {
        let cipher = CIPHERS[scenario.random_range(0..CIPHERS.len())];
        let other = *CIPHERS
            .iter()
            .filter(|other| {
                **other != cipher && other.required_key_size() == cipher.required_key_size()
            })
            .nth(scenario.random_range(0..2))
            .unwrap();
        let plaintext = scenario.random_ratio(1, 8);
        let start = if !plaintext && scenario.random_ratio(1, 3) {
            Start::Cold
        } else {
            Start::Earlier
        };
        let actors = scenario.random_range(2..=4);
        let mut spec = LifecycleSpec {
            start,
            plaintext,
            cipher,
            other,
            page_size: [1024, 4096, 8192, 65536][scenario.random_range(0..4)],
            mvcc: scenario.random::<bool>(),
            target_cached: scenario.random::<bool>(),
            scripts: vec![Vec::new(); actors],
        };
        let any_setting = |rng: &mut ChaCha8Rng| match rng.random_range(0..6) {
            0..=2 => Setting::Key(1),
            3 => Setting::Key(2),
            4 => Setting::Cipher(CipherChoice::Main),
            _ => Setting::Cipher(CipherChoice::Other),
        };
        for actor in 0..actors {
            if start == Start::Cold {
                spec.scripts[actor].push(Command::OpenCached(Self::cred(operations)));
            }
            for step in 0..operations.random_range(2..=8) {
                let insert = Command::Insert(1000 * (actor as i64 + 1) + step);
                let command = if plaintext {
                    match operations.random_range(0..7) {
                        0 => Command::Read,
                        1 => Command::Schema,
                        2 => Command::TableInfo,
                        3 | 4 => insert,
                        5 => Command::Checkpoint,
                        _ => Command::Set(any_setting(operations)),
                    }
                } else {
                    match operations.random_range(0..24) {
                        0 | 1 => Command::Read,
                        2 => Command::Schema,
                        3 => Command::TableInfo,
                        4 | 5 => insert,
                        6 => Command::Checkpoint,
                        7 => Command::Schema,
                        8 => Command::Connect(Self::cred(operations)),
                        9 => Command::OpenCached(Self::cred(operations)),
                        10 => Command::Attach(
                            [
                                AttachCred::Missing,
                                AttachCred::PrimaryKey,
                                AttachCred::TargetKey,
                                AttachCred::WrongCipher,
                            ][operations.random_range(0..4)],
                        ),
                        11 => Command::OpenDatabase(Self::cred(operations), OpenApi::Sync),
                        12 => Command::OpenDatabase(Self::cred(operations), OpenApi::Async),
                        13 if spec.mvcc => Command::ReadDatabaseMvStore,
                        14 if spec.mvcc => Command::ReadConnectionMvStore,
                        15 if spec.mvcc => Command::ReadStatementMvStore,
                        16 => Command::ReadConnectionSchema,
                        17 => Command::ReadConnectionPager,
                        18 => Command::ReadStatementPager,
                        19 if !spec.mvcc => Command::BeginWalInsert,
                        20 => Command::PublishSchemaAfterRestore,
                        21 => Command::ReloadWalAfterRestore,
                        _ => Command::Set(Setting::Key(operations.random_range(1..=2))),
                    }
                };
                spec.scripts[actor].push(command);
            }
        }
        spec
    }

    /// Makes the first lifecycle case of every campaign cover every public
    /// handle that can reach shared encrypted state without executing SQL.
    fn require_public_accessor_sweep(&mut self) {
        self.start = Start::Earlier;
        self.plaintext = false;
        self.mvcc = true;
        for commands in &mut self.scripts {
            commands.clear();
        }
        self.scripts[0] = vec![
            Command::OpenDatabase(Cred::Missing, OpenApi::Sync),
            Command::OpenDatabase(Cred::OtherKey, OpenApi::Async),
            Command::ReadDatabaseMvStore,
            Command::Connect(Cred::Bound),
            Command::ReadConnectionMvStore,
            Command::ReadStatementMvStore,
            Command::ReadConnectionSchema,
            Command::ReadConnectionPager,
            Command::ReadStatementPager,
            Command::PublishSchemaAfterRestore,
            Command::ReloadWalAfterRestore,
        ];
        self.scripts[1] = vec![
            Command::Connect(Cred::Bound),
            Command::Read,
            Command::Insert(2001),
            Command::Checkpoint,
        ];
    }

    fn cred(rng: &mut ChaCha8Rng) -> Cred {
        [
            Cred::Missing,
            Cred::Bound,
            Cred::OtherKey,
            Cred::WrongCipher,
        ][rng.random_range(0..4)]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Category {
    CredentialMissing,
    CredentialWrong,
    CipherMismatch,
    SettingsRejected,
    Contention,
    InjectedIo,
    Unexpected,
}

pub(super) fn classify(error: &LimboError) -> Category {
    match error {
        LimboError::CompletionError(CompletionError::DecryptionError { .. }) => {
            Category::CredentialWrong
        }
        LimboError::CompletionError(CompletionError::IOError(_, "queued_io")) => {
            Category::InjectedIo
        }
        LimboError::Busy | LimboError::BusySnapshot | LimboError::WriteWriteConflict => {
            Category::Contention
        }
        LimboError::NotADB => Category::CredentialMissing,
        // A checkpoint reports the I/O error that stopped it as text.
        LimboError::CheckpointFailed(message) if message.contains("(queued_io)") => {
            Category::InjectedIo
        }
        LimboError::InvalidArgument(message) => {
            if message.contains("requires an encryption key")
                || message.contains("encrypted but no encryption options")
            {
                Category::CredentialMissing
            } else if message.contains("cipher does not match") {
                Category::CipherMismatch
            } else if message.contains("opt in feature")
                || message.contains("encryption must be configured when opening")
                || message.contains("without encryption")
                || message.contains("external restore is not supported")
            {
                Category::SettingsRejected
            } else {
                Category::Unexpected
            }
        }
        _ => Category::Unexpected,
    }
}

/// Established encryption of the main database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Binding {
    Plaintext,
    Bound(CipherMode, u8),
}

/// A statement an actor is driving.
struct Active {
    command: Command,
    owner: String,
    statement: Statement,
    allowed: bool,
    pending: Option<IOCompletions>,
    rows: Vec<String>,
    waiting_since: Option<usize>,
}

struct ActorState {
    connection: Option<Arc<Connection>>,
    database: Option<Arc<Database>>,
    database_authorized: bool,
    authenticated: bool,
    next: usize,
    active: Option<Active>,
}

struct World {
    io: Arc<QueuedIo>,
    inner: Arc<dyn IO>,
    path: String,
    target_path: String,
    base: OpenOptions,
    spec: LifecycleSpec,
    binding: Binding,
    actors: Vec<ActorState>,
    owner: Option<Arc<Connection>>,
    database: Option<Arc<Database>>,
    target: Option<(Arc<Database>, Arc<Connection>)>,
    acknowledged: BTreeSet<i64>,
    possible: BTreeSet<i64>,
    fault_injected: bool,
    epoch: usize,
}

impl World {
    fn cipher_of(&self, choice: CipherChoice) -> CipherMode {
        match choice {
            CipherChoice::Main => self.spec.cipher,
            CipherChoice::Other => self.spec.other,
        }
    }

    fn bound(&self) -> (CipherMode, u8) {
        match self.binding {
            Binding::Bound(cipher, key) => (cipher, key),
            _ => (self.spec.cipher, 1),
        }
    }

    fn other_key(&self) -> u8 {
        if self.bound().1 == 1 {
            2
        } else {
            1
        }
    }

    fn wrong_cipher(&self) -> CipherMode {
        let bound = self.bound().0;
        if bound == self.spec.cipher {
            self.spec.other
        } else {
            self.spec.cipher
        }
    }

    fn options(&self, cred: Cred) -> OpenOptions {
        let (cipher, key) = self.bound();
        match cred {
            Cred::Missing => self.base.clone(),
            Cred::Bound => self
                .base
                .clone()
                .encryption(encryption_options(cipher, key)),
            Cred::OtherKey => self
                .base
                .clone()
                .encryption(encryption_options(cipher, self.other_key())),
            Cred::WrongCipher => self
                .base
                .clone()
                .encryption(encryption_options(self.wrong_cipher(), key)),
        }
    }

    fn connect_key(&self, cred: Cred) -> Option<EncryptionKey> {
        let (cipher, key) = self.bound();
        match cred {
            Cred::Missing => None,
            Cred::Bound => Some(self::key(cipher, key)),
            Cred::OtherKey => Some(self::key(cipher, self.other_key())),
            Cred::WrongCipher => Some(self::key(self.wrong_cipher(), key)),
        }
    }

    fn main_file_size(&self) -> u64 {
        self.inner
            .open_file(&self.path, OpenFlags::Create, false)
            .unwrap()
            .size()
            .unwrap()
    }

    fn any_database(&self) -> Option<Arc<Database>> {
        self.database
            .clone()
            .or_else(|| self.actors.iter().find_map(|actor| actor.database.clone()))
    }

    fn authorized(&self, actor: usize) -> bool {
        self.actors[actor].connection.is_some() && self.actors[actor].authenticated
    }
}

fn lifecycle(trace: &mut Trace, spec: &LifecycleSpec) {
    #[cfg(feature = "io_memory_yield")]
    let inner: Arc<dyn IO> = Arc::new(turso_core::MemoryYieldIO::new());
    #[cfg(not(feature = "io_memory_yield"))]
    let inner: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let io = Arc::new(QueuedIo::new_with_io(inner.clone()));
    io.set_owner("setup");
    let path = format!(":encryption-fuzz-lifecycle:{}", trace.namespace);
    let base = OpenOptions::new(Arc::new(SqliteDialect))
        .db_opts(DatabaseOpts::new().with_encryption(true).with_attach(true))
        .page_size(turso_core::storage::sqlite3_ondisk::PageSize::new(spec.page_size).unwrap());
    let mut world = World {
        io: io.clone(),
        inner,
        target_path: format!("{path}-attached"),
        path,
        base,
        spec: spec.clone(),
        binding: if spec.plaintext {
            Binding::Plaintext
        } else {
            Binding::Bound(spec.cipher, 1)
        },
        actors: (0..spec.scripts.len())
            .map(|_| ActorState {
                connection: None,
                database: None,
                database_authorized: false,
                authenticated: spec.plaintext,
                next: 0,
                active: None,
            })
            .collect(),
        owner: None,
        database: None,
        target: None,
        acknowledged: BTreeSet::from([0]),
        possible: BTreeSet::from([0]),
        fault_injected: false,
        epoch: 0,
    };

    match spec.start {
        Start::Earlier => earlier_setup(&mut world),
        Start::Cold => cold_setup(trace, &mut world),
    }
    concurrent_phase(trace, &mut world);
    postconditions(trace, world);
}

fn earlier_setup(world: &mut World) {
    let db = Database::open(
        world.io.clone(),
        &world.path,
        if world.spec.plaintext {
            world.base.clone()
        } else {
            world.options(Cred::Bound)
        },
    )
    .unwrap();
    if !world.spec.plaintext {
        for cred in [Cred::Missing, Cred::OtherKey] {
            match db.connect(
                ConnectOpts::new(turso_core::SessionRole::Superuser)
                    .with_encryption_key(world.connect_key(cred)),
            ) {
                Ok(_) => panic!("{cred:?} connected before the first write"),
                Err(error) => check_credential_refusal(world, &error, "before the first write"),
            }
        }
    }
    let owner = db
        .connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser).with_encryption_key(
                if world.spec.plaintext {
                    None
                } else {
                    world.connect_key(Cred::Bound)
                },
            ),
        )
        .unwrap();
    assert_eq!(
        world.main_file_size(),
        0,
        "authentication wrote database bytes"
    );
    assert!(
        !world
            .io
            .records()
            .iter()
            .any(|record| record.event.path == world.path
                && matches!(
                    record.event.kind,
                    QueuedIoOpKind::Pwrite | QueuedIoOpKind::Pwritev
                )),
        "authentication submitted a database write"
    );
    for actor in &mut world.actors {
        if world.spec.plaintext {
            actor.connection = Some(
                db.connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
                    .unwrap(),
            );
        }
    }
    world.database = Some(db);
    populate(world, &owner);
    world.owner = Some(owner);
    create_target(world);
}

/// Creates, fills and closes an encrypted database. Actors then open it
/// themselves, so the first successful open is a cold registry open.
fn cold_setup(trace: &mut Trace, world: &mut World) {
    let (cipher, key) = (world.spec.cipher, 1);
    let weak = {
        let db = Database::open(
            world.io.clone(),
            &world.path,
            world
                .base
                .clone()
                .encryption(encryption_options(cipher, key)),
        )
        .unwrap();
        let owner = db
            .connect(
                ConnectOpts::new(turso_core::SessionRole::Superuser)
                    .with_encryption_key(Some(self::key(cipher, key))),
            )
            .unwrap();
        populate(world, &owner);
        owner.close().unwrap();
        Arc::downgrade(&db)
    };
    assert!(weak.upgrade().is_none(), "the setup database stayed open");
    trace.observe("database closed before actors open it");
    create_target(world);
}

fn populate(world: &mut World, owner: &Arc<Connection>) {
    if world.spec.mvcc {
        owner.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    }
    owner
        .execute("CREATE TABLE secrets(id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();
    owner
        .execute("INSERT INTO secrets VALUES (0, 'protected value')")
        .unwrap();
}

fn create_target(world: &mut World) {
    if world.spec.plaintext {
        return;
    }
    let (cipher, key) = world.bound();
    let target_key = if key == 1 { 2 } else { 1 };
    let db = Database::open(
        world.io.clone(),
        &world.target_path,
        world
            .base
            .clone()
            .encryption(encryption_options(cipher, target_key)),
    )
    .unwrap();
    let connection = db
        .connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser)
                .with_encryption_key(Some(self::key(cipher, target_key))),
        )
        .unwrap();
    // ATTACH requires the same page size and journal mode as the main database.
    if world.spec.mvcc {
        connection.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    }
    connection
        .execute("CREATE TABLE secrets(value TEXT)")
        .unwrap();
    connection
        .execute("INSERT INTO secrets VALUES ('attached value')")
        .unwrap();
    if world.spec.target_cached {
        world.target = Some((db, connection));
    } else {
        connection.close().unwrap();
    }
}

fn check_setting_refused(actual: &turso_core::Result<()>, setting: Setting) {
    assert!(
        matches!(actual, Err(error) if classify(error) == Category::SettingsRejected),
        "{setting:?} returned {actual:?}, expected SettingsRejected"
    );
}

fn setting_sql(world: &World, setting: Setting) -> String {
    match setting {
        Setting::Cipher(choice) => format!("PRAGMA cipher = '{}'", world.cipher_of(choice)),
        Setting::Key(byte) => {
            // Keys are valid for whichever cipher the connection will use.
            let size_cipher = world.spec.cipher;
            format!(
                "PRAGMA hexkey = '{}'",
                encryption_options(size_cipher, byte).hexkey
            )
        }
    }
}

/// The scheduler for overlapping actors. Any idle actor may start its next
/// command while other actors wait on I/O.
fn concurrent_phase(trace: &mut Trace, world: &mut World) {
    loop {
        let generating = trace.generating();
        let pending = world.io.pending_events();
        let mut enabled = Vec::new();
        for (index, actor) in world.actors.iter().enumerate() {
            match &actor.active {
                Some(active) => {
                    let ready = active
                        .pending
                        .as_ref()
                        .is_none_or(|pending| pending.finished());
                    let waiting = active.waiting_since == Some(world.epoch);
                    if ready && !waiting {
                        enabled.push(Action::Poll(index));
                    }
                    if generating {
                        enabled.push(Action::Drop(index));
                    }
                }
                None => {
                    if generating && actor.next < world.spec.scripts[index].len() {
                        enabled.push(Action::Start(index));
                    }
                }
            }
        }
        for request in &pending {
            enabled.push(Action::Complete(request.id.clone()));
            if generating
                && trace.allow_failures
                && !world.fault_injected
                && matches!(
                    request.kind,
                    QueuedIoOpKind::Pread
                        | QueuedIoOpKind::Pwrite
                        | QueuedIoOpKind::Pwritev
                        | QueuedIoOpKind::Sync
                )
            {
                enabled.push(Action::Fail(request.id.clone()));
            }
        }
        if world.io.has_underlying_completions() {
            enabled.push(Action::CompleteUnderlying);
        }
        let busy = world.actors.iter().any(|actor| actor.active.is_some());
        if enabled.is_empty() {
            assert!(!busy, "no action is possible but work remains");
            break;
        }
        let action = trace.choose(&enabled);
        let changed = match action.clone() {
            Action::Start(actor) => start(trace, world, actor),
            Action::Poll(actor) => poll(trace, world, actor),
            Action::Drop(actor) => {
                cancel(trace, world, actor);
                true
            }
            Action::Complete(id) => {
                world.io.set_owner("scheduler");
                world.io.step_request(&id, false).unwrap().unwrap();
                true
            }
            Action::Fail(id) => {
                world.io.set_owner("scheduler");
                world.io.step_request(&id, true).unwrap().unwrap();
                world.fault_injected = true;

                trace.observe(format!("injected failure into {id}"));
                true
            }
            Action::CompleteUnderlying => {
                world.io.set_owner("scheduler");
                world.io.step_underlying().unwrap();

                true
            }
        };
        if changed {
            world.epoch += 1;
        }
        world.io.set_owner("scheduler");
    }
}

/// Starts an actor's next command. Returns whether anything changed.
fn start(trace: &mut Trace, world: &mut World, actor: usize) -> bool {
    let index = world.actors[actor].next;
    let command = world.spec.scripts[actor][world.actors[actor].next];
    world.actors[actor].next += 1;
    let owner = operation_owner(actor, index);
    for (other, state) in world.actors.iter().enumerate() {
        if other != actor
            && state
                .active
                .as_ref()
                .and_then(|active| active.pending.as_ref())
                .is_some_and(|pending| !pending.finished())
        {
            trace.observe(format!(
                "overlap: a{actor} started while a{other} waited on I/O"
            ));
            break;
        }
    }
    let allowed = match command {
        Command::OpenDatabase(cred, _) => match world.binding {
            Binding::Bound(..) => cred == Cred::Bound,
            Binding::Plaintext => cred == Cred::Missing,
        },
        _ => world.authorized(actor),
    };

    world.io.set_owner(owner.clone());
    if world.actors[actor].connection.is_none()
        && !matches!(
            command,
            Command::OpenCached(_)
                | Command::OpenDatabase(..)
                | Command::ReadDatabaseMvStore
                | Command::Connect(_)
        )
    {
        trace.observe(format!("{owner} {command} skipped: no connection"));
        return false;
    }
    let outcome = match command {
        Command::Connect(cred) => connect_command(world, actor, cred, false),
        Command::OpenCached(cred) => connect_command(world, actor, cred, true),
        Command::OpenDatabase(cred, api) => open_database_command(world, actor, cred, api),
        Command::ReadDatabaseMvStore => read_database_mv_store(world, actor),
        Command::ReadConnectionMvStore => read_connection_mv_store(world, actor),
        Command::ReadStatementMvStore => read_statement_mv_store(world, actor),
        Command::ReadConnectionSchema => read_connection_schema(world, actor),
        Command::ReadConnectionPager => read_connection_pager(world, actor),
        Command::ReadStatementPager => read_statement_pager(world, actor),
        Command::BeginWalInsert => begin_wal_insert(world, actor),
        Command::PublishSchemaAfterRestore => publish_schema_after_restore(world, actor),
        Command::ReloadWalAfterRestore => reload_wal_after_restore(world, actor),
        Command::Attach(cred) => attach_command(world, actor, cred),
        Command::Set(setting) => late_setting(world, actor, setting),
        Command::Read
        | Command::Schema
        | Command::TableInfo
        | Command::Insert(_)
        | Command::Checkpoint => {
            let sql = match command {
                Command::Read => "SELECT value FROM secrets WHERE id = 0".to_string(),
                Command::Schema => "SELECT name FROM sqlite_schema WHERE name = 'secrets'".into(),
                Command::TableInfo => "PRAGMA table_info(secrets)".to_string(),
                Command::Insert(id) => {
                    format!("INSERT INTO secrets VALUES ({id}, 'protected value')")
                }
                Command::Checkpoint => "PRAGMA wal_checkpoint(TRUNCATE)".to_string(),
                _ => unreachable!(),
            };

            let connection = world.actors[actor].connection.clone().unwrap();
            match connection.prepare(&sql) {
                Ok(statement) => {
                    world.actors[actor].active = Some(Active {
                        command,
                        owner: owner.clone(),
                        statement,
                        allowed,
                        pending: None,
                        rows: Vec::new(),
                        waiting_since: None,
                    });
                    "prepared".to_string()
                }
                Err(error) => {
                    check_error(world, command, allowed, &error, "prepare");
                    format!("prepare failed: {:?}", classify(&error))
                }
            }
        }
    };
    trace.observe(format!("{owner} {command} -> {outcome}"));
    true
}

/// Checks a failed engine call against the model.
fn check_error(world: &World, command: Command, allowed: bool, error: &LimboError, stage: &str) {
    let category = classify(error);
    if allowed {
        let tolerated = match category {
            Category::InjectedIo => world.fault_injected,
            Category::Contention => matches!(command, Command::Insert(_) | Command::Checkpoint),
            _ => false,
        };
        assert!(tolerated, "{command} failed at {stage}: {error}");
    } else {
        let refused = matches!(
            category,
            Category::CredentialMissing | Category::CredentialWrong | Category::CipherMismatch
        ) || (category == Category::InjectedIo && world.fault_injected);
        assert!(
            refused,
            "unauthorized {command} failed at {stage} with {error}"
        );
    }
}

/// Steps an actor's statement once.
fn poll(trace: &mut Trace, world: &mut World, actor: usize) -> bool {
    let mut active = world.actors[actor].active.take().unwrap();

    world.io.set_owner(active.owner.clone());
    if let Some(pending) = &active.pending {
        if let Some(error) = pending.get_error() {
            trace.observe(format!("{} completion error {error:?}", active.owner));
        }
    }
    active.pending = None;
    if let (Command::Insert(id), true) = (active.command, active.allowed) {
        world.possible.insert(id);
    }
    let result = active.statement.step();
    let mut changed = true;
    let finished = match &result {
        Ok(StepResult::Row) => {
            let column = usize::from(active.command == Command::TableInfo);
            assert!(
                active.allowed,
                "{} returned a row to a connection without the established key",
                active.command
            );
            if active.command != Command::Checkpoint {
                active.rows.push(
                    active
                        .statement
                        .row()
                        .unwrap()
                        .get::<String>(column)
                        .unwrap(),
                );
            }
            false
        }
        Ok(StepResult::Done) => {
            assert!(
                active.allowed,
                "{} succeeded on a connection without the established key",
                active.command
            );
            let expected: &[&str] = match active.command {
                Command::Read => &["protected value"],
                Command::Schema => &["secrets"],
                Command::TableInfo => &["id", "value"],
                _ => &[],
            };
            if let Command::Insert(id) = active.command {
                world.acknowledged.insert(id);
            } else if active.command != Command::Checkpoint {
                assert!(
                    active.rows == expected,
                    "{} returned {:?}",
                    active.command,
                    active.rows
                );
            }
            true
        }
        Ok(StepResult::IO) => {
            let pending = active.statement.io_completions();
            if pending
                .as_ref()
                .is_some_and(|pending| pending.is_explicit_yield())
            {
                active.waiting_since = Some(world.epoch);

                changed = false;
            } else {
                active.waiting_since = None;
                active.pending = pending;
            }
            false
        }
        Ok(StepResult::Yield) | Ok(StepResult::Sleep { .. }) => {
            active.waiting_since = Some(world.epoch);

            changed = false;
            false
        }
        Ok(StepResult::Busy) => {
            assert!(
                matches!(active.command, Command::Insert(_) | Command::Checkpoint),
                "{} was busy",
                active.command
            );

            true
        }
        Ok(StepResult::Interrupt) => panic!("statement was interrupted"),
        Err(error) => {
            check_error(world, active.command, active.allowed, error, "step");
            true
        }
    };
    trace.observe(format!(
        "{} step -> {}",
        active.owner,
        match &result {
            Ok(step) => format!("{step:?}"),
            Err(error) => format!("error {:?}", classify(error)),
        }
    ));
    if !finished {
        world.actors[actor].active = Some(active);
    }
    changed
}

fn cancel(trace: &mut Trace, world: &mut World, actor: usize) {
    let active = world.actors[actor].active.take().unwrap();
    world.io.set_owner(active.owner.clone());
    let phase = match &active.pending {
        Some(pending) if !pending.finished() => "waiting on I/O",
        _ if active.waiting_since.is_some() => "waiting on another actor",
        _ => "between steps",
    };

    trace.observe(format!("{} dropped while {phase}", active.owner));
    drop(active);
}

fn connect_command(world: &mut World, actor: usize, cred: Cred, open: bool) -> String {
    // A connection takes only a key; its cipher comes from the database. A
    // wrong-cipher credential therefore carries the right key bytes when it is
    // used to connect, and only an open can present a wrong cipher.
    let allowed = match world.binding {
        Binding::Bound(..) => cred == Cred::Bound || (!open && cred == Cred::WrongCipher),
        _ => cred == Cred::Missing,
    };
    let kind = if open { "open" } else { "connect" };

    let key = world.connect_key(cred);
    let result = if open {
        Database::open(world.io.clone(), &world.path, world.options(cred)).and_then(|db| {
            db.connect(
                ConnectOpts::new(turso_core::SessionRole::Superuser).with_encryption_key(key),
            )
            .map(|connection| (Some(db), connection))
        })
    } else {
        match world.any_database() {
            Some(db) => db
                .connect(
                    ConnectOpts::new(turso_core::SessionRole::Superuser).with_encryption_key(key),
                )
                .map(|connection| (None, connection)),
            None => return "skipped: no open database".to_string(),
        }
    };
    match result {
        Ok((db, connection)) => {
            assert!(
                allowed,
                "{kind} with {cred:?} succeeded on {:?}",
                world.binding
            );
            let state = &mut world.actors[actor];
            state.connection = Some(connection);
            if db.is_some() {
                state.database = db;
                state.database_authorized = allowed;
            }
            state.authenticated = true;
            "connected".to_string()
        }
        Err(error) => {
            let category = classify(&error);
            if allowed {
                assert!(
                    category == Category::InjectedIo && world.fault_injected,
                    "{kind} with the established key failed: {error}"
                );
            } else {
                check_credential_refusal(world, &error, &format!("{kind} with {cred:?}"));
            }
            format!("refused: {category:?}")
        }
    }
}

fn open_database_command(world: &mut World, actor: usize, cred: Cred, api: OpenApi) -> String {
    let allowed = match world.binding {
        Binding::Bound(..) => cred == Cred::Bound,
        Binding::Plaintext => cred == Cred::Missing,
    };

    let result = match api {
        OpenApi::Sync => Database::open(world.io.clone(), &world.path, world.options(cred)),
        OpenApi::Async => open_database_async(world, cred),
    };
    match result {
        Ok(database) => {
            assert!(
                allowed,
                "{api:?} Database::open with {cred:?} returned protected shared state"
            );
            let state = &mut world.actors[actor];
            state.database = Some(database);
            state.database_authorized = true;
            "opened database".to_string()
        }
        Err(error) => {
            let category = classify(&error);
            if allowed {
                assert!(
                    category == Category::InjectedIo && world.fault_injected,
                    "{api:?} Database::open with {cred:?} failed: {error}"
                );
            } else {
                check_credential_refusal(
                    world,
                    &error,
                    &format!("{api:?} Database::open with {cred:?}"),
                );
            }
            format!("refused: {category:?}")
        }
    }
}

fn open_database_async(world: &World, cred: Cred) -> turso_core::Result<Arc<Database>> {
    let file = world.io.open_file(&world.path, OpenFlags::Create, false)?;
    let options =
        world
            .options(cred)
            .storage(Arc::new(turso_core::storage::database::DatabaseFile::new(
                file,
            )));
    let mut state = OpenDbAsyncState::new();
    loop {
        match Database::open_async(&mut state, world.io.clone(), &world.path, &options)
            .map_err(|error| *error)?
        {
            IOResult::Done(database) => return Ok(database),
            IOResult::IO(pending) => pending.wait(world.io.as_ref())?,
        }
    }
}

fn read_database_mv_store(world: &World, actor: usize) -> String {
    let (database, allowed) = match &world.actors[actor].database {
        Some(database) => (database, world.actors[actor].database_authorized),
        None => return "skipped: no authenticated database handle".to_string(),
    };
    let exposed = database_mv_store_contains(database, b"protected value");

    assert!(
        allowed || !exposed,
        "Database::get_mv_store exposed protected data without authenticated credentials"
    );
    if allowed && world.spec.mvcc {
        assert!(
            database
                .get_mv_store()
                .as_ref()
                .is_some_and(|store| world.any_database().is_some_and(|db| db
                    .get_mv_store()
                    .as_ref()
                    .is_some_and(|shared| Arc::ptr_eq(store, shared)))),
            "an authenticated Database handle did not share the MVCC store"
        );
    }
    format!("protected marker exposed={exposed}")
}

fn read_connection_mv_store(world: &World, actor: usize) -> String {
    let connection = world.actors[actor].connection.as_ref().unwrap();
    let allowed = world.authorized(actor);
    let exposed = connection_mv_store_contains(connection, b"protected value");

    assert!(
        allowed || !exposed,
        "Connection::mv_store exposed protected data without authenticated credentials"
    );
    if allowed && world.spec.mvcc {
        assert!(
            connection
                .mv_store()
                .as_ref()
                .is_some_and(|store| world.any_database().is_some_and(|db| db
                    .get_mv_store()
                    .as_ref()
                    .is_some_and(|shared| Arc::ptr_eq(store, shared)))),
            "an authenticated connection did not share the MVCC store"
        );
    }
    format!("protected marker exposed={exposed}")
}

fn read_statement_mv_store(world: &World, actor: usize) -> String {
    let connection = world.actors[actor].connection.as_ref().unwrap();
    let statement = match connection.prepare("SELECT 1") {
        Ok(statement) => statement,
        Err(error) => {
            check_credential_refusal(world, &error, "Statement::mv_store preparation");
            return format!("refused: {:?}", classify(&error));
        }
    };
    let allowed = world.authorized(actor);
    let exposed = statement_mv_store_contains(&statement, b"protected value");

    assert!(
        allowed || !exposed,
        "Statement::mv_store exposed protected data without authenticated credentials"
    );
    format!("protected marker exposed={exposed}")
}

fn read_connection_schema(world: &World, actor: usize) -> String {
    let connection = world.actors[actor].connection.as_ref().unwrap();
    let allowed = world.authorized(actor);
    let exposed = connection
        .current_schema()
        .has_table(turso_core::NamespaceId::ROOT, "secrets")
        || connection.with_schema(turso_core::MAIN_DB_ID, |schema| {
            schema.has_table(turso_core::NamespaceId::ROOT, "secrets")
        });

    assert!(
        allowed || !exposed,
        "Connection schema accessors exposed the protected table without authenticated credentials"
    );
    format!("protected schema exposed={exposed}")
}

fn read_connection_pager(world: &World, actor: usize) -> String {
    let connection = world.actors[actor].connection.as_ref().unwrap();
    read_pager(
        world,
        actor,
        &connection.get_pager(),
        "Connection::get_pager",
    )
}

fn read_statement_pager(world: &World, actor: usize) -> String {
    let connection = world.actors[actor].connection.as_ref().unwrap();
    let statement = match connection.prepare("SELECT 1") {
        Ok(statement) => statement,
        Err(error) => {
            check_credential_refusal(world, &error, "Statement::get_pager preparation");
            return format!("refused: {:?}", classify(&error));
        }
    };
    read_pager(world, actor, statement.get_pager(), "Statement::get_pager")
}

fn read_pager(world: &World, actor: usize, pager: &Arc<turso_core::Pager>, api: &str) -> String {
    let allowed = world.authorized(actor);
    let result = pager_contains(pager, world.io.as_ref(), b"protected value");

    match result {
        Ok(exposed) => {
            assert!(
                allowed || !exposed,
                "{api} exposed a decrypted protected page without authenticated credentials"
            );
            format!("protected marker exposed={exposed}")
        }
        Err(error) => {
            if allowed {
                assert!(
                    world.fault_injected && classify(&error) == Category::InjectedIo,
                    "{api} failed for an authenticated connection: {error}"
                );
            } else {
                check_credential_refusal(world, &error, api);
            }
            format!("refused: {:?}", classify(&error))
        }
    }
}

fn pager_contains(
    pager: &Arc<turso_core::Pager>,
    io: &QueuedIo,
    expected: &[u8],
) -> turso_core::Result<bool> {
    loop {
        match pager.read_page(2).map_err(|error| *error)? {
            IOResult::Done((page, completion)) => {
                if let Some(completion) = completion {
                    io.wait_for_completion(completion)?;
                }
                let bytes = page.get_contents().as_ptr();
                return Ok(bytes
                    .windows(expected.len())
                    .any(|window| window == expected));
            }
            IOResult::IO(pending) => pending.wait(io)?,
        }
    }
}

fn restore_is_idle(world: &World) -> bool {
    world.io.pending_events().is_empty() && world.actors.iter().all(|actor| actor.active.is_none())
}

fn begin_wal_insert(world: &World, actor: usize) -> String {
    if !restore_is_idle(world) {
        return "skipped: concurrent database activity".to_string();
    }
    assert!(world.authorized(actor), "raw WAL caller is unauthenticated");
    let connection = world.actors[actor].connection.as_ref().unwrap();

    match connection
        .wal_insert_begin()
        .and_then(|()| connection.wal_insert_end(false))
    {
        Ok(()) => "authenticated WAL session completed".to_string(),
        Err(error) => {
            assert!(
                matches!(
                    classify(&error),
                    Category::Contention | Category::InjectedIo
                ),
                "unexpected WAL import failure: {error}"
            );
            format!("refused: {:?}", classify(&error))
        }
    }
}

fn publish_schema_after_restore(world: &World, actor: usize) -> String {
    if !restore_is_idle(world) {
        return "skipped: concurrent database activity".to_string();
    }
    assert!(world.authorized(actor), "schema caller is unauthenticated");
    let connection = world.actors[actor].connection.as_ref().unwrap();

    match connection.publish_schema_after_external_restore() {
        Ok(()) => {
            check_owner_after_restore(world, "publish_schema_after_external_restore");
            "schema published".to_string()
        }
        Err(error) => {
            assert!(
                matches!(
                    classify(&error),
                    Category::Contention | Category::InjectedIo
                ),
                "unexpected schema publication failure: {error}"
            );
            check_owner_after_restore(world, "publish_schema_after_external_restore");
            format!("refused: {:?}", classify(&error))
        }
    }
}

fn reload_wal_after_restore(world: &World, actor: usize) -> String {
    if !restore_is_idle(world) {
        return "skipped: concurrent database activity".to_string();
    }
    assert!(world.authorized(actor), "reload caller is unauthenticated");
    let connection = world.actors[actor].connection.as_ref().unwrap();

    match connection.reload_wal_after_external_restore() {
        Ok(()) => {
            for peer in world
                .actors
                .iter()
                .filter_map(|actor| actor.connection.as_ref())
                .chain(world.owner.iter())
            {
                peer.force_reparse_schema().unwrap();
            }
            "authenticated WAL reloaded".to_string()
        }
        Err(error) => {
            assert!(
                matches!(
                    classify(&error),
                    Category::Contention | Category::InjectedIo
                ),
                "unexpected reload failure: {error}"
            );
            check_owner_after_restore(world, "reload_wal_after_external_restore");
            format!("refused: {:?}", classify(&error))
        }
    }
}

fn check_owner_after_restore(world: &World, api: &str) {
    let Some(owner) = &world.owner else {
        return;
    };
    let rows = first_column(owner, "SELECT value FROM secrets WHERE id = 0");
    assert!(
        matches!(&rows, Ok(rows) if rows == &["protected value"]),
        "{api} disturbed the authenticated owner: {rows:?}"
    );
}

fn database_mv_store_contains(database: &Database, expected: &[u8]) -> bool {
    let mv_store = database.get_mv_store();
    mv_store.as_ref().is_some_and(|mv_store| {
        mv_store.rows.iter().any(|entry| {
            entry.value().read().iter().any(|version| {
                version.data.as_ref().is_some_and(|data| {
                    data.windows(expected.len())
                        .any(|window| window == expected)
                })
            })
        })
    })
}

fn connection_mv_store_contains(connection: &Connection, expected: &[u8]) -> bool {
    let mv_store = connection.mv_store();
    mv_store.as_ref().is_some_and(|mv_store| {
        mv_store.rows.iter().any(|entry| {
            entry.value().read().iter().any(|version| {
                version.data.as_ref().is_some_and(|data| {
                    data.windows(expected.len())
                        .any(|window| window == expected)
                })
            })
        })
    })
}

fn statement_mv_store_contains(statement: &Statement, expected: &[u8]) -> bool {
    let mv_store = statement.mv_store();
    mv_store.as_ref().is_some_and(|mv_store| {
        mv_store.rows.iter().any(|entry| {
            entry.value().read().iter().any(|version| {
                version.data.as_ref().is_some_and(|data| {
                    data.windows(expected.len())
                        .any(|window| window == expected)
                })
            })
        })
    })
}

fn check_credential_refusal(world: &World, error: &LimboError, context: &str) {
    let category = classify(error);
    assert!(
        matches!(
            category,
            Category::CredentialMissing | Category::CredentialWrong | Category::CipherMismatch
        ) || (category == Category::InjectedIo && world.fault_injected),
        "{context} failed with {error}"
    );
}

fn attach_command(world: &mut World, actor: usize, cred: AttachCred) -> String {
    let connection = world.actors[actor].connection.clone().unwrap();
    let primary = world.authorized(actor);
    let (cipher, key) = world.bound();
    let target_key = if key == 1 { 2 } else { 1 };
    let uri = match cred {
        AttachCred::Missing => world.target_path.clone(),
        AttachCred::PrimaryKey => format!(
            "file:{}?cipher={cipher}&hexkey={}",
            world.target_path,
            encryption_options(cipher, key).hexkey
        ),
        AttachCred::TargetKey => format!(
            "file:{}?cipher={cipher}&hexkey={}",
            world.target_path,
            encryption_options(cipher, target_key).hexkey
        ),
        AttachCred::WrongCipher => format!(
            "file:{}?cipher={}&hexkey={}",
            world.target_path,
            world.wrong_cipher(),
            encryption_options(world.wrong_cipher(), target_key).hexkey
        ),
    };
    let allowed = primary && cred == AttachCred::TargetKey;

    let result = connection.execute(format!("ATTACH '{uri}' AS aux"));
    match result {
        Ok(()) => {
            assert!(
                allowed,
                "ATTACH with {cred:?} succeeded (primary authorized: {primary})"
            );
            let rows: Vec<(String,)> = connection.exec_rows("SELECT value FROM aux.secrets");
            assert!(
                rows == [("attached value".to_string(),)],
                "attached database returned {rows:?}"
            );
            connection.execute("DETACH aux").unwrap();
            "attached and detached".to_string()
        }
        Err(error) => {
            let category = classify(&error);
            if allowed {
                assert!(
                    category == Category::InjectedIo && world.fault_injected,
                    "ATTACH with the target key failed: {error}"
                );
            } else {
                check_credential_refusal(world, &error, &format!("ATTACH with {cred:?}"));
            }
            if primary {
                let aliases: Vec<(i64, String, String)> =
                    connection.exec_rows("PRAGMA database_list");
                assert!(
                    !aliases.iter().any(|alias| alias.1 == "aux"),
                    "a refused ATTACH left the alias registered"
                );
                let rows: Vec<(String,)> =
                    connection.exec_rows("SELECT value FROM secrets WHERE id = 0");
                assert!(
                    rows == [("protected value".to_string(),)],
                    "the main database became unusable after a refused ATTACH"
                );
            }
            format!("refused: {category:?}")
        }
    }
}

fn late_setting(world: &World, actor: usize, setting: Setting) -> String {
    let connection = world.actors[actor].connection.as_ref().unwrap();
    let before = connection.get_encryption_cipher_mode();
    let actual = connection.execute(setting_sql(world, setting));
    check_setting_refused(&actual, setting);
    assert!(
        connection.get_encryption_cipher_mode() == before,
        "a refused setting changed the connection cipher"
    );

    format!("refused: {:?}", Category::SettingsRejected)
}

/// Final checks: invalid handles stay refused, sharing works for valid ones,
/// and after every owner is gone a cold reopen sees exactly the permitted rows.
fn postconditions(trace: &mut Trace, mut world: World) {
    world.io.set_owner("post");

    world.io.drain_all().unwrap();
    for actor in &mut world.actors {
        actor.active = None;
    }
    world.io.drain_all().unwrap();

    let binding = world.binding;
    for (index, actor) in world.actors.iter().enumerate() {
        let Some(connection) = &actor.connection else {
            continue;
        };
        if actor.authenticated {
            let rows = first_column(connection, "SELECT value FROM secrets WHERE id = 0");
            assert!(
                matches!(&rows, Ok(rows) if rows == &["protected value"]),
                "a{index} could not read the marker at the end: {rows:?}"
            );
            if world.spec.mvcc {
                assert!(
                    connection.mv_store().as_ref().is_some_and(|store| world
                        .any_database()
                        .is_some_and(|db| db
                            .get_mv_store()
                            .as_ref()
                            .is_some_and(|shared| Arc::ptr_eq(store, shared)))),
                    "a{index} did not share the MVCC store"
                );
            }
        } else {
            for sql in [
                "SELECT value FROM secrets WHERE id = 0",
                "SELECT name FROM sqlite_schema",
                "PRAGMA table_info(secrets)",
            ] {
                match connection.execute(sql) {
                    Ok(()) => panic!("a{index} without the established key ran {sql}"),
                    Err(error) => check_credential_refusal(
                        &world,
                        &error,
                        &format!("a{index} without the established key ran {sql}"),
                    ),
                }
            }
        }
    }
    let authorized: Vec<Arc<Connection>> = world
        .actors
        .iter()
        .filter(|actor| actor.authenticated)
        .filter_map(|actor| actor.connection.clone())
        .collect();
    let check_connection = world.owner.clone().or_else(|| authorized.first().cloned());
    // With every statement gone, nothing may still hold a read lock.
    if let (Some(owner), false) = (check_connection.clone(), world.spec.mvcc) {
        let busy = first_column(&owner, "PRAGMA wal_checkpoint(TRUNCATE)")
            .map(|rows| rows.first().is_some_and(|busy| busy != "0"))
            .unwrap_or(true);

        assert!(
            !busy,
            "a TRUNCATE checkpoint was busy after every statement finished"
        );
    }
    // Valid connections still share the parsed schema.
    if let (Some(owner), Some(peer)) = (world.owner.clone(), authorized.first()) {
        owner.execute("CREATE TABLE shared_check(x)").unwrap();
        let rows: Vec<(String,)> =
            peer.exec_rows("SELECT name FROM sqlite_schema WHERE name = 'shared_check'");
        assert!(
            rows == [("shared_check".to_string(),)],
            "a valid connection did not see the owner's new table"
        );
    }
    drop(check_connection);
    drop(authorized);
    // Drop every owner of the main database and prove the instance is gone.
    let weak = world.any_database().map(|db| Arc::downgrade(&db));
    for actor in &mut world.actors {
        if let Some(connection) = actor.connection.take() {
            connection.close().unwrap();
        }
        actor.database = None;
    }
    if let Some(owner) = world.owner.take() {
        owner.close().unwrap();
    }
    world.database = None;
    world.io.drain_all().unwrap();
    if let Some(weak) = weak {
        assert_eq!(
            weak.strong_count(),
            0,
            "the main database is still referenced after closing all handles"
        );
    }
    let (options, key) = match binding {
        Binding::Bound(cipher, key) => (
            world
                .base
                .clone()
                .encryption(encryption_options(cipher, key)),
            Some(self::key(cipher, key)),
        ),
        _ => (world.base.clone(), None),
    };
    let reopened = Database::open(world.io.clone(), &world.path, options).unwrap();
    let recovery = reopened
        .connect(ConnectOpts::new(turso_core::SessionRole::Superuser).with_encryption_key(key))
        .unwrap();

    let rows: Vec<(i64, String)> = recovery.exec_rows("SELECT id, value FROM secrets ORDER BY id");

    let survivors: BTreeSet<i64> = rows.iter().map(|row| row.0).collect();
    for id in &world.acknowledged {
        assert!(
            survivors.contains(id),
            "acknowledged insert {id} is missing after reopening"
        );
    }
    for (id, value) in &rows {
        assert!(
            world.possible.contains(id),
            "row {id} was never inserted by an authorized connection"
        );
        assert!(value == "protected value", "row {id} has value {value}");
    }

    let integrity: Vec<(String,)> = recovery.exec_rows("PRAGMA integrity_check");
    assert!(
        integrity == [("ok".to_string(),)],
        "integrity check after reopening: {integrity:?}"
    );
    if let Binding::Bound(cipher, bound_key) = binding {
        let other = if bound_key == 1 { 2 } else { 1 };
        check_connection_refusal(
            &world,
            reopened.connect(ConnectOpts::new(turso_core::SessionRole::Superuser)),
            "the reopened database accepted a keyless connection",
        );
        check_connection_refusal(
            &world,
            reopened.connect(
                ConnectOpts::new(turso_core::SessionRole::Superuser)
                    .with_encryption_key(Some(self::key(cipher, other))),
            ),
            "the reopened database accepted a different key",
        );
        check_database_open_refusal(
            &world,
            Database::open(world.io.clone(), &world.path, world.base.clone()),
            "the reopened database opened without credentials",
        );
    }
    trace.observe(format!(
        "recovered {} rows ({} acknowledged, {} possible)",
        rows.len(),
        world.acknowledged.len(),
        world.possible.len()
    ));
}

fn check_connection_refusal(
    world: &World,
    result: turso_core::Result<Arc<Connection>>,
    context: &str,
) {
    match result {
        Ok(_) => panic!("{context}"),
        Err(error) => check_credential_refusal(world, &error, context),
    }
}

fn check_database_open_refusal(
    world: &World,
    result: turso_core::Result<Arc<Database>>,
    context: &str,
) {
    match result {
        Ok(_) => panic!("{context}"),
        Err(error) => check_credential_refusal(world, &error, context),
    }
}

/// Runs a query and returns its first column as text.
pub(super) fn first_column(
    connection: &Arc<Connection>,
    sql: &str,
) -> turso_core::Result<Vec<String>> {
    let mut statement = connection.prepare(sql)?;
    let mut rows = Vec::new();
    statement.run_with_row_callback(|row| {
        rows.push(
            row.get_values()
                .next()
                .map(|value| value.to_string())
                .unwrap_or_default(),
        );
        Ok(())
    })?;
    Ok(rows)
}

#[test]
fn schedule_seed_reproduces_the_run() {
    let mut rng = ChaCha8Rng::seed_from_u64(11);
    let mut case = Case::generate(&mut rng, 11, 2);
    let Family::Lifecycle(spec) = &mut case.family else {
        unreachable!()
    };
    spec.scripts = vec![
        vec![
            Command::Connect(Cred::Bound),
            Command::Insert(1001),
            Command::Read,
        ],
        vec![Command::Connect(Cred::Bound), Command::Schema],
    ];
    let run = || {
        execute(
            &case,
            Box::new(RandomChooser {
                rng: ChaCha8Rng::seed_from_u64(case.seeds.schedule),
            }),
        )
    };
    let first = run();
    let second = run();
    assert_eq!(first.actions, second.actions);
    assert_eq!(first.observations, second.observations);
}
