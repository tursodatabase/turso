use crate::common::ExecRows;
use crate::queued_io::{QueuedIo, QueuedIoOpKind, QueuedIoRequestId};
use std::sync::Arc;
use turso_core::types::IOCompletions;
use turso_core::{
    Buffer, CipherMode, Completion, ConnectOpts, Connection, Database, DatabaseOpts, EncryptionKey,
    EncryptionOpts, IOResult, LimboError, MemoryIO, OpenDbAsyncState, OpenFlags, OpenOptions, IO,
};
use turso_sqlite::SqliteDialect;

#[path = "encryption_fuzz.rs"]
mod randomized;

fn manager_schedule(
    trace: &mut Trace,
    existing: bool,
    mvcc: bool,
    credential: Credential,
    cancel_waiter: bool,
    wait_for_bytes: bool,
) {
    let inner = Arc::new(MemoryIO::new());
    let io = Arc::new(QueuedIo::new_with_io(inner.clone()));
    let path_name = format!(":encrypted-manager-schedules:{}", trace.namespace);
    let path = path_name.as_str();
    io.set_owner("setup");
    let raw = inner.open_file(path, OpenFlags::Create, false).unwrap();
    let file = io.open_file(path, OpenFlags::Create, false).unwrap();
    let base = OpenOptions::new(Arc::new(SqliteDialect))
        .storage(Arc::new(turso_core::storage::database::DatabaseFile::new(
            file,
        )))
        .db_opts(DatabaseOpts::new().with_encryption(true).with_mvcc(mvcc));
    let encrypted = base
        .clone()
        .encryption(encryption_options(CipherMode::Aegis256, 1));
    if existing {
        let db = Database::open(io.clone(), path, encrypted.clone()).unwrap();
        let conn = db
            .connect(
                ConnectOpts::new(turso_core::SessionRole::Superuser)
                    .with_encryption_key(Some(key(CipherMode::Aegis256, 1))),
            )
            .unwrap();
        conn.execute("CREATE TABLE secrets(value TEXT)").unwrap();
        conn.execute("INSERT INTO secrets VALUES ('protected value')")
            .unwrap();
        conn.close().unwrap();
    }
    let mut owner = OpeningActor::new(encrypted.clone(), Some(key(CipherMode::Aegis256, 1)));
    io.set_owner("actor0");
    loop {
        let result = Database::open_async(
            owner.state.as_mut().unwrap(),
            io.clone(),
            path,
            &owner.options,
        )
        .unwrap();
        let IOResult::IO(pending) = result else {
            panic!("opening must yield before registry publication")
        };
        owner.pending = Some(IOCompletions(pending.0.clone()));
        if existing || !wait_for_bytes {
            break;
        }
        pending.wait(io.as_ref()).unwrap();
        if raw.size().unwrap() > 0 {
            break;
        }
    }
    let (options, reader_key) = match credential {
        Credential::Missing => (base.clone(), None),
        Credential::Wrong => (
            base.clone()
                .encryption(encryption_options(CipherMode::Aegis256, 2)),
            Some(key(CipherMode::Aegis256, 2)),
        ),
        Credential::WrongCipher => (
            base.clone()
                .encryption(encryption_options(CipherMode::Aes256Gcm, 2)),
            Some(key(CipherMode::Aes256Gcm, 2)),
        ),
        Credential::Correct => (encrypted.clone(), Some(key(CipherMode::Aegis256, 1))),
    };
    let mut peer = OpeningActor::new(options, reader_key);
    io.set_owner("actor1");
    trace.observe(format!(
        "creator paused, database bytes={}",
        raw.size().unwrap()
    ));
    let pending = Database::open_async(
        peer.state.as_mut().unwrap(),
        io.clone(),
        path,
        &peer.options,
    )
    .unwrap();
    let IOResult::IO(pending) = pending else {
        panic!("the competing open must wait for the registry owner");
    };
    assert!(
        pending.is_explicit_yield(),
        "a competing open must wait on the registry owner"
    );
    trace.observe("waiter yielded on the registry owner");
    peer.pending = Some(pending);
    peer.waiting_epoch = Some(0);
    if cancel_waiter {
        peer.state = None;
        peer.pending = None;
        peer.done = true;
        peer.dropped = true;
    }
    let mut actors = [owner, peer];
    let mut registry_epoch = 0;
    let mut injected_failure = false;
    // Whether the creator wrote database bytes or published the database
    // before it went away. Until then no key is established, and a waiting
    // opener may create the database with its own credentials.
    let mut creator_established = existing;
    let mut resumed_open_observed = false;
    loop {
        let pending = io.pending_events();
        if actors.iter().all(|actor| actor.done)
            && pending.is_empty()
            && !io.has_underlying_completions()
        {
            break;
        }
        // Interrupted fresh MVCC opens also fail for plaintext databases on main.
        // Allow cancellation in the registry window, then finish a resumed fresh open.
        let finishing_fresh_open = !existing
            && mvcc
            && actors
                .iter()
                .any(|actor| actor.state.is_some() && actor.open_resumed);
        if finishing_fresh_open && !resumed_open_observed {
            trace.observe("resumed fresh MVCC open completes without interruption");
            resumed_open_observed = true;
        }
        let mut enabled = Vec::new();
        for (index, actor) in actors.iter().enumerate() {
            if !actor.done
                && actor
                    .waiting_epoch
                    .is_none_or(|epoch| epoch != registry_epoch)
                && actor
                    .pending
                    .as_ref()
                    .is_none_or(|pending| pending.finished())
            {
                enabled.push(Action::Poll(index));
            }
            if !actor.dropped
                && (!finishing_fresh_open || actor.state.is_none())
                && actor.waiting_epoch != Some(registry_epoch)
                && (actor.state.is_some() || actor.db.is_some() || actor.connection.is_some())
            {
                enabled.push(Action::Drop(index));
            }
        }
        for operation in &pending {
            enabled.push(Action::Complete(operation.id.clone()));
            if trace.allow_failures
                && !finishing_fresh_open
                && !injected_failure
                && matches!(
                    operation.kind,
                    QueuedIoOpKind::Pread
                        | QueuedIoOpKind::Pwrite
                        | QueuedIoOpKind::Pwritev
                        | QueuedIoOpKind::Sync
                )
            {
                enabled.push(Action::Fail(operation.id.clone()));
            }
        }
        if io.has_underlying_completions() {
            enabled.push(Action::CompleteUnderlying);
        }
        assert!(!enabled.is_empty(), "registry opening deadlocked");
        let action = trace.choose(&enabled);
        if let Action::Poll(index) | Action::Drop(index) = action {
            io.set_owner(format!("actor{index}"));
        }
        match action {
            Action::Poll(index) => {
                let actor = &mut actors[index];
                if actor
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.get_error().is_some())
                {
                    if index == 0 {
                        creator_established |= raw.size().unwrap() > 0;
                    }
                    actor.state = None;
                    actor.pending = None;
                    actor.done = true;
                    registry_epoch += 1;
                    continue;
                }
                actor.pending = None;
                actor.waiting_epoch = None;
                if let Some(db) = &actor.db {
                    let result = db.connect(
                        ConnectOpts::new(turso_core::SessionRole::Superuser)
                            .with_encryption_key(actor.key.clone()),
                    );
                    let peer_created = index == 1 && !creator_established;
                    if peer_created {
                        trace.observe("the waiter created the database after the creator left");
                    }
                    if index == 0 || credential == Credential::Correct || peer_created {
                        assert!(
                            result
                                .as_ref()
                                .err()
                                .is_none_or(|error| injected_failure && injected_io_error(error)),
                            "valid connection failed: {:?}",
                            result.as_ref().err()
                        );
                    }
                    actor.connection = result.ok();
                    if index == 1 && credential != Credential::Correct && !peer_created {
                        assert!(
                            actor.connection.is_none(),
                            "invalid credentials connected to an encrypted database"
                        );
                    }
                    actor.done = true;
                } else {
                    actor.open_resumed = true;
                    match Database::open_async(
                        actor.state.as_mut().unwrap(),
                        io.clone(),
                        path,
                        &actor.options,
                    ) {
                        Ok(IOResult::Done(db)) => {
                            if index == 0 {
                                creator_established = true;
                            }
                            actor.db = Some(db);
                            actor.state = None;
                            registry_epoch += 1;
                        }
                        Ok(IOResult::IO(pending)) => {
                            if pending.is_explicit_yield() {
                                actor.waiting_epoch = Some(registry_epoch);
                            }
                            actor.pending = Some(pending);
                        }
                        Err(error) => {
                            assert!(
                                index != 0 || (injected_failure && injected_io_error(&error)),
                                "creator failed without an injected error: {error}"
                            );
                            assert!(
                                (credential != Credential::Correct
                                    && (index == 0 || creator_established))
                                    || (injected_failure && injected_io_error(&error)),
                                "a peer that may open the database failed: {error}"
                            );
                            if index == 0 {
                                creator_established |= raw.size().unwrap() > 0;
                            }
                            actor.state = None;
                            actor.done = true;
                            registry_epoch += 1;
                        }
                    }
                }
            }
            Action::Drop(index) => {
                if index == 0 && actors[0].db.is_none() {
                    creator_established |= raw.size().unwrap() > 0;
                }
                actors[index].state = None;
                actors[index].connection = None;
                actors[index].db = None;
                actors[index].pending = None;
                actors[index].done = true;
                actors[index].dropped = true;
                registry_epoch += 1;
            }
            Action::Complete(id) => {
                io.step_request(&id, false).unwrap().unwrap();
            }
            Action::Fail(id) => {
                injected_failure = true;
                io.step_request(&id, true).unwrap().unwrap();
            }
            Action::CompleteUnderlying => io.step_underlying().unwrap(),
            Action::Start(_) => unreachable!(),
        }
        io.set_owner("scheduler");
    }
    io.set_owner("recovery");
    // The surviving first page decides the database's key: normally the
    // creator's, but the waiter's when it created the database after the
    // creator left without establishing anything.
    let peer_owns_file = credential != Credential::Correct
        && io
            .records()
            .iter()
            .rev()
            .find(|record| {
                record.event.path == path
                    && record.event.offset == 0
                    && record.succeeded()
                    && matches!(
                        record.event.kind,
                        QueuedIoOpKind::Pwrite | QueuedIoOpKind::Pwritev
                    )
            })
            .is_some_and(|record| record.event.id.owner == "actor1");
    let peer_options = actors[1].options.clone();
    let peer_key = actors[1].key.clone();
    let (recovery_options, recovery_key) = if peer_owns_file {
        assert!(
            !existing,
            "an invalid credential rewrote page one of an existing database"
        );
        trace.observe("the waiter's credentials own the database");
        (peer_options, peer_key)
    } else {
        (encrypted.clone(), Some(key(CipherMode::Aegis256, 1)))
    };
    let opened = Database::open(io.clone(), path, recovery_options);
    assert!(
        opened.is_ok(),
        "the credentials of the surviving first page cannot open it: {:?}",
        opened.as_ref().err()
    );
    let db = opened.unwrap();
    let connected = db.connect(
        ConnectOpts::new(turso_core::SessionRole::Superuser).with_encryption_key(recovery_key),
    );
    assert!(
        connected.is_ok(),
        "the credentials of the surviving first page cannot connect: {:?}",
        connected.as_ref().err()
    );
    let conn = connected.unwrap();
    conn.execute("CREATE TABLE IF NOT EXISTS secrets(value TEXT)")
        .unwrap();
    if !existing {
        conn.execute("INSERT INTO secrets VALUES ('protected value')")
            .unwrap();
    }
    let rows: Vec<(String,)> = conn.exec_rows("SELECT value FROM secrets");
    assert!(
        rows == [("protected value".to_string(),)],
        "recovery read {rows:?}"
    );
    let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
    assert!(
        integrity == [("ok".to_string(),)],
        "the recovered database failed integrity_check: {integrity:?}"
    );
    if peer_owns_file {
        trace.observe("the replacement waiter wrote and read the marker");
        assert!(
            db.connect(
                ConnectOpts::new(turso_core::SessionRole::Superuser)
                    .with_encryption_key(Some(key(CipherMode::Aegis256, 1)))
            )
            .is_err(),
            "the creator's abandoned key still opens a database the waiter created"
        );

        return;
    }
    for actor in actors {
        if let Some(connection) = actor.connection {
            let rows: Vec<(String,)> = connection.exec_rows("SELECT value FROM secrets");
            assert!(
                rows == [("protected value".to_string(),)],
                "a valid connection read {rows:?}"
            );
        }
    }
    assert!(
        Database::open(io.clone(), path, base).is_err(),
        "a cached encrypted database opened without credentials"
    );
}

struct OpeningActor {
    options: OpenOptions,
    key: Option<EncryptionKey>,
    state: Option<OpenDbAsyncState>,
    pending: Option<IOCompletions>,
    waiting_epoch: Option<usize>,
    db: Option<Arc<Database>>,
    connection: Option<Arc<Connection>>,
    open_resumed: bool,
    done: bool,
    dropped: bool,
}

impl OpeningActor {
    fn new(options: OpenOptions, key: Option<EncryptionKey>) -> Self {
        Self {
            options,
            key,
            state: Some(OpenDbAsyncState::new()),
            pending: None,
            waiting_epoch: None,
            db: None,
            connection: None,
            open_resumed: false,
            done: false,
            dropped: false,
        }
    }
}

fn encryption_options(cipher: CipherMode, byte: u8) -> EncryptionOpts {
    EncryptionOpts {
        cipher: cipher.to_string(),
        hexkey: format!("{byte:02x}").repeat(cipher.required_key_size()),
    }
}

fn page_one_schedule(
    trace: &mut Trace,
    cipher: CipherMode,
    page_size: u32,
    credential: Credential,
) {
    let inner = Arc::new(MemoryIO::new());
    let io = Arc::new(QueuedIo::new_with_io(inner.clone()));
    let path = ":memory:exhaustive-encrypted-page-one";
    io.set_owner("setup");
    let db = Database::open(
        io.clone(),
        path,
        OpenOptions::new(Arc::new(SqliteDialect))
            .db_opts(DatabaseOpts::new().with_encryption(true))
            .encryption(encryption_options(cipher, 1))
            .page_size(turso_core::storage::sqlite3_ondisk::PageSize::new(page_size).unwrap()),
    )
    .unwrap();
    let writer = db
        .connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser)
                .with_encryption_key(Some(key(cipher, 1))),
        )
        .unwrap();
    let reader_opts = ConnectOpts::new(turso_core::SessionRole::Superuser).with_encryption_key(
        match credential {
            Credential::Missing => None,
            Credential::Wrong | Credential::WrongCipher => Some(key(cipher, 2)),
            Credential::Correct => Some(key(cipher, 1)),
        },
    );
    let mut actors = [Actor::new(writer), Actor::connecting(reader_opts)];
    let mut injected_failure = false;
    let mut allocation_owner = None;
    trace.observe(
        "initial page-one writes are serialized without write/sync faults or owner cancellation",
    );
    loop {
        let pending = io.pending_events();
        if actors.iter().all(|actor| actor.done)
            && pending.is_empty()
            && !io.has_underlying_completions()
        {
            break;
        }
        let mut enabled = Vec::new();
        for (index, actor) in actors.iter().enumerate() {
            if actor.done {
                continue;
            }
            if actor
                .pending
                .as_ref()
                .is_none_or(|pending| pending.finished())
                && (actor.connection.is_none()
                    || allocation_owner.is_none_or(|owner| owner == index))
            {
                enabled.push(Action::Poll(index));
            }
            if allocation_owner != Some(index) {
                enabled.push(Action::Drop(index));
            }
        }
        for operation in &pending {
            enabled.push(Action::Complete(operation.id.clone()));
            if trace.allow_failures && !injected_failure && operation.kind == QueuedIoOpKind::Pread
            {
                enabled.push(Action::Fail(operation.id.clone()));
            }
        }
        if io.has_underlying_completions() {
            enabled.push(Action::CompleteUnderlying);
        }
        assert!(!enabled.is_empty(), "initialization deadlocked");
        let action = trace.choose(&enabled);
        if let Action::Poll(index) | Action::Drop(index) = action {
            io.set_owner(format!("actor{index}"));
            let other = &actors[1 - index];
            if matches!(action, Action::Poll(_))
                && other
                    .pending
                    .as_ref()
                    .is_some_and(|pending| !pending.finished())
            {
                trace.observe(format!(
                    "actor{index} polled while the other actor waited on I/O"
                ));
            }
        }
        match action {
            Action::Poll(index) => {
                let actor = &mut actors[index];
                let allowed = index == 0 || credential == Credential::Correct;
                actor.pending = None;
                if actor.connection.is_none() {
                    match db.connect_async(
                        actor.connect_opts.as_ref().unwrap().clone(),
                        &mut actor.connect_state,
                    ) {
                        Ok(IOResult::Done(connection)) => {
                            assert!(
                                allowed,
                                "an invalid key returned a connection during initialization"
                            );
                            actor.connection = Some(connection);
                        }
                        Ok(IOResult::IO(pending)) => {
                            actor.pending = Some(pending);
                            continue;
                        }
                        Err(error) => {
                            assert!(
                                !allowed || (injected_failure && injected_io_error(&error)),
                                "connection result: {error}"
                            );
                            actor.done = true;
                            continue;
                        }
                    }
                }
                if allocation_owner.is_some_and(|owner| owner != index) {
                    trace.observe(format!(
                        "actor{index} authenticated while initial allocation continued"
                    ));
                    continue;
                }
                allocation_owner = Some(index);
                match actor
                    .connection
                    .as_ref()
                    .unwrap()
                    .get_pager()
                    .maybe_allocate_page1()
                {
                    Ok(IOResult::Done(())) => {
                        assert!(allowed, "an invalid key completed initialization");
                        allocation_owner = None;
                        actor.done = true;
                    }
                    Ok(IOResult::IO(pending)) => {
                        assert!(!pending.is_explicit_yield(), "unexpected lock wait");
                        actor.pending = Some(pending);
                    }
                    Err(error) => {
                        assert!(
                            !allowed || (injected_failure && injected_io_error(&error)),
                            "valid key rejected: {error}"
                        );
                        allocation_owner = None;
                        actor.done = true;
                    }
                }
            }
            Action::Drop(index) => {
                actors[index].connect_state = turso_core::ConnectAsyncState::new();
                actors[index].connection = None;
                actors[index].pending = None;
                actors[index].done = true;
            }
            Action::Complete(id) => {
                io.step_request(&id, false).unwrap().unwrap();
            }
            Action::Fail(id) => {
                injected_failure = true;
                io.step_request(&id, true).unwrap().unwrap();
            }
            Action::CompleteUnderlying => io.step_underlying().unwrap(),
            Action::Start(_) => unreachable!(),
        }
        io.set_owner("scheduler");

        let raw = inner.open_file(path, OpenFlags::None, false).unwrap();
        if raw.size().unwrap() > 0 {
            let buffer = Arc::new(Buffer::new_temporary(100));
            let completion = Completion::new_read(buffer.clone(), Box::new(|_| None));
            let completion = raw.pread(0, completion).unwrap();
            assert!(completion.succeeded());
            assert!(
                &buffer.as_slice()[..5] == b"Turso" && buffer.as_slice()[6] == cipher.cipher_id(),
                "page one header is not encrypted with the established cipher"
            );
        }
    }

    if !injected_failure {
        let allocations = io
            .records()
            .iter()
            .filter(|record| {
                record.event.path == path
                    && record.event.kind == QueuedIoOpKind::Pwrite
                    && record.event.offset == 0
                    && record.succeeded()
            })
            .count();
        trace.observe(format!("initial page-one writes: {allocations}"));
        assert!(
            allocations <= 1,
            "page one was allocated {allocations} times"
        );
        if actors.iter().any(|actor| {
            actor.done
                && actor.connection.is_some()
                && (credential == Credential::Correct || std::ptr::eq(actor, &actors[0]))
        }) {
            assert!(
                allocations == 1,
                "successful initialization did not allocate page one"
            );
        }
    }
    io.set_owner("recovery");
    let recovery = db
        .connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser)
                .with_encryption_key(Some(key(cipher, 1))),
        )
        .unwrap();
    let result = recovery.execute("CREATE TABLE secrets(value TEXT)");
    if let Err(error) = result {
        assert!(
            injected_failure && injected_io_error(&error),
            "recovery failed: {error}"
        );
        recovery
            .execute("CREATE TABLE secrets(value TEXT)")
            .unwrap();
    }
    recovery
        .execute("INSERT INTO secrets VALUES ('protected value')")
        .unwrap();
    for (index, actor) in actors.iter().enumerate() {
        if let Some(connection) = &actor.connection {
            if index == 0 || credential == Credential::Correct {
                let rows: Vec<(String,)> = connection.exec_rows("SELECT value FROM secrets");
                assert!(
                    rows == [("protected value".to_string(),)],
                    "a valid connection read {rows:?}"
                );
            } else {
                assert!(
                    connection.execute("SELECT value FROM secrets").is_err(),
                    "an invalid key read protected rows"
                );
                assert!(
                    connection
                        .execute("SELECT name FROM sqlite_schema")
                        .is_err(),
                    "an invalid key read protected schema"
                );
            }
        }
    }
    assert!(
        db.connect(ConnectOpts::new(turso_core::SessionRole::Superuser))
            .is_err(),
        "a keyless connection opened an encrypted database"
    );
    assert!(
        db.connect(
            ConnectOpts::new(turso_core::SessionRole::Superuser)
                .with_encryption_key(Some(key(cipher, 2)))
        )
        .is_err(),
        "a wrong key opened an encrypted database"
    );
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Credential {
    Missing,
    Wrong,
    WrongCipher,
    Correct,
}

struct Actor {
    connect_opts: Option<ConnectOpts>,
    connect_state: turso_core::ConnectAsyncState,
    connection: Option<Arc<Connection>>,
    pending: Option<IOCompletions>,
    done: bool,
}

impl Actor {
    fn connecting(opts: ConnectOpts) -> Self {
        Self {
            connection: None,
            connect_opts: Some(opts),
            connect_state: turso_core::ConnectAsyncState::new(),
            pending: None,
            done: false,
        }
    }

    fn new(connection: Arc<Connection>) -> Self {
        Self {
            connect_opts: None,
            connect_state: turso_core::ConnectAsyncState::new(),
            connection: Some(connection),
            pending: None,
            done: false,
        }
    }
}

/// One scheduling decision. Requests are named by their logical identity, so
/// a recorded decision never refers to whatever request now sits at a queue
/// position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Action {
    /// Resume the actor's current operation.
    Poll(usize),
    /// Let a queued request run.
    Complete(QueuedIoRequestId),
    /// Make a queued request fail with an injected I/O error.
    Fail(QueuedIoRequestId),
    /// Let the wrapped asynchronous backend deliver dispatched completions.
    CompleteUnderlying,
    /// Drop the actor's current operation or handles.
    Drop(usize),
    /// Start the actor's next scripted command.
    Start(usize),
}

pub(super) struct Trace {
    pub(super) actions: Vec<Action>,
    pub(super) allow_failures: bool,
    pub(super) namespace: String,
    chooser: Box<dyn Chooser>,
    pub(super) observations: Vec<String>,
    /// Decisions during which new work, failures and drops may be chosen.
    /// After that only pending work is finished.
    pub(super) generated: usize,
}

pub(super) trait Chooser {
    fn choose(&mut self, enabled: &[Action], decided: &[Action]) -> Action;
}

impl Trace {
    pub(super) fn new(
        namespace: impl Into<String>,
        allow_failures: bool,
        chooser: Box<dyn Chooser>,
    ) -> Self {
        Self {
            actions: Vec::new(),
            allow_failures,
            namespace: namespace.into(),
            chooser,
            observations: Vec::new(),
            generated: usize::MAX,
        }
    }

    pub(super) fn choose(&mut self, enabled: &[Action]) -> Action {
        let action = self.chooser.choose(enabled, &self.actions);
        assert!(
            enabled.contains(&action),
            "action {action:?} is not enabled"
        );
        eprintln!("action {}: {action:?}", self.actions.len());
        self.actions.push(action.clone());
        action
    }

    pub(super) fn observe(&mut self, observation: impl Into<String>) {
        let observation = observation.into();
        eprintln!("{observation}");
        self.observations.push(observation);
    }
}

fn injected_io_error(error: &LimboError) -> bool {
    matches!(
        error,
        LimboError::CompletionError(turso_core::CompletionError::IOError(_, "queued_io"))
    )
}

fn key(cipher: CipherMode, byte: u8) -> EncryptionKey {
    match cipher.required_key_size() {
        16 => EncryptionKey::Key128([byte; 16]),
        32 => EncryptionKey::new_256([byte; 32]),
        _ => unreachable!(),
    }
}
