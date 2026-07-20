use std::path::PathBuf;
use std::sync::Arc;

use tempfile::TempDir;
use turso_core::partition::{
    DefaultPathResolver, PartitionConfig, PartitionPathResolver, VideoAnalyticsPathResolver,
};
use turso_core::{Connection, Database, PlatformIO, SqliteDialect, Value};

const DAY_MICROS: i64 = 86_400_000_000;
const FIRST_DAY: i64 = 1_735_689_600_000_000;
const EVENTS_SCHEMA: &str = "CREATE TABLE events(\
    id INTEGER PRIMARY KEY,\
    ts INTEGER NOT NULL,\
    camera TEXT NOT NULL,\
    event_type TEXT NOT NULL,\
    confidence REAL NOT NULL\
)";
const EVENTS_PARTITION_SCHEMA: &str = "CREATE TABLE events(\
    id INTEGER PRIMARY KEY,\
    ts INTEGER NOT NULL,\
    camera TEXT NOT NULL,\
    event_type TEXT NOT NULL,\
    confidence REAL NOT NULL\
);\
CREATE INDEX idx_events_ts ON events(ts);\
CREATE INDEX idx_events_type_ts ON events(event_type, ts)";

struct RecorderDatabase {
    _directory: TempDir,
    database: Arc<Database>,
    connection: Arc<Connection>,
    database_path: PathBuf,
    partitions_directory: PathBuf,
}

fn register_events(
    connection: &Arc<Connection>,
    partitions_directory: PathBuf,
) -> anyhow::Result<()> {
    connection.register_partitioned_table(
        "events",
        PartitionConfig::new(
            Box::new(DefaultPathResolver::daily(partitions_directory)),
            EVENTS_PARTITION_SCHEMA.to_string(),
            "ts".to_string(),
        ),
    )?;
    Ok(())
}

impl RecorderDatabase {
    fn new() -> anyhow::Result<Self> {
        let directory = tempfile::tempdir()?;
        let database_path = directory.path().join("recorder.db");
        let partitions_directory = directory.path().join("events");
        let io = Arc::new(PlatformIO::new()?);
        let database = Database::open_file(
            io,
            database_path.to_string_lossy().as_ref(),
            Arc::new(SqliteDialect),
        )?;
        let connection = database.connect()?;

        connection.execute(format!("{EVENTS_SCHEMA} PARTITION BY (ts)"))?;
        register_events(&connection, partitions_directory.clone())?;

        Ok(Self {
            _directory: directory,
            database,
            connection,
            database_path,
            partitions_directory,
        })
    }

    fn insert_event(
        &self,
        id: i64,
        ts: i64,
        camera: &str,
        event_type: &str,
        confidence: f64,
    ) -> anyhow::Result<()> {
        self.connection.execute(format!(
            "INSERT INTO events VALUES ({id}, {ts}, '{camera}', '{event_type}', {confidence})"
        ))?;
        Ok(())
    }

    fn partition_path(&self, timestamp: i64) -> PathBuf {
        DefaultPathResolver::daily(self.partitions_directory.clone())
            .resolve_path("events", timestamp)
    }

    fn create_external_partition(
        &self,
        id: i64,
        timestamp: i64,
        event_type: &str,
    ) -> anyhow::Result<PathBuf> {
        let path = self.partition_path(timestamp);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let io = Arc::new(PlatformIO::new()?);
        let database =
            Database::open_file(io, path.to_string_lossy().as_ref(), Arc::new(SqliteDialect))?;
        let connection = database.connect()?;
        connection.execute(EVENTS_PARTITION_SCHEMA)?;
        connection.execute(format!(
            "INSERT INTO events VALUES ({id}, {timestamp}, 'external', '{event_type}', 1.0)"
        ))?;
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
        connection.close()?;
        Ok(path)
    }

    fn connect_and_register(&self) -> anyhow::Result<Arc<Connection>> {
        let connection = self.database.connect()?;
        register_events(&connection, self.partitions_directory.clone())?;
        Ok(connection)
    }
}

fn query_rows(connection: &Arc<Connection>, sql: &str) -> anyhow::Result<Vec<String>> {
    let mut statement = connection.prepare(sql)?;
    let mut rows = Vec::new();
    statement.run_with_row_callback(|row| {
        rows.push(
            row.get_values()
                .map(|value| format!("{value}"))
                .collect::<Vec<_>>()
                .join("|"),
        );
        Ok(())
    })?;
    Ok(rows)
}

#[test]
fn recorder_queries_are_transparent_across_daily_partitions() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    recorder.insert_event(1, FIRST_DAY + 1_000, "gate-a", "motion", 0.7)?;
    recorder.insert_event(2, FIRST_DAY + DAY_MICROS + 2_000, "gate-b", "plate", 0.95)?;
    recorder.insert_event(3, FIRST_DAY + DAY_MICROS + 3_000, "gate-a", "motion", 0.8)?;
    recorder.insert_event(
        4,
        FIRST_DAY + 2 * DAY_MICROS + 4_000,
        "gate-c",
        "line-crossing",
        0.9,
    )?;

    assert_eq!(recorder.connection.list_partitions("events")?.len(), 3);
    assert_eq!(
        query_rows(
            &recorder.connection,
            "SELECT id, camera, event_type FROM events ORDER BY ts"
        )?,
        vec![
            "1|gate-a|motion",
            "2|gate-b|plate",
            "3|gate-a|motion",
            "4|gate-c|line-crossing",
        ]
    );
    assert_eq!(
        query_rows(
            &recorder.connection,
            "SELECT event_type, count(*) FROM events GROUP BY event_type ORDER BY event_type"
        )?,
        vec!["line-crossing|1", "motion|2", "plate|1"]
    );
    assert_eq!(
        query_rows(
            &recorder.connection,
            &format!(
                "SELECT id FROM events WHERE ts >= {} AND ts < {} ORDER BY id",
                FIRST_DAY + DAY_MICROS,
                FIRST_DAY + 2 * DAY_MICROS
            )
        )?,
        vec!["2", "3"]
    );
    assert!(recorder.partitions_directory.exists());

    Ok(())
}

#[test]
fn recorder_discovers_new_files_before_reusing_a_prepared_statement() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    recorder.insert_event(1, FIRST_DAY + 1_000, "gate-a", "motion", 0.7)?;

    let mut statement = recorder
        .connection
        .prepare("SELECT id, event_type FROM events ORDER BY id")?;
    recorder.create_external_partition(2, FIRST_DAY + DAY_MICROS + 2_000, "archive-import")?;

    let mut rows = Vec::new();
    statement.run_with_row_callback(|row| {
        rows.push(
            row.get_values()
                .map(|value| format!("{value}"))
                .collect::<Vec<_>>()
                .join("|"),
        );
        Ok(())
    })?;
    assert_eq!(rows, vec!["1|motion", "2|archive-import"]);
    assert_eq!(recorder.connection.list_partitions("events")?.len(), 2);

    Ok(())
}

#[cfg(unix)]
#[test]
fn recorder_excludes_a_partition_removed_between_statements() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    recorder.insert_event(1, FIRST_DAY + 1_000, "gate-a", "motion", 0.7)?;
    recorder.insert_event(2, FIRST_DAY + DAY_MICROS + 2_000, "gate-b", "plate", 0.95)?;
    recorder.insert_event(
        3,
        FIRST_DAY + 2 * DAY_MICROS + 3_000,
        "gate-c",
        "line-crossing",
        0.9,
    )?;

    let removed_path = recorder.partition_path(FIRST_DAY + DAY_MICROS);
    let mut statement = recorder
        .connection
        .prepare("SELECT id FROM events ORDER BY id")?;
    std::fs::remove_file(&removed_path)?;

    let mut rows = Vec::new();
    statement.run_with_row_callback(|row| {
        rows.push(format!("{}", row.get_value(0)));
        Ok(())
    })?;
    assert_eq!(rows, vec!["1", "3"]);
    assert_eq!(recorder.connection.list_partitions("events")?.len(), 2);

    recorder.insert_event(
        4,
        FIRST_DAY + DAY_MICROS + 4_000,
        "gate-d",
        "replacement",
        0.6,
    )?;
    assert_eq!(
        query_rows(&recorder.connection, "SELECT id FROM events ORDER BY id")?,
        vec!["1", "3", "4"]
    );

    Ok(())
}

#[cfg(unix)]
#[test]
fn recorder_reopens_a_partition_replaced_at_the_same_path() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    recorder.insert_event(1, FIRST_DAY + 1_000, "gate-a", "old", 0.7)?;

    let mut statement = recorder
        .connection
        .prepare("SELECT id, event_type FROM events ORDER BY id")?;
    let replacement_path = recorder.partitions_directory.join(".replacement.db");
    let replacement_io = Arc::new(PlatformIO::new()?);
    let replacement_database = Database::open_file(
        replacement_io,
        replacement_path.to_string_lossy().as_ref(),
        Arc::new(SqliteDialect),
    )?;
    let replacement_connection = replacement_database.connect()?;
    replacement_connection.execute(EVENTS_PARTITION_SCHEMA)?;
    replacement_connection.execute(format!(
        "INSERT INTO events VALUES (2, {}, 'gate-b', 'replacement', 0.9)",
        FIRST_DAY + 2_000
    ))?;
    replacement_connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    replacement_connection.close()?;
    drop(replacement_connection);
    drop(replacement_database);

    std::fs::rename(&replacement_path, recorder.partition_path(FIRST_DAY))?;

    let mut rows = Vec::new();
    statement.run_with_row_callback(|row| {
        rows.push(format!("{}|{}", row.get_value(0), row.get_value(1)));
        Ok(())
    })?;
    assert_eq!(rows, vec!["2|replacement"]);
    assert_eq!(recorder.connection.list_partitions("events")?.len(), 1);

    Ok(())
}

#[test]
fn recorder_safe_delete_removes_only_the_selected_day() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    recorder.insert_event(1, FIRST_DAY + 1_000, "gate-a", "motion", 0.7)?;
    recorder.insert_event(2, FIRST_DAY + DAY_MICROS + 2_000, "gate-b", "plate", 0.95)?;

    let first_path = recorder.partition_path(FIRST_DAY);
    let coordination_path = PathBuf::from(format!("{}-tshm", first_path.to_string_lossy()));
    std::fs::write(&coordination_path, b"stale coordination state")?;
    recorder
        .connection
        .delete_partition("events", &first_path)?;

    assert!(!first_path.exists());
    assert!(!coordination_path.exists());
    assert_eq!(
        query_rows(&recorder.connection, "SELECT id FROM events ORDER BY id")?,
        vec!["2"]
    );
    assert_eq!(recorder.connection.list_partitions("events")?.len(), 1);

    Ok(())
}

#[test]
fn recorder_new_connection_recovers_existing_daily_files() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    recorder.insert_event(1, FIRST_DAY + 1_000, "gate-a", "motion", 0.7)?;
    recorder.insert_event(2, FIRST_DAY + DAY_MICROS + 2_000, "gate-b", "plate", 0.95)?;

    let reopened = recorder.connect_and_register()?;
    assert_eq!(reopened.list_partitions("events")?.len(), 2);
    assert_eq!(
        query_rows(&reopened, "SELECT id FROM events ORDER BY id")?,
        vec!["1", "2"]
    );

    Ok(())
}

#[test]
fn recorder_refuses_to_delete_a_partition_used_by_an_active_query() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    recorder.insert_event(1, FIRST_DAY + 1_000, "gate-a", "motion", 0.7)?;
    recorder.insert_event(2, FIRST_DAY + DAY_MICROS + 2_000, "gate-b", "plate", 0.95)?;

    let mut statement = recorder.connection.prepare("SELECT id FROM events")?;
    assert!(matches!(statement.step()?, turso_core::StepResult::Row));
    let first_path = recorder.partition_path(FIRST_DAY);
    let error = recorder
        .connection
        .delete_partition("events", &first_path)
        .expect_err("an active reader must pin its partition");
    assert!(error.to_string().contains("locked"));

    drop(statement);
    recorder
        .connection
        .delete_partition("events", &first_path)?;
    assert_eq!(
        query_rows(&recorder.connection, "SELECT id FROM events")?,
        vec!["2"]
    );

    Ok(())
}

#[test]
fn recorder_reuses_parameterized_insert_across_days() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    let mut statement = recorder.connection.prepare(
        "INSERT INTO events(id, ts, camera, event_type, confidence) VALUES (?, ?, ?, ?, ?)",
    )?;

    for (id, timestamp, event_type) in [
        (1, FIRST_DAY + 1_000, "motion"),
        (2, FIRST_DAY + DAY_MICROS + 2_000, "plate"),
    ] {
        statement.bind_at(1.try_into()?, Value::from_i64(id))?;
        statement.bind_at(2.try_into()?, Value::from_i64(timestamp))?;
        statement.bind_at(3.try_into()?, Value::build_text("gate-a"))?;
        statement.bind_at(4.try_into()?, Value::build_text(event_type.to_string()))?;
        statement.bind_at(5.try_into()?, Value::from_f64(0.9))?;
        statement.run_ignore_rows()?;
        statement.reset()?;
    }

    assert_eq!(
        query_rows(&recorder.connection, "SELECT id FROM events ORDER BY id")?,
        vec!["1", "2"]
    );
    assert_eq!(recorder.connection.list_partitions("events")?.len(), 2);

    Ok(())
}

#[test]
fn recorder_accepts_multi_row_insert_only_within_one_day() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    recorder.connection.execute(format!(
        "INSERT INTO events VALUES \
         (1, {}, 'gate-a', 'motion', 0.8), \
         (2, {}, 'gate-b', 'plate', 0.9)",
        FIRST_DAY + 1_000,
        FIRST_DAY + 2_000
    ))?;

    assert_eq!(
        query_rows(&recorder.connection, "SELECT id FROM events ORDER BY id")?,
        vec!["1", "2"]
    );
    assert_eq!(recorder.connection.list_partitions("events")?.len(), 1);

    let error = recorder
        .connection
        .execute(format!(
            "INSERT INTO events VALUES \
             (3, {}, 'gate-a', 'motion', 0.8), \
             (4, {}, 'gate-b', 'plate', 0.9)",
            FIRST_DAY + 3_000,
            FIRST_DAY + DAY_MICROS + 4_000
        ))
        .expect_err("a statement spanning files must be rejected before writing");
    assert!(error.to_string().contains("one INSERT statement"));
    assert_eq!(
        query_rows(&recorder.connection, "SELECT id FROM events ORDER BY id")?,
        vec!["1", "2"]
    );
    assert_eq!(recorder.connection.list_partitions("events")?.len(), 1);

    Ok(())
}

#[test]
fn recorder_rejects_cross_partition_explicit_transaction() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    recorder.connection.execute("BEGIN")?;
    recorder.insert_event(1, FIRST_DAY + 1_000, "gate-a", "motion", 0.8)?;

    let error = recorder
        .insert_event(2, FIRST_DAY + DAY_MICROS + 2_000, "gate-b", "plate", 0.9)
        .expect_err("one transaction must not commit two partition files");
    assert!(error.to_string().contains("cross-partition write"));
    recorder.connection.execute("ROLLBACK")?;

    assert!(query_rows(&recorder.connection, "SELECT id FROM events")?.is_empty());
    recorder.insert_event(2, FIRST_DAY + DAY_MICROS + 2_000, "gate-b", "plate", 0.9)?;
    assert_eq!(
        query_rows(&recorder.connection, "SELECT id FROM events")?,
        vec!["2"]
    );

    Ok(())
}

#[test]
fn recorder_recreates_a_deleted_current_day_on_next_insert() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    let timestamp = FIRST_DAY + 1_000;
    recorder.insert_event(1, timestamp, "gate-a", "motion", 0.8)?;
    let path = recorder.partition_path(timestamp);
    recorder.connection.delete_partition("events", &path)?;

    recorder.insert_event(2, timestamp + 1_000, "gate-a", "plate", 0.9)?;

    assert!(path.exists());
    assert_eq!(
        query_rows(&recorder.connection, "SELECT id FROM events")?,
        vec!["2"]
    );
    assert_eq!(recorder.connection.list_partitions("events")?.len(), 1);

    Ok(())
}

#[test]
fn recorder_routes_utc_boundaries_and_pre_epoch_timestamps() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    recorder.insert_event(1, -1, "gate-a", "motion", 0.8)?;
    recorder.insert_event(2, 0, "gate-a", "motion", 0.8)?;
    recorder.insert_event(3, DAY_MICROS - 1, "gate-a", "motion", 0.8)?;
    recorder.insert_event(4, DAY_MICROS, "gate-a", "motion", 0.8)?;

    let partitions = recorder.connection.list_partitions("events")?;
    assert_eq!(partitions.len(), 3);
    let first_partition = partitions
        .first()
        .ok_or_else(|| anyhow::anyhow!("expected the pre-epoch partition"))?;
    assert_eq!(first_partition.range_start, -DAY_MICROS);
    assert_eq!(first_partition.range_end, 0);
    assert_eq!(
        query_rows(&recorder.connection, "SELECT id FROM events ORDER BY ts")?,
        vec!["1", "2", "3", "4"]
    );

    Ok(())
}

#[test]
fn recorder_accepts_sql_integer_literal_formats_for_timestamps() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    let first_timestamp = FIRST_DAY + 1_000;
    let hexadecimal = format!("0x{first_timestamp:x}");
    recorder.connection.execute(format!(
        "INSERT INTO events VALUES (1, {hexadecimal}, 'gate-a', 'motion', 0.8)"
    ))?;
    recorder.connection.execute(
        "INSERT INTO events VALUES \
         (2, 1_735_689_600_002_000, 'gate-b', 'plate', 0.9)",
    )?;

    assert_eq!(
        query_rows(&recorder.connection, "SELECT id FROM events ORDER BY id")?,
        vec!["1", "2"]
    );
    assert_eq!(recorder.connection.list_partitions("events")?.len(), 1);

    Ok(())
}

#[test]
fn recorder_rejects_unbound_or_non_integer_partition_parameter() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    let sql = "INSERT INTO events(id, ts, camera, event_type, confidence) \
               VALUES (1, ?, 'gate-a', 'motion', 0.8)";

    let mut unbound = recorder.connection.prepare(sql)?;
    let error = unbound
        .run_ignore_rows()
        .expect_err("an unbound routing key must not create a partition");
    assert!(error.to_string().contains("not bound"));

    let mut wrong_type = recorder.connection.prepare(sql)?;
    wrong_type.bind_at(1.try_into()?, Value::build_text("2025-01-01"))?;
    let error = wrong_type
        .run_ignore_rows()
        .expect_err("a text routing key must be rejected");
    assert!(error.to_string().contains("non-NULL integer"));
    assert!(recorder.connection.list_partitions("events")?.is_empty());

    Ok(())
}

#[test]
fn recorder_preserves_standard_select_semantics_across_files() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    recorder
        .connection
        .execute("CREATE TABLE cameras(name TEXT PRIMARY KEY, zone TEXT NOT NULL)")?;
    recorder
        .connection
        .execute("INSERT INTO cameras VALUES ('gate-a', 'north'), ('gate-b', 'south')")?;
    recorder.insert_event(1, FIRST_DAY + 1_000, "gate-a", "motion", 0.7)?;
    recorder.insert_event(2, FIRST_DAY + DAY_MICROS + 2_000, "gate-b", "plate", 0.95)?;
    recorder.insert_event(
        3,
        FIRST_DAY + 2 * DAY_MICROS + 3_000,
        "gate-a",
        "motion",
        0.8,
    )?;

    assert_eq!(
        query_rows(
            &recorder.connection,
            "SELECT DISTINCT event_type FROM events ORDER BY event_type"
        )?,
        vec!["motion", "plate"]
    );
    assert_eq!(
        query_rows(
            &recorder.connection,
            "SELECT e.id, c.zone FROM events AS e \
             JOIN cameras AS c ON c.name = e.camera ORDER BY e.id"
        )?,
        vec!["1|north", "2|south", "3|north"]
    );
    assert_eq!(
        query_rows(
            &recorder.connection,
            "WITH recent AS (SELECT id, camera FROM events WHERE id >= 2) \
             SELECT camera, count(*) FROM recent GROUP BY camera ORDER BY camera"
        )?,
        vec!["gate-a|1", "gate-b|1"]
    );
    assert_eq!(
        query_rows(
            &recorder.connection,
            "SELECT id, row_number() OVER (ORDER BY ts) FROM events ORDER BY id"
        )?,
        vec!["1|1", "2|2", "3|3"]
    );
    assert_eq!(
        query_rows(
            &recorder.connection,
            "SELECT id FROM events ORDER BY ts DESC LIMIT 2 OFFSET 1"
        )?,
        vec!["2", "1"]
    );
    assert_eq!(
        query_rows(
            &recorder.connection,
            "SELECT c.name FROM cameras c WHERE EXISTS (\
               SELECT 1 FROM events e WHERE e.camera = c.name AND e.event_type = 'plate'\
             ) ORDER BY c.name"
        )?,
        vec!["gate-b"]
    );

    let mut parameterized = recorder
        .connection
        .prepare("SELECT id FROM events WHERE ts >= ? AND ts < ? ORDER BY id")?;
    parameterized.bind_at(1.try_into()?, Value::from_i64(FIRST_DAY + DAY_MICROS))?;
    parameterized.bind_at(2.try_into()?, Value::from_i64(FIRST_DAY + 3 * DAY_MICROS))?;
    assert_eq!(
        parameterized
            .run_collect_rows()?
            .into_iter()
            .filter_map(|row| row.into_iter().next())
            .map(|value| format!("{value}"))
            .collect::<Vec<_>>(),
        vec!["2", "3"]
    );

    Ok(())
}

#[test]
fn recorder_prunes_literal_and_bound_time_ranges() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    for day in 0..3 {
        recorder.insert_event(
            day + 1,
            FIRST_DAY + day * DAY_MICROS + 1_000,
            "gate-a",
            "motion",
            0.8,
        )?;
    }

    let literal_plan = query_rows(
        &recorder.connection,
        &format!(
            "EXPLAIN QUERY PLAN SELECT id FROM events \
             WHERE ts >= {} AND ts < {}",
            FIRST_DAY + DAY_MICROS,
            FIRST_DAY + 2 * DAY_MICROS
        ),
    )?;
    let full_archive_plan = query_rows(
        &recorder.connection,
        "EXPLAIN QUERY PLAN SELECT id FROM events",
    )?;

    let mut bound_plan = recorder
        .connection
        .prepare("EXPLAIN QUERY PLAN SELECT id FROM events WHERE ts >= ? AND ts < ?")?;
    bound_plan.bind_at(1.try_into()?, Value::from_i64(FIRST_DAY + DAY_MICROS))?;
    bound_plan.bind_at(2.try_into()?, Value::from_i64(FIRST_DAY + 2 * DAY_MICROS))?;
    let bound_plan = bound_plan
        .run_collect_rows()?
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| format!("{value}"))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect::<Vec<_>>();
    assert_eq!(bound_plan, literal_plan);
    assert!(literal_plan.len() < full_archive_plan.len());
    assert!(literal_plan.iter().any(|row| row.contains("idx_events_ts")));
    assert_eq!(
        query_rows(
            &recorder.connection,
            &format!(
                "SELECT id FROM events WHERE ts >= {} AND ts < {}",
                FIRST_DAY + DAY_MICROS,
                FIRST_DAY + 2 * DAY_MICROS
            )
        )?,
        vec!["2"]
    );

    Ok(())
}

#[test]
fn recorder_recovers_after_a_full_reopen_and_retention_delete() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    recorder.insert_event(1, FIRST_DAY + 1_000, "gate-a", "motion", 0.8)?;
    recorder.insert_event(2, FIRST_DAY + DAY_MICROS + 2_000, "gate-b", "plate", 0.9)?;
    let first_path = recorder.partition_path(FIRST_DAY);
    let RecorderDatabase {
        _directory,
        database,
        connection,
        database_path,
        partitions_directory,
    } = recorder;
    connection.close()?;
    drop(connection);
    drop(database);

    let io = Arc::new(PlatformIO::new()?);
    let reopened_database = Database::open_file(
        io,
        database_path.to_string_lossy().as_ref(),
        Arc::new(SqliteDialect),
    )?;
    let reopened = reopened_database.connect()?;
    register_events(&reopened, partitions_directory.clone())?;
    assert_eq!(
        query_rows(&reopened, "SELECT id FROM events ORDER BY id")?,
        vec!["1", "2"]
    );
    reopened.delete_partition("events", &first_path)?;
    reopened.close()?;
    drop(reopened);
    drop(reopened_database);

    let io = Arc::new(PlatformIO::new()?);
    let after_retention_database = Database::open_file(
        io,
        database_path.to_string_lossy().as_ref(),
        Arc::new(SqliteDialect),
    )?;
    let after_retention = after_retention_database.connect()?;
    register_events(&after_retention, partitions_directory)?;
    assert_eq!(
        query_rows(&after_retention, "SELECT id FROM events ORDER BY id")?,
        vec!["2"]
    );
    assert!(!first_path.exists());

    Ok(())
}

#[test]
fn recorder_connection_registered_early_sees_new_daily_files() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    let reader = recorder.connect_and_register()?;
    let mut prepared = reader.prepare("SELECT id FROM events ORDER BY id")?;

    recorder.insert_event(1, FIRST_DAY + 1_000, "gate-a", "motion", 0.8)?;
    recorder.insert_event(2, FIRST_DAY + DAY_MICROS + 2_000, "gate-b", "plate", 0.9)?;

    assert_eq!(
        prepared
            .run_collect_rows()?
            .into_iter()
            .filter_map(|row| row.into_iter().next())
            .map(|value| format!("{value}"))
            .collect::<Vec<_>>(),
        vec!["1", "2"]
    );
    assert_eq!(reader.list_partitions("events")?.len(), 2);

    Ok(())
}

#[test]
fn recorder_unregister_detaches_files_and_allows_clean_reregistration() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    recorder.insert_event(1, FIRST_DAY + 1_000, "gate-a", "motion", 0.8)?;
    let alias = recorder
        .connection
        .list_partitions("events")?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("expected one daily partition"))?
        .db_alias;
    let mut prepared = recorder
        .connection
        .prepare("SELECT id FROM events ORDER BY id")?;

    recorder.connection.execute("BEGIN")?;
    let error = recorder
        .connection
        .unregister_partitioned_table("events")
        .expect_err("configuration must remain stable during a transaction");
    assert!(error.to_string().contains("active transaction"), "{error}");
    recorder.connection.execute("ROLLBACK")?;

    recorder.connection.unregister_partitioned_table("events")?;
    assert!(!recorder
        .connection
        .list_attached_databases()
        .iter()
        .any(|attached| attached == &alias));
    let error = prepared
        .run_ignore_rows()
        .expect_err("an unconfigured logical archive must fail closed");
    assert!(
        error
            .to_string()
            .contains("requires partition configuration"),
        "{error}"
    );

    register_events(&recorder.connection, recorder.partitions_directory.clone())?;
    assert_eq!(
        query_rows(&recorder.connection, "SELECT id FROM events")?,
        vec!["1"]
    );

    Ok(())
}

#[test]
fn recorder_concurrent_creators_publish_one_complete_daily_file() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    let second = recorder.connect_and_register()?;
    let timestamp = FIRST_DAY + 1_000;
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let mut handles = Vec::new();

    for connection in [recorder.connection.clone(), second.clone()] {
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            connection
                .ensure_partition("events", timestamp)
                .map_err(|error| error.to_string())
        }));
    }
    barrier.wait();
    for handle in handles {
        let result = handle
            .join()
            .map_err(|_| anyhow::anyhow!("partition creator thread panicked"))?;
        result.map_err(anyhow::Error::msg)?;
    }

    recorder.insert_event(1, timestamp, "gate-a", "motion", 0.8)?;
    second.execute(format!(
        "INSERT INTO events VALUES (2, {}, 'gate-b', 'plate', 0.9)",
        timestamp + 1_000
    ))?;
    assert_eq!(
        query_rows(&second, "SELECT id FROM events ORDER BY id")?,
        vec!["1", "2"]
    );

    let path = recorder.partition_path(timestamp);
    let io = Arc::new(PlatformIO::new()?);
    let physical_database =
        Database::open_file(io, path.to_string_lossy().as_ref(), Arc::new(SqliteDialect))?;
    let physical_connection = physical_database.connect()?;
    assert_eq!(
        query_rows(&physical_connection, "PRAGMA integrity_check")?,
        vec!["ok"]
    );

    Ok(())
}

#[cfg(unix)]
#[test]
fn recorder_retention_delete_propagates_to_other_connections() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    recorder.insert_event(1, FIRST_DAY + 1_000, "gate-a", "motion", 0.8)?;
    recorder.insert_event(2, FIRST_DAY + DAY_MICROS + 2_000, "gate-b", "plate", 0.9)?;
    let reader = recorder.connect_and_register()?;
    assert_eq!(
        query_rows(&reader, "SELECT id FROM events ORDER BY id")?,
        vec!["1", "2"]
    );

    let first_path = recorder.partition_path(FIRST_DAY);
    recorder
        .connection
        .delete_partition("events", &first_path)?;
    assert_eq!(
        query_rows(&reader, "SELECT id FROM events ORDER BY id")?,
        vec!["2"]
    );
    assert_eq!(reader.list_partitions("events")?.len(), 1);

    Ok(())
}

#[cfg(unix)]
#[test]
fn recorder_transaction_keeps_a_stable_daily_file_snapshot() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    recorder.insert_event(1, FIRST_DAY + 1_000, "gate-a", "motion", 0.8)?;
    recorder.insert_event(2, FIRST_DAY + DAY_MICROS + 2_000, "gate-b", "plate", 0.9)?;
    let reader = recorder.connect_and_register()?;
    reader.execute("BEGIN")?;
    assert_eq!(
        query_rows(&reader, "SELECT id FROM events ORDER BY id")?,
        vec!["1", "2"]
    );
    let error = reader
        .delete_partition("events", &recorder.partition_path(FIRST_DAY))
        .expect_err("retention must not change a transaction's file snapshot");
    assert!(error.to_string().contains("active transaction"), "{error}");

    recorder.insert_event(
        3,
        FIRST_DAY + 2 * DAY_MICROS + 3_000,
        "gate-c",
        "motion",
        0.7,
    )?;
    recorder
        .connection
        .delete_partition("events", &recorder.partition_path(FIRST_DAY))?;

    assert_eq!(
        query_rows(&reader, "SELECT id FROM events ORDER BY id")?,
        vec!["1", "2"]
    );
    reader.execute("COMMIT")?;
    assert_eq!(
        query_rows(&reader, "SELECT id FROM events ORDER BY id")?,
        vec!["2", "3"]
    );

    Ok(())
}

#[test]
fn recorder_rejects_mismatched_external_schema_and_recovers() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    let timestamp = FIRST_DAY + DAY_MICROS + 1_000;
    let path = recorder.partition_path(timestamp);
    std::fs::create_dir_all(&recorder.partitions_directory)?;
    let io = Arc::new(PlatformIO::new()?);
    let bad_database =
        Database::open_file(io, path.to_string_lossy().as_ref(), Arc::new(SqliteDialect))?;
    let bad_connection = bad_database.connect()?;
    bad_connection.execute("CREATE TABLE events(id INTEGER PRIMARY KEY, ts INTEGER NOT NULL)")?;
    bad_connection.close()?;
    drop(bad_connection);
    drop(bad_database);

    let error = query_rows(&recorder.connection, "SELECT id FROM events")
        .expect_err("a partition with a different schema must fail closed");
    assert!(error.to_string().contains("does not match"));

    std::fs::remove_file(&path)?;
    let wal_path = PathBuf::from(format!("{}-wal", path.to_string_lossy()));
    if wal_path.exists() {
        std::fs::remove_file(wal_path)?;
    }
    assert!(query_rows(&recorder.connection, "SELECT id FROM events")?.is_empty());

    recorder.insert_event(1, timestamp, "gate-a", "motion", 0.8)?;
    assert_eq!(
        query_rows(&recorder.connection, "SELECT id FROM events")?,
        vec!["1"]
    );

    Ok(())
}

#[test]
fn recorder_rejects_external_partition_missing_configured_indexes() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    let timestamp = FIRST_DAY + DAY_MICROS + 1_000;
    let path = recorder.partition_path(timestamp);
    std::fs::create_dir_all(&recorder.partitions_directory)?;
    let io = Arc::new(PlatformIO::new()?);
    let database =
        Database::open_file(io, path.to_string_lossy().as_ref(), Arc::new(SqliteDialect))?;
    let connection = database.connect()?;
    connection.execute(EVENTS_SCHEMA)?;
    connection.close()?;
    drop(connection);
    drop(database);

    let error = query_rows(&recorder.connection, "SELECT id FROM events")
        .expect_err("an imported file must contain every configured local index");
    assert!(error.to_string().contains("required index"), "{error}");

    Ok(())
}

#[test]
fn recorder_rejects_external_rows_outside_the_filename_range() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    let path = recorder.partition_path(FIRST_DAY);
    std::fs::create_dir_all(&recorder.partitions_directory)?;
    let io = Arc::new(PlatformIO::new()?);
    let database =
        Database::open_file(io, path.to_string_lossy().as_ref(), Arc::new(SqliteDialect))?;
    let connection = database.connect()?;
    connection.execute(EVENTS_PARTITION_SCHEMA)?;
    connection.execute(format!(
        "INSERT INTO events VALUES (1, {}, 'gate-a', 'misrouted', 0.8)",
        FIRST_DAY + DAY_MICROS
    ))?;
    connection.close()?;
    drop(connection);
    drop(database);

    let error = query_rows(&recorder.connection, "SELECT id FROM events")
        .expect_err("filename-based pruning requires every row to fit the file range");
    assert!(error.to_string().contains("outside"), "{error}");

    Ok(())
}

#[test]
fn recorder_ignores_noncanonical_partition_candidates() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    std::fs::create_dir_all(&recorder.partitions_directory)?;
    let junk = recorder
        .partitions_directory
        .join("events_2025-01-01-copy.db");
    std::fs::write(&junk, b"not a database")?;

    assert!(query_rows(&recorder.connection, "SELECT id FROM events")?.is_empty());
    assert!(recorder.connection.list_partitions("events")?.is_empty());
    assert!(junk.exists());

    Ok(())
}

#[test]
fn recorder_validates_partition_ddl_and_configuration() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("validation.db");
    let io = Arc::new(PlatformIO::new()?);
    let database =
        Database::open_file(io, path.to_string_lossy().as_ref(), Arc::new(SqliteDialect))?;
    let connection = database.connect()?;

    for (sql, expected) in [
        (
            "CREATE TABLE missing_key(id INTEGER) PARTITION BY (ts)",
            "not found",
        ),
        (
            "CREATE TABLE text_key(ts TEXT) PARTITION BY (ts)",
            "INTEGER affinity",
        ),
        (
            "CREATE TABLE generated_key(raw INTEGER, ts INTEGER AS (raw)) PARTITION BY (ts)",
            "generated column",
        ),
        (
            "CREATE TABLE nullable_key(ts INTEGER) PARTITION BY (ts)",
            "NOT NULL",
        ),
    ] {
        let error = connection
            .execute(sql)
            .expect_err("an invalid partition definition must be rejected");
        assert!(error.to_string().contains(expected), "{error}");
    }
    connection.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")?;
    let error = connection
        .execute(
            "CREATE TABLE foreign_key_event(\
               id INTEGER PRIMARY KEY, ts INTEGER NOT NULL REFERENCES parent(id)\
             ) PARTITION BY (ts)",
        )
        .expect_err("cross-database foreign keys cannot be enforced");
    assert!(error.to_string().contains("foreign keys"));

    connection.execute("CREATE TABLE plain(id INTEGER, ts INTEGER)")?;
    let error = connection
        .register_partitioned_table(
            "plain",
            PartitionConfig::new(
                Box::new(DefaultPathResolver::daily(directory.path().join("plain"))),
                "CREATE TABLE plain(id INTEGER, ts INTEGER)".to_string(),
                "ts".to_string(),
            ),
        )
        .expect_err("a regular table must not be registered as partitioned");
    assert!(error.to_string().contains("PARTITION BY"));

    connection.execute(EVENTS_SCHEMA.to_string() + " PARTITION BY (ts)")?;
    let error = connection
        .register_partitioned_table(
            "events",
            PartitionConfig::new(
                Box::new(DefaultPathResolver::daily(directory.path().join("events"))),
                EVENTS_PARTITION_SCHEMA.to_string(),
                "id".to_string(),
            ),
        )
        .expect_err("configuration must use the declared partition key");
    assert!(error.to_string().contains("schema uses 'ts'"));

    let error = connection
        .register_partitioned_table(
            "events",
            PartitionConfig::new(
                Box::new(DefaultPathResolver::new(
                    directory.path().join("events"),
                    u64::MAX,
                )),
                EVENTS_PARTITION_SCHEMA.to_string(),
                "ts".to_string(),
            ),
        )
        .expect_err("an overflowing interval must be rejected");
    assert!(error.to_string().contains("must be positive"));

    let error = connection
        .register_partitioned_table(
            "events",
            PartitionConfig::new(
                Box::new(DefaultPathResolver::daily(directory.path().join("events"))),
                "CREATE TABLE events(id INTEGER PRIMARY KEY, ts TEXT)".to_string(),
                "ts".to_string(),
            ),
        )
        .expect_err("the physical schema must match before recording begins");
    assert!(error.to_string().contains("does not match"));

    let error = connection
        .register_partitioned_table(
            "events",
            PartitionConfig::new(
                Box::new(DefaultPathResolver::daily(directory.path().join("events"))),
                format!(
                    "{EVENTS_PARTITION_SCHEMA}; \
                     INSERT INTO events VALUES (1, 1, 'seed', 'invalid', 1.0)"
                ),
                "ts".to_string(),
            ),
        )
        .expect_err("physical schema must not seed or mutate partition data");
    assert!(
        error
            .to_string()
            .contains("only CREATE TABLE and CREATE INDEX"),
        "{error}"
    );

    register_events(&connection, directory.path().join("events"))?;
    assert!(connection.list_partitions("events")?.is_empty());

    Ok(())
}

#[test]
fn recorder_unsupported_mutations_fail_without_touching_archive() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    recorder.insert_event(1, FIRST_DAY + 1_000, "gate-a", "motion", 0.8)?;
    recorder.insert_event(2, FIRST_DAY + DAY_MICROS + 2_000, "gate-b", "plate", 0.9)?;

    for (sql, expected) in [
        ("UPDATE events SET confidence = 0.1 WHERE id = 1", "UPDATE"),
        ("DELETE FROM events WHERE id = 1", "DELETE"),
        ("DROP TABLE events", "DROP TABLE"),
        ("ALTER TABLE events ADD COLUMN payload TEXT", "ALTER TABLE"),
        (
            "CREATE INDEX idx_events_camera ON events(camera)",
            "PartitionConfig::schema_sql",
        ),
        (
            "CREATE TRIGGER events_ai AFTER INSERT ON events BEGIN SELECT 1; END",
            "triggers",
        ),
        (
            "INSERT INTO events SELECT 3, 1, 'gate-a', 'motion', 0.8",
            "INSERT ... SELECT",
        ),
    ] {
        let error = recorder
            .connection
            .execute(sql)
            .expect_err("unsupported archive mutations must fail explicitly");
        assert!(error.to_string().contains(expected), "{sql}: {error}");
    }

    let alias = recorder
        .connection
        .list_partitions("events")?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("expected an attached partition"))?
        .db_alias;
    assert_eq!(
        query_rows(
            &recorder.connection,
            &format!("SELECT id FROM {alias}.events ORDER BY id")
        )?,
        vec!["1"]
    );
    for sql in [
        format!(
            "INSERT INTO {alias}.events VALUES (9, {}, 'gate-x', 'tamper', 1.0)",
            FIRST_DAY + 9_000
        ),
        format!("UPDATE {alias}.events SET event_type = 'tamper'"),
        format!("DELETE FROM {alias}.events"),
        format!("ALTER TABLE {alias}.events ADD COLUMN payload TEXT"),
        format!("DROP TABLE {alias}.events"),
        format!("CREATE INDEX {alias}.tamper_idx ON events(camera)"),
    ] {
        let error = recorder
            .connection
            .execute(&sql)
            .expect_err("managed physical files must be read-only through SQL");
        assert!(
            error.to_string().contains("managed partition"),
            "{sql}: {error}"
        );
    }

    assert_eq!(
        query_rows(
            &recorder.connection,
            "SELECT id, confidence FROM events ORDER BY id"
        )?,
        vec!["1|0.8", "2|0.9"]
    );
    assert_eq!(recorder.connection.list_partitions("events")?.len(), 2);

    Ok(())
}

#[test]
fn recorder_savepoints_restore_the_partition_write_target() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;

    recorder.connection.execute("SAVEPOINT batch")?;
    recorder.insert_event(1, FIRST_DAY + 1_000, "gate-a", "motion", 0.8)?;
    recorder.connection.execute("ROLLBACK TO batch")?;
    recorder.insert_event(2, FIRST_DAY + DAY_MICROS + 2_000, "gate-b", "plate", 0.9)?;
    recorder.connection.execute("RELEASE batch")?;
    assert_eq!(
        query_rows(&recorder.connection, "SELECT id FROM events ORDER BY id")?,
        vec!["2"]
    );

    recorder.connection.execute("BEGIN")?;
    recorder.insert_event(3, FIRST_DAY + DAY_MICROS + 3_000, "gate-b", "motion", 0.7)?;
    recorder.connection.execute("SAVEPOINT nested")?;
    recorder.insert_event(4, FIRST_DAY + DAY_MICROS + 4_000, "gate-b", "motion", 0.7)?;
    recorder.connection.execute("ROLLBACK TO nested")?;
    let error = recorder
        .insert_event(5, FIRST_DAY + 2 * DAY_MICROS, "gate-c", "plate", 0.9)
        .expect_err("rollback to a nested savepoint must preserve the outer target");
    assert!(error.to_string().contains("cross-partition write"));
    recorder.connection.execute("COMMIT")?;

    assert_eq!(
        query_rows(&recorder.connection, "SELECT id FROM events ORDER BY id")?,
        vec!["2", "3"]
    );

    Ok(())
}

#[test]
fn recorder_routes_parameterized_multi_row_insert_atomically() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    let sql = "INSERT INTO events(id, ts, camera, event_type, confidence) VALUES \
               (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)";
    let mut same_day = recorder.connection.prepare(sql)?;
    for (index, value) in [
        Value::from_i64(1),
        Value::from_i64(FIRST_DAY + 1_000),
        Value::build_text("gate-a"),
        Value::build_text("motion"),
        Value::from_f64(0.8),
        Value::from_i64(2),
        Value::from_i64(FIRST_DAY + 2_000),
        Value::build_text("gate-b"),
        Value::build_text("plate"),
        Value::from_f64(0.9),
    ]
    .into_iter()
    .enumerate()
    {
        same_day.bind_at((index + 1).try_into()?, value)?;
    }
    same_day.run_ignore_rows()?;

    let mut cross_day = recorder.connection.prepare(sql)?;
    for (index, value) in [
        Value::from_i64(3),
        Value::from_i64(FIRST_DAY + 3_000),
        Value::build_text("gate-a"),
        Value::build_text("motion"),
        Value::from_f64(0.8),
        Value::from_i64(4),
        Value::from_i64(FIRST_DAY + DAY_MICROS + 4_000),
        Value::build_text("gate-b"),
        Value::build_text("plate"),
        Value::from_f64(0.9),
    ]
    .into_iter()
    .enumerate()
    {
        cross_day.bind_at((index + 1).try_into()?, value)?;
    }
    let error = cross_day
        .run_ignore_rows()
        .expect_err("a bound multi-row insert must not be partially applied");
    assert!(error.to_string().contains("one INSERT statement"));
    assert_eq!(
        query_rows(&recorder.connection, "SELECT id FROM events ORDER BY id")?,
        vec!["1", "2"]
    );
    assert_eq!(recorder.connection.list_partitions("events")?.len(), 1);

    Ok(())
}

#[test]
fn recorder_supports_returning_upsert_and_explicit_local_index() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    let timestamp = FIRST_DAY + 1_000;
    let mut insert = recorder.connection.prepare(
        "INSERT INTO events(id, ts, camera, event_type, confidence) \
         VALUES (?, ?, 'gate-a', 'motion', 0.8) RETURNING id, ts",
    )?;
    insert.bind_at(1.try_into()?, Value::from_i64(1))?;
    insert.bind_at(2.try_into()?, Value::from_i64(timestamp))?;
    let returned = insert
        .run_collect_rows()?
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| format!("{value}"))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect::<Vec<_>>();
    assert_eq!(returned, vec![format!("1|{timestamp}")]);

    assert_eq!(
        query_rows(
            &recorder.connection,
            &format!(
                "INSERT INTO events VALUES \
                 (1, {timestamp}, 'gate-a', 'plate', 0.95) \
                 ON CONFLICT(id) DO UPDATE SET event_type = excluded.event_type, \
                 confidence = excluded.confidence RETURNING event_type, confidence"
            )
        )?,
        vec!["plate|0.95"]
    );
    assert_eq!(
        query_rows(
            &recorder.connection,
            &format!("SELECT id FROM events INDEXED BY idx_events_ts WHERE ts = {timestamp}")
        )?,
        vec!["1"]
    );

    let partitions = recorder.connection.list_partitions("events")?;
    for partition in partitions {
        let io = Arc::new(PlatformIO::new()?);
        let physical_database =
            Database::open_file(io, &partition.file_path, Arc::new(SqliteDialect))?;
        let physical_connection = physical_database.connect()?;
        assert_eq!(
            query_rows(&physical_connection, "PRAGMA integrity_check")?,
            vec!["ok"]
        );
    }

    Ok(())
}

#[test]
fn recorder_time_predicates_remain_correct_across_partition_boundaries() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    for day in 0..3 {
        recorder.insert_event(
            day + 1,
            FIRST_DAY + day * DAY_MICROS + 1_000,
            "gate-a",
            "motion",
            0.8,
        )?;
    }

    assert_eq!(
        query_rows(
            &recorder.connection,
            &format!(
                "SELECT id FROM events WHERE ts BETWEEN {} AND {} ORDER BY id",
                FIRST_DAY + DAY_MICROS,
                FIRST_DAY + 2 * DAY_MICROS + 1_000
            )
        )?,
        vec!["2", "3"]
    );
    assert_eq!(
        query_rows(
            &recorder.connection,
            &format!(
                "SELECT id FROM events WHERE ts IN ({}, {}) ORDER BY id",
                FIRST_DAY + 1_000,
                FIRST_DAY + 2 * DAY_MICROS + 1_000
            )
        )?,
        vec!["1", "3"]
    );
    assert_eq!(
        query_rows(
            &recorder.connection,
            &format!(
                "SELECT id FROM events WHERE {} <= ts AND {} > ts ORDER BY id",
                FIRST_DAY + DAY_MICROS,
                FIRST_DAY + 2 * DAY_MICROS
            )
        )?,
        vec!["2"]
    );
    assert_eq!(
        query_rows(
            &recorder.connection,
            &format!(
                "SELECT id FROM events WHERE ts < {} OR ts >= {} ORDER BY id",
                FIRST_DAY + DAY_MICROS,
                FIRST_DAY + 2 * DAY_MICROS
            )
        )?,
        vec!["1", "3"]
    );

    let mut statement = recorder
        .connection
        .prepare("SELECT id FROM events WHERE ts >= ? AND ts < ? ORDER BY id")?;
    for (start, end, expected) in [
        (FIRST_DAY, FIRST_DAY + DAY_MICROS, "1"),
        (FIRST_DAY + 2 * DAY_MICROS, FIRST_DAY + 3 * DAY_MICROS, "3"),
    ] {
        statement.bind_at(1.try_into()?, Value::from_i64(start))?;
        statement.bind_at(2.try_into()?, Value::from_i64(end))?;
        let rows = statement
            .run_collect_rows()?
            .into_iter()
            .filter_map(|row| row.into_iter().next())
            .map(|value| format!("{value}"))
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![expected]);
        statement.reset()?;
    }

    Ok(())
}

#[test]
fn recorder_views_compounds_and_self_joins_see_one_logical_table() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    assert_eq!(
        query_rows(
            &recorder.connection,
            "SELECT count(*), min(ts), max(ts) FROM events"
        )?,
        vec!["0||"]
    );

    recorder.insert_event(1, FIRST_DAY + 1_000, "gate-a", "motion", 0.8)?;
    recorder.connection.execute(
        "CREATE VIEW motion_events AS \
         SELECT id, ts, camera FROM events WHERE event_type = 'motion'",
    )?;
    let mut prepared_view = recorder
        .connection
        .prepare("SELECT id FROM motion_events ORDER BY id")?;
    recorder.insert_event(2, FIRST_DAY + DAY_MICROS + 2_000, "gate-a", "motion", 0.9)?;
    recorder.insert_event(
        3,
        FIRST_DAY + 2 * DAY_MICROS + 3_000,
        "gate-b",
        "plate",
        0.95,
    )?;

    assert_eq!(
        prepared_view
            .run_collect_rows()?
            .into_iter()
            .filter_map(|row| row.into_iter().next())
            .map(|value| format!("{value}"))
            .collect::<Vec<_>>(),
        vec!["1", "2"]
    );
    assert_eq!(
        query_rows(
            &recorder.connection,
            "SELECT older.id, newer.id FROM events older \
             JOIN events newer ON newer.camera = older.camera AND newer.ts > older.ts \
             ORDER BY older.id, newer.id"
        )?,
        vec!["1|2"]
    );
    assert_eq!(
        query_rows(
            &recorder.connection,
            "SELECT id FROM events WHERE event_type = 'plate' \
             UNION ALL SELECT id FROM events WHERE event_type = 'motion' ORDER BY id"
        )?,
        vec!["1", "2", "3"]
    );

    Ok(())
}

#[test]
fn recorder_supports_multiple_independent_partitioned_event_tables() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    const ALERTS_SCHEMA: &str =
        "CREATE TABLE alerts(id INTEGER PRIMARY KEY, ts INTEGER NOT NULL, event_id INTEGER NOT NULL)";
    recorder
        .connection
        .execute(ALERTS_SCHEMA.to_string() + " PARTITION BY (ts)")?;
    recorder.connection.register_partitioned_table(
        "alerts",
        PartitionConfig::new(
            Box::new(DefaultPathResolver::daily(
                recorder._directory.path().join("alerts"),
            )),
            ALERTS_SCHEMA.to_string(),
            "ts".to_string(),
        ),
    )?;

    recorder.insert_event(1, FIRST_DAY + 1_000, "gate-a", "motion", 0.8)?;
    recorder.connection.execute(format!(
        "INSERT INTO alerts VALUES (10, {}, 1)",
        FIRST_DAY + 2_000
    ))?;
    assert_eq!(
        query_rows(
            &recorder.connection,
            "SELECT e.id, a.id FROM events e JOIN alerts a ON a.event_id = e.id"
        )?,
        vec!["1|10"]
    );

    recorder.connection.execute("BEGIN")?;
    recorder.insert_event(2, FIRST_DAY + 3_000, "gate-b", "plate", 0.9)?;
    let error = recorder
        .connection
        .execute(format!(
            "INSERT INTO alerts VALUES (11, {}, 2)",
            FIRST_DAY + 4_000
        ))
        .expect_err("one transaction cannot atomically write two event files");
    assert!(error.to_string().contains("cross-partition write"));
    recorder.connection.execute("ROLLBACK")?;

    assert_eq!(
        query_rows(&recorder.connection, "SELECT id FROM events")?,
        vec!["1"]
    );
    assert_eq!(
        query_rows(&recorder.connection, "SELECT id FROM alerts")?,
        vec!["10"]
    );

    Ok(())
}

#[test]
fn recorder_handles_a_month_of_daily_files_and_bulk_retention() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    const DAYS: i64 = 31;
    for day in 0..DAYS {
        recorder.insert_event(
            day + 1,
            FIRST_DAY + day * DAY_MICROS + 1_000,
            "gate-a",
            if day % 2 == 0 { "motion" } else { "plate" },
            0.8,
        )?;
    }

    assert_eq!(recorder.connection.list_partitions("events")?.len(), 31);
    assert_eq!(
        query_rows(
            &recorder.connection,
            "SELECT count(*), min(id), max(id), count(DISTINCT event_type) FROM events"
        )?,
        vec!["31|1|31|2"]
    );

    let expired = recorder
        .connection
        .list_partitions("events")?
        .into_iter()
        .filter(|partition| partition.range_start < FIRST_DAY + 16 * DAY_MICROS)
        .collect::<Vec<_>>();
    assert_eq!(expired.len(), 16);
    for partition in expired {
        recorder
            .connection
            .delete_partition("events", PathBuf::from(partition.file_path).as_path())?;
    }

    assert_eq!(recorder.connection.list_partitions("events")?.len(), 15);
    assert_eq!(
        query_rows(
            &recorder.connection,
            "SELECT count(*), min(id), max(id) FROM events"
        )?,
        vec!["15|17|31"]
    );

    Ok(())
}

#[test]
fn recorder_discovers_and_queries_a_year_of_daily_files() -> anyhow::Result<()> {
    let recorder = RecorderDatabase::new()?;
    const DAYS: i64 = 366;
    for day in 0..DAYS {
        recorder.create_external_partition(
            day + 1,
            FIRST_DAY + day * DAY_MICROS + 1_000,
            if day % 2 == 0 { "motion" } else { "plate" },
        )?;
    }

    assert_eq!(
        recorder.connection.list_partitions("events")?.len(),
        DAYS as usize
    );
    assert_eq!(
        query_rows(
            &recorder.connection,
            "SELECT count(*), min(id), max(id), count(DISTINCT event_type) FROM events"
        )?,
        vec!["366|1|366|2"]
    );
    assert_eq!(
        query_rows(
            &recorder.connection,
            &format!(
                "SELECT id FROM events WHERE ts >= {} AND ts < {} ORDER BY ts DESC LIMIT 3",
                FIRST_DAY + 100 * DAY_MICROS,
                FIRST_DAY + 110 * DAY_MICROS
            )
        )?,
        vec!["110", "109", "108"]
    );

    Ok(())
}

#[test]
fn video_analytics_layout_creates_discovers_and_recreates_daily_files() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let database_path = directory.path().join("plugin-main.db");
    let archive_path = directory.path().join("archive");
    let io = Arc::new(PlatformIO::new()?);
    let database = Database::open_file(
        io,
        database_path.to_string_lossy().as_ref(),
        Arc::new(SqliteDialect),
    )?;
    let connection = database.connect()?;
    connection.execute(EVENTS_SCHEMA.to_string() + " PARTITION BY (ts)")?;
    connection.register_partitioned_table(
        "events",
        PartitionConfig::new(
            Box::new(VideoAnalyticsPathResolver::daily(
                archive_path.clone(),
                "line_crossing".to_string(),
            )),
            EVENTS_PARTITION_SCHEMA.to_string(),
            "ts".to_string(),
        ),
    )?;

    connection.execute(format!(
        "INSERT INTO events VALUES (1, {}, 'gate-a', 'motion', 0.8)",
        FIRST_DAY + 1_000
    ))?;
    connection.execute(format!(
        "INSERT INTO events VALUES (2, {}, 'gate-b', 'plate', 0.9)",
        FIRST_DAY + DAY_MICROS + 1_000
    ))?;

    let resolver =
        VideoAnalyticsPathResolver::daily(archive_path.clone(), "line_crossing".to_string());
    let first_path = resolver.resolve_path("events", FIRST_DAY);
    let second_path = resolver.resolve_path("events", FIRST_DAY + DAY_MICROS);
    assert!(first_path.exists());
    assert!(second_path.exists());
    assert_eq!(
        query_rows(&connection, "SELECT id FROM events ORDER BY id")?,
        vec!["1", "2"]
    );

    connection.delete_partition("events", &first_path)?;
    assert_eq!(query_rows(&connection, "SELECT id FROM events")?, vec!["2"]);
    connection.execute(format!(
        "INSERT INTO events VALUES (3, {}, 'gate-a', 'motion', 0.7)",
        FIRST_DAY + 2_000
    ))?;
    assert!(first_path.exists());
    assert_eq!(
        query_rows(&connection, "SELECT id FROM events ORDER BY id")?,
        vec!["2", "3"]
    );

    Ok(())
}
