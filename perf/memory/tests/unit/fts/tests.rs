use super::*;

#[tokio::test]
async fn index_size_includes_segments_on_both_sides_of_document_flush() -> Result<()> {
    for (documents, segments) in [(1000, 1), (1001, 2)] {
        let fixture = FtsFixture::create(documents).await?;
        let stats = fixture.index_stats(None)?;
        assert_eq!(stats.segments, segments);
        assert_eq!(stats.page_size, 4096);
        assert!(stats.largest_segment_bytes > 0);
        if segments == 1 {
            assert_eq!(stats.segment_bytes, stats.largest_segment_bytes);
        } else {
            assert!(stats.segment_bytes > stats.largest_segment_bytes);
        }
        let session = fixture
            .session(QueryCase::Common, QueryState::First)
            .await?;
        assert_eq!(session.query(QueryCase::Common).await?.rows, documents);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mvcc_transactions_overlap_repeat_and_finish_before_cleanup() -> Result<()> {
    let mut observer = RecordingObserver::default();
    let config = FtsConfig {
        query: QueryCase::And,
        state: QueryState::Warm,
        documents: 103,
        corpus: CorpusConfig {
            extra_tokens: 1024,
            cache_pages: Some(200),
            min_index_bytes: 200 * 4096 + 1,
        },
        mode: JournalMode::Mvcc,
        connections: 2,
        execution: Execution::Transactions {
            per_connection: 3,
            queries_per_transaction: 2,
        },
    };
    let workload = FtsWorkload::prepare(config, &mut observer).await?;
    let mut mode = workload.sessions[0]
        .conn
        .query("PRAGMA journal_mode", ())
        .await?;
    assert_eq!(mode.next().await?.unwrap().get::<String>(0)?, "mvcc");
    drop(mode);
    let result = workload.run(&mut observer).await?;
    assert_eq!(result.queries, 12);
    assert_eq!(result.transactions, 6);
    assert_eq!(result.max_active_transactions, 2);
    assert_eq!(result.rows, 216);
    assert_eq!(result.id_sum, 11_016);
    assert_eq!(observer.batches, [(4, 2), (8, 4), (12, 6)]);
    for session in &workload.sessions {
        assert!(session.conn.is_autocommit()?);
        let mut cache = session.conn.query("PRAGMA cache_size", ()).await?;
        assert_eq!(cache.next().await?.unwrap().get::<i64>(0)?, 200);
    }
    workload.finish(&mut observer);
    assert_eq!(
        observer.phases,
        [
            FtsPhase::Setup,
            FtsPhase::Open,
            FtsPhase::Warmup,
            FtsPhase::Run,
            FtsPhase::Cleanup,
            FtsPhase::Done
        ]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_query_workers_finish_before_transactions_are_rolled_back() -> Result<()> {
    let config = FtsConfig {
        query: QueryCase::Common,
        state: QueryState::First,
        documents: 5,
        corpus: CorpusConfig::default(),
        mode: JournalMode::Mvcc,
        connections: 2,
        execution: Execution::Transactions {
            per_connection: 1,
            queries_per_transaction: 1,
        },
    };
    let workload = FtsWorkload::prepare(config, &mut ()).await?;
    workload.sessions[0]
        .conn
        .execute("DROP TABLE docs", ())
        .await?;
    let mut observer = RecordingObserver::default();
    assert!(workload.run(&mut observer).await.is_err());
    assert!(observer.batches.is_empty());
    for session in &workload.sessions {
        assert!(session.conn.is_autocommit()?);
        session.begin("BEGIN CONCURRENT").await?;
        session.commit().await?;
    }
    workload.finish(&mut observer);
    assert_eq!(
        observer.phases,
        [FtsPhase::Run, FtsPhase::Cleanup, FtsPhase::Done]
    );
    Ok(())
}

#[tokio::test]
async fn query_cases_return_expected_rows_after_reopening_and_warming() -> Result<()> {
    let fixture = FtsFixture::create(103).await?;
    for state in [QueryState::First, QueryState::Warm] {
        for (case, count, sum) in [
            (QueryCase::Rare, 2, 100),
            (QueryCase::Common, 103, 5253),
            (QueryCase::And, 18, 918),
            (QueryCase::Or, 69, 3519),
            (QueryCase::Phrase, 1, 0),
        ] {
            let session = fixture.session(case, state).await?;
            for _ in 0..2 {
                assert_eq!(
                    session.query(case).await?,
                    QueryResult {
                        rows: count,
                        id_sum: sum
                    }
                );
            }
        }
        let session = fixture.session(QueryCase::Ranked, state).await?;
        let mut rows = session.conn.query(QueryCase::Ranked.sql(), ()).await?;
        let mut previous = f64::INFINITY;
        let mut ids = Vec::new();
        while let Some(row) = rows.next().await? {
            let id = row.get::<i64>(0)?;
            let score = row.get::<f64>(1)?;
            assert!((0..103).contains(&id));
            assert_eq!(id % 6, 0);
            assert!(!ids.contains(&id));
            assert!(score > 0.0 && score <= previous);
            previous = score;
            ids.push(id);
        }
        assert_eq!(ids.len(), 10);
    }
    Ok(())
}

#[tokio::test]
async fn ranked_limit_handles_fewer_than_ten_matches() -> Result<()> {
    let fixture = FtsFixture::create(5).await?;
    let session = fixture
        .session(QueryCase::Ranked, QueryState::First)
        .await?;
    assert_eq!(
        session.query(QueryCase::Ranked).await?,
        QueryResult { rows: 4, id_sum: 9 }
    );
    Ok(())
}

#[derive(Default)]
struct RecordingObserver {
    phases: Vec<FtsPhase>,
    batches: Vec<(usize, usize)>,
}

impl FtsObserver for RecordingObserver {
    fn on_phase(&mut self, phase: FtsPhase) {
        self.phases.push(phase);
    }

    fn after_batch(&mut self, progress: &RunResult) {
        assert_eq!(self.phases.last(), Some(&FtsPhase::Run));
        self.batches.push((progress.queries, progress.transactions));
    }
}
