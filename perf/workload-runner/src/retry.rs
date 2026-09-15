use crate::{
    db::{Connection, Error, Result, Statement, Transaction},
    Signal,
};
use serde::{Deserialize, Serialize};
use std::{
    cell::Cell,
    ops::AsyncFnMut,
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Retry {
    pub max_attempts: u64,
    pub timeout: Duration,
    pub backoff: Duration,
}

pub struct TransactionRunner<'a> {
    conn: &'a Connection,
    begin: Statement<'a>,
    commit: Statement<'a>,
    rollback: Statement<'a>,
}

impl<'a> TransactionRunner<'a> {
    pub async fn prepare(conn: &'a Connection, mode: Transaction) -> Result<Self> {
        Ok(Self {
            conn,
            begin: conn.prepare(mode.sql()).await?,
            commit: conn.prepare("COMMIT").await?,
            rollback: conn.prepare("ROLLBACK").await?,
        })
    }

    pub async fn run<I>(
        &mut self,
        input: &I,
        retry: &Retry,
        cancel: &Signal,
        attempts: &Cell<u64>,
        mut body: impl AsyncFnMut(&I) -> Result<()>,
    ) -> Result<()> {
        if retry.max_attempts == 0 || retry.timeout.is_zero() {
            return Err(Error::Workload("retry limits must be positive".into()));
        }
        if !self.conn.is_autocommit()? {
            return Err(Error::Workload(
                "transaction retry requires an idle connection".into(),
            ));
        }
        let started = Instant::now();
        loop {
            if cancel.is_set() {
                return Err(Error::Cancelled);
            }
            attempts.set(attempts.get() + 1);
            let result = async {
                self.begin.execute(&[]).await?;
                body(input).await?;
                self.commit.execute(&[]).await?;
                Ok(())
            }
            .await;
            let Err(error): Result<()> = result else {
                return Ok(());
            };
            if !self.conn.is_autocommit()? {
                self.rollback.execute(&[]).await.map_err(|rollback| {
                    Error::Workload(format!("{error}; rollback failed: {rollback}"))
                })?;
            }
            if !error.is_conflict()
                || attempts.get() >= retry.max_attempts
                || started.elapsed() + retry.backoff >= retry.timeout
            {
                return Err(error);
            }
            cancel.sleep(retry.backoff).await?;
            if started.elapsed() >= retry.timeout {
                return Err(error);
            }
        }
    }
}
