use std::{
  path::PathBuf,
  sync::{Arc, Mutex, OnceLock},
};

use genawaiter::{sync::Gen, GeneratorState};
use napi_derive::napi;
use tracing_subscriber::{filter::LevelFilter, fmt::format::FmtSpan, EnvFilter};
use turso_sync_protocol::{database_inner::DatabaseInner, types::DbSyncStatus};

static TRACING_INIT: OnceLock<()> = OnceLock::new();

fn init_tracing() {
  TRACING_INIT.get_or_init(|| {
    tracing_subscriber::fmt()
      .with_thread_ids(true)
      .with_span_events(FmtSpan::ACTIVE)
      .with_ansi(false)
      .with_max_level(LevelFilter::INFO)
      .with_env_filter(EnvFilter::from_default_env())
      .init();
  });
}

#[derive(Clone)]
#[napi]
pub struct Database {
  path: String,
  io: Arc<dyn turso_core::IO>,
  db: Arc<Mutex<Option<DatabaseInner>>>,
  conn: Arc<Mutex<Option<Arc<turso_core::Connection>>>>,
}

trait Protocol {
  fn next(
    &mut self,
    resume: turso_sync_protocol::Result<turso_sync_protocol::types::ProtocolResume>,
  ) -> genawaiter::GeneratorState<
    turso_sync_protocol::types::ProtocolYield,
    turso_sync_protocol::Result<()>,
  >;
}

pub struct ProtocolGen<F: std::future::Future<Output = turso_sync_protocol::Result<()>>> {
  io: Arc<dyn turso_core::IO>,
  gen: genawaiter::sync::Gen<
    turso_sync_protocol::types::ProtocolYield,
    turso_sync_protocol::Result<turso_sync_protocol::types::ProtocolResume>,
    F,
  >,
}

impl<F: std::future::Future<Output = turso_sync_protocol::Result<()>>> Protocol for ProtocolGen<F> {
  fn next(
    &mut self,
    mut resume: turso_sync_protocol::Result<turso_sync_protocol::types::ProtocolResume>,
  ) -> genawaiter::GeneratorState<
    turso_sync_protocol::types::ProtocolYield,
    turso_sync_protocol::Result<()>,
  > {
    loop {
      let result = self.gen.resume_with(resume);
      let GeneratorState::Yielded(turso_sync_protocol::types::ProtocolYield::IO) = result else {
        break result;
      };
      if let Err(err) = self.io.run_once() {
        return GeneratorState::Complete(Err(err.into()));
      }
      resume = Ok(turso_sync_protocol::types::ProtocolResume::None);
    }
  }
}

#[napi]
pub struct ProtocolNapi {
  value: Arc<Mutex<dyn Protocol>>,
}

#[napi]
pub enum ProtocolResume {
  None,
  Exists(bool),
  Content(Option<Vec<u8>>),
  DbInfo {
    generation: u32,
  },
  DbStatus {
    baton: Option<String>,
    status: String,
    generation: u32,
    max_frame_no: u32,
  },
  Err(String),
}

#[napi]
pub enum ProtocolYield {
  DbInfo,
  DbBootstrap {
    generation: u32,
  },
  WalPush {
    baton: Option<String>,
    generation: u32,
    start_frame: u32,
    end_frame: u32,
    frames: Vec<u8>,
  },
  WalPull {
    generation: u32,
    start_frame: u32,
  },
  ExistsFile {
    path: String,
  },
  ReadFile {
    path: String,
  },
  WriteFile {
    path: String,
    data: Vec<u8>,
  },
  TruncateFile {
    path: String,
    size: u32,
  },
  RemoveFile {
    path: String,
  },
  Continue,
}

#[napi]
impl ProtocolNapi {
  #[napi]
  pub fn next(&self, resume: ProtocolResume) -> napi::Result<Option<ProtocolYield>> {
    let resume = match resume {
      ProtocolResume::None => Ok(turso_sync_protocol::types::ProtocolResume::None),
      ProtocolResume::Exists(x) => Ok(turso_sync_protocol::types::ProtocolResume::Exists(x)),
      ProtocolResume::Content(x) => Ok(turso_sync_protocol::types::ProtocolResume::Content(x)),
      ProtocolResume::DbInfo { generation } => {
        Ok(turso_sync_protocol::types::ProtocolResume::DbInfo {
          generation: generation as usize,
        })
      }
      ProtocolResume::DbStatus {
        baton,
        generation,
        max_frame_no,
        status,
      } => Ok(turso_sync_protocol::types::ProtocolResume::DbStatus(
        DbSyncStatus {
          baton,
          generation: generation as usize,
          max_frame_no: max_frame_no as usize,
          status,
        },
      )),
      ProtocolResume::Err(e) => Err(turso_sync_protocol::errors::Error::DatabaseSyncError(e)),
    };
    let mut value = self.value.lock().unwrap();
    let result = value.next(resume);
    drop(value);
    match result {
      genawaiter::GeneratorState::Yielded(y) => match y {
        turso_sync_protocol::types::ProtocolYield::IO => {
          panic!("IO yield should be handled earlier")
        }
        turso_sync_protocol::types::ProtocolYield::DbInfo => Ok(Some(ProtocolYield::DbInfo)),
        turso_sync_protocol::types::ProtocolYield::DbBootstrap { generation } => {
          Ok(Some(ProtocolYield::DbBootstrap {
            generation: generation as u32,
          }))
        }
        turso_sync_protocol::types::ProtocolYield::WalPush {
          baton,
          generation,
          start_frame,
          end_frame,
          frames,
        } => Ok(Some(ProtocolYield::WalPush {
          baton,
          generation: generation as u32,
          start_frame: start_frame as u32,
          end_frame: end_frame as u32,
          frames,
        })),
        turso_sync_protocol::types::ProtocolYield::WalPull {
          generation,
          start_frame,
        } => Ok(Some(ProtocolYield::WalPull {
          generation: generation as u32,
          start_frame: start_frame as u32,
        })),
        turso_sync_protocol::types::ProtocolYield::ExistsFile { path } => {
          Ok(Some(ProtocolYield::ExistsFile {
            path: path.to_str().unwrap_or("").to_string(),
          }))
        }
        turso_sync_protocol::types::ProtocolYield::ReadFile { path } => {
          Ok(Some(ProtocolYield::ReadFile {
            path: path.to_str().unwrap_or("").to_string(),
          }))
        }
        turso_sync_protocol::types::ProtocolYield::WriteFile { path, data } => {
          Ok(Some(ProtocolYield::WriteFile {
            path: path.to_str().unwrap_or("").to_string(),
            data,
          }))
        }
        turso_sync_protocol::types::ProtocolYield::RemoveFile { path } => {
          Ok(Some(ProtocolYield::RemoveFile {
            path: path.to_str().unwrap_or("").to_string(),
          }))
        }
        turso_sync_protocol::types::ProtocolYield::Continue => Ok(Some(ProtocolYield::Continue)),
      },
      genawaiter::GeneratorState::Complete(Ok(())) => Ok(None),
      genawaiter::GeneratorState::Complete(Err(err)) => Err(into_napi_error(err)),
    }
  }
}

#[napi]
impl Database {
  #[napi(constructor)]
  pub fn new(path: String) -> napi::Result<Self> {
    init_tracing();
    let io = Arc::new(turso_core::MemoryIO::new());
    Ok(Self {
      path,
      io,
      db: Arc::new(Mutex::new(None)),
      conn: Arc::new(Mutex::new(None)),
    })
  }

  #[napi]
  pub fn init(&self) -> napi::Result<ProtocolNapi> {
    let io = self.io.clone();
    let path = self.path.clone();
    let db = self.db.clone();
    let conn = self.conn.clone();
    let gen = Gen::new(|coro| async move {
      let db_new = DatabaseInner::new(&coro, io, &PathBuf::from(path)).await?;
      let conn_new = db_new.connect(&coro).await?;
      let mut db = db.lock().unwrap();
      let mut conn = conn.lock().unwrap();
      *db = Some(db_new);
      *conn = Some(conn_new);
      Ok(())
    });
    Ok(ProtocolNapi {
      value: Arc::new(Mutex::new(ProtocolGen {
        io: self.io.clone(),
        gen,
      })),
    })
  }

  #[napi]
  pub fn push(&self) -> napi::Result<ProtocolNapi> {
    let io = self.io.clone();
    let db = self.db.clone();
    let gen = Gen::new(|coro| async move {
      let mut db = db.lock().unwrap();
      db.as_mut().unwrap().push(&coro).await?;
      Ok(())
    });
    Ok(ProtocolNapi {
      value: Arc::new(Mutex::new(ProtocolGen { io, gen })),
    })
  }

  #[napi]
  pub fn pull(&self) -> napi::Result<ProtocolNapi> {
    let io = self.io.clone();
    let db = self.db.clone();
    let gen = Gen::new(|coro| async move {
      let mut db = db.lock().unwrap();
      db.as_mut().unwrap().pull(&coro).await?;
      Ok(())
    });
    Ok(ProtocolNapi {
      value: Arc::new(Mutex::new(ProtocolGen { io, gen })),
    })
  }

  #[napi]
  pub fn sync(&self) -> napi::Result<ProtocolNapi> {
    let io = self.io.clone();
    let db = self.db.clone();
    let gen = Gen::new(|coro| async move {
      let mut db = db.lock().unwrap();
      db.as_mut().unwrap().sync(&coro).await?;
      Ok(())
    });
    Ok(ProtocolNapi {
      value: Arc::new(Mutex::new(ProtocolGen { io, gen })),
    })
  }

  #[napi]
  pub fn execute(&self, sql: String) -> napi::Result<Vec<Vec<String>>> {
    let conn = self.conn.lock().unwrap();
    let conn = conn.as_ref().unwrap();
    let mut rows = Vec::new();
    let mut stmt = conn.prepare(sql).unwrap();
    let columns = stmt.num_columns();
    loop {
      match stmt.step().map_err(into_napi_error)? {
        turso_core::StepResult::Busy => {
          return Err(napi::Error::new(
            napi::Status::GenericFailure,
            "database is busy".to_string(),
          ))
        }
        turso_core::StepResult::Interrupt => {
          return Err(napi::Error::new(
            napi::Status::GenericFailure,
            "query was interrupted".to_string(),
          ))
        }
        turso_core::StepResult::Row => {
          let mut converted = Vec::new();
          let row = stmt.row().unwrap();
          for i in 0..columns {
            converted.push(row.get_value(i).to_string());
          }
          rows.push(converted);
        }
        turso_core::StepResult::IO => {
          self.io.run_once().map_err(into_napi_error)?;
        }
        turso_core::StepResult::Done => break,
      }
    }
    Ok(rows)
  }
}

#[inline]
fn into_napi_error(err: impl std::fmt::Display) -> napi::Error {
  napi::Error::new(napi::Status::GenericFailure, format!("{err}"))
}
