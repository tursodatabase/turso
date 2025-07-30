use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
};

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use tokio::sync::Mutex;
use turso_sync_protocol::Result;

type PinnedFuture = Pin<Box<dyn Future<Output = bool> + Send>>;

pub struct FaultInjectionPlan {
    pub is_fault: Box<dyn Fn(String, String) -> PinnedFuture + Send + Sync>,
}

pub enum FaultInjectionStrategy {
    Disabled,
    Record,
    Enabled { plan: FaultInjectionPlan },
}

impl std::fmt::Debug for FaultInjectionStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disabled => write!(f, "Disabled"),
            Self::Record => write!(f, "Record"),
            Self::Enabled { .. } => write!(f, "Enabled"),
        }
    }
}

pub struct TestContext {
    fault_injection: Mutex<FaultInjectionStrategy>,
    faulty_call: Mutex<HashSet<(String, String)>>,
    rng: Mutex<ChaCha8Rng>,
}

pub struct FaultSession {
    ctx: Arc<TestContext>,
    recording: bool,
    plans: Option<Vec<FaultInjectionPlan>>,
}

impl FaultSession {
    pub async fn next(&mut self) -> Option<FaultInjectionStrategy> {
        if !self.recording {
            self.recording = true;
            return Some(FaultInjectionStrategy::Record);
        }
        if self.plans.is_none() {
            self.plans = Some(self.ctx.enumerate_simple_plans().await);
        }

        let plans = self.plans.as_mut().unwrap();
        if plans.is_empty() {
            return None;
        }

        let plan = plans.pop().unwrap();
        Some(FaultInjectionStrategy::Enabled { plan })
    }
}

impl TestContext {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: Mutex::new(ChaCha8Rng::seed_from_u64(seed)),
            fault_injection: Mutex::new(FaultInjectionStrategy::Disabled),
            faulty_call: Mutex::new(HashSet::new()),
        }
    }
    pub async fn rng(&self) -> tokio::sync::MutexGuard<ChaCha8Rng> {
        self.rng.lock().await
    }
    pub fn fault_session(self: &Arc<Self>) -> FaultSession {
        FaultSession {
            ctx: self.clone(),
            recording: false,
            plans: None,
        }
    }
    pub async fn switch_mode(&self, updated: FaultInjectionStrategy) {
        let mut mode = self.fault_injection.lock().await;
        tracing::info!("switch fault injection mode: {:?}", updated);
        *mode = updated;
    }
    pub async fn enumerate_simple_plans(&self) -> Vec<FaultInjectionPlan> {
        let mut plans = vec![];
        for call in self.faulty_call.lock().await.iter() {
            let mut fault_counts = HashMap::new();
            fault_counts.insert(call.clone(), 1);

            let count = Arc::new(Mutex::new(1));
            let call = call.clone();
            plans.push(FaultInjectionPlan {
                is_fault: Box::new(move |name, bt| {
                    let call = call.clone();
                    let count = count.clone();
                    Box::pin(async move {
                        if (name, bt) != call {
                            return false;
                        }
                        let mut count = count.lock().await;
                        *count -= 1;
                        *count >= 0
                    })
                }),
            })
        }
        plans
    }
    pub async fn faulty_call(&self, name: &str) -> Result<()> {
        tokio::task::yield_now().await;
        if let FaultInjectionStrategy::Disabled = &*self.fault_injection.lock().await {
            return Ok(());
        }
        let bt = std::backtrace::Backtrace::force_capture().to_string();
        match &mut *self.fault_injection.lock().await {
            FaultInjectionStrategy::Record => {
                let mut call_sites = self.faulty_call.lock().await;
                call_sites.insert((name.to_string(), bt));
                Ok(())
            }
            FaultInjectionStrategy::Enabled { plan } => {
                if plan.is_fault.as_ref()(name.to_string(), bt.clone()).await {
                    Err(turso_sync_protocol::errors::Error::DatabaseSyncError(
                        "injected fault".to_string(),
                    ))
                } else {
                    Ok(())
                }
            }
            _ => unreachable!("Disabled case handled above"),
        }
    }
}
