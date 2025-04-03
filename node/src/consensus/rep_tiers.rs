use std::{
    collections::HashSet,
    mem::size_of,
    ops::Deref,
    sync::{Arc, Condvar, Mutex},
    thread::JoinHandle,
    time::Duration,
};

use strum_macros::EnumIter;
use tracing::debug;

use rsnano_core::{
    utils::{ContainerInfo, ContainerInfoProvider},
    PublicKey,
};
use rsnano_ledger::RepWeightCache;
use rsnano_stats::{DetailType, Direction, StatType, Stats};

use crate::{config::NetworkParams, representatives::OnlineReps};

// Higher number means higher priority
#[derive(Copy, Clone, PartialOrd, Ord, PartialEq, Eq, EnumIter, Hash, Debug)]
pub enum RepTier {
    /// Not a principal representatives
    None,
    /// (0.1-1%) of online stake
    Tier1,
    /// (1-5%) of online stake
    Tier2,
    /// (> 5%) of online stake
    Tier3,
}

impl From<RepTier> for DetailType {
    fn from(value: RepTier) -> Self {
        match value {
            RepTier::None => DetailType::None,
            RepTier::Tier1 => DetailType::Tier1,
            RepTier::Tier2 => DetailType::Tier2,
            RepTier::Tier3 => DetailType::Tier3,
        }
    }
}

#[derive(Default, Clone)]
pub struct RepTiers {
    /// 0.1% or above
    tier1: HashSet<PublicKey>,
    /// 1% or above
    tier2: HashSet<PublicKey>,
    /// 5% or above
    tier3: HashSet<PublicKey>,
}

impl RepTiers {
    pub fn tier(&self, representative: &PublicKey) -> RepTier {
        if self.tier3.contains(representative) {
            RepTier::Tier3
        } else if self.tier2.contains(representative) {
            RepTier::Tier2
        } else if self.tier1.contains(representative) {
            RepTier::Tier1
        } else {
            RepTier::None
        }
    }
}

pub struct CurrentRepTiers(Mutex<RepTiers>);

impl CurrentRepTiers {
    pub fn new() -> Self {
        Self(Mutex::new(RepTiers::default()))
    }
}

impl RepTiersConsumer for CurrentRepTiers {
    fn update_rep_tiers(&self, new_tiers: RepTiers) {
        *self.0.lock().unwrap() = new_tiers;
    }
}

impl Deref for CurrentRepTiers {
    type Target = Mutex<RepTiers>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ContainerInfoProvider for CurrentRepTiers {
    fn container_info(&self) -> ContainerInfo {
        let tiers = self.lock().unwrap();
        [
            (
                "representatives_1",
                tiers.tier1.len(),
                size_of::<PublicKey>(),
            ),
            (
                "representatives_2",
                tiers.tier2.len(),
                size_of::<PublicKey>(),
            ),
            (
                "representatives_3",
                tiers.tier3.len(),
                size_of::<PublicKey>(),
            ),
        ]
        .into()
    }
}

pub trait RepTiersConsumer {
    fn update_rep_tiers(&self, new_tiers: RepTiers);
}

impl<T> RepTiersConsumer for Arc<T>
where
    T: RepTiersConsumer,
{
    fn update_rep_tiers(&self, new_tiers: RepTiers) {
        self.as_ref().update_rep_tiers(new_tiers);
    }
}

pub struct RepTiersCalculator {
    network_params: NetworkParams,
    thread: Mutex<Option<JoinHandle<()>>>,
    stopped: Arc<Mutex<bool>>,
    condition: Arc<Condvar>,
    rep_weights: Arc<RepWeightCache>,
    online_reps: Arc<Mutex<OnlineReps>>,
    stats: Arc<Stats>,
    tiers: Arc<Mutex<RepTiers>>,
    consumers: Mutex<Vec<Box<dyn RepTiersConsumer + Send + Sync>>>,
}

impl RepTiersCalculator {
    pub fn new(
        rep_weights: Arc<RepWeightCache>,
        network_params: NetworkParams,
        online_reps: Arc<Mutex<OnlineReps>>,
        stats: Arc<Stats>,
    ) -> Self {
        let tiers = Arc::new(Mutex::new(RepTiers::default()));
        Self {
            network_params,
            thread: Mutex::new(None),
            stopped: Arc::new(Mutex::new(false)),
            condition: Arc::new(Condvar::new()),
            rep_weights,
            online_reps,
            stats,
            tiers,
            consumers: Mutex::new(Vec::new()),
        }
    }

    pub fn add_tiers_consumer(&self, consumer: impl RepTiersConsumer + Send + Sync + 'static) {
        self.consumers.lock().unwrap().push(Box::new(consumer));
    }

    pub fn start(&self) {
        debug_assert!(self.thread.lock().unwrap().is_none());
        let stopped_mutex = Arc::clone(&self.stopped);
        let condition = Arc::clone(&self.condition);
        let consumers = std::mem::replace(self.consumers.lock().unwrap().as_mut(), Vec::new());
        let mut rep_tiers_impl = RepTiersImpl::new(
            self.stats.clone(),
            self.online_reps.clone(),
            self.rep_weights.clone(),
            self.tiers.clone(),
            consumers,
        );
        let interval = if self.network_params.network.is_dev_network() {
            Duration::from_millis(500)
        } else {
            Duration::from_secs(10 * 60)
        };

        let join_handle = std::thread::Builder::new()
            .name("Rep tiers".to_string())
            .spawn(move || {
                let mut stopped = stopped_mutex.lock().unwrap();
                while !*stopped {
                    drop(stopped);

                    rep_tiers_impl.calculate_tiers();

                    stopped = stopped_mutex.lock().unwrap();
                    stopped = condition
                        .wait_timeout_while(stopped, interval, |stop| !*stop)
                        .unwrap()
                        .0;
                }
            })
            .unwrap();
        *self.thread.lock().unwrap() = Some(join_handle);
    }

    pub fn stop(&self) {
        *self.stopped.lock().unwrap() = true;
        self.condition.notify_all();
        let join_handle = self.thread.lock().unwrap().take();
        if let Some(join_handle) = join_handle {
            join_handle.join().unwrap();
        }
    }

    pub fn tier(&self, representative: &PublicKey) -> RepTier {
        self.tiers.lock().unwrap().tier(representative)
    }
}

impl Drop for RepTiersCalculator {
    fn drop(&mut self) {
        // Thread must be stopped before destruction
        debug_assert!(self.thread.lock().unwrap().is_none());
    }
}

struct RepTiersImpl {
    stats: Arc<Stats>,
    online_reps: Arc<Mutex<OnlineReps>>,
    rep_weights: Arc<RepWeightCache>,
    tiers: Arc<Mutex<RepTiers>>,
    consumers: Vec<Box<dyn RepTiersConsumer + Send + Sync>>,
}

impl RepTiersImpl {
    fn new(
        stats: Arc<Stats>,
        online_reps: Arc<Mutex<OnlineReps>>,
        rep_weights: Arc<RepWeightCache>,
        tiers: Arc<Mutex<RepTiers>>,
        consumers: Vec<Box<dyn RepTiersConsumer + Send + Sync>>,
    ) -> Self {
        Self {
            stats,
            online_reps,
            rep_weights,
            tiers,
            consumers,
        }
    }

    fn calculate_tiers(&mut self) {
        self.stats.inc(StatType::RepTiers, DetailType::Loop);
        let trended = self.online_reps.lock().unwrap().trended_or_minimum_weight();
        let mut new_tier1 = HashSet::new();
        let mut new_tier2 = HashSet::new();
        let mut new_tier3 = HashSet::new();
        let mut ignored = 0;
        let reps_count;
        {
            let rep_weights = self.rep_weights.read();
            reps_count = rep_weights.len();
            for (&representative, &weight) in rep_weights.iter() {
                if weight > trended / 1000 {
                    // 0.1% or above (level 1)
                    new_tier1.insert(representative);
                    if weight > trended / 100 {
                        // 1% or above (level 2)
                        new_tier2.insert(representative);
                        if weight > trended / 20 {
                            // 5% or above (level 3)
                            new_tier3.insert(representative);
                        }
                    }
                } else {
                    ignored += 1;
                }
            }
        }

        self.stats.add_dir(
            StatType::RepTiers,
            DetailType::Processed,
            Direction::In,
            reps_count as u64,
        );

        self.stats.add_dir(
            StatType::RepTiers,
            DetailType::Ignored,
            Direction::In,
            ignored,
        );

        debug!(
            "Representative tiers updated, tier 1: {}, tier 2: {}, tier 3: {} ({} ignored)",
            new_tier1.len(),
            new_tier2.len(),
            new_tier3.len(),
            ignored
        );

        let new_rep_tiers = RepTiers {
            tier1: new_tier1,
            tier2: new_tier2,
            tier3: new_tier3,
        };
        {
            let mut guard = self.tiers.lock().unwrap();
            guard.tier1 = new_rep_tiers.tier1.clone();
            guard.tier2 = new_rep_tiers.tier2.clone();
            guard.tier3 = new_rep_tiers.tier3.clone();
        }

        for consumer in &self.consumers {
            consumer.update_rep_tiers(new_rep_tiers.clone());
        }

        self.stats.inc(StatType::RepTiers, DetailType::Updated);
    }
}
