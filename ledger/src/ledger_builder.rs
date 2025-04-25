use crate::{BootstrapWeights, Ledger, LedgerConstants, RepWeightCache};
use rsnano_core::{utils::get_cpu_count, Amount};
use rsnano_stats::Stats;
use rsnano_store_lmdb::{
    get_env_flags, EnvironmentOptions, LedgerCache, LmdbConfig, LmdbEnvFactory, TransactionTracker,
};
use std::{
    cmp::{max, min},
    path::PathBuf,
    sync::Arc,
};
use tracing::info;

pub struct LedgerBuilder<'a> {
    path: PathBuf,
    config: Option<LmdbConfig>,
    txn_tracker: Option<Arc<dyn TransactionTracker>>,
    env_factory: Option<&'a LmdbEnvFactory>,
    bootstrap_weights: Option<BootstrapWeights>,
    stats: Option<Arc<Stats>>,
    min_rep_weight: Amount,
    ledger_constants: Option<LedgerConstants>,
    thread_count: usize,
}

impl<'a> LedgerBuilder<'a> {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            config: None,
            txn_tracker: None,
            env_factory: None,
            bootstrap_weights: None,
            stats: None,
            min_rep_weight: Amount::zero(),
            ledger_constants: None,
            thread_count: 0,
        }
    }

    pub fn env_factory(mut self, env_factory: &'a LmdbEnvFactory) -> Self {
        self.env_factory = Some(env_factory);
        self
    }

    pub fn bootstrap_weights(mut self, weights: BootstrapWeights) -> Self {
        self.bootstrap_weights = Some(weights);
        self
    }

    pub fn txn_tracker(mut self, tracker: Arc<dyn TransactionTracker>) -> Self {
        self.txn_tracker = Some(tracker);
        self
    }

    pub fn constants(mut self, constants: LedgerConstants) -> Self {
        self.ledger_constants = Some(constants);
        self
    }

    pub fn stats(mut self, stats: Arc<Stats>) -> Self {
        self.stats = Some(stats);
        self
    }

    pub fn min_rep_weight(mut self, weight: Amount) -> Self {
        self.min_rep_weight = weight;
        self
    }

    pub fn init_thread_count(mut self, count: usize) -> Self {
        self.thread_count = count;
        self
    }

    pub fn finish(mut self) -> anyhow::Result<Ledger> {
        let ledger_cache = Arc::new(LedgerCache::new());
        let bootstrap_weights = self.bootstrap_weights.unwrap_or_default();

        let rep_weights = Arc::new(RepWeightCache::with_bootstrap_weights(
            bootstrap_weights,
            ledger_cache.clone(),
        ));

        let config = self.config.unwrap_or_default();
        let default_env_factory = LmdbEnvFactory::default();
        let env_factory = self.env_factory.unwrap_or(&default_env_factory);

        let env_options = EnvironmentOptions {
            max_dbs: config.max_databases,
            map_size: config.map_size,
            flags: get_env_flags(&config),
            path: &self.path,
        };
        let mut env = env_factory.create_with_options(env_options)?;
        if let Some(txn_tracker) = self.txn_tracker {
            env.set_transaction_tracker(txn_tracker);
        }

        let stats = self.stats.unwrap_or_else(|| Arc::new(Stats::default()));
        let ledger_constants = self
            .ledger_constants
            .unwrap_or_else(|| LedgerConstants::live());

        if self.thread_count == 0 {
            // Between 10 and 40 threads, scales well even in low power systems as long as actions are I/O bound
            self.thread_count = max(10, min(40, 11 * get_cpu_count()));
        }

        info!("Loading ledger, this may take a while...");
        Ledger::new(
            env,
            ledger_constants,
            self.min_rep_weight,
            rep_weights.clone(),
            stats.clone(),
            self.thread_count,
        )
    }
}
