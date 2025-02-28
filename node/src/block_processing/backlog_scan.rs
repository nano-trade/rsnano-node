use std::{
    cmp::max,
    sync::{Arc, Condvar, Mutex, MutexGuard, RwLock},
    thread::{self, JoinHandle},
    time::Duration,
};

use rsnano_core::{Account, AccountInfo, ConfirmationHeightInfo};
use rsnano_ledger::{AnySet, ConfirmedSet, Ledger};
use rsnano_network::bandwidth_limiter::RateLimiter;
use rsnano_stats::{DetailType, StatType};

use crate::stats::Stats;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BacklogScanConfig {
    /// Control if ongoing backlog population is enabled. If not, backlog population can still be triggered by RPC
    pub enabled: bool,

    /// Number of accounts per second to process.
    pub batch_size: usize,

    /// Number of accounts to scan per second
    pub rate_limit: usize,
}

impl Default for BacklogScanConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            batch_size: 1000,
            rate_limit: 10_000,
        }
    }
}

/// Continuously scan the ledger for unconfirmed blocks and activate them
pub struct BacklogScan {
    ledger: Arc<Ledger>,
    stats: Arc<Stats>,
    /// Callback called for each backlogged account
    unconfirmed_observers: Arc<RwLock<Vec<Box<dyn Fn(&[UnconfirmedInfo]) + Send + Sync>>>>,
    up_to_date_observers: Arc<RwLock<Vec<Box<dyn Fn(&[Account]) + Send + Sync>>>>,

    config: BacklogScanConfig,
    mutex: Arc<Mutex<BacklogScanFlags>>,
    condition: Arc<Condvar>,
    /** Thread that runs the backlog implementation logic. The thread always runs, even if
     *  backlog population is disabled, so that it can service a manual trigger (e.g. via RPC). */
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl BacklogScan {
    pub(crate) fn new(config: BacklogScanConfig, ledger: Arc<Ledger>, stats: Arc<Stats>) -> Self {
        Self {
            config,
            ledger,
            stats,
            unconfirmed_observers: Arc::new(RwLock::new(Vec::new())),
            up_to_date_observers: Arc::new(RwLock::new(Vec::new())),
            mutex: Arc::new(Mutex::new(BacklogScanFlags {
                stopped: false,
                triggered: false,
            })),
            condition: Arc::new(Condvar::new()),
            thread: Mutex::new(None),
        }
    }

    pub fn on_unconfirmed_found(
        &self,
        callback: impl Fn(&[UnconfirmedInfo]) + Send + Sync + 'static,
    ) {
        self.unconfirmed_observers
            .write()
            .unwrap()
            .push(Box::new(callback));
    }

    /// Accounts scanned but not activated
    pub fn on_up_to_date(&self, callback: impl Fn(&[Account]) + Send + Sync + 'static) {
        self.up_to_date_observers
            .write()
            .unwrap()
            .push(Box::new(callback));
    }

    pub fn start(&self) {
        debug_assert!(self.thread.lock().unwrap().is_none());

        let thread = BacklogScanThread {
            ledger: self.ledger.clone(),
            stats: self.stats.clone(),
            unconfirmed_observers: self.unconfirmed_observers.clone(),
            up_to_date_observers: self.up_to_date_observers.clone(),
            config: self.config.clone(),
            mutex: self.mutex.clone(),
            condition: self.condition.clone(),
            limiter: RateLimiter::new(self.config.rate_limit),
        };

        *self.thread.lock().unwrap() = Some(
            thread::Builder::new()
                .name("Backlog".to_owned())
                .spawn(move || {
                    thread.run();
                })
                .unwrap(),
        );
    }

    pub fn stop(&self) {
        let mut lock = self.mutex.lock().unwrap();
        lock.stopped = true;
        drop(lock);
        self.notify();
        let handle = self.thread.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.join().unwrap()
        }
    }

    /** Manually trigger backlog population */
    pub fn trigger(&self) {
        {
            let mut lock = self.mutex.lock().unwrap();
            lock.triggered = true;
        }
        self.notify();
    }

    /** Notify about AEC vacancy */
    pub fn notify(&self) {
        self.condition.notify_all();
    }
}

impl Drop for BacklogScan {
    fn drop(&mut self) {
        self.stop();
    }
}

struct BacklogScanFlags {
    stopped: bool,
    /** This is a manual trigger, the ongoing backlog population does not use this.
     *  It can be triggered even when backlog population (frontiers confirmation) is disabled. */
    triggered: bool,
}

struct BacklogScanThread {
    ledger: Arc<Ledger>,
    stats: Arc<Stats>,
    unconfirmed_observers: Arc<RwLock<Vec<Box<dyn Fn(&[UnconfirmedInfo]) + Send + Sync>>>>,
    up_to_date_observers: Arc<RwLock<Vec<Box<dyn Fn(&[Account]) + Send + Sync>>>>,
    config: BacklogScanConfig,
    mutex: Arc<Mutex<BacklogScanFlags>>,
    condition: Arc<Condvar>,
    limiter: RateLimiter,
}

impl BacklogScanThread {
    fn run(&self) {
        let mut lock = self.mutex.lock().unwrap();
        while !lock.stopped {
            if self.predicate(&lock) {
                self.stats.inc(StatType::BacklogScan, DetailType::Loop);

                lock.triggered = false;
                // Does a single iteration over all accounts
                lock = self.populate_backlog(lock);
            } else {
                lock = self
                    .condition
                    .wait_while(lock, |l| !l.stopped && !self.predicate(l))
                    .unwrap();
            }
        }
    }

    fn predicate(&self, lock: &BacklogScanFlags) -> bool {
        lock.triggered || self.config.enabled
    }

    fn populate_backlog<'a>(
        &'a self,
        mut lock: MutexGuard<'a, BacklogScanFlags>,
    ) -> MutexGuard<'a, BacklogScanFlags> {
        let mut next = Account::zero();
        let mut done = false;
        while !lock.stopped && !done {
            // Wait for the rate limiter
            while !self.limiter.should_pass(self.config.batch_size) {
                let wait_time = Duration::from_millis(
                    1000 / max(self.config.rate_limit / self.config.batch_size, 1) as u64 / 2,
                );

                lock = self
                    .condition
                    .wait_timeout_while(lock, max(wait_time, Duration::from_millis(10)), |i| {
                        !i.stopped
                    })
                    .unwrap()
                    .0;
                if lock.stopped {
                    return lock;
                }
            }

            drop(lock);

            let mut scanned = 0;
            let mut up_to_date = Vec::new();
            let mut unconfirmed = Vec::new();
            {
                let any = self.ledger.any();
                let mut count = 0;
                let mut it = any.accounts_range(next..);
                while let Some((account, account_info)) = it.next() {
                    if count >= self.config.batch_size {
                        break;
                    }

                    self.stats.inc(StatType::BacklogScan, DetailType::Total);

                    let conf_info = any.confirmed().get_conf_info(&account).unwrap_or_default();

                    let is_unconfirmed = conf_info.height < account_info.block_count;
                    if is_unconfirmed {
                        unconfirmed.push(UnconfirmedInfo {
                            account,
                            account_info,
                            conf_info,
                        });
                    } else {
                        up_to_date.push(account);
                    }

                    scanned += 1;
                    next = account.inc_or_max();
                    count += 1;
                }
                done = any.accounts_range(next..).next().is_none();
            }

            self.stats
                .add(StatType::BacklogScan, DetailType::Scanned, scanned as u64);
            self.stats.add(
                StatType::BacklogScan,
                DetailType::Activated,
                unconfirmed.len() as u64,
            );

            // Notify about scanned and activated accounts without holding database transaction
            {
                let observers = self.up_to_date_observers.read().unwrap();
                for observer in &*observers {
                    observer(&up_to_date);
                }
            }
            {
                let observers = self.unconfirmed_observers.read().unwrap();
                for observer in &*observers {
                    observer(&unconfirmed);
                }
            }

            lock = self.mutex.lock().unwrap();
        }
        lock
    }
}

#[derive(Clone)]
pub struct UnconfirmedInfo {
    pub account: Account,
    pub account_info: AccountInfo,
    pub conf_info: ConfirmationHeightInfo,
}
