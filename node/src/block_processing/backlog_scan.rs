use std::{
    cmp::max,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Condvar, Mutex, MutexGuard, RwLock,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use rsnano_core::{Account, AccountInfo, ConfirmationHeightInfo};
use rsnano_ledger::{AnySet, ConfirmedSet, Ledger};
use rsnano_network::bandwidth_limiter::RateLimiter;
use rsnano_stats::{StatsCollection, StatsSource};

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

impl BacklogScanConfig {
    fn wait_time(&self) -> Duration {
        let wait_time =
            Duration::from_millis(1000 / max(self.rate_limit / self.batch_size, 1) as u64 / 2);
        max(wait_time, Duration::from_millis(10))
    }
}

/// Continuously scan the ledger for unconfirmed blocks and activate them
pub struct BacklogScan {
    ledger: Arc<Ledger>,
    stats: Arc<BacklogScanStats>,

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
    pub(crate) fn new(config: BacklogScanConfig, ledger: Arc<Ledger>) -> Self {
        Self {
            config,
            ledger,
            stats: Arc::new(Default::default()),
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
    stats: Arc<BacklogScanStats>,
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
                self.stats.looped.fetch_add(1, Ordering::Relaxed);

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
            self.wait_for_rate_limiter(lock);
            let result = Self::scan_batch(&self.ledger, next, self.config.batch_size);
            done = result.done;
            next = result.next;
            self.add_stats(&result);
            self.notify_observers(result);
            lock = self.mutex.lock().unwrap();
        }
        lock
    }

    fn wait_for_rate_limiter<'a>(&'a self, mut lock: MutexGuard<'a, BacklogScanFlags>) {
        // Wait for the rate limiter
        while !self.limiter.should_pass(self.config.batch_size) {
            lock = self
                .condition
                .wait_timeout_while(lock, self.config.wait_time(), |i| !i.stopped)
                .unwrap()
                .0;

            if lock.stopped {
                break;
            }
        }
    }

    fn scan_batch(ledger: &Ledger, next: Account, batch_size: usize) -> BacklogScanResult {
        let mut result = BacklogScanResult {
            next,
            done: true,
            ..Default::default()
        };
        let any = ledger.any();
        let mut it = any.accounts_range(result.next..);
        while let Some((account, account_info)) = it.next() {
            let conf_info = any.confirmed().get_conf_info(&account).unwrap_or_default();
            let is_confirmed = conf_info.height >= account_info.block_count;

            if is_confirmed {
                result.fully_confirmed.push(account);
            } else {
                result.unconfirmed.push(UnconfirmedInfo {
                    account,
                    account_info,
                    conf_info,
                });
            }

            result.next = account.inc_or_max();
            if result.len() >= batch_size {
                break;
            }
        }
        result.done = it.next().is_none();
        result
    }

    fn add_stats(&self, result: &BacklogScanResult) {
        self.stats
            .scanned
            .fetch_add(result.len() as u64, Ordering::Relaxed);

        self.stats
            .activated
            .fetch_add(result.unconfirmed.len() as u64, Ordering::Relaxed);
    }

    fn notify_observers(&self, result: BacklogScanResult) {
        {
            let observers = self.up_to_date_observers.read().unwrap();
            for observer in &*observers {
                observer(&result.fully_confirmed);
            }
        }
        {
            let observers = self.unconfirmed_observers.read().unwrap();
            for observer in &*observers {
                observer(&result.unconfirmed);
            }
        }
    }
}

impl StatsSource for BacklogScan {
    fn collect_stats(&self, result: &mut StatsCollection) {
        self.stats.collect_stats(result);
    }
}

#[derive(Default)]
pub struct BacklogScanResult {
    fully_confirmed: Vec<Account>,
    unconfirmed: Vec<UnconfirmedInfo>,
    next: Account,
    done: bool,
}

impl BacklogScanResult {
    pub fn len(&self) -> usize {
        self.fully_confirmed.len() + self.unconfirmed.len()
    }
}

#[derive(Clone)]
pub struct UnconfirmedInfo {
    pub account: Account,
    pub account_info: AccountInfo,
    pub conf_info: ConfirmationHeightInfo,
}

#[derive(Default)]
struct BacklogScanStats {
    looped: AtomicU64,
    scanned: AtomicU64,
    activated: AtomicU64,
}

impl StatsSource for BacklogScanStats {
    fn collect_stats(&self, result: &mut StatsCollection) {
        result.insert("backlog_scan", "loop", self.looped.load(Ordering::Relaxed));
        result.insert(
            "backlog_scan",
            "scanned",
            self.scanned.load(Ordering::Relaxed),
        );
        result.insert(
            "backlog_scan",
            "activated",
            self.activated.load(Ordering::Relaxed),
        );
    }
}
