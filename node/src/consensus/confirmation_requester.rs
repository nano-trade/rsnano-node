use std::{
    cmp::max,
    sync::{Arc, Condvar, Mutex},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use rsnano_stats::{DetailType, StatType, Stats};

use super::ActiveElections;

/// Periodically request confirmations for active elections from peered representatives
pub(crate) struct ConfirmationRequester {
    active_elections: Arc<ActiveElections>,
    stats: Arc<Stats>,
    handle: Option<JoinHandle<()>>,
    stopped: Arc<(Mutex<bool>, Condvar)>,
    loop_interval: Duration,
}

impl ConfirmationRequester {
    pub(crate) fn new(active_elections: Arc<ActiveElections>, stats: Arc<Stats>) -> Self {
        Self {
            active_elections,
            stats,
            stopped: Arc::new((Mutex::new(true), Condvar::new())),
            handle: None,
            loop_interval: Duration::from_millis(300),
        }
    }

    pub fn set_loop_interval(&mut self, interval: Duration) {
        self.loop_interval = interval;
    }

    pub fn start(&mut self) {
        assert!(self.handle.is_none());
        *self.stopped.0.lock().unwrap() = false;
        let mut request_loop = RequestLoop {
            active_elections: self.active_elections.clone(),
            stats: self.stats.clone(),
            stopped: self.stopped.clone(),
            loop_interval: self.loop_interval,
        };
        self.handle = Some(
            std::thread::Builder::new()
                .name("Request loop".to_string())
                .spawn(Box::new(move || {
                    request_loop.run();
                }))
                .unwrap(),
        );
    }

    pub fn stop(&mut self) {
        *self.stopped.0.lock().unwrap() = true;
        self.stopped.1.notify_all();
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap();
        }
    }
}

struct RequestLoop {
    active_elections: Arc<ActiveElections>,
    stats: Arc<Stats>,
    stopped: Arc<(Mutex<bool>, Condvar)>,
    loop_interval: Duration,
}

impl RequestLoop {
    fn run(&mut self) {
        let mut stopped = self.stopped.0.lock().unwrap();
        while !*stopped {
            drop(stopped);
            let now = Instant::now();
            self.stats.inc(StatType::Active, DetailType::Loop);

            self.active_elections.request_confirm();

            stopped = self.stopped.0.lock().unwrap();

            let min_sleep = self.loop_interval / 2;

            let wait_duration = max(
                min_sleep,
                (now + self.loop_interval).saturating_duration_since(now),
            );

            stopped = self
                .stopped
                .1
                .wait_timeout_while(stopped, wait_duration, |s| !*s)
                .unwrap()
                .0
        }
    }
}
