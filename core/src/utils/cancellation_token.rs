use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};

use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};

#[derive(Clone)]
pub struct CancellationToken {
    strategy: Arc<CancellationTokenStrategy>,
    wait_listener: Arc<OutputListenerMt<Duration>>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            strategy: Arc::new(CancellationTokenStrategy::Real(CancellationTokenImpl {
                mutex: Mutex::new(()),
                condition: Condvar::new(),
                stopped: AtomicBool::new(false),
            })),
            wait_listener: Arc::new(OutputListenerMt::new()),
        }
    }

    pub fn new_null() -> Self {
        Self::new_null_with_uncancelled_waits(usize::MAX)
    }

    pub fn new_null_with_uncancelled_waits(uncancelled_wait_count: usize) -> Self {
        Self {
            strategy: Arc::new(CancellationTokenStrategy::Nulled(
                CancellationTokenStub::new(uncancelled_wait_count),
            )),
            wait_listener: Arc::new(OutputListenerMt::new()),
        }
    }

    pub fn wait_for_cancellation(&self, timeout: Duration) -> bool {
        self.wait_listener.emit(timeout);
        match &*self.strategy {
            CancellationTokenStrategy::Real(i) => i.wait_for_cancellation(timeout),
            CancellationTokenStrategy::Nulled(i) => i.wait_for_cancellation(),
        }
    }

    pub fn cancel(&self) {
        match &*self.strategy {
            CancellationTokenStrategy::Real(i) => i.cancel(),
            CancellationTokenStrategy::Nulled(_) => {}
        }
    }

    pub fn is_cancelled(&self) -> bool {
        match &*self.strategy {
            CancellationTokenStrategy::Real(i) => i.is_cancelled(),
            CancellationTokenStrategy::Nulled(i) => i.is_cancelled(),
        }
    }

    pub fn track_waits(&self) -> Arc<OutputTrackerMt<Duration>> {
        self.wait_listener.track()
    }
}

enum CancellationTokenStrategy {
    Real(CancellationTokenImpl),
    Nulled(CancellationTokenStub),
}

struct CancellationTokenImpl {
    mutex: Mutex<()>,
    condition: Condvar,
    stopped: AtomicBool,
}

impl CancellationTokenImpl {
    fn wait_for_cancellation(&self, timeout: Duration) -> bool {
        let guard = self.mutex.lock().unwrap();
        if self.is_cancelled() {
            return true;
        }

        drop(
            self.condition
                .wait_timeout_while(guard, timeout, |_| !self.is_cancelled())
                .unwrap()
                .0,
        );

        self.is_cancelled()
    }

    fn cancel(&self) {
        {
            let _guard = self.mutex.lock().unwrap();
            self.stopped.store(true, Ordering::SeqCst);
        }
        self.condition.notify_all();
    }

    fn is_cancelled(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }
}

struct CancellationTokenStub {
    uncancelled_waits: Mutex<usize>,
    cancelled: AtomicBool,
}

impl CancellationTokenStub {
    fn new(uncancelled_waits: usize) -> Self {
        Self {
            cancelled: AtomicBool::new(uncancelled_waits == 0),
            uncancelled_waits: Mutex::new(uncancelled_waits),
        }
    }

    fn wait_for_cancellation(&self) -> bool {
        let mut waits = self.uncancelled_waits.lock().unwrap();
        if *waits > 0 {
            *waits -= 1;
            false
        } else {
            self.cancelled.store(true, Ordering::SeqCst);
            true
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_be_nulled() {
        let token = CancellationToken::new_null();
        assert_eq!(token.wait_for_cancellation(Duration::MAX), false);
        assert_eq!(token.is_cancelled(), false);
        assert_eq!(token.wait_for_cancellation(Duration::MAX), false);
        assert_eq!(token.is_cancelled(), false);
    }

    #[test]
    fn nulled_cancellation_token_returns_configured_responses() {
        let token = CancellationToken::new_null_with_uncancelled_waits(2);

        assert_eq!(token.wait_for_cancellation(Duration::MAX), false);
        assert_eq!(token.is_cancelled(), false);
        assert_eq!(token.wait_for_cancellation(Duration::MAX), false);
        assert_eq!(token.is_cancelled(), false);
        assert_eq!(token.wait_for_cancellation(Duration::MAX), true);
        assert_eq!(token.is_cancelled(), true);
        assert_eq!(token.wait_for_cancellation(Duration::MAX), true);
        assert_eq!(token.is_cancelled(), true);
    }

    #[test]
    fn can_track_waits() {
        let token = CancellationToken::new_null();
        let wait_tracker = token.track_waits();
        let duration = Duration::from_secs(123);

        token.wait_for_cancellation(duration);

        assert_eq!(wait_tracker.output(), [duration]);
    }
}
