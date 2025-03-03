use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Condvar, Mutex,
    },
};

/** Distinct areas write locking is done, order is irrelevant */
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Writer {
    ConfirmationHeight,
    BlockProcessor,
    Pruning,
    VotingFinal,
    BoundedBacklog,
    OnlineReps,
    Testing, // Used in tests to emulate a write lock
}

pub struct WriteGuard {
    pub writer: Writer,
    guard_finish_callback: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl WriteGuard {
    pub fn new(writer: Writer, guard_finish_callback: Arc<dyn Fn() + Send + Sync>) -> Self {
        Self {
            writer,
            guard_finish_callback: Some(guard_finish_callback),
        }
    }

    pub fn release(&mut self) {
        if let Some(callback) = self.guard_finish_callback.take() {
            callback();
        }
    }

    pub fn is_owned(&self) -> bool {
        self.guard_finish_callback.is_some()
    }

    pub fn null() -> Self {
        Self {
            writer: Writer::Testing,
            guard_finish_callback: None,
        }
    }
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        self.release();
    }
}

pub struct WriteQueue {
    data: Arc<WriteQueueData>,
    guard_finish_callback: Arc<dyn Fn() + Send + Sync>,
    next: AtomicU64,
}

struct WriteQueueData {
    queue: Mutex<VecDeque<(Writer, u64)>>,
    condition: Condvar,
}

impl WriteQueue {
    pub fn new() -> Self {
        let data = Arc::new(WriteQueueData {
            queue: Mutex::new(VecDeque::new()),
            condition: Condvar::new(),
        });

        let data_clone = data.clone();

        Self {
            data,
            guard_finish_callback: Arc::new(move || {
                let mut guard = data_clone.queue.lock().unwrap();
                guard.pop_front();
                data_clone.condition.notify_all();
            }),
            next: AtomicU64::new(0),
        }
    }

    /// Blocks until we are at the head of the queue and blocks other waiters until write_guard goes out of scope
    pub fn wait(&self, writer: Writer) -> WriteGuard {
        let mut lk = self.data.queue.lock().unwrap();
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        lk.push_back((writer, id));

        let _result = self
            .data
            .condition
            .wait_while(lk, |queue| queue.front() != Some(&(writer, id)));

        self.create_write_guard(writer)
    }

    /// Returns true if this writer is anywhere in the queue. Currently only used in tests
    pub fn contains(&self, writer: Writer) -> bool {
        self.data
            .queue
            .lock()
            .unwrap()
            .iter()
            .any(|(w, _)| *w == writer)
    }

    fn create_write_guard(&self, writer: Writer) -> WriteGuard {
        WriteGuard::new(writer, self.guard_finish_callback.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicBool, Ordering},
        thread::{self, sleep},
        time::Duration,
    };

    use rsnano_core::utils::OneShotNotification;

    use super::*;

    #[test]
    fn first_request_succeeds() {
        let queue = WriteQueue::new();
        let guard = queue.wait(Writer::Pruning);
        assert!(queue.contains(Writer::Pruning));
        drop(guard);
        assert!(!queue.contains(Writer::Pruning));
    }

    #[test]
    fn second_request_waits() {
        let queue = WriteQueue::new();
        let guard = queue.wait(Writer::Pruning);
        let started = OneShotNotification::new();
        let ended = OneShotNotification::new();
        let second_wait_finished = AtomicBool::new(false);
        thread::scope(|s| {
            s.spawn(|| {
                started.notify(());
                queue.wait(Writer::BlockProcessor);
                second_wait_finished.store(true, Ordering::SeqCst);
                ended.notify(());
            });
            started.wait();
            sleep(Duration::from_millis(1));
            assert_eq!(second_wait_finished.load(Ordering::SeqCst), false);
            drop(guard);
            ended.wait();
        });
    }

    #[test]
    fn can_request_same_writer_twice() {
        let queue = WriteQueue::new();
        let guard = queue.wait(Writer::Pruning);
        let started = OneShotNotification::new();
        let ended = OneShotNotification::new();
        let second_wait_finished = AtomicBool::new(false);
        thread::scope(|s| {
            s.spawn(|| {
                started.notify(());
                queue.wait(Writer::Pruning);
                second_wait_finished.store(true, Ordering::SeqCst);
                ended.notify(());
            });
            started.wait();
            sleep(Duration::from_millis(1));
            assert_eq!(second_wait_finished.load(Ordering::SeqCst), false);
            drop(guard);
            ended.wait();
        });
    }
}
