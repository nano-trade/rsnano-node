use super::BlockContext;
use rsnano_ledger::BlockStatus;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex, RwLock,
    },
    thread::JoinHandle,
};

#[derive(Clone)]
pub struct LedgerNotifications {
    callbacks: Arc<RwLock<Callbacks>>,
}

impl LedgerNotifications {
    pub(crate) fn new() -> (Self, LedgerNotifier) {
        let callbacks = Arc::new(RwLock::new(Callbacks::default()));
        let notifier = LedgerNotifier {
            callbacks: callbacks.clone(),
        };
        let notifications = Self { callbacks };
        (notifications, notifier)
    }

    /// All processed blocks including forks, rejected etc
    pub fn on_blocks_processed(
        &self,
        observer: Box<dyn Fn(&[(BlockStatus, Arc<BlockContext>)]) + Send + Sync>,
    ) {
        self.callbacks
            .write()
            .unwrap()
            .batch_processed
            .push(observer);
    }
}

#[derive(Default)]
struct Callbacks {
    batch_processed: Vec<Box<dyn Fn(&[(BlockStatus, Arc<BlockContext>)]) + Send + Sync>>,
}

/// publishes ledger notifications
// TODO: Remove clone!
#[derive(Clone)]
pub(crate) struct LedgerNotifier {
    callbacks: Arc<RwLock<Callbacks>>,
}

impl LedgerNotifier {
    pub fn notify_batch_processed(&self, blocks: &[(BlockStatus, Arc<BlockContext>)]) {
        let guard = self.callbacks.read().unwrap();
        for observer in guard.batch_processed.iter() {
            observer(&blocks);
        }
    }
}

pub(crate) enum LedgerEvent2 {
    BatchProcessed(Vec<(BlockStatus, Arc<BlockContext>)>),
}

pub(crate) struct LedgerNotificationQueue {
    events: Arc<Mutex<VecDeque<LedgerEvent2>>>,
    changed: Condvar,
    stopped: AtomicBool,
    max_size: usize,
}

impl LedgerNotificationQueue {
    pub(crate) fn new(max_size: usize) -> (Self, LedgerNotifications, LedgerNotifier) {
        let (notifications, notifier) = LedgerNotifications::new();
        let queue = Self {
            events: Arc::new(Mutex::new(VecDeque::new())),
            changed: Condvar::new(),
            stopped: AtomicBool::new(false),
            max_size,
        };
        (queue, notifications, notifier)
    }

    pub fn pop(&self) -> Option<LedgerEvent2> {
        let mut guard = self.events.lock().unwrap();
        if guard.is_empty() {
            guard = self
                .changed
                .wait_while(guard, |i| {
                    i.is_empty() && !self.stopped.load(Ordering::SeqCst)
                })
                .unwrap();
        }
        if self.stopped.load(Ordering::SeqCst) {
            return None;
        }
        let result = guard.pop_front();
        drop(guard);
        self.changed.notify_all();
        result
    }

    /// Returns if waiting happened
    pub fn wait(&self) -> bool {
        let guard = self.events.lock().unwrap();
        let predicate = |i: &VecDeque<LedgerEvent2>| {
            i.len() >= self.max_size && !self.stopped.load(Ordering::SeqCst)
        };

        if predicate(&guard) {
            return false;
        }
        drop(self.changed.wait_while(guard, |g| predicate(g)).unwrap());
        true
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    pub fn notify_batch_processed(&self, blocks: Vec<(BlockStatus, Arc<BlockContext>)>) {
        self.push_event(LedgerEvent2::BatchProcessed(blocks));
    }

    fn push_event(&self, event: LedgerEvent2) {
        if self.stopped.load(Ordering::SeqCst) {
            return;
        }
        self.events.lock().unwrap().push_back(event);
        self.changed.notify_one();
    }

    fn stop(&self) {
        {
            let mut guard = self.events.lock().unwrap();
            guard.clear();
            self.stopped.store(true, Ordering::SeqCst);
        }
        self.changed.notify_one();
    }
}

pub(crate) struct LedgerNotificationProcessor {
    queue: Arc<LedgerNotificationQueue>,
    notifier: LedgerNotifier,
}

impl LedgerNotificationProcessor {
    pub(crate) fn new(
        max_queue_len: usize,
    ) -> (Self, Arc<LedgerNotificationQueue>, LedgerNotifications) {
        let (queue, notifications, notifier) = LedgerNotificationQueue::new(max_queue_len);
        let queue = Arc::new(queue);
        let processor = Self {
            queue: queue.clone(),
            notifier,
        };
        (processor, queue, notifications)
    }

    pub fn process_one(&self) -> bool {
        let Some(event) = self.queue.pop() else {
            return false;
        };

        match event {
            LedgerEvent2::BatchProcessed(batch) => self.notifier.notify_batch_processed(&batch),
        }

        true
    }

    pub fn stop(&self) {
        self.queue.stop();
    }
}

pub(crate) struct LedgerNotificationThread {
    processor: Arc<LedgerNotificationProcessor>,
    handle: Option<JoinHandle<()>>,
}

impl LedgerNotificationThread {
    pub(crate) fn new(
        max_queue_len: usize,
    ) -> (Self, Arc<LedgerNotificationQueue>, LedgerNotifications) {
        let (processor, queue, notifications) = LedgerNotificationProcessor::new(max_queue_len);
        let thread = Self {
            processor: Arc::new(processor),
            handle: None,
        };
        (thread, queue, notifications)
    }

    pub fn start(&mut self) {
        let processor = self.processor.clone();
        self.handle = Some(
            std::thread::Builder::new()
                .name("Ledger notif".to_owned())
                .spawn(move || while processor.process_one() {})
                .unwrap(),
        );
    }

    pub fn stop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        self.processor.stop();
        handle.join().unwrap();
    }
}

impl Drop for LedgerNotificationThread {
    fn drop(&mut self) {
        self.stop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Condvar,
        },
        time::Duration,
    };

    #[test]
    fn empty() {
        let (queue, _, _) = LedgerNotificationQueue::new(8);
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn enqueue_batch_processed() {
        let (queue, notifications, _) = LedgerNotificationQueue::new(8);
        let notified = Arc::new(AtomicBool::new(false));
        let notified2 = notified.clone();
        notifications.on_blocks_processed(Box::new(move |_| {
            notified2.store(true, Ordering::SeqCst);
        }));

        queue.notify_batch_processed(vec![]);

        assert_eq!(queue.len(), 1);
        assert_eq!(notified.load(Ordering::SeqCst), false);
    }

    #[test]
    fn process_event() {
        let (processor, queue, notifications) = LedgerNotificationProcessor::new(8);

        let notified = Arc::new(AtomicBool::new(false));
        let notified2 = notified.clone();
        notifications.on_blocks_processed(Box::new(move |_| {
            notified2.store(true, Ordering::SeqCst);
        }));

        queue.notify_batch_processed(vec![]);

        processor.process_one();

        assert_eq!(queue.len(), 0, "queue wasn't drained");
        assert!(
            notified.load(Ordering::SeqCst),
            "event handler wasn't called"
        );
    }

    #[test]
    fn notification_thread() {
        let (mut thread, queue, notifications) = LedgerNotificationThread::new(8);

        let notified = Arc::new((Condvar::new(), Mutex::new(0)));
        let notified2 = notified.clone();
        notifications.on_blocks_processed(Box::new(move |_| {
            *notified2.1.lock().unwrap() += 1;
            notified2.0.notify_one();
        }));

        queue.notify_batch_processed(vec![]);

        thread.start();

        queue.notify_batch_processed(vec![]);
        queue.notify_batch_processed(vec![]);

        let guard = notified.1.lock().unwrap();
        let result = notified
            .0
            .wait_timeout_while(guard, Duration::from_secs(5), |i| *i < 3)
            .unwrap()
            .1;

        assert_eq!(*notified.1.lock().unwrap(), 3);
        assert!(!result.timed_out());
    }
}
