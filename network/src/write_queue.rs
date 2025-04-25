use crate::{channel_stats::ChannelStats, TrafficType};
use rsnano_core::utils::FairQueue;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::Notify;

pub struct WriteQueue {
    queue: Mutex<FairQueue<TrafficType, Entry>>,
    notify_enqueued: Notify,
    notify_dequeued: Notify,
    closed: AtomicBool,
    stats: Arc<ChannelStats>,
}

impl WriteQueue {
    pub(crate) fn new(max_size: usize, stats: Arc<ChannelStats>) -> Self {
        Self {
            queue: Mutex::new(FairQueue::new(
                move |_| max_size,
                |traffic_type| match traffic_type {
                    TrafficType::BlockBroadcast | TrafficType::VoteRebroadcast => 1,
                    _ => 4,
                },
            )),
            notify_enqueued: Notify::new(),
            notify_dequeued: Notify::new(),
            closed: AtomicBool::new(false),
            stats,
        }
    }

    pub async fn insert(&self, buffer: Arc<Vec<u8>>, traffic_type: TrafficType) {
        loop {
            if self.closed.load(Ordering::SeqCst) {
                return;
            }

            {
                let mut guard = self.queue.lock().unwrap();
                if guard.free_capacity(&traffic_type) > 0 {
                    let entry = Entry { buffer };
                    guard.push(traffic_type, entry);
                    break;
                }
            }

            self.notify_dequeued.notified().await;
        }

        self.notify_enqueued.notify_one();
    }

    /// returns: inserted
    pub fn try_insert(&self, buffer: Arc<Vec<u8>>, traffic_type: TrafficType) -> bool {
        if self.closed.load(Ordering::SeqCst) {
            return false;
        }
        let entry = Entry { buffer };
        let inserted = self.queue.lock().unwrap().push(traffic_type, entry);

        if inserted {
            self.notify_enqueued.notify_one();
        }

        inserted
    }

    pub fn free_capacity(&self, traffic_type: TrafficType) -> usize {
        self.queue.lock().unwrap().free_capacity(&traffic_type)
    }

    pub fn len(&self) -> usize {
        self.queue.lock().unwrap().len()
    }

    pub async fn pop(&self) -> Option<Entry> {
        let entry;
        let traffic_type;

        loop {
            if self.closed.load(Ordering::SeqCst) {
                return None;
            }

            let result = self.queue.lock().unwrap().next();
            if let Some((ttype, ent)) = result {
                traffic_type = ttype;
                entry = ent;
                break;
            }

            self.notify_enqueued.notified().await;
        }

        self.notify_dequeued.notify_one();
        self.stats
            .write_succeeded
            .fetch_add(entry.buffer.len(), Ordering::Relaxed);
        self.stats.sent_by_type[traffic_type as usize]
            .fetch_add(entry.buffer.len(), Ordering::Relaxed);
        Some(entry)
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.notify_enqueued.notify_one();
        self.notify_dequeued.notify_one();
    }
}

impl Drop for WriteQueue {
    fn drop(&mut self) {
        self.close();
    }
}

pub struct Entry {
    pub buffer: Arc<Vec<u8>>,
}
