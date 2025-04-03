use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
};

use super::WalletRepsConsumer;
use crate::{
    consensus::{RepTiers, RepTiersConsumer},
    wallets::WalletRepresentatives,
};
use rsnano_core::{
    utils::{ContainerInfo, ContainerInfoProvider},
    BlockHash, PublicKey, Vote, VoteCode,
};
use rsnano_stats::{DetailType, StatType, Stats};

pub(crate) struct VoteRebroadcastQueueBuilder {
    stats: Option<Arc<Stats>>,
    block_when_empty: bool,
    max_len: usize,
}

impl VoteRebroadcastQueueBuilder {
    pub fn stats(mut self, stats: Arc<Stats>) -> Self {
        self.stats = Some(stats);
        self
    }

    #[allow(dead_code)]
    pub fn block_when_empty(mut self, block: bool) -> Self {
        self.block_when_empty = block;
        self
    }

    #[allow(dead_code)]
    pub fn max_len(mut self, max: usize) -> Self {
        self.max_len = max;
        self
    }

    pub fn finish(self) -> VoteRebroadcastQueue {
        let stats = self.stats.unwrap_or_default();
        VoteRebroadcastQueue::new(stats, self.block_when_empty, self.max_len)
    }
}

impl Default for VoteRebroadcastQueueBuilder {
    fn default() -> Self {
        Self {
            stats: Default::default(),
            block_when_empty: true,
            max_len: VoteRebroadcastQueue::DEFAULT_MAX_QUEUE,
        }
    }
}

pub(crate) struct VoteRebroadcastQueue {
    queue: Mutex<QueueImpl>,
    enqueued: Condvar,
    stopped: AtomicBool,
    stats: Arc<Stats>,
    block_when_empty: bool,
    max_len: usize,
}

impl VoteRebroadcastQueue {
    const DEFAULT_MAX_QUEUE: usize = 1024 * 16;

    pub fn build() -> VoteRebroadcastQueueBuilder {
        Default::default()
    }

    fn new(stats: Arc<Stats>, block_when_empty: bool, max_queue: usize) -> Self {
        Self {
            queue: Mutex::new(Default::default()),
            enqueued: Condvar::new(),
            stopped: AtomicBool::new(false),
            stats,
            block_when_empty,
            max_len: max_queue,
        }
    }

    pub fn try_enqueue(&self, vote: &Arc<Vote>, results: &HashMap<BlockHash, VoteCode>) {
        let processed = results.iter().any(|(_, code)| *code == VoteCode::Vote);
        if processed {
            self.enqueue(vote.clone());
        }
    }

    pub fn enqueue(&self, vote: Arc<Vote>) {
        let added = {
            let mut queue = self.queue.lock().unwrap();

            if self.stopped() {
                return;
            }

            if queue.len() < self.max_len && !self.stopped() {
                queue.enqueue(vote)
            } else {
                false
            }
        };

        if added {
            self.enqueued.notify_all();
        } else {
            self.stats
                .inc(StatType::VoteRebroadcaster, DetailType::Overfill);
        }
    }

    /// This will wait for a vote to be enqueued or for the
    /// queue to be stopped.
    pub fn dequeue_blocking(&self) -> Option<Arc<Vote>> {
        let mut queue = self.queue.lock().unwrap();
        if queue.len() == 0 && !self.block_when_empty {
            return None;
        }

        queue = self
            .enqueued
            .wait_while(queue, |q| q.len() == 0 && !self.stopped())
            .unwrap();

        return queue.dequeue();
    }

    pub fn stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    pub fn stop(&self) {
        {
            let mut guard = self.queue.lock().unwrap();
            guard.clear();
            self.stopped.store(true, Ordering::SeqCst);
        }
        self.enqueued.notify_all();
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.queue.lock().unwrap().len()
    }
}

impl Default for VoteRebroadcastQueue {
    fn default() -> Self {
        Self::build().finish()
    }
}

impl ContainerInfoProvider for VoteRebroadcastQueue {
    fn container_info(&self) -> ContainerInfo {
        let queue = self.queue.lock().unwrap();
        [("queue", queue.len(), 0)].into()
    }
}

impl RepTiersConsumer for VoteRebroadcastQueue {
    fn update_rep_tiers(&self, new_tiers: RepTiers) {
        self.queue.lock().unwrap().set_rep_tiers(new_tiers);
    }
}

impl WalletRepsConsumer for VoteRebroadcastQueue {
    fn update_wallet_reps(&self, reps: &WalletRepresentatives) {
        let mut queue = self.queue.lock().unwrap();
        queue.is_close_to_pr(reps.have_half_rep());
        queue.set_local_reps(reps.accounts.iter().map(|a| a.into()).collect());
    }
}

#[derive(Default)]
struct QueueImpl {
    queue: VecDeque<Arc<Vote>>,
    rep_tiers: RepTiers,
    is_close_to_pr: bool,
    local_reps: HashSet<PublicKey>,
}

impl QueueImpl {
    fn enqueue(&mut self, vote: Arc<Vote>) -> bool {
        if self.is_close_to_pr {
            // Enable vote rebroadcasting only if the node does not host a representative
            return false;
        }

        if self.local_reps.contains(&vote.voter) {
            // Don't republish votes created by this node
            return false;
        }

        // Do not rebroadcast votes from non-principal representatives
        // TODO

        self.queue.push_back(vote);
        true
    }

    fn dequeue(&mut self) -> Option<Arc<Vote>> {
        self.queue.pop_front()
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    fn clear(&mut self) {
        self.queue.clear();
    }

    fn is_close_to_pr(&mut self, is_pr: bool) {
        self.is_close_to_pr = is_pr;
    }

    fn set_local_reps(&mut self, reps: HashSet<PublicKey>) {
        self.local_reps = reps;
    }

    fn set_rep_tiers(&mut self, new_tiers: RepTiers) {
        self.rep_tiers = new_tiers;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty() {
        let queue = VoteRebroadcastQueue::build().finish();
        assert_eq!(queue.len(), 0);
        assert_eq!(queue.max_len, VoteRebroadcastQueue::DEFAULT_MAX_QUEUE);
    }

    #[test]
    fn enqueue_and_dequeue_a_vote() {
        let queue = VoteRebroadcastQueue::build().finish();
        queue.enqueue(test_vote());
        assert_eq!(queue.len(), 1);

        let dequeued = queue.dequeue_blocking();
        assert!(dequeued.is_some());
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn dequeue_waits_when_queue_empty() {
        let queue = VoteRebroadcastQueue::build().finish();
        let notify = Condvar::new();
        let waiting = Mutex::new(false);
        let mut dequeued = None;

        std::thread::scope(|s| {
            // spawn blocking dequeue
            s.spawn(|| {
                *waiting.lock().unwrap() = true;
                notify.notify_one();
                dequeued = queue.dequeue_blocking();
            });

            // enqueue when waiting
            {
                let guard = waiting.lock().unwrap();
                drop(notify.wait_while(guard, |i| !*i).unwrap());
            }
            queue.enqueue(test_vote());
        });

        assert!(dequeued.is_some());
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn disable_blocking_dequeue() {
        let queue = VoteRebroadcastQueue::build()
            .block_when_empty(false)
            .finish();

        let result = queue.dequeue_blocking();
        assert!(result.is_none());
    }

    #[test]
    fn stop() {
        let queue = VoteRebroadcastQueue::build().finish();
        queue.enqueue(test_vote());

        queue.stop();

        assert!(queue.stopped());
        assert_eq!(queue.len(), 0);
        assert!(queue.dequeue_blocking().is_none());

        queue.enqueue(test_vote());
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn max_len() {
        let queue = VoteRebroadcastQueue::build().max_len(2).finish();
        queue.enqueue(test_vote());
        queue.enqueue(test_vote());
        assert_eq!(queue.len(), 2);

        queue.enqueue(test_vote());
        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn container_info() {
        let queue = VoteRebroadcastQueue::build().max_len(2).finish();
        queue.enqueue(test_vote());
        let info = queue.container_info();
        let expected: ContainerInfo = [("queue", 1, 0)].into();
        assert_eq!(info, expected);
    }

    #[test]
    fn ignore_unprocessed_vote() {
        let queue = VoteRebroadcastQueue::build().finish();
        let mut results = HashMap::new();
        results.insert(BlockHash::from(1), VoteCode::Invalid);
        results.insert(BlockHash::from(2), VoteCode::Replay);
        results.insert(BlockHash::from(3), VoteCode::Indeterminate);
        results.insert(BlockHash::from(4), VoteCode::Ignored);

        queue.try_enqueue(&test_vote(), &results);

        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn enqueue_processed_vote() {
        let queue = VoteRebroadcastQueue::build().finish();
        let mut results = HashMap::new();
        results.insert(BlockHash::from(1), VoteCode::Invalid);
        results.insert(BlockHash::from(2), VoteCode::Replay);
        results.insert(BlockHash::from(3), VoteCode::Indeterminate);
        results.insert(BlockHash::from(4), VoteCode::Ignored);

        //This means a processed vote:
        results.insert(BlockHash::from(5), VoteCode::Vote);

        queue.try_enqueue(&test_vote(), &results);

        assert_eq!(queue.len(), 1);
    }

    fn test_vote() -> Arc<Vote> {
        Arc::new(Vote::new_test_instance())
    }
}
