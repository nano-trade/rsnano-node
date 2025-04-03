use std::{sync::Arc, thread::JoinHandle};

use rsnano_stats::Stats;

use super::{rebroadcast_processor::RebroadcastProcessor, VoteRebroadcastQueue};
use crate::transport::MessageFlooder;

/// Rebroadcasts votes that were created by other nodes
pub(crate) struct VoteRebroadcaster {
    queue: Arc<VoteRebroadcastQueue>,
    join_handle: Option<JoinHandle<()>>,
    message_flooder: Option<MessageFlooder>,
    stats: Arc<Stats>,
    vote_processed_callback: Option<Box<dyn Fn() + Send + Sync>>,
}

impl VoteRebroadcaster {
    pub(crate) fn new(
        queue: Arc<VoteRebroadcastQueue>,
        message_flooder: MessageFlooder,
        stats: Arc<Stats>,
    ) -> Self {
        Self {
            queue,
            join_handle: None,
            message_flooder: Some(message_flooder),
            stats,
            vote_processed_callback: None,
        }
    }

    pub fn start(&mut self) {
        let queue = self.queue.clone();
        let mut rebroadcast_processor =
            RebroadcastProcessor::new(self.message_flooder.take().unwrap(), self.stats.clone());
        let callback = self.vote_processed_callback.take();

        let handle = std::thread::Builder::new()
            .name("Vote rebroad".to_owned())
            .spawn(move || {
                while let Some(vote) = queue.dequeue_blocking() {
                    rebroadcast_processor.rebroadcast(&vote);
                    if let Some(cb) = &callback {
                        cb();
                    }
                }
            })
            .unwrap();
        self.join_handle = Some(handle);
    }

    pub fn stop(&mut self) {
        self.queue.stop();
        if let Some(handle) = self.join_handle.take() {
            handle.join().unwrap();
        }
    }

    #[allow(dead_code)]
    pub fn on_vote_processed(&mut self, callback: impl Fn() + Send + Sync + 'static) {
        self.vote_processed_callback = Some(Box::new(callback));
    }
}

impl Drop for VoteRebroadcaster {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        consensus::{RepTiers, RepTiersConsumer},
        transport::FloodEvent,
    };
    use rsnano_core::{utils::OneShotNotification, Vote};
    use rsnano_output_tracker::OutputTrackerMt;

    #[test]
    fn rebroadcast() {
        let (mut rebroadcaster, queue, flood_tracker) = create_fixture();

        let done = OneShotNotification::new();
        let done2 = done.clone();
        rebroadcaster.on_vote_processed(move || done2.notify(()));
        rebroadcaster.start();

        let vote = Arc::new(Vote::new_test_instance());
        let mut rep_tiers = RepTiers::default();
        rep_tiers.tier1.insert(vote.voter);
        queue.update_rep_tiers(rep_tiers);
        queue.enqueue(vote);

        done.wait();

        assert_eq!(flood_tracker.output().len(), 1);
    }

    fn create_fixture() -> (
        VoteRebroadcaster,
        Arc<VoteRebroadcastQueue>,
        Arc<OutputTrackerMt<FloodEvent>>,
    ) {
        let queue = Arc::new(VoteRebroadcastQueue::default());
        let message_flooder = MessageFlooder::new_null();
        let flood_tracker = message_flooder.track_floods();
        let stats = Arc::new(Stats::default());
        let rebroadcaster = VoteRebroadcaster::new(queue.clone(), message_flooder, stats);

        (rebroadcaster, queue, flood_tracker)
    }
}
