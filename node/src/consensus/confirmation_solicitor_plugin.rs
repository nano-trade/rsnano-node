use std::{
    any::Any,
    sync::{Arc, Mutex},
};

use super::{
    confirm_req_sender::ConfirmReqSender,
    election::{Election, ElectionState},
    winner_block_broadcaster::WinnerBlockBroadcaster,
    AecTickerPlugin, BlockVoteRequest, BlockVoter, ConfirmationSolicitor,
};
use crate::{representatives::OnlineReps, transport::MessageFlooder};

pub(crate) struct ConfirmationSolicitorPlugin {
    pub(crate) message_flooder: MessageFlooder,
    pub(crate) online_reps: Arc<Mutex<OnlineReps>>,
    pub(crate) block_voter: Arc<BlockVoter>,
    pub(crate) winner_block_broadcaster: Arc<Mutex<WinnerBlockBroadcaster>>,
    pub(crate) confirm_req_sender: ConfirmReqSender,
}

impl ConfirmationSolicitorPlugin {
    #[cfg(test)]
    pub fn new_null() -> Self {
        Self {
            message_flooder: MessageFlooder::new_null(),
            online_reps: Arc::new(Mutex::new(OnlineReps::new_test_instance())),
            block_voter: Arc::new(BlockVoter::new_null()),
            winner_block_broadcaster: Arc::new(Mutex::new(WinnerBlockBroadcaster::new_null())),
            confirm_req_sender: ConfirmReqSender::new_null(),
        }
    }
}

impl AecTickerPlugin for ConfirmationSolicitorPlugin {
    fn process(&mut self, elections: &[Election]) {
        let peered_prs = self.online_reps.lock().unwrap().peered_principal_reps();

        // TODO don't clone flooder!'
        let flooder = self.message_flooder.clone();
        let mut solicitor = ConfirmationSolicitor::new(flooder);
        solicitor.prepare(&peered_prs);

        /*
         * Loop through active elections requesting confirmation
         *
         * Only up to a certain amount of elections are queued for confirmation request and block rebroadcasting.
         * The remaining elections can still be confirmed if votes arrive
         * Elections extending the soft config.size limit are flushed after a certain time-to-live cutoff
         * Flushed elections are later re-activated via frontier confirmation
         */
        for election in elections {
            match election.state() {
                ElectionState::Passive => {
                    let request = BlockVoteRequest {
                        block_hash: election.winner().hash(),
                        root: election.winner().root(),
                        vote_type: election.vote_type(),
                    };
                    //self.block_voter.try_vote(request);
                }
                ElectionState::Active => {
                    let request = BlockVoteRequest {
                        block_hash: election.winner().hash(),
                        root: election.winner().root(),
                        vote_type: election.vote_type(),
                    };
                    //self.block_voter.try_vote(request);
                    self.winner_block_broadcaster
                        .lock()
                        .unwrap()
                        .try_broadcast_winner(&election.winner().clone(), election.votes());
                    self.confirm_req_sender
                        .send_confirm_req(&mut solicitor, &election);
                }
                _ => {}
            }
        }

        solicitor.flush();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::{
        election::{ElectionBehavior, VoteType},
        BlockVoteRequest,
    };
    use rsnano_core::SavedBlock;
    use rsnano_nullable_clock::Timestamp;
    use std::time::Duration;

    //#[test]
    //fn vote_for_passive_block() {
    //    let mut plugin = ConfirmationSolicitorPlugin::new_null();
    //    let vote_tracker = plugin.block_voter.track();
    //    let block = SavedBlock::new_test_instance_with_key(1);
    //    let now = Timestamp::new_test_instance();
    //    let election = Election::new(
    //        block.clone(),
    //        ElectionBehavior::Manual,
    //        Duration::from_secs(1),
    //        now,
    //    );

    //    plugin.process(&[election]);

    //    let output = vote_tracker.output();
    //    assert_eq!(
    //        output,
    //        [BlockVoteRequest {
    //            block_hash: block.hash(),
    //            root: block.root(),
    //            vote_type: VoteType::NonFinal,
    //        },]
    //    );
    //}

    //#[test]
    //fn vote_for_active_block() {
    //    let mut plugin = ConfirmationSolicitorPlugin::new_null();
    //    let vote_tracker = plugin.block_voter.track();
    //    let block = SavedBlock::new_test_instance();
    //    let now = Timestamp::new_test_instance();
    //    let mut election = Election::new(
    //        block.clone(),
    //        ElectionBehavior::Manual,
    //        Duration::from_secs(1),
    //        now,
    //    );
    //    election.transition_active();

    //    plugin.process(&[election]);

    //    let output = vote_tracker.output();
    //    assert_eq!(
    //        output,
    //        [BlockVoteRequest {
    //            block_hash: block.hash(),
    //            root: block.root(),
    //            vote_type: VoteType::NonFinal,
    //        },]
    //    );
    //}
}
