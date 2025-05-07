use std::{
    any::Any,
    sync::{Arc, Mutex, RwLock},
};

use rsnano_network::Network;

use super::{
    confirm_req_sender::ConfirmReqSender,
    election::{Election, ElectionState},
    winner_block_broadcaster::WinnerBlockBroadcaster,
    AecTickerPlugin, BlockVoter, ConfirmationSolicitor,
};
use crate::{config::NetworkParams, representatives::OnlineReps, transport::MessageFlooder};

pub(crate) struct ConfirmationSolicitorPlugin {
    pub(crate) message_flooder: MessageFlooder,
    pub(crate) network_params: NetworkParams,
    pub(crate) online_reps: Arc<Mutex<OnlineReps>>,
    pub(crate) network: Arc<RwLock<Network>>,
    pub(crate) block_voter: Arc<BlockVoter>,
    pub(crate) winner_block_broadcaster: WinnerBlockBroadcaster,
    pub(crate) confirm_req_sender: ConfirmReqSender,
}

impl ConfirmationSolicitorPlugin {
    #[cfg(test)]
    pub fn new_null() -> Self {
        use rsnano_core::Networks;
        Self {
            message_flooder: MessageFlooder::new_null(),
            network_params: NetworkParams::new(Networks::NanoLiveNetwork),
            online_reps: Arc::new(Mutex::new(OnlineReps::new_test_instance())),
            network: Arc::new(RwLock::new(Network::new_test_instance())),
            block_voter: Arc::new(BlockVoter::new_null()),
            winner_block_broadcaster: WinnerBlockBroadcaster::new_null(),
            confirm_req_sender: ConfirmReqSender::new_null(),
        }
    }
}

impl AecTickerPlugin for ConfirmationSolicitorPlugin {
    fn process(&mut self, elections: &[Election]) {
        let peered_prs = self.online_reps.lock().unwrap().peered_principal_reps();

        // TODO don't clone flooder!'
        let flooder = self.message_flooder.clone();
        let mut solicitor =
            ConfirmationSolicitor::new(&self.network_params, &self.network, flooder);
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
                    self.block_voter.try_vote_for_block(
                        election.winner().hash(),
                        election.winner().root(),
                        election.vote_type(),
                    );
                }
                ElectionState::Active => {
                    self.block_voter.try_vote_for_block(
                        election.winner().hash(),
                        election.winner().root(),
                        election.vote_type(),
                    );
                    self.winner_block_broadcaster
                        .try_broadcast_winner(&mut solicitor, &election);
                    self.confirm_req_sender
                        .send_confirm_req(&mut solicitor, &election);
                }
                _ => {}
            }

            if election.state() == ElectionState::Confirmed {
                // Ensure election winner is broadcasted
                self.winner_block_broadcaster
                    .try_broadcast_winner(&mut solicitor, &election);
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

    #[test]
    fn vote_for_passive_block() {
        let mut plugin = ConfirmationSolicitorPlugin::new_null();
        let vote_tracker = plugin.block_voter.track();
        let block = SavedBlock::new_test_instance_with_key(1);
        let now = Timestamp::new_test_instance();
        let election = Election::new(
            block.clone(),
            ElectionBehavior::Manual,
            Duration::from_secs(1),
            now,
        );

        plugin.process(&[election]);

        let output = vote_tracker.output();
        assert_eq!(
            output,
            [BlockVoteRequest {
                block_hash: block.hash(),
                root: block.root(),
                vote_type: VoteType::NonFinal,
            },]
        );
    }

    #[test]
    fn vote_for_active_block() {
        let mut plugin = ConfirmationSolicitorPlugin::new_null();
        let vote_tracker = plugin.block_voter.track();
        let block = SavedBlock::new_test_instance();
        let now = Timestamp::new_test_instance();
        let mut election = Election::new(
            block.clone(),
            ElectionBehavior::Manual,
            Duration::from_secs(1),
            now,
        );
        election.transition_active();

        plugin.process(&[election]);

        let output = vote_tracker.output();
        assert_eq!(
            output,
            [BlockVoteRequest {
                block_hash: block.hash(),
                root: block.root(),
                vote_type: VoteType::NonFinal,
            },]
        );
    }
}
