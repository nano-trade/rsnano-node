use std::sync::{Arc, Mutex, RwLock};

use rsnano_core::{
    utils::{CancellationToken, Runnable},
    Networks,
};
use rsnano_network::Network;

use crate::{config::NetworkParams, representatives::OnlineReps, transport::MessageFlooder};

use super::{
    confirm_req_sender::ConfirmReqSender,
    election::{Election, ElectionState},
    winner_block_broadcaster::WinnerBlockBroadcaster,
    ActiveElectionsContainer, BlockVoter, ConfirmationSolicitor,
};
use rsnano_nullable_clock::SteadyClock;

/// Every 300ms tries to transitions election state and send votes + blocks
pub struct AecTicker {
    pub(crate) active_elections: Arc<RwLock<ActiveElectionsContainer>>,
    pub(crate) message_flooder: MessageFlooder,
    pub(crate) network_params: NetworkParams,
    pub(crate) online_reps: Arc<Mutex<OnlineReps>>,
    pub(crate) network: Arc<RwLock<Network>>,
    pub(crate) block_voter: Arc<BlockVoter>,
    pub(crate) winner_block_broadcaster: WinnerBlockBroadcaster,
    pub(crate) confirm_req_sender: ConfirmReqSender,
    pub(crate) clock: Arc<SteadyClock>,
    pub(crate) plugins: Vec<Box<dyn AecTickerPlugin>>,
}

impl AecTicker {
    pub fn new_null() -> Self {
        Self {
            active_elections: Arc::new(RwLock::new(ActiveElectionsContainer::default())),
            message_flooder: MessageFlooder::new_null(),
            network_params: NetworkParams::new(Networks::NanoLiveNetwork),
            online_reps: Arc::new(Mutex::new(OnlineReps::new_test_instance())),
            network: Arc::new(RwLock::new(Network::new_test_instance())),
            block_voter: Arc::new(BlockVoter::new_null()),
            winner_block_broadcaster: WinnerBlockBroadcaster::new_null(),
            confirm_req_sender: ConfirmReqSender::new_null(),
            clock: Arc::new(SteadyClock::new_null()),
            plugins: Vec::new(),
        }
    }

    pub fn add_plugin(&mut self, plugin: impl AecTickerPlugin + 'static) {
        self.plugins.push(Box::new(plugin));
    }
}

impl Runnable for AecTicker {
    fn run(&mut self, _cancel_token: &CancellationToken) {
        let elections = self
            .active_elections
            .write()
            .unwrap()
            .transition_time(self.clock.now());

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
        for election in &elections {
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

        for plugin in &mut self.plugins {
            plugin.process(&elections);
        }
    }
}

pub trait AecTickerPlugin: Send {
    fn process(&mut self, elections: &[Election]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::{election::VoteType, AecInsertRequest, BlockVoteRequest};
    use rsnano_core::SavedBlock;
    use rsnano_nullable_clock::Timestamp;

    #[test]
    fn call_plugins() {
        let mut ticker = AecTicker::new_null();
        let plugin = StubPlugin::default();
        let called = plugin.0.clone();
        ticker.add_plugin(plugin);

        let block = SavedBlock::new_test_instance_with_key(1);
        let now = Timestamp::new_test_instance();

        ticker
            .active_elections
            .write()
            .unwrap()
            .insert(AecInsertRequest::new_manual(block.clone()), now)
            .unwrap();

        ticker.run(&CancellationToken::new_null());

        assert_eq!(called.lock().unwrap().len(), 1);
    }

    #[derive(Default)]
    struct StubPlugin(Arc<Mutex<Vec<Election>>>);

    impl AecTickerPlugin for StubPlugin {
        fn process(&mut self, elections: &[Election]) {
            *self.0.lock().unwrap() = elections.to_vec();
        }
    }

    #[test]
    fn vote_for_passive_block() {
        let mut ticker = AecTicker::new_null();
        let vote_tracker = ticker.block_voter.track();
        let block = SavedBlock::new_test_instance_with_key(1);
        let now = Timestamp::new_test_instance();
        {
            let mut aec = ticker.active_elections.write().unwrap();
            aec.insert(AecInsertRequest::new_manual(block.clone()), now)
                .unwrap();
        }

        ticker.run(&CancellationToken::new_null());

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
        let mut ticker = AecTicker::new_null();
        let vote_tracker = ticker.block_voter.track();
        let block = SavedBlock::new_test_instance();
        let now = Timestamp::new_test_instance();
        {
            let mut aec = ticker.active_elections.write().unwrap();
            aec.insert(AecInsertRequest::new_manual(block.clone()), now)
                .unwrap();
            aec.transition_active(&block.hash());
        }

        ticker.run(&CancellationToken::new_null());

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
