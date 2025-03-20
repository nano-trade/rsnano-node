use std::sync::{Arc, Mutex, RwLock};

use rsnano_network::Network;
use rsnano_nullable_clock::SteadyClock;
use rsnano_stats::{DetailType, StatType, Stats};

use crate::{
    config::NetworkParams,
    representatives::OnlineReps,
    transport::MessageFlooder,
    utils::{CancellationToken, Runnable},
};

use super::{
    confirm_req_sender::ConfirmReqSender, winner_block_broadcaster::WinnerBlockBroadcaster,
    ActiveElections, BlockVoter, ConfirmationSolicitor, ElectionState,
};

/// Periodically tries to transitions election state and send votes + blocks
pub struct ActiveElectionsDriver {
    pub(crate) active_elections: Arc<ActiveElections>,
    pub(crate) stats: Arc<Stats>,
    pub(crate) message_flooder: MessageFlooder,
    pub(crate) network_params: NetworkParams,
    pub(crate) online_reps: Arc<Mutex<OnlineReps>>,
    pub(crate) network: Arc<RwLock<Network>>,
    pub(crate) clock: Arc<SteadyClock>,
    pub(crate) block_voter: Arc<BlockVoter>,
    pub(crate) winner_block_broadcaster: WinnerBlockBroadcaster,
    pub(crate) confirm_req_sender: ConfirmReqSender,
}

impl Runnable for ActiveElectionsDriver {
    fn run(&mut self, _cancel_token: &CancellationToken) {
        self.stats.inc(StatType::Active, DetailType::Loop);
        let peered_prs = self.online_reps.lock().unwrap().peered_principal_reps();
        let elections: Vec<_> = self.active_elections.read().iter().cloned().collect();

        // TODO don't clone flooder!'
        let flooder = self.message_flooder.clone();
        let mut solicitor =
            ConfirmationSolicitor::new(&self.network_params, &self.network, flooder);
        solicitor.prepare(&peered_prs);

        /*
         * Loop through active elections requesting confirmation
         *
         * Only up to a certain amount of elections are queued for confirmation request and block rebroadcasting. The remaining elections can still be confirmed if votes arrive
         * Elections extending the soft config.size limit are flushed after a certain time-to-live cutoff
         * Flushed elections are later re-activated via frontier confirmation
         */
        for election_mutex in elections {
            let root;
            let new_state;
            {
                let mut election = election_mutex.lock().unwrap();
                let old_state = election.state();

                election.transition_time(self.clock.now());

                root = election.qualified_root().clone();
                new_state = election.state();

                match new_state {
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
                            .send_confirm_req(&mut solicitor, &mut election);
                    }
                    _ => {}
                }

                if old_state == ElectionState::Confirmed {
                    self.winner_block_broadcaster
                        .try_broadcast_winner(&mut solicitor, &mut election);
                    // Ensure election winner is broadcasted
                }
            };

            if new_state.has_ended() {
                self.active_elections.erase(&root);
            }
        }

        solicitor.flush();
    }
}
