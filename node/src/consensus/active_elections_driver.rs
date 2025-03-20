use std::sync::{Arc, Mutex, RwLock};

use rsnano_network::Network;
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
    pub(crate) block_voter: Arc<BlockVoter>,
    pub(crate) winner_block_broadcaster: WinnerBlockBroadcaster,
    pub(crate) confirm_req_sender: ConfirmReqSender,
}

impl Runnable for ActiveElectionsDriver {
    fn run(&mut self, _cancel_token: &CancellationToken) {
        self.stats.inc(StatType::Active, DetailType::Loop);

        self.active_elections.transition_time();

        let peered_prs = self.online_reps.lock().unwrap().peered_principal_reps();

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
        for election_mutex in self.active_elections.read().iter() {
            let election = election_mutex.lock().unwrap();

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

        self.active_elections.erase_ended_elections();

        solicitor.flush();
    }
}
