use std::sync::{Arc, Mutex, RwLock};

use rsnano_network::Network;
use rsnano_stats::{DetailType, StatType, Stats};

use crate::{
    config::NetworkParams,
    representatives::OnlineReps,
    transport::MessageFlooder,
    utils::{CancellationToken, Runnable},
};

use super::{ActiveElections, ConfirmationSolicitor, Election, ElectionState};

/// Periodically tries to transitions election state and send votes + blocks
pub(crate) struct ActiveElectionsDriver {
    pub active_elections: Arc<ActiveElections>,
    pub stats: Arc<Stats>,
    pub message_flooder: MessageFlooder,
    pub network_params: NetworkParams,
    pub online_reps: Arc<Mutex<OnlineReps>>,
    pub network: Arc<RwLock<Network>>,
}

impl ActiveElectionsDriver {
    fn try_broadcast_winner_block(
        &self,
        solicitor: &mut ConfirmationSolicitor,
        election: &mut Election,
    ) {
        if election.should_broadcast_winner_block() {
            if solicitor.broadcast_winner_block(election).is_ok() {
                let is_initial = election.was_winner_block_broadcasted();

                self.stats.inc(
                    StatType::Election,
                    if is_initial {
                        DetailType::BroadcastBlockInitial
                    } else {
                        DetailType::BroadcastBlockRepeat
                    },
                );
            }
        }
    }

    fn send_confirm_req(&self, solicitor: &mut ConfirmationSolicitor, election: &mut Election) {
        if election.should_send_confirm_req() {
            if solicitor.add(election) {
                election.confirm_request_sent();
                self.stats
                    .inc(StatType::Election, DetailType::ConfirmationRequest);
            }
        }
    }
}

impl Runnable for ActiveElectionsDriver {
    fn run(&mut self, _cancel_token: &CancellationToken) {
        self.stats.inc(StatType::Active, DetailType::Loop);
        let elections = self.active_elections.get_all();

        // TODO don't clone flooder!'
        let flooder = self.message_flooder.clone();
        let mut solicitor =
            ConfirmationSolicitor::new(&self.network_params, &self.network, flooder);
        let peered_prs = self.online_reps.lock().unwrap().peered_principal_reps();
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

                election.transition_time();

                root = election.qualified_root().clone();
                new_state = election.state();

                match new_state {
                    ElectionState::Active => {
                        self.active_elections.try_generate_vote(&mut election);
                        self.try_broadcast_winner_block(&mut solicitor, &mut election);
                        self.send_confirm_req(&mut solicitor, &mut election);
                    }
                    _ => {}
                }

                if old_state == ElectionState::Confirmed {
                    self.try_broadcast_winner_block(&mut solicitor, &mut election);
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
