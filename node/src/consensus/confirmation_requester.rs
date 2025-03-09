use std::sync::{Arc, Mutex, RwLock};

use rsnano_network::Network;
use rsnano_stats::{DetailType, StatType, Stats};

use crate::{
    config::NetworkParams,
    representatives::OnlineReps,
    transport::MessageFlooder,
    utils::{CancellationToken, Runnable},
};

use super::{ActiveElections, ConfirmationSolicitor};

/// Requests confirmations for active elections from peered representatives
pub(crate) struct ConfirmationRequester {
    pub active_elections: Arc<ActiveElections>,
    pub stats: Arc<Stats>,
    pub message_flooder: MessageFlooder,
    pub network_params: NetworkParams,
    pub online_reps: Arc<Mutex<OnlineReps>>,
    pub network: Arc<RwLock<Network>>,
}

impl Runnable for ConfirmationRequester {
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
         * Loop through active elections in descending order of proof-of-work difficulty, requesting confirmation
         *
         * Only up to a certain amount of elections are queued for confirmation request and block rebroadcasting. The remaining elections can still be confirmed if votes arrive
         * Elections extending the soft config.size limit are flushed after a certain time-to-live cutoff
         * Flushed elections are later re-activated via frontier confirmation
         */
        for election in elections {
            let success;
            let root;
            {
                let mut election_guard = election.lock().unwrap();
                success = self
                    .active_elections
                    .transition_time(&mut solicitor, &mut election_guard);
                root = election_guard.qualified_root().clone();
            };

            if success {
                self.active_elections.erase(&root);
            }
        }

        solicitor.flush();
    }
}
