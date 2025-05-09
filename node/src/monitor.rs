use std::{
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};

use tracing::info;

use rsnano_core::utils::{CancellationToken, Runnable};
use rsnano_ledger::Ledger;
use rsnano_network::Network;

use crate::{consensus::ActiveElectionsContainer, representatives::OnlineReps};

pub struct Monitor {
    ledger: Arc<Ledger>,
    network: Arc<RwLock<Network>>,
    online_reps: Arc<Mutex<OnlineReps>>,
    active_elections: Arc<RwLock<ActiveElectionsContainer>>,
    last_time: Option<Instant>,
    last_blocks_confirmed: u64,
    last_blocks_total: u64,
}

impl Monitor {
    pub fn new(
        ledger: Arc<Ledger>,
        network: Arc<RwLock<Network>>,
        online_peers: Arc<Mutex<OnlineReps>>,
        active_elections: Arc<RwLock<ActiveElectionsContainer>>,
    ) -> Self {
        Self {
            ledger,
            network,
            online_reps: online_peers,
            active_elections,
            last_time: None,
            last_blocks_total: 0,
            last_blocks_confirmed: 0,
        }
    }

    fn log(&self, last: Instant, blocks_confirmed: u64, blocks_total: u64) {
        // TODO: Maybe emphasize somehow that confirmed doesn't need to be equal to total; backlog is OK
        info!(
            "Blocks confirmed: {} | total: {} (backlog: {})",
            blocks_confirmed,
            blocks_total,
            blocks_total - blocks_confirmed
        );

        // Calculate the rates
        let elapsed_secs = last.elapsed().as_secs() as f64;
        let blocks_confirmed_rate =
            (blocks_confirmed - self.last_blocks_confirmed) as f64 / elapsed_secs;

        // Block rollback can cause the block count to go down!
        let blocks_checked_rate =
            if let Some(diff) = blocks_total.checked_sub(self.last_blocks_total) {
                diff as f64 / elapsed_secs
            } else {
                0.0
            };

        info!(
            "Blocks rate (avg over {}s): confirmed: {:.2}/s | total {:.2}/s)",
            elapsed_secs, blocks_confirmed_rate, blocks_checked_rate
        );

        let channels = self.network.read().unwrap().channels_info();
        info!(
            "Peers: {} (realtime: {} | inbound: {} | outbound: {})",
            channels.total, channels.realtime, channels.inbound, channels.outbound
        );

        {
            let (delta, online, peered) = {
                let online_reps = self.online_reps.lock().unwrap();
                (
                    online_reps.quorum_delta(),
                    online_reps.online_weight(),
                    online_reps.peered_weight(),
                )
            };
            info!(
                "Quorum: {} (stake peered: {} | online stake: {})",
                delta.format_balance(0),
                online.format_balance(0),
                peered.format_balance(0)
            );
        }

        let elections = self.active_elections.read().unwrap().info();
        info!(
            "Elections active: {} (priority: {} | hinted: {} | optimistic: {})",
            elections.total, elections.priority, elections.hinted, elections.optimistic
        );
    }
}

impl Runnable for Monitor {
    fn run(&mut self, _cancel_token: &CancellationToken) {
        let blocks_confirmed = self.ledger.confirmed_count();
        let blocks_total = self.ledger.block_count();

        if let Some(last) = self.last_time {
            self.log(last, blocks_confirmed, blocks_total);
        } else {
            // Wait for node to warm up before logging
        }
        self.last_time = Some(Instant::now());
        self.last_blocks_confirmed = blocks_confirmed;
        self.last_blocks_total = blocks_total;
    }
}
