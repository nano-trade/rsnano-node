use std::{net::SocketAddrV6, sync::Arc};

use anyhow::Error;
use tracing::{debug, trace};

use rsnano_network::{ChannelDirection, NetworkError, NetworkObserver};
use rsnano_stats::{DetailType, Direction, StatType, Stats};

#[derive(Clone)]
pub struct NetworkStats(Arc<Stats>);

impl NetworkStats {
    pub fn new(stats: Arc<Stats>) -> Self {
        Self(stats)
    }
}

impl NetworkObserver for NetworkStats {
    fn connect_error(&self, peer: SocketAddrV6, e: Error) {
        self.0.inc_dir(
            StatType::TcpListener,
            DetailType::ConnectError,
            Direction::Out,
        );
        debug!("Error connecting to: {} ({:?})", peer, e);
    }

    fn attempt_timeout(&self, peer: SocketAddrV6) {
        self.0
            .inc(StatType::TcpListener, DetailType::AttemptTimeout);
        debug!("Connection attempt timed out: {}", peer);
    }

    fn attempt_cancelled(&self, peer: SocketAddrV6) {
        debug!("Connection attempt cancelled: {}", peer,);
    }

    fn merge_peer(&self) {
        self.0.inc(StatType::Network, DetailType::MergePeer);
    }

    fn accept_failure(&self) {
        self.0.inc_dir(
            StatType::TcpListener,
            DetailType::AcceptFailure,
            Direction::In,
        );
    }
}
