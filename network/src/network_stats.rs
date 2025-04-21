use crate::{ChannelDirection, NetworkError};
use rsnano_stats::{Direction, StatsCollection, StatsSource};
use std::{
    net::SocketAddrV6,
    sync::atomic::{AtomicUsize, Ordering::Relaxed},
};
use tracing::{debug, trace};

pub(crate) struct NetworkStats {
    connect_success: AtomicUsize,
    connect_rejected: AtomicUsize,
    accept_success: AtomicUsize,
    accept_rejected: AtomicUsize,
    pub connection_attempts: AtomicUsize,
    connect_errors: ErrorStats,
    accept_errors: ErrorStats,
    pub connect_error: AtomicUsize,
    pub attempt_timeout: AtomicUsize,
    pub merge_peer: AtomicUsize,
    pub accept_failure: AtomicUsize,
}

impl Default for NetworkStats {
    fn default() -> Self {
        Self {
            connect_success: Default::default(),
            connect_rejected: Default::default(),
            accept_success: Default::default(),
            accept_rejected: Default::default(),
            connection_attempts: Default::default(),
            connect_errors: ErrorStats::new(Direction::Out),
            accept_errors: ErrorStats::new(Direction::In),
            connect_error: Default::default(),
            attempt_timeout: Default::default(),
            merge_peer: Default::default(),
            accept_failure: Default::default(),
        }
    }
}

impl NetworkStats {
    pub fn accepted(&self, peer: &SocketAddrV6, direction: ChannelDirection) {
        if direction == ChannelDirection::Outbound {
            self.connect_success.fetch_add(1, Relaxed);
        } else {
            self.accept_success.fetch_add(1, Relaxed);
        }
        debug!(%peer, ?direction, "New channel added");
    }

    pub fn error(&self, error: NetworkError, peer: &SocketAddrV6, direction: ChannelDirection) {
        match direction {
            ChannelDirection::Inbound => {
                self.accept_rejected.fetch_add(1, Relaxed);
            }
            ChannelDirection::Outbound => {
                self.connect_rejected.fetch_add(1, Relaxed);
            }
        }

        match direction {
            ChannelDirection::Inbound => self.accept_errors.error(error, peer),
            ChannelDirection::Outbound => self.connect_errors.error(error, peer),
        }
    }
}

impl StatsSource for NetworkStats {
    fn collect_stats(&self, result: &mut StatsCollection) {
        result.insert_dir(
            "tcp_listener",
            "connect_success",
            Direction::Out,
            self.connect_success.load(Relaxed),
        );
        result.insert_dir(
            "tcp_listener",
            "accept_success",
            Direction::In,
            self.accept_success.load(Relaxed),
        );
        result.insert_dir(
            "tcp_listener",
            "connect_initiate",
            Direction::Out,
            self.connection_attempts.load(Relaxed),
        );
        result.insert_dir(
            "tcp_listener",
            "accept_rejected",
            Direction::In,
            self.accept_rejected.load(Relaxed),
        );
        result.insert_dir(
            "tcp_listener",
            "connect_rejected",
            Direction::Out,
            self.connect_rejected.load(Relaxed),
        );
        result.insert_dir(
            "tcp_listener",
            "connect_error",
            Direction::Out,
            self.connect_error.load(Relaxed),
        );
        result.insert(
            "tcp_listener",
            "attempt_timeout",
            self.attempt_timeout.load(Relaxed),
        );
        result.insert("network", "merge_peer", self.merge_peer.load(Relaxed));
        result.insert(
            "tcp_listener",
            "accept_failure",
            self.accept_failure.load(Relaxed),
        );
        self.connect_errors.collect_stats(result);
        self.accept_errors.collect_stats(result);
    }
}

struct ErrorStats {
    dir: Direction,
    max_attempts: AtomicUsize,
    excluded: AtomicUsize,
    max_per_subnet: AtomicUsize,
    max_per_ip: AtomicUsize,
    invalid_ip: AtomicUsize,
    duplicate: AtomicUsize,
}

impl ErrorStats {
    fn new(dir: Direction) -> Self {
        Self {
            dir,
            max_attempts: Default::default(),
            excluded: Default::default(),
            max_per_subnet: Default::default(),
            max_per_ip: Default::default(),
            invalid_ip: Default::default(),
            duplicate: Default::default(),
        }
    }

    fn error(&self, error: NetworkError, peer: &SocketAddrV6) {
        match error {
            NetworkError::MaxConnections => {
                self.max_attempts.fetch_add(1, Relaxed);
                debug!(
                    %peer,
                    dir = ?self.dir,
                    "Max connections reached, unable to make new connection",
                );
            }
            NetworkError::PeerExcluded => {
                self.excluded.fetch_add(1, Relaxed);
                debug!(
                    %peer,
                    dir = ?self.dir,
                    "Peer excluded, unable to make new connection",
                );
            }
            NetworkError::MaxConnectionsPerSubnetwork => {
                self.max_per_subnet.fetch_add(1, Relaxed);
                debug!(
                    %peer,
                    dir = ?self.dir,
                    "Max connections per subnetwork reached, unable to open new connection",
                );
            }
            NetworkError::MaxConnectionsPerIp => {
                self.max_per_ip.fetch_add(1, Relaxed);
                debug!(
                    %peer,
                    dir = ?self.dir,
                    "Max connections per IP reached, unable to open new connection");
            }
            NetworkError::InvalidIp => {
                self.invalid_ip.fetch_add(1, Relaxed);
                debug!(
                    %peer,
                    dir = ?self.dir,
                    "Invalid IP, unable to open new connection");
            }
            NetworkError::DuplicateConnection => {
                self.duplicate.fetch_add(1, Relaxed);
                trace!(
                    %peer,
                    dir = ?self.dir,
                    "Already connected to that peer, unable to open new connection");
            }
            NetworkError::Cancelled => {}
        }
    }
}

impl StatsSource for ErrorStats {
    fn collect_stats(&self, result: &mut StatsCollection) {
        result.insert_dir(
            "tcp_listener_rejected",
            "max_attempts",
            self.dir,
            self.max_attempts.load(Relaxed),
        );
        result.insert_dir(
            "tcp_listener_rejected",
            "excluded",
            self.dir,
            self.excluded.load(Relaxed),
        );
        result.insert_dir(
            "tcp_listener_rejected",
            "max_per_subnetwork",
            self.dir,
            self.max_per_subnet.load(Relaxed),
        );
        result.insert_dir(
            "tcp_listener_rejected",
            "max_per_ip",
            self.dir,
            self.max_per_ip.load(Relaxed),
        );
        result.insert_dir(
            "tcp_listener_rejected",
            "not_a_peer",
            self.dir,
            self.invalid_ip.load(Relaxed),
        );
        result.insert_dir(
            "tcp_listener_rejected",
            "duplicate",
            self.dir,
            self.duplicate.load(Relaxed),
        );
    }
}
