use crate::HandshakeResponseError;
use rsnano_stats::{Direction, StatsCollection, StatsSource};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use strum::{EnumCount, IntoEnumIterator};

#[derive(Default)]
pub struct HandshakeStats {
    pub handshakes_sent: AtomicUsize,
    pub handshakes_received: AtomicUsize,
    pub initiate: AtomicUsize,
    pub response_sent: AtomicUsize,
    pub network_error: AtomicUsize,
    pub handshake_error: AtomicUsize,
    pub response_ok: AtomicUsize,
    pub errors: [AtomicUsize; HandshakeResponseError::COUNT],
}

impl StatsSource for HandshakeStats {
    fn collect_stats(&self, result: &mut StatsCollection) {
        result.insert_dir(
            "tcp_server",
            "handshake",
            Direction::Out,
            self.handshakes_sent.load(Relaxed),
        );
        result.insert_dir(
            "tcp_server",
            "handshake_initiate",
            Direction::Out,
            self.initiate.load(Relaxed),
        );
        result.insert_dir(
            "tcp_server",
            "handshake_response",
            Direction::Out,
            self.handshakes_sent.load(Relaxed),
        );
        result.insert(
            "tcp_server",
            "handshake_network_error",
            self.network_error.load(Relaxed),
        );
        result.insert(
            "tcp_server",
            "handshake_error",
            self.handshake_error.load(Relaxed),
        );
        result.insert_dir(
            "tcp_server",
            "handshake",
            Direction::In,
            self.handshakes_received.load(Relaxed),
        );

        result.insert("handshake", "ok", self.response_ok.load(Relaxed));

        for e in HandshakeResponseError::iter() {
            let detail = match e {
                HandshakeResponseError::OwnNodeId => "invalid_node_id",
                HandshakeResponseError::InvalidGenesis => "invalid_genesis",
                HandshakeResponseError::MissingCookie => "missing_cookie",
                HandshakeResponseError::InvalidSignature => "invalid_signature",
                HandshakeResponseError::EmptyResponse => "empty_response",
                HandshakeResponseError::MultipleQueries => "multiple_queries",
            };
            result.insert("handshake", detail, self.handshake_error.load(Relaxed));
        }
    }
}
