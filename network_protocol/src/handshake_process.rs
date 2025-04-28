use std::{
    net::SocketAddrV6,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use tracing::{debug, warn};

use rsnano_core::{BlockHash, NodeId, PrivateKey, ProtocolInfo};
use rsnano_messages::{
    Message, MessageSerializer, NodeIdHandshake, NodeIdHandshakeQuery, NodeIdHandshakeResponse,
};
use rsnano_network::{Channel, TrafficType};

use crate::{handshake_stats::HandshakeStats, SynCookies};

pub enum HandshakeStatus {
    Abort,
    AbortOwnNodeId,
    Handshake,
    Realtime(NodeId),
    Bootstrap,
}

/// Responsible for performing a correct handshake when connecting to another node
pub struct HandshakeProcess {
    genesis_hash: BlockHash,
    node_id: PrivateKey,
    syn_cookies: Arc<SynCookies>,
    stats: Arc<HandshakeStats>,
    handshake_received: AtomicBool,
    protocol: ProtocolInfo,
}

impl HandshakeProcess {
    pub fn new(
        genesis_hash: BlockHash,
        node_id: PrivateKey,
        syn_cookies: Arc<SynCookies>,
        stats: Arc<HandshakeStats>,
        protocol: ProtocolInfo,
    ) -> Self {
        Self {
            genesis_hash,
            node_id,
            syn_cookies,
            stats,
            handshake_received: AtomicBool::new(false),
            protocol,
        }
    }

    #[allow(dead_code)]
    pub fn new_null() -> Self {
        Self {
            genesis_hash: BlockHash::from(1),
            node_id: PrivateKey::from(2),
            syn_cookies: Arc::new(SynCookies::new(1)),
            stats: Arc::new(HandshakeStats::default()),
            handshake_received: AtomicBool::new(false),
            protocol: ProtocolInfo::default(),
        }
    }

    pub fn initiate_handshake(&mut self, channel: &Channel) -> Result<(), ()> {
        let peer = channel.peer_addr();
        let query = self.prepare_query(&peer);
        if query.is_none() {
            warn!("Could not create cookie for {:?}. Closing channel.", peer);
            return Err(());
        }
        let message = Message::NodeIdHandshake(NodeIdHandshake {
            query,
            response: None,
            is_v2: true,
        });

        debug!("Initiating handshake query ({})", peer);

        let mut serializer = MessageSerializer::new(self.protocol);
        let data = serializer.serialize(&message);

        let enqueued = channel.send(data, TrafficType::Generic);

        if enqueued {
            self.stats.handshakes_sent.fetch_add(1, Ordering::Relaxed);
            self.stats.initiate.fetch_add(1, Ordering::Relaxed);
            Ok(())
        } else {
            self.stats.network_error.fetch_add(1, Ordering::Relaxed);
            debug!(peer = %peer, "Could not enqueue handshake query");
            // Stop invalid handshake
            Err(())
        }
    }

    pub fn process_handshake(
        &self,
        message: &NodeIdHandshake,
        channel: &Channel,
    ) -> HandshakeStatus {
        if message.query.is_none() && message.response.is_none() {
            self.stats.handshake_error.fetch_add(1, Ordering::Relaxed);
            debug!(
                peer = %channel.peer_addr(),
                ?message,
                "Invalid handshake message received",
            );
            return HandshakeStatus::Abort;
        }
        if message.query.is_some() && self.handshake_received.load(Ordering::SeqCst) {
            // Second handshake message should be a response only
            self.stats.handshake_error.fetch_add(1, Ordering::Relaxed);
            warn!(
                "Detected multiple handshake queries ({})",
                channel.peer_addr()
            );
            return HandshakeStatus::Abort;
        }

        self.handshake_received.store(true, Ordering::SeqCst);

        self.stats
            .handshakes_received
            .fetch_add(1, Ordering::Relaxed);

        let log_type = match (message.query.is_some(), message.response.is_some()) {
            (true, true) => "query + response",
            (true, false) => "query",
            (false, true) => "response",
            (false, false) => "none",
        };
        debug!(
            "Handshake message received: {} ({})",
            log_type,
            channel.peer_addr()
        );

        if let Some(query) = message.query.clone() {
            // Send response + our own query
            if self.send_response(&query, message.is_v2, &channel).is_err() {
                // Stop invalid handshake
                return HandshakeStatus::Abort;
            }
            // Fall through and continue handshake
        }
        if let Some(response) = &message.response {
            match self.verify_response(response, &channel.peer_addr()) {
                Ok(()) => {
                    self.stats.response_ok.fetch_add(1, Ordering::Relaxed);
                    return HandshakeStatus::Realtime(response.node_id); // Switch to realtime
                }
                Err(HandshakeResponseError::OwnNodeId) => {
                    warn!(
                        "This node tried to connect to itself. Closing channel ({})",
                        channel.peer_addr()
                    );
                    return HandshakeStatus::AbortOwnNodeId;
                }
                Err(e) => {
                    self.stats.errors[e as usize].fetch_add(1, Ordering::Relaxed);
                    self.stats.response_invalid.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        peer = %channel.peer_addr(),
                        error = ?e,
                        ?response,
                        "Invalid handshake response received",
                    );
                    return HandshakeStatus::Abort;
                }
            }
        }
        HandshakeStatus::Handshake // Handshake is in progress
    }

    fn send_response(
        &self,
        query: &NodeIdHandshakeQuery,
        v2: bool,
        channel: &Channel,
    ) -> anyhow::Result<()> {
        let response = self.prepare_response(query, v2);
        let own_query = self.prepare_query(&channel.peer_addr());

        let handshake_response = Message::NodeIdHandshake(NodeIdHandshake {
            is_v2: own_query.is_some() || response.v2.is_some(),
            query: own_query,
            response: Some(response),
        });

        debug!("Responding to handshake ({})", channel.peer_addr());

        let mut serializer = MessageSerializer::new(self.protocol);
        let buffer = serializer.serialize(&handshake_response);

        let enqueued = channel.send(buffer, TrafficType::Generic);

        if enqueued {
            self.stats.handshakes_sent.fetch_add(1, Ordering::Relaxed);
            self.stats.response_sent.fetch_add(1, Ordering::Relaxed);
            Ok(())
        } else {
            self.stats.network_error.fetch_add(1, Ordering::Relaxed);
            warn!(peer = %channel.peer_addr(), "Error sending handshake response");
            Err(anyhow!("Could now enqueue handshake response"))
        }
    }

    fn verify_response(
        &self,
        response: &NodeIdHandshakeResponse,
        peer_addr: &SocketAddrV6,
    ) -> Result<(), HandshakeResponseError> {
        // Prevent connection with ourselves
        if response.node_id == self.node_id.public_key().into() {
            return Err(HandshakeResponseError::OwnNodeId);
        }

        // Prevent mismatched genesis
        if let Some(v2) = &response.v2 {
            if v2.genesis != self.genesis_hash {
                return Err(HandshakeResponseError::InvalidGenesis);
            }
        }

        let Some(cookie) = self.syn_cookies.cookie(peer_addr) else {
            return Err(HandshakeResponseError::MissingCookie);
        };

        if response.validate(&cookie).is_err() {
            return Err(HandshakeResponseError::InvalidSignature);
        }

        Ok(())
    }

    pub(crate) fn prepare_response(
        &self,
        query: &NodeIdHandshakeQuery,
        v2: bool,
    ) -> NodeIdHandshakeResponse {
        if v2 {
            NodeIdHandshakeResponse::new_v2(&query.cookie, &self.node_id, self.genesis_hash)
        } else {
            NodeIdHandshakeResponse::new_v1(&query.cookie, &self.node_id)
        }
    }

    pub(crate) fn prepare_query(&self, peer_addr: &SocketAddrV6) -> Option<NodeIdHandshakeQuery> {
        self.syn_cookies
            .assign(peer_addr)
            .map(|cookie| NodeIdHandshakeQuery { cookie })
    }
}

#[derive(Debug, Clone, Copy, EnumCount, EnumIter)]
pub(crate) enum HandshakeResponseError {
    /// The node tried to connect to itself
    OwnNodeId,
    InvalidGenesis,
    MissingCookie,
    InvalidSignature,
}
