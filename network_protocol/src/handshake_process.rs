use std::{
    net::SocketAddrV6,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use tracing::{debug, warn};

use rsnano_core::{BlockHash, NodeId, PrivateKey};
use rsnano_messages::{NodeIdHandshake, NodeIdHandshakeQuery, NodeIdHandshakeResponse};

use crate::SynCookies;

pub enum HandshakeStatus {
    Abort,
    AbortOwnNodeId,
    Handshake,
    Realtime(NodeId),
}

/// Responsible for performing a correct handshake when connecting to another node
pub struct HandshakeProcess {
    genesis_hash: BlockHash,
    node_id: PrivateKey,
    syn_cookies: Arc<SynCookies>,
    handshake_received: AtomicBool,
}

impl HandshakeProcess {
    pub fn new(genesis_hash: BlockHash, node_id: PrivateKey, syn_cookies: Arc<SynCookies>) -> Self {
        Self {
            genesis_hash,
            node_id,
            syn_cookies,
            handshake_received: AtomicBool::new(false),
        }
    }

    pub fn initiate_handshake(&mut self, peer: SocketAddrV6) -> anyhow::Result<NodeIdHandshake> {
        let query = self.prepare_query(peer);
        if query.is_none() {
            return Err(anyhow!("Could not create cookie for {:?}", peer));
        }

        Ok(NodeIdHandshake {
            query,
            response: None,
            is_v2: true,
        })
    }

    pub fn process_handshake(
        &self,
        message: &NodeIdHandshake,
        peer: SocketAddrV6,
    ) -> Result<(Option<NodeId>, Option<NodeIdHandshake>), HandshakeResponseError> {
        if message.query.is_none() && message.response.is_none() {
            // There must be a query or a response or both!
            return Err(HandshakeResponseError::EmptyResponse);
        }

        if message.query.is_some() && self.handshake_received.load(Ordering::SeqCst) {
            // Second handshake message should be a response only
            return Err(HandshakeResponseError::MultipleQueries);
        }

        self.handshake_received.store(true, Ordering::SeqCst);

        let log_type = match (message.query.is_some(), message.response.is_some()) {
            (true, true) => "query + response",
            (true, false) => "query",
            (false, true) => "response",
            (false, false) => "none",
        };
        debug!("Handshake message received: {} ({})", log_type, peer);

        let our_response = if let Some(query) = message.query.clone() {
            // Send response + our own query
            Some(self.create_response(&query, message.is_v2, peer))
        } else {
            None
        };

        if let Some(their_response) = &message.response {
            match self.verify_response(their_response, peer) {
                Ok(()) => {
                    return Ok((Some(their_response.node_id), our_response));
                }
                Err(HandshakeResponseError::OwnNodeId) => {
                    warn!(
                        "This node tried to connect to itself. Closing channel ({})",
                        peer
                    );
                    return Err(HandshakeResponseError::OwnNodeId);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        // Handshake is in progress
        Ok((None, our_response))
    }

    fn create_response(
        &self,
        query: &NodeIdHandshakeQuery,
        v2: bool,
        peer: SocketAddrV6,
    ) -> NodeIdHandshake {
        let response = self.prepare_response(query, v2);
        let own_query = self.prepare_query(peer);

        NodeIdHandshake {
            is_v2: own_query.is_some() || response.v2.is_some(),
            query: own_query,
            response: Some(response),
        }
    }

    fn verify_response(
        &self,
        response: &NodeIdHandshakeResponse,
        peer_addr: SocketAddrV6,
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

        let Some(cookie) = self.syn_cookies.cookie(&peer_addr) else {
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

    pub(crate) fn prepare_query(&self, peer_addr: SocketAddrV6) -> Option<NodeIdHandshakeQuery> {
        self.syn_cookies
            .assign(&peer_addr)
            .map(|cookie| NodeIdHandshakeQuery { cookie })
    }
}

#[derive(Debug, Clone, Copy, EnumCount, EnumIter)]
pub enum HandshakeResponseError {
    /// The node tried to connect to itself
    OwnNodeId,
    InvalidGenesis,
    MissingCookie,
    InvalidSignature,
    EmptyResponse,
    MultipleQueries,
}
